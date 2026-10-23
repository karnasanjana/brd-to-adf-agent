"""Compiler: LogicalPlan -> PipelineSpec. Deterministic, no LLM, no cloud.

This is where the search-space collapse pays off. The model produced only
business intent (entities, ops, joins, outputs); here CODE assigns every ADF
name, wires the medallion layers, reuses datasets across boundaries, chains the
master pipeline, and renders every expression from structured ops. Nothing is
invented — same LogicalPlan in always yields the same PipelineSpec out.

Because the compiler owns naming/wiring/references, the whole class of
spec.py `_repair_*` validators (layer sinks, linked-service refs, container
names, copy-kind harmonization) has no input to repair: their failure
conditions are unreachable from this path.

Naming formulas are fixed (see IMPLEMENTATION_SPEC.md section 3.1). Stream names
are pre-cleaned to ADF's alphanumeric rule so spec.py `_sanitize_stream_names`
is a no-op on compiler output.
"""

from __future__ import annotations

import os
import re
from typing import Dict, List, Optional
from urllib.parse import urlparse

from models.logical import (
    CleaningOp,
    EntityTransform,
    FilterRule,
    LogicalColumn,
    LogicalPlan,
    LogicalSource,
    OutputPlan,
    SourceFormat,
)
from models.spec import (
    Activity,
    AuthKind,
    Column,
    DataFlow,
    DataFlowEndpoint,
    Dataset,
    DatasetKind,
    DatasetLocation,
    DerivedColumn,
    LinkedService,
    MedallionLayer,
    PipelineDef,
    PipelineSpec,
    Predicate,
    SortKey,
    StorageAccount,
    Transform,
    TransformKind,
    Trigger,
    TriggerKind,
)

STORAGE_LS = "ls_storage"


# --------------------------------------------------------------------------- #
# Name helpers
# --------------------------------------------------------------------------- #


def _camel(entity: str) -> str:
    """orders -> Orders, order_items -> OrderItems (alnum, starts with a letter)."""
    parts = re.split(r"[^A-Za-z0-9]+", entity)
    camel = "".join(p[:1].upper() + p[1:] for p in parts if p)
    if camel and not camel[0].isalpha():
        camel = "s" + camel
    return camel or "Stream"


def _host_token(url: str) -> str:
    host = urlparse(url).netloc or url
    return re.sub(r"[^a-z0-9]", "_", host.lower()).strip("_") or "host"


def _server_token(server: str) -> str:
    label = (server or "").split(".")[0]
    return re.sub(r"[^a-z0-9]", "_", label.lower()).strip("_") or "sql"


# LogicalColumn.type -> ADF data-flow cast function.
# Delta rejects these characters in column names with DF-SINK-005
# ("Column name(s) [Country Name] are invalid for delta format").
_DELTA_UNSAFE = re.compile(r"[ ,;{}()=\n\t]")


def _delta_safe(name: str) -> str:
    s = _DELTA_UNSAFE.sub("_", name or "")
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "col"


def _sanitize_delta_columns(plan: LogicalPlan):
    """Silver/Gold sink as Delta, and Delta forbids spaces & co in column
    names — grounded CSV headers like 'Country Name' must be renamed before
    the sink. Returns (plan-copy with every reference renamed, rename map,
    original per-entity columns). The silver flow keeps the ORIGINAL names in
    its source projection (they must match the file header) and renames via a
    select right after the source."""
    rename: Dict[str, str] = {}
    original: Dict[str, list] = {}
    for s in plan.sources:
        original[s.entity] = [c.model_copy() for c in s.columns]
        taken = {c.name for c in s.columns}
        for c in s.columns:
            if _DELTA_UNSAFE.search(c.name):
                safe = _delta_safe(c.name)
                while safe in taken or safe in rename.values():
                    safe += "_c"
                rename[c.name] = safe
    if not rename:
        return plan, rename, original

    plan = plan.model_copy(deep=True)

    def fix(n: str) -> str:
        return rename.get(n, n)

    for s in plan.sources:
        for c in s.columns:
            c.name = fix(c.name)
    for t in plan.transforms:
        for c in t.casts:
            c.name = fix(c.name)
        for op in t.cleaning:
            if op.column:
                op.column = fix(op.column)
            if op.new_column:
                op.new_column = fix(op.new_column)
            op.args = [rename.get(a, a) for a in op.args]
        for f in t.filters:
            f.column = fix(f.column)
        t.dedup_keys = [fix(k) for k in t.dedup_keys]
    for out in plan.outputs:
        for j in out.joins:
            j.left_keys = [fix(k) for k in j.left_keys]
            j.right_keys = [fix(k) for k in j.right_keys]
        out.group_by = [fix(g) for g in out.group_by]
        for a in out.aggregations:
            a.column = fix(a.column)
        for srt in out.sort:
            srt.column = fix(srt.column)
        for f in out.filters:
            f.column = fix(f.column)
        for d in list(out.derived) + list(out.post_agg_derived):
            if d.column:
                d.column = fix(d.column)
            d.args = [rename.get(a, a) for a in d.args]
        out.columns = [fix(c) for c in out.columns]
    return plan, rename, original


def _sanitize_select(original_cols, current_cols) -> Transform:
    """select() right after the silver source: maps every ORIGINAL (header)
    name to its Delta-safe name; unchanged columns map to themselves."""
    from models.spec import DerivedColumn, dsl_name

    return Transform(
        kind=TransformKind.select,
        name="sanitizeNames",
        columns=[
            DerivedColumn(name=cur.name, expression=dsl_name(orig.name))
            for orig, cur in zip(original_cols, current_cols)
        ],
    )


def _cast_expression(col: LogicalColumn) -> str:
    from models.spec import dsl_name

    name = dsl_name(col.name)
    t = (col.type or "string").strip().lower()
    m = re.fullmatch(r"decimal\((\d+)\s*,\s*(\d+)\)", t)
    if m:
        return f"toDecimal({name}, {m.group(1)}, {m.group(2)})"
    fn = {
        "integer": "toInteger",
        "int": "toInteger",
        "long": "toLong",
        "bigint": "toLong",
        "double": "toDouble",
        "float": "toDouble",
        "decimal": "toDecimal",
        "boolean": "toBoolean",
        "bool": "toBoolean",
        "date": "toDate",
        "timestamp": "toTimestamp",
        "datetime": "toTimestamp",
        "string": "toString",
    }.get(t, "toString")
    return f"{fn}({name})"


_DS_KIND_BY_FORMAT = {
    SourceFormat.csv: DatasetKind.delimited_text,
    SourceFormat.text: DatasetKind.delimited_text,
    SourceFormat.parquet: DatasetKind.parquet,
    SourceFormat.json: DatasetKind.json,
    SourceFormat.binary: DatasetKind.binary,
    SourceFormat.http_csv: DatasetKind.delimited_text,
    SourceFormat.sql_table: DatasetKind.azure_sql_table,
}


# Cosmetic string cleaners: dropping one from a numeric column loses nothing
# (a number has no whitespace, case, or special characters). Intent-bearing
# string ops (replace/concat/substring/regex) are NOT here — a type clash on
# those means a real requirement mismatch that typecheck must surface.
_COSMETIC_STRING_OPS = {
    "trim", "upper", "lower", "proper", "propercase", "initcap",
    "titlecase", "title",
}


def _drop_string_ops_on_non_string(plan: LogicalPlan) -> None:
    """Remove in-place cosmetic string ops targeting a column whose effective
    type (source schema + casts, tracked through prior ops) is known non-string.
    Each drop is recorded in plan.assumptions. Unknown types are left alone —
    ungrounded plans keep today's behavior."""
    from agent.typecheck import _op_result_type
    from models.spec import _norm_op, _OP_BLANK_DEFAULT, _OP_REMOVE_SPECIAL

    cosmetic = _COSMETIC_STRING_OPS | _OP_BLANK_DEFAULT | _OP_REMOVE_SPECIAL
    src_by_entity = {s.entity: s for s in plan.sources}
    for xf in plan.transforms:
        src = src_by_entity.get(xf.entity)
        types = {c.name: (c.type or "string") for c in (src.columns if src else [])}
        types.update({c.name: c.type for c in xf.casts})
        kept = []
        for op in xf.cleaning:
            key = _norm_op(op.op)
            in_place = not op.new_column or op.new_column == op.column
            ty = types.get(op.column, "").strip().lower()
            if key in cosmetic and in_place and ty and ty != "string":
                plan.assumptions.append(
                    f"'{xf.entity}': skipped {op.op}({op.column}) — column is "
                    f"{ty}, a cosmetic string cleanup does not apply"
                )
                continue
            types[op.new_column or op.column] = _op_result_type(key, op.column, types)
            kept.append(op)
        xf.cleaning = kept


# --------------------------------------------------------------------------- #
# Compiler
# --------------------------------------------------------------------------- #


def compile_plan(plan: LogicalPlan) -> PipelineSpec:
    # Delta-safe column names first: every downstream reference sees the safe
    # names; the silver source projection keeps the file's original header.
    plan, _delta_renames, _original_cols = _sanitize_delta_columns(plan)

    # Drop cosmetic string cleaners on known non-string columns BEFORE anything
    # renders. "Trim all string columns" makes the model trim zip_code (typed
    # integer by grounding); typecheck then fails preflight and only a lucky
    # LLM repair round saves the run. Deterministic here = no dice-roll.
    _drop_string_ops_on_non_string(plan)

    project = plan.project
    src_by_entity: Dict[str, LogicalSource] = {s.entity: s for s in plan.sources}
    xf_by_entity: Dict[str, EntityTransform] = {t.entity: t for t in plan.transforms}
    # Layer sink containers — the BRD's names when it gives them ("copy into
    # the raw container"), medallion defaults otherwise.
    c_bronze, c_silver, c_gold = (
        plan.containers.bronze, plan.containers.silver, plan.containers.gold
    )

    linked_services: List[LinkedService] = []
    datasets: List[Dataset] = []
    dataflows: List[DataFlow] = []
    pipelines: List[PipelineDef] = []
    triggers: List[Trigger] = []
    assumptions: List[str] = list(plan.assumptions)
    # ONE pipeline, activities chained by dependsOn — the minimal medallion shape
    # any BRD implies, not a per-entity explosion. bronze copies first, then
    # silver flows (each after its own copy), then gold (after its silver inputs).
    bronze_acts: List[Activity] = []
    silver_acts: List[Activity] = []
    gold_acts: List[Activity] = []

    # ------------------------------------------------------------------ #
    # Linked services
    # ------------------------------------------------------------------ #
    storage_type = "AzureBlobFS"
    linked_services.append(
        LinkedService(name=STORAGE_LS, type=storage_type, auth=AuthKind.managed_identity)
    )

    http_ls: Dict[str, str] = {}  # host token -> ls name
    sql_ls: Dict[str, str] = {}  # server token -> ls name
    for s in plan.sources:
        if s.format == SourceFormat.http_csv and s.http_url:
            tok = _host_token(s.http_url)
            if tok not in http_ls:
                name = f"ls_http_{tok}"
                http_ls[tok] = name
                parsed = urlparse(s.http_url)
                base = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else s.http_url
                linked_services.append(
                    LinkedService(name=name, type="HttpServer", auth=AuthKind.anonymous, url=base)
                )
        elif s.format == SourceFormat.sql_table and s.server:
            tok = _server_token(s.server)
            if tok not in sql_ls:
                name = f"ls_sql_{tok}"
                sql_ls[tok] = name
                linked_services.append(
                    LinkedService(
                        name=name,
                        type="AzureSqlDatabase",
                        auth=AuthKind.managed_identity,
                        server=s.server,
                        database=s.database,
                    )
                )

    # ------------------------------------------------------------------ #
    # Ingest (landing -> bronze Copy) per source
    # ------------------------------------------------------------------ #
    used_landing_container = False
    bronze_ds: Dict[str, str] = {}  # entity -> bronze dataset name

    for s in plan.sources:
        ent = s.entity
        kind = _DS_KIND_BY_FORMAT[s.format]
        cols = [Column(name=c.name, type=c.type) for c in s.columns]

        landing_name = f"ds_landing_{ent}"
        bronze_name = f"ds_bronze_{ent}"
        bronze_ds[ent] = bronze_name

        if s.format in (
            SourceFormat.csv,
            SourceFormat.text,
            SourceFormat.parquet,
            SourceFormat.json,
            SourceFormat.binary,
        ):
            container = s.container or "landing"
            if container == "landing":
                used_landing_container = True
            # Models routinely echo the container into the folder ("in landing"
            # -> container=landing, folder=landing) which doubles the path to
            # landing/landing/<file> and fails the source-file gate.
            folder = (s.folder or "").strip().strip("/")
            if folder.lower() == container.lower():
                folder = ""
            landing = Dataset(
                name=landing_name,
                kind=kind,
                linked_service=STORAGE_LS,
                location=DatasetLocation(
                    container=container, folder=folder or None, file_name=s.file_name
                ),
                columns=cols,
            )
            bronze_file = s.file_name or f"{ent}.csv"
        elif s.format == SourceFormat.http_csv:
            path = urlparse(s.http_url).path.lstrip("/")
            landing = Dataset(
                name=landing_name,
                kind=DatasetKind.delimited_text,
                linked_service=http_ls[_host_token(s.http_url)],
                relative_url=path,
                columns=cols,
            )
            bronze_file = f"{ent}.csv"
        else:  # sql_table
            landing = Dataset(
                name=landing_name,
                kind=DatasetKind.azure_sql_table,
                linked_service=sql_ls[_server_token(s.server)],
                schema_name=s.schema_name,
                table_name=s.table_name,
                columns=cols,
            )
            bronze_file = f"{ent}.csv"

        bronze_kind = (
            kind if s.format in (
                SourceFormat.csv,
                SourceFormat.text,
                SourceFormat.parquet,
                SourceFormat.json,
                SourceFormat.binary,
            )
            else DatasetKind.delimited_text
        )
        bronze = Dataset(
            name=bronze_name,
            kind=bronze_kind,
            linked_service=STORAGE_LS,
            location=DatasetLocation(container=c_bronze, folder=ent, file_name=bronze_file),
            columns=cols,
        )
        datasets.extend([landing, bronze])

        copy_deps: List[str] = []
        if s.validate_first:
            if s.format in (
                SourceFormat.csv,
                SourceFormat.text,
                SourceFormat.parquet,
                SourceFormat.json,
                SourceFormat.binary,
            ):
                # Validation gate: waits for the landing file to exist AND have
                # >= 1 byte; on timeout it FAILS, blocking this copy and every
                # activity downstream of it (silver/gold via dependsOn).
                bronze_acts.append(
                    Activity(
                        name=f"validate_{ent}",
                        type="Validation",
                        layer=MedallionLayer.bronze,
                        source_dataset=landing_name,
                    )
                )
                copy_deps = [f"validate_{ent}"]
            else:
                assumptions.append(
                    f"'{ent}': file-existence validation applies to storage files "
                    f"only — skipped for {s.format.value} source"
                )

        bronze_acts.append(
            Activity(
                name=f"copy_{ent}",
                type="Copy",
                layer=MedallionLayer.bronze,
                source_dataset=landing_name,
                sink_dataset=bronze_name,
                depends_on=copy_deps,
            )
        )

    # ------------------------------------------------------------------ #
    # Silver flows per transform
    # ------------------------------------------------------------------ #
    silver_ds: Dict[str, str] = {}  # entity -> silver dataset name

    for xf in plan.transforms:
        ent = xf.entity
        if ent not in src_by_entity:
            assumptions.append(f"transform for '{ent}' skipped: no matching source")
            continue
        camel = _camel(ent)
        silver_name = f"ds_silver_{ent}"
        silver_ds[ent] = silver_name
        datasets.append(
            Dataset(
                name=silver_name,
                kind=DatasetKind.delta,
                linked_service=STORAGE_LS,
                location=DatasetLocation(container=c_silver, folder=ent),
            )
        )

        transforms = _silver_transforms(xf, src_by_entity[ent].columns)
        cur_cols = src_by_entity[ent].columns
        orig_cols = _original_cols.get(ent) or cur_cols
        entity_renamed = [o.name for o in orig_cols] != [c.name for c in cur_cols]
        if entity_renamed:
            # Source projection must match the FILE header; rename to the
            # Delta-safe names immediately after the source.
            transforms.insert(0, _sanitize_select(orig_cols, cur_cols))
            for orig, cur in zip(orig_cols, cur_cols):
                if orig.name != cur.name:
                    assumptions.append(
                        f"'{ent}': column '{orig.name}' renamed to "
                        f"'{cur.name}' (Delta forbids spaces/special "
                        "characters in column names)"
                    )
        src_cols = [
            Column(name=o.name, type=c.type)
            for o, c in zip(orig_cols, cur_cols)
        ] if entity_renamed else [Column(name=c.name, type=c.type) for c in cur_cols]
        dataflows.append(
            DataFlow(
                name=f"df_silver_{ent}",
                layer=MedallionLayer.silver,
                source=DataFlowEndpoint(stream_name=f"src{camel}", dataset=bronze_ds[ent]),
                sink=DataFlowEndpoint(stream_name=f"sink{camel}", dataset=silver_name),
                source_columns=src_cols,
                transforms=transforms,
            )
        )
        silver_acts.append(
            Activity(
                name=f"flow_silver_{ent}",
                type="ExecuteDataFlow",
                layer=MedallionLayer.silver,
                data_flow=f"df_silver_{ent}",
                depends_on=[f"copy_{ent}"],
            )
        )

    # Pass-through silver for entities a gold output reads while a silver layer
    # exists elsewhere. Without this, gold reads bronze for those entities while
    # a silver layer exists — the "gold skips silver" medallion break preflight
    # blocks (medallion_wiring). A transform-less silver flow copies bronze ->
    # silver Delta, keeping every gold source on the silver layer.
    if silver_ds:
        gold_entities = {
            e for out in plan.outputs for e in out.from_entities if e in src_by_entity
        }
        for ent in sorted(gold_entities - set(silver_ds)):
            camel = _camel(ent)
            silver_name = f"ds_silver_{ent}"
            silver_ds[ent] = silver_name
            datasets.append(
                Dataset(
                    name=silver_name,
                    kind=DatasetKind.delta,
                    linked_service=STORAGE_LS,
                    location=DatasetLocation(container=c_silver, folder=ent),
                )
            )
            cur_cols = src_by_entity[ent].columns
            orig_cols = _original_cols.get(ent) or cur_cols
            entity_renamed = [o.name for o in orig_cols] != [c.name for c in cur_cols]
            pass_transforms = (
                [_sanitize_select(orig_cols, cur_cols)] if entity_renamed else []
            )
            src_cols = [
                Column(name=o.name, type=c.type)
                for o, c in zip(orig_cols, cur_cols)
            ] if entity_renamed else [Column(name=c.name, type=c.type) for c in cur_cols]
            dataflows.append(
                DataFlow(
                    name=f"df_silver_{ent}",
                    layer=MedallionLayer.silver,
                    source=DataFlowEndpoint(stream_name=f"src{camel}", dataset=bronze_ds[ent]),
                    sink=DataFlowEndpoint(stream_name=f"sink{camel}", dataset=silver_name),
                    source_columns=src_cols,
                    transforms=pass_transforms,
                )
            )
            silver_acts.append(
                Activity(
                    name=f"flow_silver_{ent}",
                    type="ExecuteDataFlow",
                    layer=MedallionLayer.silver,
                    data_flow=f"df_silver_{ent}",
                    depends_on=[f"copy_{ent}"],
                )
            )
            assumptions.append(
                f"'{ent}' feeds a gold output but had no cleaning — added a "
                f"pass-through silver flow so gold reads the silver layer"
            )

    def best_dataset(entity: str) -> str:
        """Silver dataset if a silver flow exists for the entity, else bronze."""
        return silver_ds.get(entity) or bronze_ds[entity]

    def entity_schema(entity: str) -> List[Column]:
        """The entity's EFFECTIVE schema where gold reads it: source columns,
        casts applied, cleaning-op result types included. Stamped onto gold flow
        endpoints so downstream type-checking sees real types — without it the
        spec-level name-inference fallback types every column 'string' and
        flags valid aggregations. Empty when the source declared no columns."""
        from agent.typecheck import _op_result_type
        from models.spec import _norm_op

        s = src_by_entity[entity]
        if not s.columns:
            return []
        types = {c.name: (c.type or "string") for c in s.columns}
        xf = xf_by_entity.get(entity)
        if xf is not None:
            for c in xf.casts:
                types[c.name] = c.type
            for op in xf.cleaning:
                types[op.new_column or op.column] = _op_result_type(
                    _norm_op(op.op), op.column, types
                )
        return [Column(name=n, type=t) for n, t in types.items()]

    # ------------------------------------------------------------------ #
    # Gold flows per output
    # ------------------------------------------------------------------ #
    for out in plan.outputs:
        entities = [e for e in out.from_entities if e in src_by_entity]
        if not entities:
            assumptions.append(f"output '{out.name}' skipped: no known source entities")
            continue
        gold_name = f"ds_gold_{out.name}"
        gold_kind = {
            "delta": DatasetKind.delta,
            "csv": DatasetKind.delimited_text,
            "parquet": DatasetKind.parquet,
        }[out.format.value]
        gold_loc = DatasetLocation(
            container=c_gold,
            folder=out.name,
            file_name=f"{out.name}.csv" if out.format.value == "csv" else None,
        )
        datasets.append(
            Dataset(name=gold_name, kind=gold_kind, linked_service=STORAGE_LS, location=gold_loc)
        )

        first = entities[0]
        source_ep = DataFlowEndpoint(stream_name=f"src{_camel(first)}", dataset=best_dataset(first))
        additional = [
            DataFlowEndpoint(
                stream_name=f"src{_camel(e)}",
                dataset=best_dataset(e),
                columns=entity_schema(e),
            )
            for e in entities[1:]
        ]
        # Column types as gold sees them: effective per-entity schemas (source
        # types + casts + cleaning results), merged across the entities this
        # output reads (coalesce defaults + downstream type-checking need them).
        gold_types: Dict[str, str] = {}
        for e in entities:
            gold_types.update({c.name: c.type for c in entity_schema(e)})
        transforms = _gold_transforms(out, gold_types)
        dataflows.append(
            DataFlow(
                name=f"df_gold_{out.name}",
                layer=MedallionLayer.gold,
                source=source_ep,
                sink=DataFlowEndpoint(stream_name=f"sink{_camel(out.name)}", dataset=gold_name),
                source_columns=entity_schema(first),
                additional_sources=additional,
                transforms=transforms,
            )
        )
        gold_deps = [
            f"flow_silver_{e}" if e in silver_ds else f"copy_{e}" for e in entities
        ]
        gold_acts.append(
            Activity(
                name=f"flow_gold_{out.name}",
                type="ExecuteDataFlow",
                layer=MedallionLayer.gold,
                data_flow=f"df_gold_{out.name}",
                depends_on=gold_deps,
            )
        )

    # ------------------------------------------------------------------ #
    # Pipelines — driven by the BRD. If the plan lists pipelines, build exactly
    # those (any count, master + stages). If it lists none, build ONE pipeline.
    # The compiler assigns every ADF name and dependency; the plan only says how
    # the work is grouped. No fixed pipeline count is baked into this code.
    # ------------------------------------------------------------------ #
    bronze_by_ent = {
        a.name[len("copy_"):]: a for a in bronze_acts if a.name.startswith("copy_")
    }
    validate_by_ent = {
        a.name[len("validate_"):]: a for a in bronze_acts if a.name.startswith("validate_")
    }
    silver_by_ent = {a.name[len("flow_silver_"):]: a for a in silver_acts}
    gold_by_out = {a.name[len("flow_gold_"):]: a for a in gold_acts}

    def _pl_name(logical: str) -> str:
        s = re.sub(r"[^a-z0-9_]", "_", (logical or "").strip().lower())
        s = re.sub(r"_+", "_", s).strip("_")
        if s.startswith("pl_"):
            s = s[3:]
        return f"pl_{s or 'pipeline'}"

    def _localize(acts: List[Activity]) -> List[Activity]:
        # Cross-stage ordering comes from the master's ExecutePipeline chain;
        # only dependencies on activities INSIDE the same pipeline survive
        # (e.g. validate_x -> copy_x both placed in the ingest stage).
        members = {a.name for a in acts}
        return [
            a.model_copy(update={"depends_on": [d for d in a.depends_on if d in members]})
            for a in acts
        ]

    if plan.pipelines:
        adf_name_of = {lp.name: _pl_name(lp.name) for lp in plan.pipelines}
        placed: set = set()
        trigger_target: Optional[str] = None
        for lp in plan.pipelines:
            adf = adf_name_of[lp.name]
            if lp.invokes:  # orchestrator / master
                acts: List[Activity] = []
                prev: Optional[str] = None
                for child in lp.invokes:
                    child_adf = adf_name_of.get(child, _pl_name(child))
                    run_name = f"run_{child_adf[len('pl_'):]}"
                    acts.append(
                        Activity(
                            name=run_name,
                            type="ExecutePipeline",
                            invoked_pipeline=child_adf,
                            depends_on=[prev] if prev else [],
                        )
                    )
                    prev = run_name
                pipelines.append(PipelineDef(name=adf, activities=acts))
                trigger_target = adf  # the master is what the trigger fires
            else:  # work stage
                acts = []
                for e in lp.ingest:
                    if e in validate_by_ent:  # gate travels with its copy
                        acts.append(validate_by_ent[e])
                        placed.add(validate_by_ent[e].name)
                    if e in bronze_by_ent:
                        acts.append(bronze_by_ent[e])
                        placed.add(bronze_by_ent[e].name)
                for e in lp.silver:
                    if e in silver_by_ent:
                        acts.append(silver_by_ent[e])
                        placed.add(silver_by_ent[e].name)
                for o in lp.gold:
                    if o in gold_by_out:
                        acts.append(gold_by_out[o])
                        placed.add(gold_by_out[o].name)
                pipelines.append(PipelineDef(name=adf, activities=_localize(acts)))

        # Nothing silently dropped: any activity the BRD's pipelines didn't place
        # goes into a catch-all pipeline (completeness normally prevents this).
        leftover = [
            a for a in (bronze_acts + silver_acts + gold_acts) if a.name not in placed
        ]
        if leftover:
            extra = f"pl_{project}_unassigned"
            pipelines.append(PipelineDef(name=extra, activities=_localize(leftover)))
            assumptions.append(
                f"{len(leftover)} activities were not assigned to any BRD pipeline "
                f"-> placed in {extra}"
            )
        trigger_target = trigger_target or (pipelines[-1].name if pipelines else f"pl_{project}")
    else:
        # No structure specified -> one pipeline, ordered by dependsOn.
        pipeline_name = f"pl_{project}"
        pipelines.append(
            PipelineDef(name=pipeline_name, activities=bronze_acts + silver_acts + gold_acts)
        )
        trigger_target = pipeline_name

    if plan.schedule:
        triggers.append(
            Trigger(
                name=f"tr_schedule_{project}",
                kind=TriggerKind.schedule,
                pipeline=trigger_target,
                frequency=plan.schedule.frequency,
                interval=plan.schedule.interval,
                start_time=plan.schedule.start_time,
            )
        )

    # ------------------------------------------------------------------ #
    # Storage + assumptions + capability contract
    # ------------------------------------------------------------------ #
    containers = [c_bronze, c_silver, c_gold]
    if used_landing_container:
        containers = ["landing"] + containers
    if plan.containers != type(plan.containers)():  # BRD named its own containers
        assumptions.append(
            "Layer containers from the BRD: bronze -> "
            f"'{c_bronze}', silver -> '{c_silver}', gold -> '{c_gold}'"
        )
    account = os.environ.get("ADLS_ACCOUNT_NAME", "storage")

    for line in plan.unsupported:
        assumptions.append(f"NOT BUILT (unsupported): {line}")

    return PipelineSpec(
        name=project,
        storage=StorageAccount(account_name=account, containers=containers),
        linked_services=linked_services,
        datasets=datasets,
        dataflows=dataflows,
        pipelines=pipelines,
        triggers=triggers,
        assumptions=assumptions,
        clarifications=list(plan.clarifications),
        suggestions=list(plan.suggestions),
        unsupported=list(plan.unsupported),
    )


# --------------------------------------------------------------------------- #
# Transform builders
# --------------------------------------------------------------------------- #


_NUMERIC_LITERAL = re.compile(r"-?\d+(\.\d+)?")
_NUMERIC_TYPE = re.compile(r"^(integer|long|double|float|short|decimal)", re.IGNORECASE)
# Cleaning ops that change the column's type mid-chain (normalized op -> type).
_OP_RESULT_TYPE = {
    "tointeger": "integer",
    "tolong": "long",
    "todouble": "double",
    "todecimal": "decimal",
    "toboolean": "boolean",
    "tostring": "string",
    "todate": "date",
    "totimestamp": "timestamp",
}


def _coalesce_column_ref(arg: str, own_column: "Optional[str]", col_types: Dict[str, str]) -> Optional[str]:
    """True column-reference resolution for one coalesce arg: an arg naming
    ANOTHER known column of this entity (e.g. "fall back to order_date") is a
    reference, not a default literal — `_render_op`'s generic renderer has no
    column context and force-quotes every non-numeric arg into a string
    literal (`coalesce(shipped_date, 'order_date')`), which is the exact
    DF-DT-001 root cause: a real column silently became a string constant.
    Returns the bare column name, or None when the arg is a genuine literal."""
    s = str(arg).strip()
    core = s[1:-1] if len(s) >= 2 and s.startswith("'") and s.endswith("'") else s
    if core in col_types and core != own_column:
        return core
    return None


def _typed_coalesce_args(op: CleaningOp, col_types: Dict[str, str]) -> List[str]:
    """coalesce's default literal must match the column's type or the flow dies
    at runtime (DF-DT-001 'Types for Function coalesce not compatible'): the
    renderer emits bare 0 for '0', which breaks string columns. Quote numeric
    defaults for string columns and unquote quoted numerics for numeric
    columns; columns of unknown type pass through untouched. An arg that is
    actually a column reference (see _coalesce_column_ref) is left bare — the
    caller (_clean_col) renders it as an expression, bypassing the generic
    literal-only renderer entirely."""
    if re.sub(r"[^a-z]", "", (op.op or "").lower()) != "coalesce":
        return list(op.args)
    col_type = col_types.get(op.column)
    fixed: List[str] = []
    for a in op.args:
        ref = _coalesce_column_ref(a, op.column, col_types)
        if ref is not None:
            fixed.append(ref)
            continue
        if col_type is None:
            fixed.append(a)
            continue
        s = str(a).strip()
        quoted = len(s) >= 2 and s.startswith("'") and s.endswith("'")
        core = s[1:-1] if quoted else s
        if not _NUMERIC_LITERAL.fullmatch(core):
            fixed.append(a)
        elif col_type.startswith("string") and not quoted:
            fixed.append(f"'{core}'")
        elif _NUMERIC_TYPE.match(col_type) and quoted:
            fixed.append(core)
        else:
            fixed.append(a)
    return fixed


_AGG_RESULT_TYPE = {
    "sum": "double", "avg": "double", "mean": "double",
    "count": "long", "countdistinct": "long",
}
_CASE_CMP = {"lt": "<", "le": "<=", "gt": ">", "ge": ">=", "eq": "==", "ne": "!="}


def _case_value(v: str) -> str:
    """A case branch's `value`/`then`/`otherwise`: bare when it looks like a
    column reference, a quoted/typed literal otherwise. Mirrors arithmetic's
    _render_operand — the same ambiguity (is 'quantity' a column or a string?)
    with the same resolution (identifier = column)."""
    from models.spec import _render_operand

    return _render_operand(v)


def _case_expression(op: CleaningOp) -> str:
    """CleaningOp(op=case) -> a right-associated iif() chain, first matching
    `when` branch wins: iif(c1, t1, iif(c2, t2, ... otherwise))."""
    expr = _case_value(op.otherwise or "")
    for branch in reversed(op.when):
        cmp = _CASE_CMP[_norm_op_local(branch.op)]
        cond = f"{op.column} {cmp} {_case_value(branch.value)}"
        expr = f"iif({cond}, {_case_value(branch.then)}, {expr})"
    return expr


def _norm_op_local(op: str) -> str:
    from models.spec import _norm_op

    return _norm_op(op)


def _op_consumes(op: CleaningOp) -> set:
    """Column names one CleaningOp reads — used to decide whether it must
    start a NEW derived-column batch (ADF can't see a sibling transform's
    output within the same transform). Case branches can reference other
    columns via `then`/`otherwise`, same as arithmetic's `args`."""
    consumed = {op.column, *op.args}
    if _norm_op_local(op.op) == "case":
        consumed |= {b.then for b in op.when}
        if op.otherwise:
            consumed.add(op.otherwise)
    return consumed


def _clean_col(op: CleaningOp, col_types: Dict[str, str]) -> DerivedColumn:
    if _norm_op_local(op.op) == "case":
        return DerivedColumn(
            name=op.new_column or op.column,
            op=op.op,
            column=op.column,
            expression=_case_expression(op),
        )
    fixed_args = _typed_coalesce_args(op, col_types)
    # Any coalesce arg that resolved to a column reference must render bare —
    # the generic op/args -> DSL renderer (models.spec._render_op) has no
    # column context and quotes every non-numeric arg as a string literal, so
    # build the expression directly here instead of handing it op/args.
    is_coalesce = re.sub(r"[^a-z]", "", (op.op or "").lower()) == "coalesce"
    is_column_ref = [
        is_coalesce and _coalesce_column_ref(a, op.column, col_types) is not None
        for a in fixed_args
    ]
    if any(is_column_ref):
        from models.spec import _render_literal

        parts = [op.column] + [
            a if ref else _render_literal(a) for a, ref in zip(fixed_args, is_column_ref)
        ]
        return DerivedColumn(
            name=op.new_column or op.column,
            op=op.op,
            column=op.column,
            expression=f"coalesce({', '.join(parts)})",
        )
    return DerivedColumn(
        name=op.new_column or op.column,
        op=op.op,
        column=op.column,
        args=fixed_args,
    )


def _derived_transforms(
    ops: List[CleaningOp], base_name: str, col_types: Optional[Dict[str, str]] = None
) -> List[Transform]:
    """Derived-column ops -> one or MORE sequential transforms. Inside a single
    ADF derived-column transform, sibling expressions read the INPUT stream —
    they cannot see columns produced by the same transform. So an op that
    consumes a column produced earlier in the list starts a new transform
    (base, base2, base3, ...), making chained ops like
    one_minus_discount = 1 - discount, then line_total = quantity * list_price
    * one_minus_discount render correctly."""
    types = dict(col_types or {})
    batches: List[List[CleaningOp]] = [[]]
    produced: set = set()
    for op in ops:
        if produced & _op_consumes(op):
            batches.append([])
            produced = set()  # previous batch's outputs are upstream now
        batches[-1].append(op)
        produced.add(op.new_column or op.column)
    columns_by_batch: List[List[DerivedColumn]] = []
    for batch in batches:
        columns_by_batch.append([_clean_col(op, types) for op in batch])
        for op in batch:  # a to* op retypes its output for later batches
            result = _OP_RESULT_TYPE.get(re.sub(r"[^a-z]", "", (op.op or "").lower()))
            if result is not None:
                types[op.new_column or op.column] = result
    return [
        Transform(
            kind=TransformKind.derived,
            name=base_name if i == 0 else f"{base_name}{i + 1}",
            columns=cols,
        )
        for i, (batch, cols) in enumerate(zip(batches, columns_by_batch))
        if batch
    ]


def _predicate(f: FilterRule) -> Predicate:
    return Predicate(op=f.op, column=f.column, value=f.value)


def _silver_transforms(
    xf: EntityTransform, source_cols: Optional[List[LogicalColumn]] = None
) -> List[Transform]:
    transforms: List[Transform] = []
    # Column types as they stand when cleaning runs: source schema, then casts
    # (castTypes precedes cleanColumns in the flow, so casts win).
    col_types = {c.name: c.type for c in source_cols or []}
    col_types.update({c.name: c.type for c in xf.casts})
    if xf.casts:
        transforms.append(
            Transform(
                kind=TransformKind.cast,
                name="castTypes",
                columns=[
                    DerivedColumn(name=c.name, expression=_cast_expression(c)) for c in xf.casts
                ],
            )
        )
    if xf.cleaning:
        transforms.extend(_derived_transforms(xf.cleaning, "cleanColumns", col_types))
    if xf.filters:
        transforms.append(
            Transform(
                kind=TransformKind.filter,
                name="filterRows",
                predicates=[_predicate(f) for f in xf.filters],
            )
        )
    if xf.dedup_keys:
        # columns=[] -> builder emits each(match(!in([keys], name)), $$ = first($$)),
        # preserving every non-key column via schema drift (no schema needed here).
        transforms.append(
            Transform(
                kind=TransformKind.aggregate,
                name="dedupRows",
                group_by=list(xf.dedup_keys),
                columns=[],
            )
        )
    return transforms


# ADF data-flow script joinType values — the DSL takes [outer,left,cross,
# inner,right], NOT the SQL names (DF-ARG-005 "Join type should be one of
# [outer,left,cross,inner,right]", stress BRD 10: 'left_outer' rejected).
_JOIN_TYPE = {
    "inner": "inner",
    "left": "left",
    "left_outer": "left",
    "right": "right",
    "right_outer": "right",
    "full": "outer",
    "full_outer": "outer",
    "outer": "outer",
    "cross": "cross",
}


def _gold_transforms(
    out: OutputPlan, col_types: Optional[Dict[str, str]] = None
) -> List[Transform]:
    transforms: List[Transform] = []
    agg_names = {a.name for a in out.aggregations}
    post_agg_names = {op.new_column or op.column for op in out.post_agg_derived}
    pre_filters = [
        f for f in out.filters
        if f.column not in agg_names and f.column not in post_agg_names
    ]
    post_agg_filters = [f for f in out.filters if f.column in agg_names]
    post_derived_filters = [f for f in out.filters if f.column in post_agg_names]

    for i, j in enumerate(out.joins):
        left_stream = f"src{_camel(j.left_entity)}"
        right_stream = f"src{_camel(j.right_entity)}"
        from models.spec import dsl_name

        cond = " && ".join(
            f"{left_stream}@{dsl_name(lk)} == {right_stream}@{dsl_name(rk)}"
            for lk, rk in zip(j.left_keys, j.right_keys)
        )
        if not cond:
            # Defense in depth: completeness flags keyless joins for repair;
            # an empty condition compiled to `join()` and failed at runtime
            # with DF-ARG-005 (stress BRD 10) — never emit it.
            raise ValueError(
                f"output {out.name}: join {j.left_entity} -> {j.right_entity} "
                "has no key columns"
            )
        transforms.append(
            Transform(
                kind=TransformKind.join,
                name=f"join{i + 1}",
                right_stream=right_stream,
                join_type=_JOIN_TYPE.get(j.type, "inner"),
                condition=cond,
            )
        )

    if out.joins:
        # Joins keep BOTH sides' same-named columns (at minimum the join key);
        # a later plain-name reference — countDistinct(order_id) in
        # aggregateRows — is ambiguous and the Studio flags "columns which are
        # not present in current context" even though the pipeline run reports
        # success (bike-store gold). One dedup select AFTER all joins (never
        # between them — join conditions use src@col qualifiers, which a
        # select would strip) keeps the left copy of each duplicate.
        transforms.append(
            Transform(kind=TransformKind.select, name="dedupJoinColumns", columns=[])
        )

    if out.derived:
        transforms.extend(_derived_transforms(out.derived, "deriveColumns", col_types))
    if pre_filters:
        transforms.append(
            Transform(
                kind=TransformKind.filter,
                name="filterRows",
                predicates=[_predicate(f) for f in pre_filters],
            )
        )
    if out.group_by or out.aggregations:
        group_names = set(out.group_by)
        for a in out.aggregations:
            if a.name in group_names:
                raise ValueError(
                    f"output {out.name}: aggregation '{a.name}' duplicates a group_by column"
                )
        transforms.append(
            Transform(
                kind=TransformKind.aggregate,
                name="aggregateRows",
                group_by=list(out.group_by),
                columns=[
                    DerivedColumn(name=a.name, op=a.op, column=a.column)
                    for a in out.aggregations
                ],
            )
        )
    if post_agg_filters:
        transforms.append(
            Transform(
                kind=TransformKind.filter,
                name="filterAfterAgg",
                predicates=[_predicate(f) for f in post_agg_filters],
            )
        )
    if out.post_agg_derived:
        # Types after aggregation: group_by columns pass through from the
        # pre-agg schema, each aggregation's output is typed by its op (sum/avg
        # -> double, count* -> long, min/max/first -> the source column's type).
        post_agg_types = dict(col_types or {})
        for a in out.aggregations:
            post_agg_types[a.name] = _AGG_RESULT_TYPE.get(
                _norm_op_local(a.op), post_agg_types.get(a.column, "string")
            )
        transforms.extend(
            _derived_transforms(out.post_agg_derived, "deriveAfterAgg", post_agg_types)
        )
    if post_derived_filters:
        transforms.append(
            Transform(
                kind=TransformKind.filter,
                name="filterAfterDerived",
                predicates=[_predicate(f) for f in post_derived_filters],
            )
        )
    if out.sort:
        transforms.append(
            Transform(
                kind=TransformKind.sort,
                name="sortRows",
                order_by=[SortKey(column=s.column, order=s.order) for s in out.sort],
            )
        )
    if out.top_n is not None:
        transforms.append(
            Transform(kind=TransformKind.limit, name="limitRows", take=out.top_n)
        )
    if out.columns:
        transforms.append(
            Transform(
                kind=TransformKind.select,
                name="selectFinal",
                columns=[DerivedColumn(name=c) for c in out.columns],
            )
        )
    return transforms
