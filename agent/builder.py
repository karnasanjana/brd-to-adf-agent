"""Deterministic builder: PipelineSpec -> ADF ARM JSON + Data Flow script DSL.

No LLM, no cloud. Given a validated ``PipelineSpec`` this produces the exact
artifact JSON the Deployer feeds to ``create_or_update``, plus the Mapping Data
Flow script DSL embedded in each Silver/Gold data flow.

Layer rules (enforced here):
  * BRONZE  = a Copy Activity only (source -> bronze), byte-for-byte, NO script.
  * SILVER  = MappingDataFlow with a script (cast/null/filter/dedup/audit -> Parquet).
  * GOLD    = MappingDataFlow with a script (aggregate/derive/sort).
  * pl_master_pipeline chains them with dependsOn:[Succeeded].
"""

from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, List

from models.spec import (
    Column,
    DataFlow,
    Dataset,
    DatasetKind,
    LinkedService,
    PipelineDef,
    PipelineSpec,
    Transform,
    TransformKind,
    Trigger,
    TriggerKind,
    dsl_name,
)

# --------------------------------------------------------------------------- #
# Linked services
# --------------------------------------------------------------------------- #


def _blob_mode() -> bool:
    """True when targeting plain Azure Blob (no HNS) instead of ADLS Gen2."""
    from config import storage_kind

    return storage_kind() == "blob"


def _to_blob_endpoint(account_url: "str | None") -> "str | None":
    """Rewrite a .dfs URL the LLM produced to the .blob endpoint for blob mode."""
    if not account_url:
        return account_url
    return account_url.replace(".dfs.core.windows.net", ".blob.core.windows.net")


def _acct_from_url(account_url: "str | None") -> "str | None":
    """The storage account name from a .dfs/.blob URL (or a bare account name)."""
    if not account_url:
        return None
    host = account_url
    if "://" in account_url:
        from urllib.parse import urlparse

        host = urlparse(account_url).netloc
    return host.split(".")[0] if host else None


def _storage_endpoint(spec_account_url: "str | None") -> "str | None":
    """The storage endpoint the linked service should target.

    The deploy target is the storage account configured in the environment
    (ADLS_ACCOUNT_NAME). A linked service with NO account URL, or one naming the SAME
    account, collapses onto that deploy account — so an LLM-hallucinated account name
    can't point the pipeline at a non-existent store. A linked service that names a
    DISTINCT account (multi-account BRD: e.g. a source lake separate from the medallion
    lake) keeps its own account, so multiple storage accounts are supported.
    Falls back to the spec's URL when no deploy account env is set."""
    from config import blob_endpoint, dfs_endpoint

    deploy = os.environ.get("ADLS_ACCOUNT_NAME")
    spec_acct = _acct_from_url(spec_account_url)
    if deploy and spec_acct and spec_acct.lower() != deploy.lower():
        # Distinct, explicitly-named account -> honor it (mapped to storage_kind).
        return blob_endpoint(spec_acct) if _blob_mode() else dfs_endpoint(spec_acct)
    if deploy:
        return blob_endpoint(deploy) if _blob_mode() else dfs_endpoint(deploy)
    return _to_blob_endpoint(spec_account_url) if _blob_mode() else spec_account_url


# Every Azure storage linked-service label the LLM might emit. They all collapse to
# ONE deploy target whose concrete type is decided by STORAGE_KIND, not the label.
# Includes common LLM shorthands (e.g. AzureDataLakeGen2 without "Storage") so a
# label variant never crashes the build.
_AZURE_STORAGE_TYPES = {
    "AzureBlobFS",
    "AzureBlobStorage",
    "AzureDataLakeStorageGen2",
    "AzureDataLakeGen2",
    "AzureDataLakeStorage",
    "AzureDataLakeStore",
    "AzureDataLake",
    "AzureStorage",
    "ADLS",
    "ADLSGen2",
}


# Azure SQL linked-service labels the LLM might emit — all build as AzureSqlDatabase.
_AZURE_SQL_TYPES = {
    "AzureSqlDatabase",
    "AzureSql",
    "AzureSQLDB",
    "SqlServer",
    "AzureSqlServer",
}


_DEPLOY_ACCT_KEY = "__deploy__"  # linked services with no/deploy account share this


def _ls_account_key(ls: LinkedService) -> str:
    """Which storage account a linked service targets. No explicit URL -> the deploy
    account bucket; a distinct account host -> its own bucket (multi-account)."""
    acct = _acct_from_url(ls.account_url)
    return acct.lower() if acct else _DEPLOY_ACCT_KEY


def _canonical_storage_by_account(spec: PipelineSpec) -> "dict[str, LinkedService]":
    """Return {account_key: canonical LinkedService} — ONE storage linked service per
    distinct account.

    The LLM often emits several storage linked services with inconsistent names
    (ls_adls, ls_blob_<acct>, ...) and points different datasets at different ones —
    some at a name that never gets created ("invalid reference"), some at a .dfs
    endpoint a non-HNS Blob account rejects (SoftDelete/BlobStorageEvents 409).
    Collapsing per account removes both failure modes while still supporting a BRD
    that legitimately spans MORE THAN ONE storage account (e.g. an external source
    lake plus the medallion lake). Same-account duplicates still collapse to one; the
    first LS declared for an account wins (stable name across reruns)."""
    groups: "dict[str, LinkedService]" = {}
    for ls in spec.linked_services:
        if ls.type in _AZURE_STORAGE_TYPES:
            groups.setdefault(_ls_account_key(ls), ls)
    return groups


def _synthetic_storage_ls() -> LinkedService:
    """A storage LS to synthesize when the spec declared none but has storage datasets."""
    from models.spec import AuthKind

    ls_type = "AzureBlobStorage" if _blob_mode() else "AzureBlobFS"
    return LinkedService(name="ls_storage", type=ls_type, auth=AuthKind.managed_identity)


def build_linked_service(ls: LinkedService) -> Dict[str, Any]:
    if ls.type == "HttpServer":
        return {
            "type": "HttpServer",
            "typeProperties": {
                "url": ls.url,
                "enableServerCertificateValidation": True,
                "authenticationType": "Anonymous",
            },
        }
    if ls.type == "RestService":
        return {
            "type": "RestService",
            "typeProperties": {
                "url": ls.url,
                "enableServerCertificateValidation": True,
                "authenticationType": "Anonymous",
            },
        }
    if ls.type in _AZURE_SQL_TYPES:
        # Azure SQL Database with Managed Identity — no user/password anywhere.
        server = ls.server or _acct_from_url(ls.url) or ""
        if server and "." not in server:
            server = f"{server}.database.windows.net"
        return {
            "type": "AzureSqlDatabase",
            "typeProperties": {
                "server": server,
                "database": ls.database or "",
                "authenticationType": "SystemAssignedManagedIdentity",
            },
        }
    if ls.type in _AZURE_STORAGE_TYPES:
        # Any Azure storage linked service the LLM names maps to the SAME deploy
        # target; the concrete type (AzureBlobStorage vs AzureBlobFS) is decided by
        # STORAGE_KIND, not the label.
        # Managed Identity: no key / SAS / connection string. The endpoint targets
        # the configured deploy account, not whatever the BRD named.
        endpoint = _storage_endpoint(ls.account_url)
        if _blob_mode():
            # Plain Blob storage (no HNS): AzureBlobStorage + blob serviceEndpoint.
            return {
                "type": "AzureBlobStorage",
                "typeProperties": {
                    "serviceEndpoint": endpoint,
                    "accountKind": "StorageV2",
                },
            }
        return {
            "type": "AzureBlobFS",
            "typeProperties": {"url": endpoint},
        }
    # Unknown label from the LLM — classify by keyword instead of crashing the build.
    low = ls.type.lower()
    if "sql" in low:
        return build_linked_service(ls.model_copy(update={"type": "AzureSqlDatabase"}))
    if any(k in low for k in ("blob", "lake", "storage", "adls")):
        return build_linked_service(ls.model_copy(update={"type": "AzureBlobFS"}))
    if "http" in low or "rest" in low:
        return build_linked_service(ls.model_copy(update={"type": "HttpServer"}))
    raise ValueError(f"Unsupported linked service type: {ls.type}")


# --------------------------------------------------------------------------- #
# Parameters
# --------------------------------------------------------------------------- #

import datetime as _dt

# Build-time date used as a safe default for date-like parameters, so a run never
# fails with "No value provided for Parameter ..." when the master forwards nothing.
_BUILD_DATE = _dt.date.today().strftime("%Y-%m-%d")


def _param_default(name: str) -> str:
    return _BUILD_DATE if "date" in name.lower() else ""


def _param_defs(names: List[str]) -> Dict[str, Any]:
    """Parameter definitions with a defaultValue for every parameter. Defaults make
    pipelines/datasets runnable even when the LLM names parameters inconsistently
    across layers or the master doesn't thread a value down."""
    return {p: {"type": "String", "defaultValue": _param_default(p)} for p in names}


# --------------------------------------------------------------------------- #
# Datasets
# --------------------------------------------------------------------------- #


def _dataset_parameters(ds: Dataset) -> Dict[str, Any]:
    return _param_defs(list(ds.parameters))


_PATH_PARAM = re.compile(r"\{(\w+)\}")


def _path_value(path: str, param_names) -> Any:
    """A dataset path that references declared dataset parameters must be an ADF
    EXPRESSION, not a literal — 'orders/{execution_date}' written as a plain string
    makes ADF create a folder literally named '{execution_date}'. Convert declared
    {param} tokens to @{dataset().param} and wrap as an Expression; paths without
    parameter tokens stay plain strings."""
    hit = False

    def repl(m: "re.Match[str]") -> str:
        nonlocal hit
        if m.group(1) in param_names:
            hit = True
            return f"@{{dataset().{m.group(1)}}}"
        return m.group(0)

    out = _PATH_PARAM.sub(repl, path)
    return {"value": out, "type": "Expression"} if hit else path


def _adls_location(ds: Dataset) -> Dict[str, Any]:
    loc = ds.location
    params = set(ds.parameters)
    if _blob_mode():
        # Plain Blob: AzureBlobStorageLocation uses 'container' (not 'fileSystem').
        store: Dict[str, Any] = {
            "type": "AzureBlobStorageLocation",
            "container": loc.container if loc else None,
        }
    else:
        store = {"type": "AzureBlobFSLocation", "fileSystem": loc.container if loc else None}
    if loc and loc.folder:
        store["folderPath"] = _path_value(loc.folder, params)
    if loc and loc.file_name:
        store["fileName"] = _path_value(loc.file_name, params)
    return store


def build_dataset(ds: Dataset) -> Dict[str, Any]:
    ls_ref = {"referenceName": ds.linked_service, "type": "LinkedServiceReference"}
    schema = [{"name": c.name, "type": c.type} for c in ds.columns]

    # REST endpoint
    if ds.kind == DatasetKind.rest_resource:
        return {
            "type": "RestResource",
            "linkedServiceName": ls_ref,
            "parameters": _dataset_parameters(ds),
            "typeProperties": {"relativeUrl": ds.relative_url or ""},
            "schema": schema,
        }

    # Azure SQL table (source only — sinks are storage)
    if ds.kind == DatasetKind.azure_sql_table:
        return {
            "type": "AzureSqlTable",
            "linkedServiceName": ls_ref,
            "parameters": _dataset_parameters(ds),
            "typeProperties": {
                "schema": ds.schema_name or "dbo",
                "table": ds.table_name or ds.name.removeprefix("ds_"),
            },
            "schema": schema,
        }

    # Binary (byte-for-byte copy of any file type; no schema)
    if ds.kind == DatasetKind.binary:
        location = (
            {"type": "HttpServerLocation", "relativeUrl": ds.relative_url}
            if ds.relative_url is not None
            else _adls_location(ds)
        )
        return {
            "type": "Binary",
            "linkedServiceName": ls_ref,
            "parameters": _dataset_parameters(ds),
            "typeProperties": {"location": location},
        }

    # JSON (over HTTP or in ADLS)
    if ds.kind == DatasetKind.json:
        location = (
            {"type": "HttpServerLocation", "relativeUrl": ds.relative_url}
            if ds.relative_url is not None
            else _adls_location(ds)
        )
        return {
            "type": "Json",
            "linkedServiceName": ls_ref,
            "parameters": _dataset_parameters(ds),
            "typeProperties": {"location": location},
            "schema": schema,
        }

    # Parquet (ADLS only)
    if ds.kind == DatasetKind.parquet:
        return {
            "type": "Parquet",
            "linkedServiceName": ls_ref,
            "parameters": _dataset_parameters(ds),
            "typeProperties": {"location": _adls_location(ds), "compressionCodec": "snappy"},
            "schema": schema,
        }

    # DelimitedText (CSV) — over HTTP (relative_url) or in ADLS (location)
    location = (
        {"type": "HttpServerLocation", "relativeUrl": ds.relative_url}
        if ds.relative_url is not None
        else _adls_location(ds)
    )
    return {
        "type": "DelimitedText",
        "linkedServiceName": ls_ref,
        "parameters": _dataset_parameters(ds),
        "typeProperties": {
            "location": location,
            "columnDelimiter": ",",
            "firstRowAsHeader": ds.first_row_as_header,
            "quoteChar": '"',
            "escapeChar": "\\",
        },
        "schema": schema,
    }


# Copy source/sink type per dataset kind.
_COPY_SOURCE_TYPE = {
    DatasetKind.delimited_text: "DelimitedTextSource",
    DatasetKind.parquet: "ParquetSource",
    DatasetKind.json: "JsonSource",
    DatasetKind.rest_resource: "RestSource",
    DatasetKind.binary: "BinarySource",
    DatasetKind.azure_sql_table: "AzureSqlSource",
}
_COPY_SINK_TYPE = {
    DatasetKind.delimited_text: "DelimitedTextSink",
    DatasetKind.parquet: "ParquetSink",
    DatasetKind.json: "JsonSink",
    DatasetKind.rest_resource: "RestSink",
    DatasetKind.binary: "BinarySink",
    DatasetKind.azure_sql_table: "AzureSqlSink",
}


# --------------------------------------------------------------------------- #
# Data flow script DSL (Silver / Gold only)
# --------------------------------------------------------------------------- #


def _transform_dsl(
    input_stream: str, t: Transform, prev_sort: "Transform | None" = None
) -> str:
    """Render one transformation line: '<in> <op>(...) ~> <name>'. `prev_sort`
    is the most recent sort transform in the chain — a limit needs its keys to
    rank deterministically (ADF has no native limit transform)."""
    if t.kind in (TransformKind.cast, TransformKind.derived):
        assigns = ",\n        ".join(
            f"{dsl_name(c.name)} = {c.expression}" for c in t.columns
        )
        return f"{input_stream} derive({assigns}) ~> {t.name}"

    if t.kind == TransformKind.filter:
        return f"{input_stream} filter({t.condition}) ~> {t.name}"

    if t.kind == TransformKind.aggregate:
        group = ", ".join(
            g if "=" in g else dsl_name(g) for g in t.group_by
        )
        # A computed group-by key is 'Name = expr'; its output column is Name.
        key_names = [g.split("=", 1)[0].strip() if "=" in g else g for g in t.group_by]
        # ADF outputs groupBy columns automatically; an aggregate column with the
        # same name is a DF-AGG-005 duplicate-column error. LLMs write exactly
        # that for dedup ("customer_id = first(customer_id)" with
        # groupBy(customer_id)) — drop the collisions instead of failing in Azure.
        agg_cols = [c for c in t.columns if c.name not in key_names]
        if not agg_cols and t.group_by:
            # Pure dedup: every declared aggregate collided. Keep every OTHER
            # column via a column pattern — preserves the full (drifted) schema
            # instead of collapsing the row to just the group keys.
            quoted = ", ".join(f"'{k}'" for k in key_names)
            aggs = f"each(match(!in([{quoted}], name)), $$ = first($$))"
        else:
            aggs = ",\n        ".join(
                f"{dsl_name(c.name)} = {c.expression}" for c in agg_cols
            )
        if not t.group_by:
            # Global aggregate (one row, no grouping) — no groupBy() clause, which
            # ADF's DSL rejects when empty (lint 'empty groupBy()').
            return f"{input_stream} aggregate({aggs}) ~> {t.name}"
        return f"{input_stream} aggregate(groupBy({group}),\n        {aggs}) ~> {t.name}"

    if t.kind == TransformKind.sort:
        keys = ", ".join(f"{k.order}({dsl_name(k.column)})" for k in t.order_by)
        return f"{input_stream} sort({keys}) ~> {t.name}"

    if t.kind == TransformKind.select:
        if not t.columns:
            # Select-all with duplicate-column dedup. A join carries BOTH
            # sides' same-named columns (the join key itself, e.g. order_id in
            # orders ⋈ order_items) downstream; any later plain-name reference
            # — countDistinct(order_id) in aggregateRows — is then ambiguous
            # and the Studio flags "columns which are not present in current
            # context" (bike-store gold). The skip flags keep the first
            # (left-stream) copy and drop the rest.
            return (
                f"{input_stream} select(mapColumn(each(match(true()))),\n"
                f"    skipDuplicateMapInputs: true,\n"
                f"    skipDuplicateMapOutputs: true) ~> {t.name}"
            )
        cols = ", ".join(f"{dsl_name(c.name)} = {c.expression}" for c in t.columns)
        return f"{input_stream} select(mapColumn({cols})) ~> {t.name}"

    if t.kind == TransformKind.window:
        # Structured render: window(over(keys), asc/desc(col, true), out = fn()).
        # A raw `condition` is the legacy escape hatch — the one place models
        # emit Spark partitionBy/orderBy (DF-DSL-001, run 24756f0f5aa8).
        if t.partition_by and t.columns:
            parts = [f"over({', '.join(dsl_name(p) for p in t.partition_by)})"]
            parts += [f"{k.order}({dsl_name(k.column)}, true)" for k in t.order_by]
            parts += [f"{dsl_name(c.name)} = {c.expression}" for c in t.columns]
            body = ",\n  ".join(parts)
            return f"{input_stream} window({body}) ~> {t.name}"
        return f"{input_stream} window({t.condition or ''}) ~> {t.name}"

    if t.kind == TransformKind.limit:
        # ADF has NO limit() transform — the Spark broker rejects it with
        # DF-FLO-005 "Transformation 'limit' not registered" (which also
        # orphans the sink: DF-FLO-001/DF-TX-001). Row-limiting is lowered to
        # the documented top-N pattern: rank on the preceding sort's keys
        # (falling back to a surrogate key when no sort precedes), filter to
        # the first `take` ranks, then drop the helper column so the sink
        # schema is unchanged. The final stream keeps t.name so downstream
        # chaining (next transform / sink input) is unaffected.
        # ADF DSL rejects identifiers starting with double underscore
        # (DF-DSL-001 parse error at runtime) — keep the helper name
        # underscore-free.
        rank_col = "xRowRank"
        if prev_sort is not None and prev_sort.order_by:
            keys = ",\n    ".join(f"{k.order}({k.column})" for k in prev_sort.order_by)
            head = (
                f"{input_stream} rank({keys},\n"
                f"    output({rank_col} as long),\n"
                f"    dense: false) ~> {t.name}Rank"
            )
        else:
            head = (
                f"{input_stream} keyGenerate(output({rank_col} as long),\n"
                f"    startAt: 1L) ~> {t.name}Rank"
            )
        cond = f"{rank_col} <= {t.skip + (t.take or 0)}"
        if t.skip:
            cond = f"{rank_col} > {t.skip} && {cond}"
        keep = f"{t.name}Rank filter({cond}) ~> {t.name}Keep"
        drop = (
            f"{t.name}Keep select(mapColumn(each(match(name != '{rank_col}'))),\n"
            f"    skipDuplicateMapInputs: true,\n"
            f"    skipDuplicateMapOutputs: true) ~> {t.name}"
        )
        return "\n".join([head, keep, drop])

    if t.kind == TransformKind.join:
        # ADF join DSL: "<left>, <right> join(<cond>, joinType:'inner', ...) ~> <name>".
        # left = the current stream; right = the other source stream.
        join_type = (t.join_type or "inner").replace("-", "_")
        return (
            f"{input_stream}, {t.right_stream} join({t.condition or 'true()'},\n"
            f"  joinType:'{join_type}',\n"
            "  matchType:'exact',\n"
            "  ignoreSpaces: false,\n"
            "  broadcast: 'auto') ~> "
            f"{t.name}"
        )

    raise ValueError(f"Unsupported transform kind: {t.kind}")


def _effective_transforms(df: DataFlow) -> "List[Transform]":
    """Transforms that can actually be rendered. LLMs emit DEGENERATE transforms
    (aggregate with no groupBy and no columns, derive/select with no columns,
    filter with no condition) — rendering those is a guaranteed DF-DSL-001 in
    Azure ('aggregate( )' parse error, run fe4102f4960d). A degenerate transform
    carries no semantics, so dropping it keeps the flow correct AND running."""
    kept: List[Transform] = []
    for t in df.transforms:
        if t.kind == TransformKind.aggregate and not t.group_by and not t.columns:
            continue
        if t.kind in (TransformKind.cast, TransformKind.derived) and not t.columns:
            continue
        # NOT select: an empty select renders as a valid select-all dedup
        # (see _render_transform) — the compiler emits one after joins.
        if t.kind == TransformKind.filter and not (t.condition or "").strip():
            continue
        if t.kind == TransformKind.sort and not t.order_by:
            continue
        if t.kind == TransformKind.window and not (t.condition or "").strip() \
                and not (t.partition_by and t.columns):
            continue
        kept.append(t)
    return kept


def _delta_ds(datasets: "Dict[str, Dataset] | None", dataset_name: str) -> "Dataset | None":
    """The Dataset object when `dataset_name` is a Delta dataset, else None."""
    ds = (datasets or {}).get(dataset_name)
    return ds if ds is not None and ds.kind == DatasetKind.delta else None


def _delta_script_props(ds: Dataset, sink: bool = False) -> List[str]:
    """Inline-dataset location lines for a Delta source/sink. Delta is not an ADF
    dataset resource — the container/folder ride inside the data-flow script.
    Parameter tokens are stripped: a Delta table lives at ONE static folder
    (versioning is Delta's job, not per-run folders).

    Sinks are FULL-RELOAD via `overwrite: true` — the Delta sink's "Overwrite"
    table action replaces the existing table's METADATA and data each run, so
    THIS run's schema always wins. That both stops append-duplication on
    re-runs AND lets a column's type change between runs (e.g. order_item_id
    string -> integer once a cast is added, or a different project reusing the
    same silver/<entity> folder): a replaced table has no old schema to
    reconcile. Per the ADF Delta format docs the only sink table actions are
    `deltaTruncate` (keeps the old schema — type changes still fail) and
    `overwrite`; `recreate` is a SQL-sink property that Delta sinks silently
    IGNORE, which left the sink appending with schema merge and produced the
    "Failed to merge incompatible data types StringType and IntegerType"
    runtime failures. We deliberately do NOT set `mergeSchema`: merging tries
    to unify the new write with the existing table and fails on exactly that
    type change; overwrite makes the old table irrelevant."""
    loc = ds.location
    lines = ["  format: 'delta',"]
    if loc and loc.container:
        lines.append(f"  fileSystem: '{loc.container}',")
    if loc and loc.folder:
        folder = _PATH_PARAM.sub("", loc.folder).strip("/").replace("//", "/")
        if folder:
            lines.append(f"  folderPath: '{folder}',")
    if sink:
        lines.append("  overwrite: true,")
    return lines


def _source_block(
    stream_name: str,
    columns: "List[Column]",
    delta: "Dataset | None" = None,
) -> List[str]:
    """One ADF `source(...) ~> Stream` block with an optional inline schema."""
    schema = ",\n    ".join(f"{dsl_name(c.name)} as {c.type}" for c in columns)
    body = f"\n    {schema}\n  " if schema else ""
    lines = [
        f"source(output({body}),",
        "  allowSchemaDrift: true,",
        "  validateSchema: false,",
    ]
    if delta is not None:
        lines.extend(_delta_script_props(delta))
    lines.append(f"  ignoreNoFilesFound: false) ~> {stream_name}")
    return lines


def build_dataflow_script(df: DataFlow, datasets: "Dict[str, Dataset] | None" = None) -> str:
    """Produce the full ADF Mapping Data Flow script:

        source(...) ~> Src            (one per input source; joins have several)
        Src derive/filter/aggregate/sort/join ... ~> ...
        Last sink(...) ~> Sink

    Delta endpoints (dataset kind Delta) become INLINE datasets: their location
    is embedded in the script and no ADF dataset resource is referenced.
    """
    lines: List[str] = _source_block(
        df.source.stream_name, df.source_columns, _delta_ds(datasets, df.source.dataset)
    )
    # Extra sources (multi-source / join flows) each get their own source() block.
    for extra in df.additional_sources:
        lines.extend(
            _source_block(extra.stream_name, extra.columns, _delta_ds(datasets, extra.dataset))
        )

    current = df.source.stream_name
    prev_sort: "Transform | None" = None
    for t in _effective_transforms(df):
        lines.append(_transform_dsl(current, t, prev_sort))
        current = t.name
        if t.kind == TransformKind.sort:
            prev_sort = t

    sink_lines = [f"{current} sink(allowSchemaDrift: true,", "  validateSchema: false,"]
    sink_delta = _delta_ds(datasets, df.sink.dataset)
    if sink_delta is not None:
        sink_lines.extend(_delta_script_props(sink_delta, sink=True))
    sink_lines.append("  skipDuplicateMapInputs: true,")
    sink_lines.append(f"  skipDuplicateMapOutputs: true) ~> {df.sink.stream_name}")
    lines.append("\n".join(sink_lines))
    return "\n".join(lines)


def build_dataflow(
    df: DataFlow,
    datasets: "Dict[str, Dataset] | None" = None,
    ls_name_for: "Any | None" = None,
) -> Dict[str, Any]:
    def endpoint_entry(ep) -> Dict[str, Any]:
        delta = _delta_ds(datasets, ep.dataset)
        if delta is not None:
            # Inline dataset: reference the linked service, not a dataset resource.
            ls = ls_name_for(delta) if ls_name_for else delta.linked_service
            return {
                "name": ep.stream_name,
                "linkedService": {"referenceName": ls, "type": "LinkedServiceReference"},
            }
        return {
            "name": ep.stream_name,
            "dataset": {"referenceName": ep.dataset, "type": "DatasetReference"},
        }

    sources = [df.source] + list(df.additional_sources)
    return {
        "type": "MappingDataFlow",
        "typeProperties": {
            "sources": [endpoint_entry(s) for s in sources],
            "sinks": [endpoint_entry(df.sink)],
            "transformations": [{"name": t.name} for t in _effective_transforms(df)],
            "scriptLines": build_dataflow_script(df, datasets).split("\n"),
        },
    }


# --------------------------------------------------------------------------- #
# Pipelines
# --------------------------------------------------------------------------- #


def _depends_on(names: List[str]) -> List[Dict[str, Any]]:
    return [{"activity": n, "dependencyConditions": ["Succeeded"]} for n in names]


def _source_read_settings(ds: Dataset) -> "Dict[str, Any] | None":
    """Wildcard read settings for a Copy SOURCE whose dataset has no concrete file
    name — without this ADF rejects the Copy with FormatBasedDatasetMissingFileName.
    Returns None when the dataset already resolves to a file (concrete name or an
    HTTP relativeUrl), or for REST sources. Set only on the source side, so sinks
    are unaffected."""
    if ds.relative_url is not None:  # HTTP file: relativeUrl already names it
        return None
    if ds.kind in (DatasetKind.rest_resource, DatasetKind.azure_sql_table):
        return None
    loc = ds.location
    if loc and loc.file_name:  # concrete file name present
        return None
    settings: Dict[str, Any] = {
        "type": "AzureBlobStorageReadSettings" if _blob_mode() else "AzureBlobFSReadSettings",
        "recursive": True,
        "wildcardFileName": "*",
    }
    if loc and loc.folder:
        # Activity-scope setting: @{dataset().x} is not valid here, so a
        # parameterized folder ('orders/{execution_date}') becomes a wildcard
        # ('orders/*') — the read matches any per-run folder.
        settings["wildcardFolderPath"] = _PATH_PARAM.sub(
            lambda m: "*" if m.group(1) in set(ds.parameters) else m.group(0), loc.folder
        )
    return settings


def _build_context(spec: PipelineSpec) -> Dict[str, Any]:
    return {
        "dataset_kinds": {ds.name: ds.kind for ds in spec.datasets},
        "dataset_params": {ds.name: list(ds.parameters) for ds in spec.datasets},
        "pipeline_params": {pl.name: list(pl.parameters) for pl in spec.pipelines},
        "source_read_settings": {ds.name: _source_read_settings(ds) for ds in spec.datasets},
        # stream name -> dataset name per data flow, for datasetParameters binding.
        "dataflow_streams": {
            df.name: {
                ep.stream_name: ep.dataset
                for ep in (df.source, df.sink, *df.additional_sources)
            }
            for df in spec.dataflows
        },
    }


def _copy_input(dataset_name: str, ctx: Dict[str, Any], pipeline_params: List[str]) -> Dict[str, Any]:
    """Build the Copy input ref. Only bind dataset params that actually exist on the
    pipeline — binding to a missing pipeline param is what produced ADF's "No value
    provided for Parameter" failure. Unbound dataset params fall back to their own
    defaultValue."""
    ref: Dict[str, Any] = {"referenceName": dataset_name, "type": "DatasetReference"}
    params = ctx["dataset_params"].get(dataset_name, [])
    bound = {p: f"@pipeline().parameters.{p}" for p in params if p in pipeline_params}
    if bound:
        ref["parameters"] = bound
    return ref


# One managed IR shared by every ExecuteDataFlow. timeToLive keeps its Spark
# cluster alive between flows (and between runs, e.g. a self-heal retry), and
# cleanup=false lets later flows reuse it — otherwise every flow boots its own
# cluster (~5 minutes each) on AutoResolve.
# NOTE: IR names reject underscores (InvalidName) — unlike every other ADF
# object type, only letters/numbers/dashes are allowed.
DATAFLOW_IR_NAME = "ir-dataflow"


def _dataflow_ttl_minutes() -> int:
    """DATAFLOW_TTL_MINUTES env override; default 10. 0 disables the shared IR
    (flows fall back to AutoResolve — the slow path, but always valid)."""
    try:
        return max(0, int(os.environ.get("DATAFLOW_TTL_MINUTES", "10")))
    except ValueError:
        return 10


def build_dataflow_ir() -> Dict[str, Any]:
    return {
        "type": "Managed",
        "typeProperties": {
            "computeProperties": {
                "location": "AutoResolve",
                "dataFlowProperties": {
                    "computeType": "General",
                    "coreCount": 8,  # matches the ExecuteDataFlow compute tier
                    "timeToLive": _dataflow_ttl_minutes(),
                    "cleanup": False,  # quick re-use: keep the cluster across runs
                },
            },
        },
    }


_SUPPORTED_ACTIVITIES = {"Copy", "ExecuteDataFlow", "ExecutePipeline", "Validation", "GetMetadata"}


def unsupported_activities(spec: "PipelineSpec") -> List[str]:
    """Activities the builder will NOT render (they'd be silently skipped).
    Surfaced in preflight so a BRD requirement never vanishes without a trace."""
    out: List[str] = []
    for pl in spec.pipelines:
        for a in pl.activities:
            if a.type not in _SUPPORTED_ACTIVITIES:
                out.append(f"{pl.name}.{a.name} ({a.type})")
            elif a.type in ("Validation", "GetMetadata") and not (a.source_dataset or a.sink_dataset):
                out.append(f"{pl.name}.{a.name} ({a.type}: no dataset)")
    return out


def build_pipeline(pl: PipelineDef, ctx: "Dict[str, Any] | None" = None) -> Dict[str, Any]:
    ctx = ctx or {"dataset_kinds": {}, "dataset_params": {}, "pipeline_params": {}}
    pipeline_params = list(pl.parameters)
    activities: List[Dict[str, Any]] = []

    # Activities outside _SUPPORTED_ACTIVITIES (e.g. IfCondition, Delete) are
    # skipped, and dangling dependsOn references to them dropped so the pipeline
    # stays valid. unsupported_activities() reports the skips so they surface in
    # preflight instead of vanishing silently.
    def _renderable(a) -> bool:
        if a.type not in _SUPPORTED_ACTIVITIES:
            return False
        if a.type in ("Validation", "GetMetadata") and not (a.source_dataset or a.sink_dataset):
            return False  # nothing to validate against
        return True

    kept = {a.name for a in pl.activities if _renderable(a)}

    for act in pl.activities:
        if not _renderable(act):
            continue
        common: Dict[str, Any] = {
            "name": act.name,
            "dependsOn": _depends_on([d for d in act.depends_on if d in kept]),
            "policy": {"timeout": "0.12:00:00", "retry": 1, "retryIntervalInSeconds": 30},
            "userProperties": [],
        }

        if act.type == "Copy":
            src_kind = ctx["dataset_kinds"].get(act.source_dataset, DatasetKind.delimited_text)
            sink_kind = ctx["dataset_kinds"].get(act.sink_dataset, DatasetKind.delimited_text)
            if DatasetKind.delta in (src_kind, sink_kind):
                raise ValueError(
                    f"Copy activity '{act.name}' uses a Delta dataset — Delta is "
                    "supported only in Mapping Data Flows (inline), not Copy. Keep "
                    "bronze as CSV/Parquet/Binary and use Delta for silver/gold flows."
                )
            source_tp: Dict[str, Any] = {"type": _COPY_SOURCE_TYPE[src_kind]}
            read_settings = ctx.get("source_read_settings", {}).get(act.source_dataset)
            if read_settings:  # source dataset has no concrete file -> wildcard read
                source_tp["storeSettings"] = read_settings
            common.update(
                {
                    "type": "Copy",
                    # Bronze: byte-for-byte raw landing, idempotent overwrite.
                    "typeProperties": {
                        "source": source_tp,
                        "sink": {"type": _COPY_SINK_TYPE[sink_kind]},
                        "enableStaging": False,
                        "validateDataConsistency": False,
                    },
                    "inputs": [_copy_input(act.source_dataset, ctx, pipeline_params)],
                    "outputs": [
                        {"referenceName": act.sink_dataset, "type": "DatasetReference"}
                    ],
                }
            )
        elif act.type == "ExecuteDataFlow":
            df_ref: Dict[str, Any] = {
                "referenceName": act.data_flow,
                "type": "DataFlowReference",
            }
            # Thread pipeline parameters down to parameterized flow datasets
            # (stream -> {param: @pipeline().parameters.param}); without this the
            # datasets silently fall back to their build-date defaultValue.
            streams = ctx.get("dataflow_streams", {}).get(act.data_flow, {})
            ds_params: Dict[str, Any] = {}
            for stream, ds_name in streams.items():
                params = ctx["dataset_params"].get(ds_name, [])
                bound = {p: f"@pipeline().parameters.{p}" for p in params if p in pipeline_params}
                if bound:
                    ds_params[stream] = bound
            if ds_params:
                df_ref["datasetParameters"] = ds_params
            df_props: Dict[str, Any] = {
                "dataflow": df_ref,
                # smallest General Purpose tier (cost)
                "compute": {"computeType": "General", "coreCount": 8},
                "traceLevel": "Fine",
            }
            if _dataflow_ttl_minutes() > 0:
                # Shared cluster: without this every flow runs on AutoResolve
                # and boots its own Spark cluster (~5 min each) — a 5-flow
                # medallion spent ~25 min just on cluster startup.
                df_props["integrationRuntime"] = {
                    "referenceName": DATAFLOW_IR_NAME,
                    "type": "IntegrationRuntimeReference",
                }
            common.update({"type": "ExecuteDataFlow", "typeProperties": df_props})
        elif act.type in ("Validation", "GetMetadata"):
            # BRD "verify the file exists / is not empty before processing" asks.
            # Both take one dataset reference; the LLM puts it in source_dataset.
            ds_name = act.source_dataset or act.sink_dataset
            ds_ref = _copy_input(ds_name, ctx, pipeline_params)
            if act.type == "Validation":
                # Waits for the file to exist AND have >=1 byte, then proceeds;
                # times out (fails, blocking downstream) when it never appears.
                common.pop("policy", None)  # control activity: no retry policy
                common.update(
                    {
                        "type": "Validation",
                        "typeProperties": {
                            "dataset": ds_ref,
                            "timeout": "0.00:10:00",
                            "sleep": 30,
                            "minimumSize": 1,
                        },
                    }
                )
            else:
                common.update(
                    {
                        "type": "GetMetadata",
                        "typeProperties": {
                            "dataset": ds_ref,
                            "fieldList": ["exists", "size"],
                        },
                    }
                )
        elif act.type == "ExecutePipeline":
            common.update(
                {
                    "type": "ExecutePipeline",
                    "typeProperties": {
                        "pipeline": {"referenceName": act.invoked_pipeline, "type": "PipelineReference"},
                        "waitOnCompletion": True,
                    },
                }
            )
            # Forward every shared parameter down to the child pipeline (the master
            # passes its params to the children that declare the same name).
            child_params = ctx["pipeline_params"].get(act.invoked_pipeline, [])
            shared = [p for p in child_params if p in pipeline_params]
            if shared:
                common["typeProperties"]["parameters"] = {
                    p: f"@pipeline().parameters.{p}" for p in shared
                }

        activities.append(common)

    props: Dict[str, Any] = {"activities": activities}
    if pl.parameters:
        props["parameters"] = _param_defs(list(pl.parameters))
    return props


# --------------------------------------------------------------------------- #
# Triggers
# --------------------------------------------------------------------------- #


def _normalize_start_time(value: "str | None") -> str:
    """Coerce whatever the LLM emits into a valid ADF ISO-8601 UTC startTime.

    ADF's ScheduleTrigger REQUIRES a full datetime startTime; the LLM often emits a
    bare time ('02:00'), a date, or nothing, which ADF rejects
    (MissingStartTimeInScheduleTriggerDefinition / bad format). Normalize:
      * 'HH:MM'        -> today at that time, UTC
      * 'YYYY-MM-DD'   -> that date at midnight, UTC
      * ISO datetime   -> as-is (ensure a trailing Z)
      * missing/other  -> a fixed valid default
    """
    import re

    default = "2024-01-01T00:00:00Z"
    if not value:
        return default
    v = value.strip()
    if "T" in v:  # already a datetime
        return v if (v.endswith("Z") or "+" in v) else v + "Z"
    m = re.match(r"^(\d{1,2}):(\d{2})$", v)
    if m:
        return f"{_BUILD_DATE}T{int(m.group(1)):02d}:{m.group(2)}:00Z"
    if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
        return f"{v}T00:00:00Z"
    return default


def build_trigger(tr: Trigger) -> Dict[str, Any]:
    # Only ScheduleTrigger is a deployable ARM resource here. Manual triggers are
    # filtered out before deploy (see build_all) because ADF has no "manual trigger"
    # resource — a pipeline is simply run on demand. This branch stays defensive.
    return {
        "type": "ScheduleTrigger",
        "typeProperties": {
            "recurrence": {
                "frequency": tr.frequency or "Day",
                "interval": tr.interval or 1,
                "startTime": _normalize_start_time(tr.start_time),
                "timeZone": "UTC",
            }
        },
        "pipelines": [
            {
                "pipelineReference": {"referenceName": tr.pipeline, "type": "PipelineReference"},
                "parameters": {},
            }
        ],
    }


# --------------------------------------------------------------------------- #
# Whole-spec assembly + persistence
# --------------------------------------------------------------------------- #


def build_all(spec: PipelineSpec) -> Dict[str, Dict[str, Dict[str, Any]]]:
    """Return {category: {name: properties_json}} for the whole spec.

    Azure storage linked services are collapsed to ONE canonical LS PER ACCOUNT and
    every storage-backed dataset is repointed at the canonical LS for its account —
    see :func:`_canonical_storage_by_account`. Single-account specs collapse to one LS
    exactly as before; multi-account specs keep one LS per distinct account."""
    ctx = _build_context(spec)

    storage_ls = {ls.name: ls for ls in spec.linked_services if ls.type in _AZURE_STORAGE_TYPES}
    storage_ls_names = set(storage_ls)
    # One canonical storage LS per distinct account (empty when the spec named none).
    canon_by_key = _canonical_storage_by_account(spec)
    # A dataset lives in storage when it has a `location` (vs an HTTP relative_url).
    needs_storage = storage_ls_names or any(ds.location is not None for ds in spec.datasets)
    # Fallback account bucket for storage datasets whose LS is missing/unknown: prefer
    # the deploy-account canonical, else the first canonical, else a synthetic LS.
    if _DEPLOY_ACCT_KEY in canon_by_key:
        default_canonical = canon_by_key[_DEPLOY_ACCT_KEY]
    elif canon_by_key:
        default_canonical = next(iter(canon_by_key.values()))
    else:
        default_canonical = _synthetic_storage_ls()
        if needs_storage:
            canon_by_key[_DEPLOY_ACCT_KEY] = default_canonical

    def _canonical_for_dataset(ds: Dataset) -> LinkedService:
        """The canonical storage LS for the account this dataset's LS targets."""
        ls = storage_ls.get(ds.linked_service)
        if ls is not None:
            return canon_by_key.get(_ls_account_key(ls), default_canonical)
        return default_canonical

    # Linked services: keep every non-storage LS (HttpServer/RestService) as-is, and
    # emit exactly ONE storage LS per distinct account when any storage dataset exists.
    linked: Dict[str, Any] = {
        ls.name: build_linked_service(ls)
        for ls in spec.linked_services
        if ls.type not in _AZURE_STORAGE_TYPES
    }
    if needs_storage:
        for canonical in canon_by_key.values():
            linked[canonical.name] = build_linked_service(canonical)

    def _dataset(ds: Dataset) -> Dict[str, Any]:
        built = build_dataset(ds)
        # Repoint storage-backed datasets at the canonical storage LS for their account
        # so no dataset can reference a missing or .dfs/.blob-mismatched linked service.
        if ds.location is not None or ds.linked_service in storage_ls_names:
            built["linkedServiceName"]["referenceName"] = _canonical_for_dataset(ds).name
        return built

    ds_map = {ds.name: ds for ds in spec.datasets}

    def _ls_name_for(ds: Dataset) -> str:
        return _canonical_for_dataset(ds).name

    return {
        # The shared data-flow IR (skipped when the spec has no flows or TTL=0).
        "integrationRuntimes": (
            {DATAFLOW_IR_NAME: build_dataflow_ir()}
            if spec.dataflows and _dataflow_ttl_minutes() > 0
            else {}
        ),
        "linkedServices": linked,
        # Delta datasets are inline-only (no ADF dataset resource) — not built.
        "datasets": {
            ds.name: _dataset(ds) for ds in spec.datasets if ds.kind != DatasetKind.delta
        },
        "dataflows": {
            df.name: build_dataflow(df, ds_map, _ls_name_for) for df in spec.dataflows
        },
        "pipelines": {pl.name: build_pipeline(pl, ctx) for pl in spec.pipelines},
        # Only schedule triggers are ARM resources. Manual triggers are not deployed —
        # ADF has no manual-trigger resource; the pipeline is run on demand instead.
        "triggers": {
            tr.name: build_trigger(tr)
            for tr in spec.triggers
            if tr.kind == TriggerKind.schedule
        },
    }


# Deploy order: IR -> linked services -> datasets -> data flows -> pipelines ->
# triggers. The IR goes first because pipelines' ExecuteDataFlow activities
# reference it by name.
DEPLOY_ORDER = ["integrationRuntimes", "linkedServices", "datasets", "dataflows", "pipelines", "triggers"]


def artifacts_dir(run_id: str, root: str = "artifacts") -> str:
    return os.path.join(root, run_id)


def write_artifacts(spec: PipelineSpec, run_id: str, root: str = "artifacts") -> str:
    """Write every ADF object as JSON under artifacts/<run_id>/<category>/<name>.json,
    plus readable data-flow scripts under dataflows/<name>.script and the validated
    spec as spec.json. Returns the run directory path."""
    run_dir = artifacts_dir(run_id, root)
    os.makedirs(run_dir, exist_ok=True)

    built = build_all(spec)
    for category, objects in built.items():
        cat_dir = os.path.join(run_dir, category)
        os.makedirs(cat_dir, exist_ok=True)
        for name, props in objects.items():
            with open(os.path.join(cat_dir, f"{name}.json"), "w", encoding="utf-8") as fh:
                json.dump({"name": name, "properties": props}, fh, indent=2)

    ds_map = {ds.name: ds for ds in spec.datasets}
    for df in spec.dataflows:
        with open(
            os.path.join(run_dir, "dataflows", f"{df.name}.script"), "w", encoding="utf-8"
        ) as fh:
            fh.write(build_dataflow_script(df, ds_map))

    with open(os.path.join(run_dir, "spec.json"), "w", encoding="utf-8") as fh:
        fh.write(spec.model_dump_json(indent=2))

    return run_dir
