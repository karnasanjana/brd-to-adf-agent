"""Schema grounding: stamp REAL column schemas into the spec, so the LLM never
has to invent (or remember to include) a column list — the #1 model-dependent
failure (a spec with columns: [] everywhere builds `source(output())` and dies
at runtime with DF-EXPR-010).

Ground truth comes from the data itself, not the model:

  1. For every DelimitedText dataset that points at an EXISTING file, read the
     first bytes from storage (MOCK: mock_tenant/, REAL: ADLS/Blob) and take the
     header row as the schema.
  2. Propagate along Copy activities (a same-format copy preserves the schema),
     so bronze gets landing's schema before any file exists in bronze.
  3. Propagate THROUGH data flows by simulating each transform's effect on the
     column set (derive adds, aggregate replaces with groupBy+aggs, select maps),
     so a silver Delta dataset gets its schema and the gold flow reading it is
     grounded too.

Grounding must NEVER block planning: any read/auth failure just skips that
dataset (the spec-level `_infer_source_columns` fallback still applies) and the
plan proceeds. Every grounded schema is reported as a note for the plan review.
"""

from __future__ import annotations

import csv
import functools
import io
import os
import re
from typing import Dict, List, Optional

from config import MOCK_TENANT_DIR, mock_mode
from models.logical import LogicalColumn, LogicalPlan, SourceFormat
from models.spec import Column, DataFlow, DatasetKind, PipelineSpec, TransformKind

# How much of the file to fetch — plenty for any header row.
_HEAD_BYTES = 64 * 1024


# --------------------------------------------------------------------------- #
# Reading the head of a file (MOCK / Blob / ADLS)
# --------------------------------------------------------------------------- #


def _join_path(folder: Optional[str], file_name: str) -> str:
    folder = (folder or "").strip("/")
    return f"{folder}/{file_name}" if folder else file_name


def _read_head_mock(container: str, path: str) -> Optional[bytes]:
    full = os.path.join(MOCK_TENANT_DIR, "storage", container, *path.split("/"))
    if not os.path.isfile(full):
        return None
    with open(full, "rb") as fh:
        return fh.read(_HEAD_BYTES)


@functools.lru_cache(maxsize=1)
def _storage_service_client():
    """ONE storage service client (and therefore one DefaultAzureCredential, one
    token acquisition) for the whole process. Building it per file turned every
    header read into a fresh Entra token round-trip — grounding a handful of
    datasets cost seconds. The client is thread-safe and refreshes its own token."""
    from config import AzureConfig, get_blob_service_client, get_datalake_client, storage_kind

    cfg = AzureConfig.from_env()
    return get_blob_service_client(cfg) if storage_kind() == "blob" else get_datalake_client(cfg)


def _read_head_real(container: str, path: str) -> Optional[bytes]:
    from config import storage_kind

    service = _storage_service_client()
    if storage_kind() == "blob":
        client = service.get_blob_client(container, path)
        return client.download_blob(offset=0, length=_HEAD_BYTES).readall()
    client = service.get_file_system_client(container).get_file_client(path)
    return client.download_file(offset=0, length=_HEAD_BYTES).readall()


# (container, path) -> header bytes, REAL storage only. A file's header does not
# change within a planning session, and /plan + every /chat refine re-ground the
# same datasets, so each remote read is paid once. Misses are cached too: an
# absent bronze/silver file stays absent until the deploy that creates it.
# MOCK reads hit the local filesystem (microseconds) and tests write files
# mid-run, so they are never cached.
_HEAD_CACHE: "Dict[tuple, Optional[bytes]]" = {}


def clear_caches() -> None:
    """Drop memoized heads + the storage client — call after a deploy writes
    files that grounding previously saw as absent."""
    _HEAD_CACHE.clear()
    _storage_service_client.cache_clear()


def _read_head(container: str, path: str) -> Optional[bytes]:
    if mock_mode():
        try:
            return _read_head_mock(container, path)
        except Exception:  # noqa: BLE001
            return None
    key = (container, path)
    if key in _HEAD_CACHE:
        return _HEAD_CACHE[key]
    try:
        head = _read_head_real(container, path)
    except Exception:  # noqa: BLE001 — missing file / no data-plane role: skip
        head = None
    _HEAD_CACHE[key] = head
    return head


def _header_columns(data: bytes) -> List[str]:
    """Column names from the first CSV row (handles quoted names with commas)."""
    text = data.decode("utf-8-sig", errors="replace")
    first_line = text.splitlines()[0] if text else ""
    if not first_line:
        return []
    names = [c.strip() for c in next(csv.reader(io.StringIO(first_line)))]
    return names if all(names) else []  # a blank header cell -> not a header row


# --------------------------------------------------------------------------- #
# Type inference from sampled data rows
# --------------------------------------------------------------------------- #

_INT_RE = re.compile(r"[+-]?\d+")
_FLOAT_RE = re.compile(r"[+-]?(\d+\.\d*|\.\d+|\d+)([eE][+-]?\d+)?")
_BOOL_VALUES = {"true", "false", "yes", "no"}
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
_TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d+)?)?")

_MAX_SAMPLE_ROWS = 200


def _classify(value: str) -> str:
    v = value.strip()
    if not v:
        return "empty"
    if v.lower() in _BOOL_VALUES:
        return "boolean"
    if _INT_RE.fullmatch(v):
        return "long" if abs(int(v)) > 2**31 - 1 else "integer"
    if _FLOAT_RE.fullmatch(v):
        return "double"
    if _TS_RE.fullmatch(v):
        return "timestamp"
    if _DATE_RE.fullmatch(v):
        return "date"
    return "string"


# What a column of mixed observations resolves to (order matters: any string
# observation wins; int+double -> double; date+timestamp -> timestamp).
def _merge(a: str, b: str) -> str:
    if a == b:
        return a
    pair = {a, b}
    if pair <= {"integer", "long"}:
        return "long"
    if pair <= {"integer", "long", "double"}:
        return "double"
    if pair <= {"date", "timestamp"}:
        return "timestamp"
    return "string"


def _infer_column_types(data: bytes, names: List[str]) -> Dict[str, str]:
    """Infer each column's type from the sampled data rows (the bytes already
    fetched for the header). All-empty or unparseable columns stay string.
    The last (possibly truncated) row of the sample is discarded."""
    try:
        text = data.decode("utf-8-sig", errors="replace")
        rows = list(csv.reader(io.StringIO(text)))
    except Exception:  # noqa: BLE001 — inference is best-effort
        return {}
    rows = rows[1:]  # drop header
    if len(rows) > 1:
        rows = rows[:-1]  # drop the truncated tail of the 64KB head
    types: Dict[str, str] = {}
    for row in rows[:_MAX_SAMPLE_ROWS]:
        for name, value in zip(names, row):
            kind = _classify(value)
            if kind == "empty":
                continue
            types[name] = _merge(types[name], kind) if name in types else kind
    return {n: t for n, t in types.items() if t != "string"}


# --------------------------------------------------------------------------- #
# Propagating a schema through a data flow's transforms
# --------------------------------------------------------------------------- #

# Cast-function -> resulting column type; anything unrecognized stays string.
_EXPR_TYPE = {
    "toTimestamp": "timestamp",
    "toDate": "date",
    "toInteger": "integer",
    "toLong": "long",
    "toDouble": "double",
    "toDecimal": "double",
    "toFloat": "double",
    "toBoolean": "boolean",
    "count": "long",
    "countDistinct": "long",
    "sum": "double",
    "avg": "double",
    "mean": "double",
    "min": "string",
    "max": "string",
}

_ARITH_EXPR = re.compile(r"^\((.+)\)$")
_ARITH_SPLIT = re.compile(r"\s+[+\-*/]\s+")
_NUMERIC_TYPES = {"integer", "int", "long", "bigint", "double", "float", "decimal", "short"}
_NUMERIC_LITERAL = re.compile(r"[+-]?\d+(\.\d+)?")


def _is_numeric_type(t: str) -> bool:
    t = (t or "").strip().lower()
    return t in _NUMERIC_TYPES or t.startswith("decimal")


def _split_args(s: str) -> "List[str]":
    """Split a function's argument list on TOP-LEVEL commas (respects nested
    parens and single-quoted strings)."""
    out: "List[str]" = []
    cur, depth, quoted = "", 0, False
    for ch in s:
        if ch == "'":
            quoted = not quoted
        if not quoted:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                out.append(cur.strip())
                cur = ""
                continue
        cur += ch
    if cur.strip():
        out.append(cur.strip())
    return out


def _expr_type(expression: str, cols: Dict[str, str]) -> str:
    expr = (expression or "").strip()
    if expr.startswith("{") and expr.endswith("}"):
        expr = expr[1:-1]  # brace-escaped column ref ({Country Name})
    if expr in cols:  # bare column reference keeps its type
        return cols[expr]
    # Structured arithmetic renders as "(a * b)" / "(1 - discount)". Preserve
    # that numeric result through grounded schemas so downstream gold flows do
    # not see derived measures like line_total as string and fail sum().
    m = _ARITH_EXPR.match(expr)
    if m:
        operands = [p.strip() for p in _ARITH_SPLIT.split(m.group(1)) if p.strip()]
        if operands and all(
            _NUMERIC_LITERAL.fullmatch(o)
            or (o in cols and _is_numeric_type(cols[o]))
            for o in operands
        ):
            return "double"
    fn, _, rest = expr.partition("(")
    fn = fn.strip()
    if fn in {"min", "max", "first"} and rest.endswith(")"):
        arg = rest[:-1].strip()
        if arg in cols:
            return cols[arg]
    # coalesce keeps its column argument's type; iif on a same-column clamp
    # ('replace negative quantity with 0') mirrors its column/otherwise branch.
    # Without this, 'cast to integer then coalesce(col, 0)' came out STRING and
    # a grounded gold join failed with a false type mismatch (bike-store run
    # 5ee8ff8cc21a: products@category_id string vs categories@category_id int).
    if fn in {"coalesce", "iif"} and rest.endswith(")"):
        args = _split_args(rest[:-1])
        candidates = args if fn == "coalesce" else list(reversed(args))
        for a in candidates:
            if a in cols:
                return cols[a]
        for a in candidates:
            if _NUMERIC_LITERAL.fullmatch(a):
                return "double"
    return _EXPR_TYPE.get(fn, "string")


def ground_logical_plan(plan: LogicalPlan) -> List[str]:
    """Stamp source schemas into a LogicalPlan before completeness.check().

    This is the earliest deterministic grounding point: if the BRD names a CSV
    file already present in storage/MOCK, read its header and sampled rows, then
    replace the model's source columns with the real columns/types. Best-effort:
    unreadable files leave the plan unchanged.
    """
    notes: List[str] = []
    for src in plan.sources:
        if src.format != SourceFormat.csv:
            continue
        if not src.file_name:
            continue
        container = src.container or "landing"
        head = _read_head(container, _join_path(src.folder, src.file_name))
        if not head:
            continue
        names = _header_columns(head)
        if not names:
            continue
        inferred = _infer_column_types(head, names)
        src.columns = [
            LogicalColumn(name=n, type=inferred.get(n, "string")) for n in names
        ]
        typed = [f"{n}:{t}" for n, t in inferred.items()]
        note = (
            f"Grounded LogicalPlan source '{src.entity}' from "
            f"{container}/{_join_path(src.folder, src.file_name)} "
            f"({len(names)} columns: {', '.join(names[:8])}"
            f"{'…' if len(names) > 8 else ''}"
            + (f"; inferred types: {', '.join(typed[:8])}" if typed else "")
            + ")"
        )
        if note not in plan.assumptions:
            plan.assumptions.append(note)
        notes.append(note)

    # LOUD when validation is blind: an entity used by transforms/outputs whose
    # schema could NOT be verified (non-CSV source, missing/unreadable file)
    # skips every column check — the model's column list is a guess. One note
    # per entity so the plan review shows exactly what is unverified.
    used = {t.entity for t in plan.transforms} | {
        e for o in plan.outputs for e in o.from_entities
    }
    for src in plan.sources:
        if src.entity in used and not src.columns and src.format != SourceFormat.binary:
            note = (
                f"schema for '{src.entity}' could not be verified against real "
                "data — its column references are UNCHECKED until the source "
                "is readable"
            )
            if note not in plan.assumptions:
                plan.assumptions.append(note)
            notes.append(note)
    return notes


def _flow_output_columns(df: DataFlow, source_cols: List[Column]) -> List[Column]:
    """Simulate the transform chain on the source schema and return the SINK
    schema. Deterministic mirror of what ADF does at runtime."""
    cols: Dict[str, str] = {c.name: c.type for c in source_cols}
    extra = {ep.stream_name: ep.columns for ep in df.additional_sources}
    for t in df.transforms:
        if t.kind == TransformKind.join and t.right_stream in extra:
            for c in extra[t.right_stream]:
                cols.setdefault(c.name, c.type)
        elif t.kind in (TransformKind.cast, TransformKind.derived):
            for c in t.columns:
                cols[c.name] = _expr_type(c.expression, cols)
        elif t.kind == TransformKind.aggregate:
            # Mirror the builder's collision guard: agg columns named like a
            # groupBy column are dropped; if ALL of them collide (pure dedup),
            # the builder emits an each()/first() pattern that keeps every column.
            # A computed key ('Year = year(ts)') outputs column Year with the
            # expression's type.
            keys = [
                (g.split("=", 1)[0].strip(), g.split("=", 1)[1].strip())
                if "=" in g else (g, None)
                for g in t.group_by
            ]
            key_names = [n for n, _ in keys]
            agg_cols = [c for c in t.columns if c.name not in key_names]
            if not agg_cols and t.group_by:
                pass  # dedup keeps the schema unchanged
            else:
                new = {
                    n: (_expr_type(expr, cols) if expr else cols.get(n, "string"))
                    for n, expr in keys
                }
                for c in agg_cols:
                    new[c.name] = _expr_type(c.expression, cols)
                cols = new
        elif t.kind == TransformKind.select and t.columns:
            cols = {c.name: _expr_type(c.expression, cols) for c in t.columns}
        elif t.kind == TransformKind.window:
            # window APPENDS its output columns (row_num = rowNumber(), ...)
            for c in t.columns:
                cols[c.name] = _expr_type(c.expression, cols)
        # filter / sort keep the schema unchanged
    return [Column(name=n, type=ty) for n, ty in cols.items()]


def _location_key(ds) -> "Optional[tuple]":
    """Identity of the physical data a dataset points at. LLMs routinely invent
    ALIAS datasets (e.g. ds_bronze_customers_csv_for_silver next to
    ds_bronze_olist_customers) for the same files — same location, different
    name. Schema belongs to the location, so aliases must share it."""
    loc = ds.location
    if loc is None or not loc.container:
        return None
    return (
        ds.kind,
        loc.container.strip().lower(),
        (loc.folder or "").strip("/").lower(),
        (loc.file_name or "").strip().lower(),
    )


def _stamp_aliases(spec: PipelineSpec, grounded: Dict[str, List[Column]], notes: List[str]) -> None:
    """Copy each grounded schema onto every ungrounded dataset with the SAME
    location (and kind) — name-based propagation can't see aliases."""
    by_loc: Dict[tuple, tuple] = {}
    for ds in spec.datasets:
        key = _location_key(ds)
        if key is not None and ds.name in grounded and key not in by_loc:
            by_loc[key] = (ds.name, grounded[ds.name])
    for ds in spec.datasets:
        if ds.name in grounded:
            continue
        key = _location_key(ds)
        hit = by_loc.get(key) if key is not None else None
        if hit is None:
            continue
        src_name, cols = hit
        ds.columns = list(cols)
        grounded[ds.name] = ds.columns
        notes.append(f"Grounded: {ds.name} schema shared from {src_name} (same location)")


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #


def ground_spec(spec: PipelineSpec) -> List[str]:
    """Stamp ground-truth schemas into the spec IN PLACE. Returns human-readable
    notes ("Grounded: ...") for the plan review; empty when nothing was readable.

    Grounded schemas OVERRIDE whatever the LLM put in the spec — the file header
    is the truth, model output is a guess. Datasets whose file can't be read keep
    their spec/inferred columns untouched.
    """
    notes: List[str] = []
    grounded: Dict[str, List[Column]] = {}  # dataset name -> ground-truth schema

    # 1) Datasets whose file exists NOW (typically landing).
    for ds in spec.datasets:
        if ds.kind != DatasetKind.delimited_text or not ds.first_row_as_header:
            continue
        loc = ds.location
        if loc is None or not loc.file_name:
            continue
        head = _read_head(loc.container, _join_path(loc.folder, loc.file_name))
        if not head:
            continue
        names = _header_columns(head)
        if not names:
            continue
        # Types inferred from the sampled data rows, not guessed by the model —
        # a column stays string unless every non-empty observation agrees.
        inferred = _infer_column_types(head, names)
        ds.columns = [Column(name=n, type=inferred.get(n, "string")) for n in names]
        grounded[ds.name] = ds.columns
        typed = [f"{n}:{t}" for n, t in inferred.items()]
        notes.append(
            f"Grounded: {ds.name} schema read from "
            f"{loc.container}/{_join_path(loc.folder, loc.file_name)} "
            f"({len(names)} columns: {', '.join(names[:8])}{'…' if len(names) > 8 else ''}"
            + (f"; inferred types: {', '.join(typed[:8])}" if typed else "")
            + ")"
        )

    if not grounded:
        return notes

    _stamp_aliases(spec, grounded, notes)

    # 2) Same-format Copy preserves the schema: source grounded -> sink grounded.
    for pl in spec.pipelines:
        for act in pl.activities:
            if act.type != "Copy":
                continue
            src = grounded.get(act.source_dataset or "")
            snk = next((d for d in spec.datasets if d.name == act.sink_dataset), None)
            if src and snk is not None and snk.name not in grounded:
                snk.columns = list(src)
                grounded[snk.name] = snk.columns
                notes.append(f"Grounded: {snk.name} schema propagated from {act.source_dataset}")

    _stamp_aliases(spec, grounded, notes)

    # 3) Data flows: ground the projection, then derive the sink dataset's schema.
    # Two passes so a gold flow reading a silver dataset grounded by the first
    # pass gets picked up. Alias stamping after each pass so a gold flow reading
    # ds_silver_x_delta_for_gold (alias of the grounded silver sink) resolves.
    for _ in range(2):
        for df in spec.dataflows:
            src = grounded.get(df.source.dataset)
            if not src:
                continue
            if [c.name for c in df.source_columns] != [c.name for c in src]:
                df.source_columns = list(src)
                notes.append(f"Grounded: {df.name} source projection set from {df.source.dataset}")
            for ep in df.additional_sources:
                if ep.dataset in grounded and not ep.columns:
                    ep.columns = list(grounded[ep.dataset])
            sink_name = df.sink.dataset
            if sink_name not in grounded:
                out = _flow_output_columns(df, src)
                snk_ds = next((d for d in spec.datasets if d.name == sink_name), None)
                if snk_ds is not None:
                    snk_ds.columns = out
                grounded[sink_name] = out
        _stamp_aliases(spec, grounded, notes)

    return notes
