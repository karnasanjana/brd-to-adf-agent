"""Deterministic expression/type checker for data flows.

The exact discipline a careful engineer follows before emitting a Mapping Data
Flow, done in code, before anything is considered complete:

  1. take the source schema (grounded from real data when readable),
  2. walk the transform chain IN ORDER, tracking every column's type through
     casts / derived columns / aggregates / selects,
  3. validate each structured op against the types it will actually receive —
     arithmetic on an uncast string column, coalesce() mixing a string column
     with a numeric default, date-part ops on non-temporal columns, sum/avg on
     text, numeric comparisons against string columns.

Failures are plain-English, actionable lines ("cast X to a numeric type
first") surfaced as a BLOCKING preflight check — the /plan repair loop feeds
them straight back to the model, which fixes its own plan in seconds instead
of ADF failing with DF-DT-001 minutes into a run.

Only STRUCTURED ops (op/column/args, predicates) are deep-checked; raw escape-
hatch expressions just propagate their result type (no false positives). A
flow whose primary source has no schema is skipped — there is nothing to check
against.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional

from models.spec import (
    DataFlow,
    DerivedColumn,
    PipelineSpec,
    Transform,
    TransformKind,
    _OP_ARITH,
    _OP_BLANK_DEFAULT,
    _OP_REMOVE_SPECIAL,
    _norm_op,
)

_NUMERIC_TYPES = {"integer", "int", "long", "bigint", "double", "float", "decimal", "short"}
_TEMPORAL_TYPES = {"date", "timestamp", "datetime"}
_NUM_LIT = re.compile(r"-?\d+(\.\d+)?")

_DATE_PART_OPS = {
    "year", "month", "quarter", "dayofmonth", "day", "dayofweek", "dayofyear",
    "hour", "minute", "second", "weekofyear", "week",
}
_NUMERIC_FN_OPS = {"round", "abs", "floor", "ceil"}
_NUMERIC_AGG_OPS = {"sum", "avg", "mean"}
_STRING_FN_OPS = {
    "trim", "upper", "lower", "proper", "propercase", "initcap",
    "titlecase", "title", "replace", "concat", "substring",
    "regexreplace", "regexmatch", "rlike", "startswith", "endswith",
    *_OP_REMOVE_SPECIAL, *_OP_BLANK_DEFAULT,
}

# result type of a cast/renderer function (mirrors grounding._EXPR_TYPE, kept
# here for op-level lookups).
_FN_RESULT = {
    "totimestamp": "timestamp", "todate": "date", "tointeger": "integer",
    "tolong": "long", "todouble": "double", "todecimal": "decimal",
    "toboolean": "boolean", "tostring": "string",
    "count": "long", "countdistinct": "long", "sum": "double", "avg": "double",
    "mean": "double", "length": "integer", "rownumber": "long", "rank": "long",
    "denserank": "long", "round": "double", "abs": "double", "floor": "long",
    "ceil": "long",
}


def _is_numeric(t: "Optional[str]") -> bool:
    t = (t or "").strip().lower()
    return t in _NUMERIC_TYPES or t.startswith("decimal")


def _is_temporal(t: "Optional[str]") -> bool:
    return (t or "").strip().lower() in _TEMPORAL_TYPES


def _expr_result_type(expression: "Optional[str]", cols: Dict[str, str]) -> str:
    """Result type of a rendered/raw expression: bare column ref keeps its type,
    otherwise the leading function decides; unknown stays string."""
    expr = (expression or "").strip()
    if expr.startswith("{") and expr.endswith("}"):
        expr = expr[1:-1]  # brace-escaped column ref ({Country Name})
    if expr in cols:
        return cols[expr]
    fn = _norm_op(expr.split("(", 1)[0])
    return _FN_RESULT.get(fn, "string")


def _select_ref(c: DerivedColumn) -> str:
    s = (c.expression or c.name).strip()
    if s.startswith("{") and s.endswith("}"):
        s = s[1:-1]  # brace-escaped column ref ({Country Name})
    return s


def _op_result_type(key: str, column: "Optional[str]", cols: Dict[str, str]) -> str:
    if key in _OP_ARITH:
        return "double"
    if key in _DATE_PART_OPS:
        return "integer"
    if key == "coalesce":
        return cols.get(column or "", "string")
    if key in _FN_RESULT:
        return _FN_RESULT[key]
    if key in ("trim", "upper", "lower", "proper", "propercase", "initcap",
               "titlecase", "title", "replace", "concat", "substring",
               "regexreplace", "regexmatch", "rlike", "startswith", "endswith",
               *_OP_REMOVE_SPECIAL, *_OP_BLANK_DEFAULT):
        return "string"
    if key in ("min", "max", "first", "identity", "copy", "passthrough", "asis", "none"):
        return cols.get(column or "", "string")
    if key == "case":
        # Branch values can be any type (numeric clamp vs. string bucket) — no
        # single result type is provable here without carrying the branches
        # through. Inherit the tested column's type: right for same-type
        # replacement (the common case, e.g. clamp negative quantity to 0),
        # a safe best-effort guess otherwise.
        return cols.get(column or "", "string")
    return "string"


_QUOTED_TOKEN = re.compile(r"'([A-Za-z_]\w*)'")


def _check_literal_shadows_column(
    flow: str, transform: str, c: DerivedColumn, cols: Dict[str, str], problems: List[str]
) -> None:
    """A quoted string literal that exactly matches an available column name is
    almost surely a mis-quoted column reference — concat(first_name, ' ',
    'last_name') writes the TEXT last_name into every row and no other check
    catches it (it is type-valid). Flag it before it ships."""
    texts = [c.expression or ""] + [str(a) for a in c.args]
    for text in texts:
        for token in _QUOTED_TOKEN.findall(text):
            if token in cols:
                problems.append(
                    f"{flow}/{transform}: string literal '{token}' matches the "
                    f"column '{token}' — if you meant the column, remove the "
                    "quotes; a literal identical to a column name is almost "
                    "always a mistake"
                )


def _check_structured_op(
    flow: str, transform: str, c: DerivedColumn, cols: Dict[str, str], problems: List[str]
) -> None:
    """Type-validate one structured derived column against the CURRENT column
    types. Unknown columns are skipped (completeness/grounding own existence)."""
    key = _norm_op(c.op or "")
    src = c.column or c.name

    if key in _OP_ARITH:
        for operand in [src, *[str(a) for a in c.args]]:
            operand = operand.strip()
            if _NUM_LIT.fullmatch(operand):
                continue
            ty = cols.get(operand)
            if ty is None:
                problems.append(
                    f"{flow}/{transform}: '{c.name}' does {c.op} on '{operand}', "
                    "but that column is not available at this point in the flow"
                )
            elif not _is_numeric(ty):
                problems.append(
                    f"{flow}/{transform}: '{c.name}' does {c.op} on '{operand}' "
                    f"which is {ty} — cast '{operand}' to a numeric type (add it "
                    f"to casts) before the arithmetic"
                )
    elif key in _DATE_PART_OPS:
        ty = cols.get(src)
        if ty is not None and not _is_temporal(ty):
            problems.append(
                f"{flow}/{transform}: '{c.name}' takes {c.op}() of '{src}' which "
                f"is {ty} — cast '{src}' to timestamp/date first"
            )
    elif key == "coalesce":
        ty = cols.get(src)
        if ty is None:
            return
        for a in c.args:
            a = str(a).strip()
            arg_numeric = bool(_NUM_LIT.fullmatch(a))
            if _is_numeric(ty) and not arg_numeric and a not in cols:
                problems.append(
                    f"{flow}/{transform}: coalesce('{src}', {a!r}) mixes numeric "
                    f"column ({ty}) with a non-numeric default — use a numeric "
                    f"default, or iif(isNull({src}), <typed default>, {src})"
                )
            elif not _is_numeric(ty) and arg_numeric:
                problems.append(
                    f"{flow}/{transform}: coalesce('{src}', {a}) mixes {ty} column "
                    f"with a numeric default — coalesce arguments must share one "
                    f"type: cast '{src}' to numeric first, or quote the default"
                )
    elif key in _STRING_FN_OPS:
        ty = cols.get(src)
        if ty is not None and ty.strip().lower() != "string":
            hint = (
                "use coalesce() for null/default filling, or cast to string first"
                if key == "replace"
                else "cast it to string first, or remove this string operation"
            )
            problems.append(
                f"{flow}/{transform}: '{c.name}' applies {c.op}() to '{src}' "
                f"which is {ty} — {hint}"
            )
        if key == "replace" and len(c.args) != 2:
            problems.append(
                f"{flow}/{transform}: replace() on '{src}' needs old and new "
                "string values; use coalesce() when the requirement is to fill "
                "missing/null values"
            )
    elif key in _NUMERIC_FN_OPS:
        ty = cols.get(src)
        if ty is not None and not _is_numeric(ty):
            problems.append(
                f"{flow}/{transform}: '{c.name}' applies {c.op}() to '{src}' "
                f"which is {ty} — cast '{src}' to a numeric type first"
            )


def _check_filter(flow: str, t: Transform, cols: Dict[str, str], problems: List[str]) -> None:
    for p in t.predicates:
        if p.column not in cols:
            problems.append(
                f"{flow}/{t.name}: filter references '{p.column}', but that "
                "column is not available at this point in the flow"
            )
            continue
        key = _norm_op(p.op)
        if key not in ("gt", "ge", "lt", "le"):
            continue  # eq/ne/null tests are type-agnostic enough
        ty = cols.get(p.column)
        if ty is None or p.value is None:
            continue
        value_numeric = bool(_NUM_LIT.fullmatch(str(p.value).strip()))
        if value_numeric and not _is_numeric(ty) and not _is_temporal(ty):
            problems.append(
                f"{flow}/{t.name}: filter compares '{p.column}' ({ty}) with "
                f"numeric {p.value} — cast '{p.column}' to a numeric type first"
            )


# join condition clause: "<stream>@<col> == <stream>@<col>" (may be &&-chained).
_JOIN_KEY_RE = re.compile(r"(\w+)@(\w+)\s*==\s*(\w+)@(\w+)")


def _check_join(
    flow: str, t: Transform, stream_cols: Dict[str, Dict[str, str]], problems: List[str]
) -> None:
    """Join keys must share a type class — ADF fails the whole flow with
    DF-FN-0015 'equals argument 1 of StringType and argument 2 of IntegerType
    should be similar' (run flow_gold_top_customers, srcCustomers⋈srcOrders).
    Qualifiers that aren't source streams (mid-chain transform names) are
    skipped — no schema to check against, no false positives."""
    for ls, lc, rs, rc in _JOIN_KEY_RE.findall(t.condition or ""):
        lt = stream_cols.get(ls, {}).get(lc)
        rt = stream_cols.get(rs, {}).get(rc)
        if lt is None or rt is None:
            continue
        if _is_numeric(lt) != _is_numeric(rt) or _is_temporal(lt) != _is_temporal(rt):
            string_side = f"{ls}@{lc}" if not _is_numeric(lt) and not _is_temporal(lt) \
                else f"{rs}@{rc}"
            problems.append(
                f"{flow}/{t.name}: join compares {ls}@{lc} ({lt}) with {rs}@{rc} "
                f"({rt}) — join keys must share one type. Cast '{string_side}' "
                f"(add the cast to that entity's casts) so both sides match"
            )


_AGG_ARG_RE = re.compile(r"\w+\(\s*(\w+)\s*\)")


def _check_aggregate(flow: str, t: Transform, cols: Dict[str, str], problems: List[str]) -> None:
    for c in t.columns:
        key = _norm_op(c.op or _norm_op((c.expression or "").split("(", 1)[0]))
        if key in _NUMERIC_AGG_OPS:
            # Legacy expression form ("sum(Confirmed)") carries the column inside the call
            m = _AGG_ARG_RE.fullmatch((c.expression or "").strip())
            src = c.column or (m.group(1) if m else "")
            ty = cols.get(src)
            if ty is None:
                problems.append(
                    f"{flow}/{t.name}: aggregation '{c.name}' takes {key}('{src}'), "
                    "but that column is not available at this point in the flow"
                )
            elif not _is_numeric(ty):
                problems.append(
                    f"{flow}/{t.name}: aggregation '{c.name}' takes {key}('{src}') "
                    f"but '{src}' is {ty} — cast it to a numeric type first"
                )


def check_dataflow(df: DataFlow) -> List[str]:
    """Plain-English type problems for one flow; empty list = clean."""
    problems: List[str] = []

    # Join key types are checked per-stream (a join compares columns from two
    # DIFFERENT sources — the flat `cols` map below can't see the clash), and
    # BEFORE the primary-schema guard: a join between typed extra sources is
    # checkable even when the primary source is ungrounded.
    stream_cols: Dict[str, Dict[str, str]] = {
        df.source.stream_name: {c.name: (c.type or "string") for c in df.source_columns}
    }
    for ep in df.additional_sources:
        stream_cols[ep.stream_name] = {c.name: (c.type or "string") for c in ep.columns}
    for t in df.transforms:
        if t.kind == TransformKind.join:
            _check_join(df.name, t, stream_cols, problems)

    if not df.source_columns:
        return problems  # no schema to walk the chain against (ungrounded flow)
    cols: Dict[str, str] = {c.name: (c.type or "string") for c in df.source_columns}
    for ep in df.additional_sources:
        for c in ep.columns:
            cols.setdefault(c.name, c.type or "string")

    for t in df.transforms:
        if t.kind in (TransformKind.cast, TransformKind.derived, TransformKind.window):
            for c in t.columns:
                _check_literal_shadows_column(df.name, t.name, c, cols, problems)
                if c.op:
                    _check_structured_op(df.name, t.name, c, cols, problems)
                    cols[c.name] = _op_result_type(_norm_op(c.op), c.column or c.name, cols)
                else:
                    cols[c.name] = _expr_result_type(c.expression, cols)
        elif t.kind == TransformKind.filter:
            _check_filter(df.name, t, cols, problems)
        elif t.kind == TransformKind.aggregate:
            _check_aggregate(df.name, t, cols, problems)
            keys = [
                (g.split("=", 1)[0].strip(), g.split("=", 1)[1].strip()) if "=" in g
                else (g, None)
                for g in t.group_by
            ]
            key_names = [n for n, _ in keys]
            agg_cols = [c for c in t.columns if c.name not in key_names]
            if agg_cols or not t.group_by:
                new = {
                    n: (_expr_result_type(e, cols) if e else cols.get(n, "string"))
                    for n, e in keys
                }
                for c in agg_cols:
                    new[c.name] = (
                        _op_result_type(_norm_op(c.op), c.column, cols) if c.op
                        else _expr_result_type(c.expression, cols)
                    )
                cols = new
            # pure dedup (columns=[]) keeps the schema unchanged
        elif t.kind == TransformKind.select and t.columns:
            for c in t.columns:
                ref = _select_ref(c)
                if ref not in cols:
                    problems.append(
                        f"{df.name}/{t.name}: select maps '{c.name}' from '{ref}', "
                        "but that column is not available after the previous "
                        "transform"
                    )
            cols = {
                c.name: cols.get(_select_ref(c), "string")
                for c in t.columns
            }
        # sort/limit: no type effects. join: key types checked above per-stream;
        # its merged columns are already in `cols` via additional_sources.
    return problems


def check_spec(spec: PipelineSpec) -> List[str]:
    """Type problems across every data flow in the spec."""
    problems: List[str] = []
    for df in spec.dataflows:
        problems.extend(check_dataflow(df))
    return problems
