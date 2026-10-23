"""Typed Pydantic v2 contract for an ADF medallion pipeline.

This is the ONE source of truth the whole system agrees on. The LLM (via
Pydantic AI) must return a validated ``PipelineSpec`` instance; the deterministic
builder, review, preflight, and deployer all consume the same object.
Nothing downstream parses free text.

The spec is BRD-driven and source-agnostic: HTTP file, Azure Blob/ADLS, and a
generic REST/CSV endpoint are all expressible. Bronze is always a Copy Activity
(no transformation); Silver/Gold are Mapping Data Flows.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator
from typing_extensions import Annotated, Literal

# --------------------------------------------------------------------------- #
# Enumerations
# --------------------------------------------------------------------------- #


class AuthKind(str, Enum):
    anonymous = "anonymous"
    managed_identity = "managed_identity"


class DatasetKind(str, Enum):
    delimited_text = "DelimitedText"  # CSV
    parquet = "Parquet"
    json = "Json"
    rest_resource = "RestResource"
    binary = "Binary"                 # byte-for-byte copy of any file type
    azure_sql_table = "AzureSqlTable"  # table in an Azure SQL Database source
    delta = "Delta"                    # Delta Lake table — data flows only (inline),
                                       # never a Copy source/sink or an ADF dataset resource


class TransformKind(str, Enum):
    select = "select"
    cast = "cast"
    derived = "derived"
    filter = "filter"
    aggregate = "aggregate"
    sort = "sort"
    window = "window"
    join = "join"  # combine the current stream with another source stream
    limit = "limit"  # keep only the first N rows (e.g. "top 10 customers")


class TriggerKind(str, Enum):
    manual = "manual"
    schedule = "schedule"


class MedallionLayer(str, Enum):
    bronze = "bronze"
    silver = "silver"
    gold = "gold"


# --------------------------------------------------------------------------- #
# Sources (discriminated by ``kind``)
# --------------------------------------------------------------------------- #


class HttpSource(BaseModel):
    """Anonymous HTTP(S) file source (e.g. Johns Hopkins CSSE daily CSVs)."""

    kind: Literal["http"] = "http"
    base_url: str
    relative_url_template: str = Field(
        ...,
        description="Relative path, optionally with a {date} / @{dataset().date} token.",
    )
    file_pattern: Optional[str] = None  # e.g. MM-DD-YYYY.csv
    auth: AuthKind = AuthKind.anonymous


class BlobSource(BaseModel):
    """Azure Blob / ADLS Gen2 source, reached with Managed Identity."""

    kind: Literal["blob"] = "blob"
    account_url: str  # https://<acct>.dfs.core.windows.net
    container: str
    path: Optional[str] = None
    auth: AuthKind = AuthKind.managed_identity


class RestSource(BaseModel):
    """Generic REST / CSV-over-HTTP endpoint."""

    kind: Literal["rest"] = "rest"
    base_url: str
    relative_url: Optional[str] = None
    method: str = "GET"
    response_format: str = "json"  # json | csv
    auth: AuthKind = AuthKind.anonymous


class SqlSource(BaseModel):
    """Azure SQL Database source, reached with Managed Identity (no passwords)."""

    kind: Literal["sql"] = "sql"
    server: str    # logical server name or full host (<name>.database.windows.net)
    database: str
    auth: AuthKind = AuthKind.managed_identity


Source = Annotated[
    Union[HttpSource, BlobSource, RestSource, SqlSource], Field(discriminator="kind")
]


# --------------------------------------------------------------------------- #
# Storage / linked services / datasets
# --------------------------------------------------------------------------- #


class StorageAccount(BaseModel):
    """ADLS Gen2 account reached with Managed Identity (no keys)."""

    account_name: str
    account_url: Optional[str] = None  # https://<acct>.dfs.core.windows.net
    containers: List[str] = Field(default_factory=lambda: ["bronze", "silver", "gold"])
    auth: AuthKind = AuthKind.managed_identity


class LinkedService(BaseModel):
    name: str
    type: str  # HttpServer | AzureBlobFS | RestService | AzureSqlDatabase
    auth: AuthKind
    url: Optional[str] = None          # HttpServer / RestService
    account_url: Optional[str] = None  # AzureBlobFS
    server: Optional[str] = None       # AzureSqlDatabase
    database: Optional[str] = None     # AzureSqlDatabase


# LLM-emitted type -> valid ADF data-flow type. Bare 'decimal' is the killer:
# the DSL requires decimal(precision,scale) and fails DF-DSL-001 with
# "mismatched input ')' expecting '('" (gemini-2.5-flash, run 7d90232abeec).
_COLUMN_TYPE_ALIASES = {
    "decimal": "decimal(38,18)",
    "numeric": "decimal(38,18)",
    "int": "integer",
    "bigint": "long",
    "smallint": "short",
    "datetime": "timestamp",
    "bool": "boolean",
    "float": "double",
    "number": "double",
    "str": "string",
    "text": "string",
    "varchar": "string",
}


class Column(BaseModel):
    name: str
    type: str = "string"  # string | integer | timestamp | double | boolean | ...

    @field_validator("type")
    @classmethod
    def _normalize_type(cls, v: str) -> str:
        key = (v or "").strip().lower()
        return _COLUMN_TYPE_ALIASES.get(key, (v or "").strip() or "string")


# Azure storage container naming rules: 3-63 chars, lowercase alphanumerics and
# hyphens, starts/ends alphanumeric. Underscores, @dataset() tokens, and empty
# strings all fail at run time with AdlsGen2InvalidResourceName.
_AZ_CONTAINER_RE = re.compile(r"[a-z0-9](?:[a-z0-9-]{1,61})[a-z0-9]")


class DatasetLocation(BaseModel):
    container: str
    folder: Optional[str] = None
    file_name: Optional[str] = None


class Dataset(BaseModel):
    name: str
    kind: DatasetKind
    linked_service: str
    location: Optional[DatasetLocation] = None
    # CSV/JSON/REST over HTTP uses a relative URL instead of a store location.
    relative_url: Optional[str] = None
    # AzureSqlTable only: the source table.
    schema_name: Optional[str] = None  # e.g. dbo
    table_name: Optional[str] = None
    parameters: List[str] = Field(default_factory=list)
    columns: List[Column] = Field(default_factory=list)
    first_row_as_header: bool = True


# --------------------------------------------------------------------------- #
# Data flow transforms
# --------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
# Structured expressions (model-independence): the LLM picks an OPERATION from a
# closed vocabulary; CODE renders the DSL string. Invalid syntax becomes
# unrepresentable — any model that can fill an enum produces valid expressions.
# Raw `expression`/`condition` strings remain as the escape hatch for logic the
# ops can't express (and still pass through _normalize_df_expression).
# ---------------------------------------------------------------------------- #

# op (normalized: lowercase, non-letters stripped) -> DSL function name.
_OP_FN = {
    "trim": "trim",
    "upper": "upper",
    "lower": "lower",
    "totimestamp": "toTimestamp",
    "todate": "toDate",
    "tointeger": "toInteger",
    "tolong": "toLong",
    "todouble": "toDouble",
    "todecimal": "toDecimal",
    "toboolean": "toBoolean",
    "tostring": "toString",
    "round": "round",
    "abs": "abs",
    "floor": "floor",
    "ceil": "ceil",
    "length": "length",
    "substring": "substring",
    "replace": "replace",
    "regexreplace": "regexReplace",
    "regexmatch": "regexMatch",
    "rlike": "rlike",
    "startswith": "startsWith",
    "endswith": "endsWith",
    "concat": "concat",
    "coalesce": "coalesce",
    "count": "count",
    "countdistinct": "countDistinct",
    "sum": "sum",
    "avg": "avg",
    "mean": "avg",
    "min": "min",
    "max": "max",
    "first": "first",
    "rownumber": "rowNumber",
    "rank": "rank",
    "denserank": "denseRank",
    # Date/time PART extraction (single timestamp/date column -> integer). These
    # unlock "group by Year, Month" style gold outputs without any raw DSL — the
    # model emits a derived column {op: year, column: order_purchase_timestamp}
    # then groups by it. Names match ADF Mapping-Data-Flow functions exactly.
    "year": "year",
    "month": "month",
    "dayofmonth": "dayOfMonth",
    "day": "dayOfMonth",
    "dayofweek": "dayOfWeek",
    "dayofyear": "dayOfYear",
    "hour": "hour",
    "minute": "minute",
    "second": "second",
    "weekofyear": "weekOfYear",
    "week": "weekOfYear",
}
_OP_IDENTITY = {"identity", "copy", "passthrough", "asis", "none"}
# Window functions that take NO arguments — rendered as fn(), never fn(column).
_OP_NOARG = {"rownumber", "rank", "denserank"}
# Proper Case: ADF's data-flow DSL has no initCap, so CODE renders the composite
# concat(upper(left(col,1)), lower(substring(col,2))) — first letter upper, rest
# lower. (Chain a `trim` op first when the column may carry whitespace.)
_OP_PROPER = {"proper", "propercase", "initcap", "titlecase", "title"}
# Quarter: the DSL has no quarter() function (its date parts stop at year/month/
# week/day), so CODE renders floor((month(col) - 1) / 3) + 1 -> 1..4.
_OP_QUARTER = {"quarter"}
# Common BRD cleansing words that are easier and safer as named operations than
# as model-authored raw expressions.
_OP_REMOVE_SPECIAL = {
    "removespecialcharacters", "removespecialchars", "stripspecialcharacters",
    "stripspecialchars", "alphanumeric", "onlyalphanumeric",
}
_OP_BLANK_DEFAULT = {
    "replaceempty", "replaceblank", "emptytodefault", "blanktodefault",
    "emptystringtodefault", "replaceemptystring", "replaceblankstring",
}
# Arithmetic: operands are the column plus args; each operand may be ANOTHER
# COLUMN NAME or a numeric literal — rendered infix, e.g. multiply(quantity,
# [list_price]) -> (quantity * list_price). This unlocks derived measures like
# line_total = quantity * list_price * (1 - discount) without any raw DSL.
_OP_ARITH = {
    "add": "+", "plus": "+",
    "subtract": "-", "minus": "-",
    "multiply": "*", "times": "*", "product": "*",
    "divide": "/", "dividedby": "/",
}


def _norm_op(op: str) -> str:
    return re.sub(r"[^a-z]", "", (op or "").lower())


_DSL_IDENT = re.compile(r"[A-Za-z_]\w*")


def dsl_name(name: str) -> str:
    """A column reference in data-flow script. Names that aren't plain
    identifiers ('Country Name', 'unit-price') must be brace-escaped
    ({Country Name}) or the Spark broker rejects the script at runtime
    (population.csv run: DFExecutorUserError on a spaced groupBy column)."""
    s = (name or "").strip()
    if _DSL_IDENT.fullmatch(s) or (s.startswith("{") and s.endswith("}")):
        return s
    if re.fullmatch(r"-?\d+(\.\d+)?", s):
        return s  # numeric literal (arithmetic's first operand can be "1")
    return "{" + s + "}"


def _render_literal(value: str) -> str:
    """A structured arg/value as a DSL literal: numbers bare, everything else a
    single-quoted string (pre-quoted values pass through). Boolean/null literals
    are ADF literals, not strings."""
    s = str(value).strip()
    if re.fullmatch(r"-?\d+(\.\d+)?", s):
        return s
    if s.lower() in {"true", "false", "null"}:
        return s.lower()
    if len(s) >= 2 and s.startswith("'") and s.endswith("'"):
        return s
    return "'" + s.replace("'", r"\'") + "'"


def _render_operand(value: str) -> str:
    """An arithmetic operand: bare when it is a column name or a number, quoted
    literal otherwise (arithmetic on a string literal is almost surely a plan
    bug, but the DSL error is clearer than silent misquoting)."""
    s = str(value).strip()
    if re.fullmatch(r"[A-Za-z_]\w*", s):
        return s  # column reference
    if s.startswith("{") and s.endswith("}"):
        return s  # already brace-escaped column reference
    return _render_literal(s)


def _render_op(op: str, column: "str | None", args: List[str]) -> "str | None":
    """Render {op, column, args} to a DSL expression; None when op is unknown.
    The `column` field is always a COLUMN REFERENCE — brace-escaped when the
    name isn't a plain identifier ('Country Name' -> {Country Name})."""
    key = _norm_op(op)
    col = dsl_name(column) if column else column
    if key in _OP_IDENTITY:
        return col or None
    if key in _OP_NOARG:
        return f"{_OP_FN[key]}()"
    if key in _OP_PROPER:
        if not col:
            return None
        return f"concat(upper(left({col}, 1)), lower(substring({col}, 2)))"
    if key in _OP_QUARTER:
        if not col:
            return None
        return f"(floor((month({col}) - 1) / 3) + 1)"
    if key in _OP_REMOVE_SPECIAL:
        if not col:
            return None
        replacement = args[0] if args else ""
        return f"regexReplace({col}, '[^A-Za-z0-9 ]', {_render_literal(replacement)})"
    if key in _OP_BLANK_DEFAULT:
        if not col or not args:
            return None
        default = _render_literal(args[0])
        return f"iif(isNull({col}) || trim({col}) == '', {default}, {col})"
    if key in _OP_ARITH:
        operands = ([col] if col else []) + [str(a) for a in args]
        if len(operands) < 2:
            return None  # arithmetic needs at least two operands
        sym = _OP_ARITH[key]
        # toDouble: Spark types int*decimal arithmetic as decimal(38,x), but
        # _op_result_type projects (and gold sources declare) 'double' — the
        # written Delta schema must match that projection or gold flows hit
        # column/type mismatches at runtime (DF-EXPR-010 / debug "mismatch").
        return "toDouble(" + f" {sym} ".join(_render_operand(o) for o in operands) + ")"
    if key == "concat":
        # concat args may be COLUMN NAMES or pre-quoted literals ("' '").
        # Rendering bare identifiers through _render_literal turned the column
        # last_name into the string 'last_name' — every CustomerName came out
        # "John last_name" (bike-store run). Same operand rule as arithmetic.
        parts = ([col] if col else []) + [_render_operand(str(a)) for a in args]
        if not parts:
            return None
        return f"concat({', '.join(parts)})"
    fn = _OP_FN.get(key)
    if fn is None:
        return None
    parts = ([col] if col else []) + [_render_literal(a) for a in args]
    return f"{fn}({', '.join(parts)})"


_PRED_CMP = {"eq": "==", "ne": "!=", "gt": ">", "ge": ">=", "lt": "<", "le": "<="}
_PRED_NOT_NULL = {"notnull", "isnotnull", "notempty", "exists"}
_PRED_NULL = {"null", "isnull", "missing"}
_PRED_LIKE = {"like", "notlike"}
_PRED_REGEX = {"regex", "regexmatch", "rlike", "matches", "notregex", "notrlike"}
_PRED_IN = {"in", "notin"}
_PRED_BETWEEN = {"between", "notbetween"}


def _split_predicate_values(value: "str | None") -> List[str]:
    """Split a BRD/tool value such as "A,B,C", "A|B|C", or "[A, B, C]".

    This is intentionally simple: the LogicalPlan layer carries strings, not a
    full expression AST, and these values are business literals in normal BRDs.
    """
    if value is None:
        return []
    s = str(value).strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    sep = "|" if "|" in s else ","
    return [p.strip().strip("'\"") for p in s.split(sep) if p.strip()]


class Predicate(BaseModel):
    """One structured filter clause; a transform's predicates are AND-ed."""

    op: str  # not_null | null | eq | ne | gt | ge | lt | le
    column: str
    value: Optional[str] = None  # comparison operand (unused for null tests)

    def render(self) -> "str | None":
        key = _norm_op(self.op)
        col = dsl_name(self.column)
        if key in _PRED_NOT_NULL:
            return f"!isNull({col})"
        if key in _PRED_NULL:
            return f"isNull({col})"
        cmp = _PRED_CMP.get(key)
        if cmp is not None and self.value is not None:
            return f"{col} {cmp} {_render_literal(self.value)}"
        if key in _PRED_LIKE and self.value is not None:
            expr = f"like({col}, {_render_literal(self.value)})"
            return f"!({expr})" if key == "notlike" else expr
        if key in _PRED_REGEX and self.value is not None:
            fn = "rlike" if key in {"rlike", "notrlike"} else "regexMatch"
            expr = f"{fn}({col}, {_render_literal(self.value)})"
            return f"!({expr})" if key in {"notregex", "notrlike"} else expr
        if key in _PRED_IN and self.value is not None:
            values = ", ".join(_render_literal(v) for v in _split_predicate_values(self.value))
            if not values:
                return None
            expr = f"in([{values}], {col})"
            return f"!({expr})" if key == "notin" else expr
        if key in _PRED_BETWEEN and self.value is not None:
            values = _split_predicate_values(self.value)
            if len(values) != 2:
                return None
            expr = f"between({col}, {_render_literal(values[0])}, {_render_literal(values[1])})"
            return f"!({expr})" if key == "notbetween" else expr
        if cmp is None or self.value is None:
            return None
        return f"{col} {cmp} {_render_literal(self.value)}"


class DerivedColumn(BaseModel):
    name: str
    # EITHER a raw Mapping-Data-Flow expression (escape hatch)...
    expression: Optional[str] = None  # e.g. toInteger(Confirmed)
    # ...OR a structured op, rendered to DSL deterministically in Transform.
    op: Optional[str] = None
    column: Optional[str] = None  # input column; defaults to `name`
    args: List[str] = Field(default_factory=list)  # extra literals (formats etc.)


class SortKey(BaseModel):
    column: str
    order: str = "desc"  # asc | desc


# LLMs emit two expression idioms ADF's data-flow DSL rejects at runtime
# (DF-DSL-001 / DF-FN-0015). Normalize them at validation instead of burning a
# ~5-minute self-heal attempt on each:
#   * pipeline-style interpolation:  @{col}      ->  col   ({col} kept when the
#     name needs escaping; stream@{col} -> stream@col for join conditions)
#   * SQL null tests:                col != null ->  !isNull(col)
#                                    col == null ->  isNull(col)
_STREAM_AT_BRACE = re.compile(r"(?<=\w)@\{(\w+)\}")
_AT_BRACE = re.compile(r"@\{([^}]+)\}")
_NULL_NE = re.compile(r"(\w+|\{[^}]+\})\s*!=\s*null\b", re.IGNORECASE)
_NULL_EQ = re.compile(r"(\w+|\{[^}]+\})\s*==\s*null\b", re.IGNORECASE)
# One non-nested ternary spanning the whole expression. ADF's data-flow language
# has NO `?:` operator (DF-EXPR-010) — only iif().
_TERNARY = re.compile(r"^([^?:]+)\?([^?:]+):([^?:]+)$")
# LLM-invented '@stream@column' qualifier (e.g. '@bronzeorders@order_status').
# The leading '@' is never valid DSL; the 'stream@' qualifier itself is only
# valid inside JOIN conditions — everywhere else columns are referenced bare.
_AT_STREAM_AT = re.compile(r"@(\w+)@")

# Bare identifiers in a data-flow expression that are column references: not
# followed by '(' (those are function calls like trim/isNull/toTimestamp) and
# not boolean/null literals. Quoted string literals are stripped before scanning.
_EXPR_IDENT = re.compile(r"\b[A-Za-z_]\w*\b(?!\s*\()")
_EXPR_LITERALS = {"true", "false", "null"}
_QUOTED_LITERAL = re.compile(r"'[^']*'")
# `<expression> as <Alias>` — SQL idiom LLMs put in aggregate group_by entries.
_GROUPBY_ALIAS = re.compile(r"^(.+?)\s+[Aa][Ss]\s+([A-Za-z_]\w*)$")


def _normalize_df_expression(s: "str | None", keep_stream_qualifier: bool = False) -> "str | None":
    if not s:
        return s
    s = _AT_STREAM_AT.sub(r"\1@" if keep_stream_qualifier else "", s)
    s = _STREAM_AT_BRACE.sub(r"@\1", s)
    s = _AT_BRACE.sub(
        lambda m: m.group(1) if re.fullmatch(r"\w+", m.group(1)) else "{" + m.group(1) + "}",
        s,
    )
    s = _NULL_NE.sub(r"!isNull(\1)", s)
    s = _NULL_EQ.sub(r"isNull(\1)", s)
    m = _TERNARY.match(s.strip())
    if m:
        cond, a, b = (g.strip() for g in m.groups())
        if a.lower() == "true" and b.lower() == "false":
            s = cond
        elif a.lower() == "false" and b.lower() == "true":
            s = f"!({cond})"
        else:
            s = f"iif({cond}, {a}, {b})"
    return s


class Transform(BaseModel):
    """A single Mapping-Data-Flow transformation, producing a named stream."""

    kind: TransformKind
    name: str  # output stream name (e.g. 'CastTypes')
    columns: List[DerivedColumn] = Field(default_factory=list)  # cast/derived/aggregate
    condition: Optional[str] = None  # filter / window; also the JOIN condition
    # Structured filter clauses (preferred over a raw condition; AND-ed).
    predicates: List[Predicate] = Field(default_factory=list)
    group_by: List[str] = Field(default_factory=list)  # aggregate
    order_by: List[SortKey] = Field(default_factory=list)  # sort; also window sort
    # window-only: partition columns. With partition_by set, the builder renders
    # window(over(...), asc/desc(col, true), name = expression) deterministically —
    # a raw `condition` for window is the legacy escape hatch and is DSL the model
    # reliably gets wrong (Spark partitionBy/orderBy, run 24756f0f5aa8).
    partition_by: List[str] = Field(default_factory=list)
    # join-only: the OTHER stream to join the current stream with, and the join type.
    # `condition` carries the join expression, e.g. "orders@order_id == items@order_id".
    right_stream: Optional[str] = None
    join_type: str = "inner"  # inner | left_outer | right_outer | full_outer | cross
    # limit-only: keep only the first `take` rows (after `skip`), e.g. "top 10
    # customers" = sort desc + limit(take=10). NOT a native ADF transform (the
    # broker rejects `limit(` with DF-FLO-005 'not registered', run
    # flow_gold_top_customers) — the builder lowers it to rank(<sort keys>) +
    # filter(xRowRank <= take) + select-drop. Keep using this kind in plans;
    # never hand-roll the rank/window pattern yourself.
    take: Optional[int] = None
    skip: int = 0

    @model_validator(mode="after")
    def _render_structured(self) -> "Transform":
        """Structured ops/predicates -> DSL strings, BEFORE idiom normalization.
        A column with neither a renderable op nor an expression is a hard parse
        error (Pydantic AI feeds it back and the model retries in seconds —
        instead of failing in Azure minutes later). Exception: a select column
        with only a name is an identity mapping (name = name)."""
        if self.predicates and not self.condition:
            clauses = [p.render() for p in self.predicates]
            bad = [p for p, cl in zip(self.predicates, clauses) if not cl]
            if bad:
                # Silently skipping these once shipped an EMPTY filter() to
                # Azure (EXE-0001 'cannot be analyzed as a graph') — the
                # requirement vanished AND the run burned 5 minutes.
                raise ValueError(
                    f"transform '{self.name}': unrenderable predicate(s): "
                    + "; ".join(
                        f"op='{p.op}' column='{p.column}' value={p.value!r}" for p in bad
                    )
                    + " — op must be one of not_null|null|eq|ne|gt|ge|lt|le|"
                    "like|regex|in|between, and value-based ops require a value"
                )
            self.condition = " && ".join(clauses)  # type: ignore[arg-type]
        for c in self.columns:
            if not c.expression and c.op:
                c.expression = _render_op(c.op, c.column or c.name, c.args)
            if not c.expression and self.kind == TransformKind.select:
                c.expression = c.name
            if not c.expression:
                raise ValueError(
                    f"transform '{self.name}' column '{c.name}': set 'op' (one of "
                    f"{', '.join(sorted(set(_OP_FN.values())))} or identity) with "
                    "optional column/args, or a raw 'expression'"
                )
        return self

    @model_validator(mode="after")
    def _normalize_expressions(self) -> "Transform":
        # Joins keep the stream@column qualifier (required to disambiguate the
        # two sides); every other transform references columns bare.
        is_join = self.kind == TransformKind.join
        self.condition = _normalize_df_expression(self.condition, keep_stream_qualifier=is_join)
        for c in self.columns:
            c.expression = _normalize_df_expression(c.expression) or c.expression
        self.group_by = [_normalize_df_expression(g) or g for g in self.group_by]
        # SQL-style computed group-by keys: `year(ts) as Year` is invalid DSL
        # (DF-DSL-001 "no viable alternative at input 'groupBy(year(...) as'",
        # gemini-2.5-flash run 7d90232abeec). The DSL form is `Year = year(ts)`.
        self.group_by = [
            f"{m.group(2)} = {m.group(1)}" if (m := _GROUPBY_ALIAS.match(g)) else g
            for g in self.group_by
        ]
        self.partition_by = [_normalize_df_expression(p) or p for p in self.partition_by]
        return self

    @model_validator(mode="after")
    def _window_needs_partition(self) -> "Transform":
        """A window that outputs columns must say what it partitions over —
        without partition_by the builder can't render over(...) and the raw
        condition path is the exact DSL models get wrong. Hard parse error so
        Pydantic AI retries in seconds instead of ADF failing in minutes."""
        if (
            self.kind == TransformKind.window
            and self.columns
            and not self.partition_by
            and not self.condition
        ):
            raise ValueError(
                f"window '{self.name}': set partition_by (the over(...) key columns), "
                "optional order_by, and columns like "
                '{"name": "row_num", "op": "row_number"}'
            )
        return self

    @model_validator(mode="after")
    def _limit_needs_take(self) -> "Transform":
        """A limit transform with no `take` is meaningless. Hard parse error so
        the model retries in seconds instead of building the fragile
        dummy-partition + window(rowNumber) + filter workaround for 'top N'.
        (The builder lowers limit to rank+filter — ADF has no limit transform —
        but `take` is still what drives the rank cutoff.)"""
        if self.kind == TransformKind.limit and self.take is None:
            raise ValueError(
                f"limit '{self.name}': set 'take' (row count to keep) — e.g. "
                'for "top 10 X" put a sort transform (desc) then a limit '
                'transform with take=10'
            )
        return self


class DataFlowEndpoint(BaseModel):
    stream_name: str
    dataset: str
    # Per-source schema (needed for the extra sources of a multi-source/join flow;
    # the primary source keeps using DataFlow.source_columns for back-compat).
    columns: List[Column] = Field(default_factory=list)


def _referenced_columns(transforms: List["Transform"]) -> List[str]:
    """Column names the transforms CONSUME from the source (filter conditions,
    derive/cast/aggregate expressions, group-by keys, sort keys) in first-use
    order. A name produced by an EARLIER transform (a derived/aggregate output
    like `order_count = count(order_id)`) is not a source column — skip it. A
    transform consuming the same name it produces (order_status = trim(order_status))
    still counts: the input read happens before the overwrite."""
    seen: "dict[str, None]" = {}  # insertion-ordered set
    produced: set = set()

    def scan(s: "str | None") -> None:
        if not s:
            return
        for m in _EXPR_IDENT.finditer(_QUOTED_LITERAL.sub("", s)):
            name = m.group(0)
            if name.lower() not in _EXPR_LITERALS and name not in produced:
                seen.setdefault(name)

    for t in transforms:
        scan(t.condition)
        for c in t.columns:
            scan(c.expression)
        for g in t.group_by:
            scan(g)
        for p in t.partition_by:
            scan(p)
        for k in t.order_by:
            scan(k.column)
        produced.update(c.name for c in t.columns)
    return list(seen)


class DataFlow(BaseModel):
    name: str
    # silver | gold when the pipeline follows the medallion pattern; None for a
    # non-medallion transformation flow (the builder treats both the same).
    layer: Optional[MedallionLayer] = None
    source: DataFlowEndpoint
    sink: DataFlowEndpoint
    source_columns: List[Column] = Field(default_factory=list)
    # Extra input sources for a multi-source flow (e.g. a Gold join of several Silver
    # datasets). Empty for the common single-source Silver/Gold flow. Each is referenced
    # by its stream_name from a `join` transform's `right_stream`.
    additional_sources: List[DataFlowEndpoint] = Field(default_factory=list)
    transforms: List[Transform] = Field(default_factory=list)
    compute_cores: int = 8  # smallest General Purpose tier

    @model_validator(mode="after")
    def _sanitize_stream_names(self) -> "DataFlow":
        """ADF Mapping-Data-Flow STREAM names (source/sink stream_name, transform
        names) allow ONLY alphanumerics and must start with a letter — unlike the
        ls_/ds_/df_/pl_ resource names, underscores are rejected with 'Provided
        name contains invalid characters'. LLMs emit snake_case anyway, so fix it
        deterministically here and rewrite every reference (right_stream, join
        conditions, expressions using stream@column) to match."""

        def clean(n: str) -> str:
            c = re.sub(r"[^A-Za-z0-9]", "", n or "")
            if c and not c[0].isalpha():
                c = "s" + c
            return c or "Stream"

        rename: dict = {}
        for ep in (self.source, self.sink, *self.additional_sources):
            new = clean(ep.stream_name)
            if new != ep.stream_name:
                rename[ep.stream_name] = new
                ep.stream_name = new
        for t in self.transforms:
            new = clean(t.name)
            if new != t.name:
                rename[t.name] = new
                t.name = new
        if rename:
            pattern = re.compile(
                "|".join(
                    rf"\b{re.escape(k)}\b"
                    for k in sorted(rename, key=len, reverse=True)
                )
            )

            def sub(s: "str | None") -> "str | None":
                return pattern.sub(lambda m: rename[m.group(0)], s) if s else s

            for t in self.transforms:
                if t.right_stream:
                    t.right_stream = rename.get(t.right_stream, clean(t.right_stream))
                t.condition = sub(t.condition)
                for c in t.columns:
                    c.expression = sub(c.expression) or c.expression
        return self

    @model_validator(mode="after")
    def _drop_noop_transforms(self) -> "DataFlow":
        """The LLM emits placeholder transforms with no working parts — e.g. an
        empty derive/filter/sort is broken DSL. A contentless transform is a
        pass-through in intent, so drop it — the chain reconnects around it
        (transforms are a linear chain; only additional-source stream names can
        be a join's right_stream). An empty SELECT is kept: the builder renders
        it as a select-all with duplicate-column dedup ('keep everything' — the
        LLM's 'selectallforsilver' intent, and the compiler's dedupJoinColumns
        fix for ambiguous post-join references)."""

        def noop(t: Transform) -> bool:
            if t.kind in (TransformKind.cast, TransformKind.derived):
                return not t.columns
            if t.kind == TransformKind.aggregate:
                return not t.columns and not t.group_by
            if t.kind == TransformKind.filter:
                return not t.condition
            if t.kind == TransformKind.window:
                return not t.condition and not (t.partition_by and t.columns)
            if t.kind == TransformKind.sort:
                return not t.order_by
            if t.kind == TransformKind.limit:
                return t.take is None
            return False  # joins are validated by cross_reference_errors

        self.transforms = [t for t in self.transforms if not noop(t)]
        return self

    @model_validator(mode="after")
    def _infer_source_columns(self) -> "DataFlow":
        """The LLM sometimes omits the source schema entirely (source_columns,
        source.columns AND every dataset column list come back empty). The builder
        then renders `source(output(), allowSchemaDrift: true, ...)` — every real
        column counts as schema DRIFT, and each expression referencing one fails at
        runtime with DF-EXPR-010 'Column X used in expression is unavailable or
        invalid' (a ~5-minute feedback loop per attempt). Recover deterministically:
        declare the columns the transforms actually consume in the source projection,
        as strings — the flow's own cast transforms handle typing. Single-source
        flows only: with joins a bare column can't be attributed to one source."""
        if self.additional_sources:
            return self
        if not self.source_columns and self.source.columns:
            # The LLM filled the per-endpoint schema instead of source_columns —
            # the builder reads only source_columns for the primary source.
            self.source_columns = list(self.source.columns)
        if not self.source_columns:
            self.source_columns = [
                Column(name=n) for n in _referenced_columns(self.transforms)
            ]
        return self


# --------------------------------------------------------------------------- #
# Pipelines & triggers
# --------------------------------------------------------------------------- #


class Activity(BaseModel):
    name: str
    type: str  # Copy | ExecuteDataFlow | ExecutePipeline
    layer: Optional[MedallionLayer] = None  # set on the Bronze Copy activity
    # Copy
    source_dataset: Optional[str] = None
    sink_dataset: Optional[str] = None
    # ExecuteDataFlow
    data_flow: Optional[str] = None
    # ExecutePipeline
    invoked_pipeline: Optional[str] = None
    # chaining
    depends_on: List[str] = Field(
        default_factory=list,
        description="Activity names this one depends on (Succeeded condition).",
    )


class PipelineDef(BaseModel):
    name: str
    parameters: List[str] = Field(default_factory=list)
    activities: List[Activity] = Field(default_factory=list)


class Trigger(BaseModel):
    name: str
    kind: TriggerKind
    pipeline: str
    frequency: Optional[str] = None  # schedule: Day | Hour | ...
    interval: Optional[int] = None
    start_time: Optional[str] = None


# --------------------------------------------------------------------------- #
# Top-level spec
# --------------------------------------------------------------------------- #

_PREFIXES = {
    "linked_services": "ls_",
    "datasets": "ds_",
    "dataflows": "df_",
    "pipelines": "pl_",
}


class PipelineSpec(BaseModel):
    """Complete, validated description of a medallion pipeline."""

    name: str = "medallion_pipeline"
    source: Optional[Source] = None
    storage: Optional[StorageAccount] = None
    linked_services: List[LinkedService] = Field(default_factory=list)
    datasets: List[Dataset] = Field(default_factory=list)
    dataflows: List[DataFlow] = Field(default_factory=list)
    pipelines: List[PipelineDef] = Field(default_factory=list)
    triggers: List[Trigger] = Field(default_factory=list)

    # If the BRD is ambiguous, the parser fills this instead of inventing
    # components. A non-empty list means "needs human clarification".
    # RESERVED FOR GENUINE BLOCKERS ONLY — if the agent can make a sensible
    # assumption it does so and records it in `assumptions`, it does NOT ask here.
    clarifications: List[str] = Field(default_factory=list)

    # Plan-first architect: decisions the agent made on its own to build a complete
    # plan without pestering the user (e.g. "dedup key not stated -> used
    # (Country_Region, Last_Update)"). Surfaced in the plan for the user to accept or
    # override via chat. These NEVER block approval.
    assumptions: List[str] = Field(default_factory=list)

    # Optional enhancements the agent proposes but did NOT build (e.g. "add a daily
    # schedule trigger?", "consider partitioning silver by date"). Advisory only —
    # only include when it would genuinely change the pipeline, never as filler.
    suggestions: List[str] = Field(default_factory=list)

    # BRD requirements the system recognized but could NOT build (from
    # LogicalPlan.unsupported). Non-empty means /approve requires an explicit
    # acknowledgement — a known-incomplete pipeline never deploys silently.
    unsupported: List[str] = Field(default_factory=list)

    # ------------------------------------------------------------------ #
    # Robust coercion for LLM output
    # ------------------------------------------------------------------ #

    @field_validator("source", mode="before")
    @classmethod
    def _coerce_source(cls, v):
        """LLMs sometimes serialize the discriminated `source` union as a JSON
        string instead of a nested object — accept and decode it."""
        if isinstance(v, str):
            import json

            try:
                return json.loads(v)
            except Exception:  # noqa: BLE001 — leave it for normal validation
                return v
        return v

    @model_validator(mode="after")
    def _harmonize_copy_kinds(self) -> "PipelineSpec":
        """ADF rejects a Copy where exactly one side is Binary
        (BinaryUnsupportedDatasetType: 'Binary dataSet can only be copied to
        binary dataSet'). The LLM does this when a BRD says "byte-for-byte":
        it marks the sink Binary but leaves the CSV source DelimitedText — which
        also breaks downstream data flows (they can't read Binary). Coerce the
        Binary side to the other side's kind; a format-preserving Copy of the
        same kind is byte-equivalent for our purposes."""
        kinds = {ds.name: ds for ds in self.datasets}
        for pl in self.pipelines:
            for act in pl.activities:
                if act.type != "Copy":
                    continue
                src = kinds.get(act.source_dataset or "")
                snk = kinds.get(act.sink_dataset or "")
                if src is None or snk is None or src.kind == snk.kind:
                    continue
                if src.kind == DatasetKind.binary:
                    src.kind = snk.kind
                elif snk.kind == DatasetKind.binary:
                    snk.kind = src.kind
        return self

    @model_validator(mode="after")
    def _repair_container_names(self) -> "PipelineSpec":
        """Containers must be CONCRETE, valid Azure names. The LLM parameterizes
        them anyway (container='@dataset().p_bronze_container_name' with an empty
        default) — at runtime the empty container makes ADF treat the FOLDER as
        the container name, failing with AdlsGen2InvalidResourceName on the
        folder's underscores (run df2db30c73a5). Deterministic repair: infer the
        layer from the dataset/parameter name and use it as the container; drop
        the now-dead container parameter. Unfixable names are left for pre-flight."""
        layer_tokens = ("landing", "bronze", "silver", "gold", "raw", "archive")
        for ds in self.datasets:
            loc = ds.location
            if loc is None:
                continue
            c = (loc.container or "").strip()
            if _AZ_CONTAINER_RE.fullmatch(c):
                continue
            hay = f"{c} {ds.name}".lower()
            layer = next((t for t in layer_tokens if t in hay), None)
            if layer is None:
                continue  # pre-flight fails with the exact name
            old = c or "(empty)"
            loc.container = layer
            ds.parameters = [
                p for p in ds.parameters
                if p not in old and "container" not in p.lower()
            ]
            self.assumptions.append(
                f"dataset '{ds.name}' had a non-concrete container '{old}' -> "
                f"replaced with the '{layer}' container (containers cannot be "
                "parameterized or contain invalid characters)"
            )
        return self

    @model_validator(mode="after")
    def _repair_layer_sinks(self) -> "PipelineSpec":
        """A data flow must never write back onto its own input. The LLM
        over-applies the "reuse datasets across layer boundaries" skill and
        points a silver flow's SINK at the very bronze dataset it reads —
        generating no ds_silver_* dataset at all. The flow then overwrites
        bronze in place, the silver container stays empty forever, and gold
        silently aggregates bronze (observed in factory RetailADFFF, 2026-07-06).

        Deterministic repair, same spirit as _harmonize_copy_kinds:

          * a silver/gold flow whose sink dataset == its source dataset gets a
            synthesized Delta sink dataset in its layer's container, and
          * gold flows still reading the bronze dataset a repaired silver flow
            consumes are rewired to that flow's new silver Delta sink,

        keeping the medallion chain (bronze -> silver -> gold) physically real.
        Flows with no inferable layer are left alone — pre-flight flags them.
        """
        ds_by_name = {ds.name: ds for ds in self.datasets}

        def flow_layer(df: DataFlow) -> Optional[str]:
            if df.layer is not None:
                return df.layer.value
            name = df.name.lower()
            for layer in ("gold", "silver"):
                if layer in name:
                    return layer
            return None

        def entity(ds_name: str) -> str:
            """ds_bronze_olist_orders_dataset_csv -> olist_orders_dataset."""
            parts = ds_name.split("_")
            if parts and parts[0] == "ds":
                parts = parts[1:]
            if parts and parts[0] in ("landing", "bronze", "silver", "gold", "raw"):
                parts = parts[1:]
            if parts and parts[-1] in ("csv", "parquet", "json", "delta", "binary"):
                parts = parts[:-1]
            return "_".join(parts) or ds_name

        # bronze dataset a repaired silver flow reads -> its new silver sink name
        rewired: Dict[str, str] = {}

        for df in self.dataflows:
            if df.sink.dataset != df.source.dataset:
                continue
            layer = flow_layer(df)
            if layer is None:
                continue
            src_ds = ds_by_name.get(df.source.dataset)
            if src_ds is None:
                continue
            ent = entity(df.source.dataset)
            new_name = f"ds_{layer}_{ent}_delta"
            if new_name not in ds_by_name:
                new_ds = Dataset(
                    name=new_name,
                    kind=DatasetKind.delta,
                    linked_service=src_ds.linked_service,
                    location=DatasetLocation(container=layer, folder=ent),
                )
                self.datasets.append(new_ds)
                ds_by_name[new_name] = new_ds
            df.sink.dataset = new_name
            if layer == "silver":
                rewired[df.source.dataset] = new_name
            self.assumptions.append(
                f"{df.name} sank to its own source dataset '{df.source.dataset}' "
                f"(in-place overwrite) -> rewired sink to new Delta dataset "
                f"'{new_name}' in the {layer} container"
            )

        if rewired:
            for df in self.dataflows:
                if flow_layer(df) != "gold":
                    continue
                for ep in (df.source, *df.additional_sources):
                    if ep.dataset in rewired:
                        old = ep.dataset
                        ep.dataset = rewired[old]
                        self.assumptions.append(
                            f"{df.name} read bronze dataset '{old}' -> rewired to "
                            f"silver Delta dataset '{ep.dataset}'"
                        )
        return self

    @model_validator(mode="after")
    def _repair_linked_service_refs(self) -> "PipelineSpec":
        """LLMs (gemini-2.5-flash, observed 2026-07-06) declare one
        linked service but reference a second, invented NAME from a dataset (e.g.
        declare ls_olist_storage_bronze, reference ls_olist_orders_storage_source).
        When the reference is unambiguous — exactly one declared linked service of
        a kind that could serve the dataset — rewire it instead of failing
        pre-flight. Ambiguous references are left for pre-flight to block."""
        if not self.linked_services:
            return self
        ls_names = {ls.name for ls in self.linked_services}
        storage_ls = [
            ls for ls in self.linked_services
            if ls.type in ("AzureBlobFS", "AzureBlobStorage")
        ]
        http_ls = [ls for ls in self.linked_services if ls.type in ("HttpServer", "RestService")]
        sql_ls = [ls for ls in self.linked_services if ls.type == "AzureSqlDatabase"]

        for ds in self.datasets:
            if ds.linked_service in ls_names:
                continue
            if ds.kind == DatasetKind.azure_sql_table:
                candidates = sql_ls
            elif ds.relative_url is not None:
                candidates = http_ls
            elif ds.location is not None:
                candidates = storage_ls
            else:
                candidates = []
            if len(candidates) != 1 and len(self.linked_services) == 1:
                # A single declared linked service is unambiguous whatever its type.
                candidates = list(self.linked_services)
            if len(candidates) != 1:
                continue
            old = ds.linked_service
            ds.linked_service = candidates[0].name
            self.assumptions.append(
                f"dataset '{ds.name}' referenced undeclared linked service '{old}' "
                f"-> rewired to '{ds.linked_service}'"
            )
        return self

    # ------------------------------------------------------------------ #
    # Naming
    # ------------------------------------------------------------------ #

    def naming_violations(self) -> List[str]:
        """Return ls_/ds_/df_/pl_ prefix violations (empty == clean)."""
        issues: List[str] = []
        groups = {
            "linked_services": self.linked_services,
            "datasets": self.datasets,
            "dataflows": self.dataflows,
            "pipelines": self.pipelines,
        }
        for field, items in groups.items():
            prefix = _PREFIXES[field]
            for item in items:
                if not item.name.startswith(prefix):
                    issues.append(f"{field}: '{item.name}' must start with '{prefix}'")
        return issues

    # ------------------------------------------------------------------ #
    # Cross-reference integrity (used by pre-flight, not at construction)
    # ------------------------------------------------------------------ #

    def cross_reference_errors(self) -> List[str]:
        errors: List[str] = []
        ls_names = {ls.name for ls in self.linked_services}
        ds_names = {ds.name for ds in self.datasets}
        df_names = {df.name for df in self.dataflows}
        pl_names = {pl.name for pl in self.pipelines}

        for ds in self.datasets:
            if ds.linked_service not in ls_names:
                errors.append(
                    f"dataset '{ds.name}' references unknown linked service "
                    f"'{ds.linked_service}'"
                )

        for df in self.dataflows:
            if df.source.dataset not in ds_names:
                errors.append(
                    f"dataflow '{df.name}' source references unknown dataset "
                    f"'{df.source.dataset}'"
                )
            if df.sink.dataset not in ds_names:
                errors.append(
                    f"dataflow '{df.name}' sink references unknown dataset "
                    f"'{df.sink.dataset}'"
                )
            for extra in df.additional_sources:
                if extra.dataset not in ds_names:
                    errors.append(
                        f"dataflow '{df.name}' additional source references unknown "
                        f"dataset '{extra.dataset}'"
                    )

        for pl in self.pipelines:
            for act in pl.activities:
                if act.type == "Copy":
                    for ref, kind in ((act.source_dataset, "source"), (act.sink_dataset, "sink")):
                        if ref is None or ref not in ds_names:
                            errors.append(
                                f"pipeline '{pl.name}' activity '{act.name}' Copy {kind} "
                                f"dataset '{ref}' not found"
                            )
                elif act.type == "ExecuteDataFlow":
                    if act.data_flow not in df_names:
                        errors.append(
                            f"pipeline '{pl.name}' activity '{act.name}' references unknown "
                            f"data flow '{act.data_flow}'"
                        )
                elif act.type == "ExecutePipeline":
                    if act.invoked_pipeline not in pl_names:
                        errors.append(
                            f"pipeline '{pl.name}' activity '{act.name}' invokes unknown "
                            f"pipeline '{act.invoked_pipeline}'"
                        )
                # NOTE: unknown depends_on names are NOT flagged. The LLM sometimes
                # points a child-pipeline activity at an activity in a SIBLING child
                # pipeline (cross-pipeline dependency). ADF has no such concept and the
                # builder deterministically DROPS any depends_on not present in the same
                # pipeline (see build_pipeline: `[d for d in depends_on if d in kept]`).
                # So a dangling dep is self-healing, not a blocker — don't fail pre-flight
                # on it.

        for tr in self.triggers:
            if tr.pipeline not in pl_names:
                errors.append(f"trigger '{tr.name}' references unknown pipeline '{tr.pipeline}'")

        return errors

    # Naming and cross-reference integrity are enforced by pre-flight (which
    # blocks approval), not as a hard parse-time failure — a single off-by-one
    # name from the LLM should surface as a fixable pre-flight finding, not make
    # the whole parse fail and burn retries.

    def is_buildable(self) -> bool:
        """True when the spec has enough structure to build SOMETHING reviewable:
        at least one pipeline with at least one activity, and at least one dataset.
        Used to decide whether open clarifications should block the plan (an
        unbuildable spec) or ride along as answerable notes (a buildable one)."""
        has_activity = any(pl.activities for pl in self.pipelines)
        return bool(self.datasets) and has_activity


def validate_naming(spec: PipelineSpec) -> List[str]:
    """Module-level helper mirroring ``PipelineSpec.naming_violations``."""
    return spec.naming_violations()
