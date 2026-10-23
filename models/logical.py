"""LogicalPlan: the ONLY thing the LLM produces. Business intent, zero ADF.

No resource names, no linked services, no containers, no DSL, no JSON schema.
The deterministic compiler (agent/compiler.py) turns this into a PipelineSpec.
Anything the model cannot express goes in `unsupported` — loud, never silent.

This is the search-space collapse: the model decides WHAT the business wants
(entities, cleanings, joins, outputs), and code decides everything ADF —
names, wiring, datasets, expressions. Invalid ADF syntax becomes
unrepresentable because there is no free-text expression field here; every
operation is a key of the closed op vocabulary shared with models/spec.py.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from models.spec import (
    _OP_ARITH,
    _OP_BLANK_DEFAULT,
    _OP_FN,
    _OP_IDENTITY,
    _OP_PROPER,
    _OP_QUARTER,
    _OP_REMOVE_SPECIAL,
    _norm_op,
)


def _snake(v: str, *, field: str) -> str:
    """Normalize a business name to snake_case (letters, digits, single _)."""
    s = re.sub(r"[^a-z0-9_]", "_", (v or "").strip().lower())
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        raise ValueError(f"{field} empty after normalization")
    return s


class SourceFormat(str, Enum):
    csv = "csv"
    text = "text"
    parquet = "parquet"
    json = "json"
    binary = "binary"
    sql_table = "sql_table"
    http_csv = "http_csv"  # CSV fetched from a public HTTP(S) URL


class OutputFormat(str, Enum):
    delta = "delta"
    csv = "csv"
    parquet = "parquet"


class LogicalColumn(BaseModel):
    name: str
    type: str = "string"  # string|integer|long|double|decimal(p,s)|boolean|date|timestamp


class LogicalSource(BaseModel):
    """One business entity to ingest. `entity` is a snake_case business name
    (e.g. 'orders', 'customers') — NEVER an ADF resource name."""

    entity: str
    format: SourceFormat
    # storage-resident source (format csv/parquet/json):
    container: Optional[str] = None
    folder: Optional[str] = None
    file_name: Optional[str] = None
    # http source (format http_csv):
    http_url: Optional[str] = None  # full URL to the file
    # sql source (format sql_table):
    server: Optional[str] = None
    database: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    columns: List[LogicalColumn] = Field(default_factory=list)  # optional; grounding overrides
    # BRD asks "verify the source file exists / is non-empty before copying":
    # the compiler emits a Validation activity gating the bronze copy — it waits
    # for the file to exist AND have >= 1 byte; on timeout it fails, which blocks
    # the copy and everything downstream of it. Storage sources only.
    validate_first: bool = False

    @field_validator("entity")
    @classmethod
    def _norm_entity(cls, v: str) -> str:
        return _snake(v, field="entity")

    @model_validator(mode="after")
    def _location_complete(self) -> "LogicalSource":
        if self.format == SourceFormat.http_csv and not self.http_url:
            raise ValueError(f"source '{self.entity}': http_csv needs http_url")
        if self.format == SourceFormat.sql_table and not (
            self.server and self.database and self.table_name
        ):
            raise ValueError(
                f"source '{self.entity}': sql_table needs server, database, table_name"
            )
        if self.format in (SourceFormat.csv, SourceFormat.parquet, SourceFormat.json) and not (
            self.file_name or self.folder
        ):
            raise ValueError(f"source '{self.entity}': storage source needs file_name (or folder)")
        return self


_CASE_CMP_OPS = {"lt", "le", "gt", "ge", "eq", "ne"}


class CaseBranch(BaseModel):
    """One WHEN clause of a `case` cleaning op: test `column <op> value`, and
    if it matches, output `then` (a column name or a literal)."""

    op: str  # lt | le | gt | ge | eq | ne
    value: str  # threshold compared against the CleaningOp's `column`
    then: str  # output when this branch matches: a column name or a literal

    @field_validator("op")
    @classmethod
    def _known(cls, v: str) -> str:
        if _norm_op(v) not in _CASE_CMP_OPS:
            raise ValueError(
                f"unknown case branch op '{v}'. Allowed: "
                f"{', '.join(sorted(_CASE_CMP_OPS))}"
            )
        return v


class CleaningOp(BaseModel):
    """One column operation. `op` MUST be in the closed vocabulary.

    Arithmetic ops (add/subtract/multiply/divide): `column` is the FIRST operand
    and `args` are the remaining operands — each may be another column name or a
    numeric literal. Chain steps through `new_column`, e.g.
    line_total = quantity * list_price * (1 - discount):
      {op: subtract, column: "1", args: [discount], new_column: one_minus_discount}
      {op: multiply, column: quantity, args: [list_price, one_minus_discount],
       new_column: line_total}

    Conditional values (op `case`): ordered `when` branches tested against
    `column`, first match wins; `otherwise` is the default. `then`/`otherwise`
    are each a column name or a literal. Clamp negatives to zero:
      {op: case, column: quantity, when: [{op: lt, value: "0", then: "0"}],
       otherwise: quantity, new_column: quantity_clamped}
    Multi-way buckets (price_band, customer segment) chain more `when`
    branches — order tightest/most-specific conditions first:
      {op: case, column: revenue,
       when: [{op: ge, value: "10000", then: "'Platinum'"},
              {op: ge, value: "5000", then: "'Gold'"}],
       otherwise: "'Bronze'", new_column: segment}
    """

    op: str  # any key of _OP_FN / _OP_ARITH / _OP_PROPER / _OP_IDENTITY, or 'case'
    column: str  # input column (first operand for arithmetic ops; test column for case)
    args: List[str] = Field(default_factory=list)  # literals / arithmetic operands
    new_column: Optional[str] = None  # output name; None = replace column in place
    # case-only:
    when: List[CaseBranch] = Field(default_factory=list)
    otherwise: Optional[str] = None

    @field_validator("op")
    @classmethod
    def _known(cls, v: str) -> str:
        key = _norm_op(v)
        if key == "case":
            return v
        if (
            key not in _OP_FN
            and key not in _OP_IDENTITY
            and key not in _OP_ARITH
            and key not in _OP_PROPER
            and key not in _OP_QUARTER
            and key not in _OP_REMOVE_SPECIAL
            and key not in _OP_BLANK_DEFAULT
        ):
            raise ValueError(
                f"unknown op '{v}'. Allowed: "
                f"{', '.join(sorted(set(_OP_FN) | set(_OP_ARITH) | {'proper', 'quarter', 'remove_special_characters', 'replace_empty_string'}))}, "
                "identity, case"
            )
        return v

    @model_validator(mode="after")
    def _case_shape(self) -> "CleaningOp":
        """`when`/`otherwise` are case-only fields — a hard error either way
        keeps the two forms from being silently mixed up (a non-case op with a
        stray `when` would otherwise just ignore it and confuse the author)."""
        is_case = _norm_op(self.op) == "case"
        if is_case and (not self.when or self.otherwise is None):
            raise ValueError(
                f"case op on '{self.column}': set at least one 'when' branch "
                "and 'otherwise' (the default value)"
            )
        if not is_case and (self.when or self.otherwise is not None):
            raise ValueError(
                f"'{self.op}' on '{self.column}': 'when'/'otherwise' are "
                "case-only — use op 'case' for conditional values"
            )
        return self


_FILTER_OPS = {
    "notnull", "null", "eq", "ne", "gt", "ge", "lt", "le",
    "isnull", "isnotnull", "notempty", "exists", "missing",
    "like", "notlike", "regex", "regexmatch", "rlike", "matches", "notregex", "notrlike",
    "in", "notin", "between", "notbetween",
}


class FilterRule(BaseModel):
    op: str  # not_null | null | eq | ne | gt | ge | lt | le
    column: str
    value: Optional[str] = None

    @field_validator("op")
    @classmethod
    def _known(cls, v: str) -> str:
        if _norm_op(v) not in _FILTER_OPS:
            raise ValueError(f"unknown filter op '{v}'")
        return v

    @model_validator(mode="after")
    def _cmp_needs_value(self) -> "FilterRule":
        """A comparison filter without a value is unrenderable — downstream it
        would silently become an EMPTY filter() and fail in Azure as EXE-0001
        'Dataflow cannot be analyzed as a graph'. Fail at extraction instead,
        where Pydantic AI feeds the error back and the model retries."""
        if _norm_op(self.op) in {
            "eq", "ne", "gt", "ge", "lt", "le", "like", "notlike", "regex",
            "regexmatch", "rlike", "matches", "notregex", "notrlike", "in",
            "notin", "between", "notbetween",
        } and (
            self.value is None or not str(self.value).strip()
        ):
            raise ValueError(
                f"filter op '{self.op}' on column '{self.column}' requires a "
                "value to compare against"
            )
        return self


class EntityTransform(BaseModel):
    """Silver-layer cleaning for one entity. Absent entity = copy-only (no silver flow)."""

    entity: str
    casts: List[LogicalColumn] = Field(default_factory=list)  # target types
    cleaning: List[CleaningOp] = Field(default_factory=list)  # trim/upper/replace...
    filters: List[FilterRule] = Field(default_factory=list)  # AND-ed
    dedup_keys: List[str] = Field(default_factory=list)  # non-empty => dedup


class JoinPlan(BaseModel):
    left_entity: str
    right_entity: str
    left_keys: List[str]
    right_keys: List[str]
    type: str = "inner"  # inner | left | right | full

    @model_validator(mode="after")
    def _keys(self) -> "JoinPlan":
        if not self.left_keys or len(self.left_keys) != len(self.right_keys):
            raise ValueError(
                f"join {self.left_entity}->{self.right_entity}: left_keys and "
                "right_keys must be non-empty and same length"
            )
        return self


_AGG_OPS = {"sum", "avg", "mean", "min", "max", "count", "countdistinct", "first"}


class AggregationPlan(BaseModel):
    name: str  # output column name
    op: str  # sum|avg|min|max|count|count_distinct|first
    column: str

    @field_validator("op")
    @classmethod
    def _known(cls, v: str) -> str:
        if _norm_op(v) not in _AGG_OPS:
            raise ValueError(f"unknown aggregation op '{v}'")
        return v


class SortPlan(BaseModel):
    column: str
    order: str = "desc"  # asc | desc


class OutputPlan(BaseModel):
    """One gold/business output."""

    name: str  # snake_case business name, e.g. 'monthly_sales'
    from_entities: List[str]
    joins: List[JoinPlan] = Field(default_factory=list)
    derived: List[CleaningOp] = Field(default_factory=list)  # computed BEFORE aggregation
    group_by: List[str] = Field(default_factory=list)
    aggregations: List[AggregationPlan] = Field(default_factory=list)
    filters: List[FilterRule] = Field(default_factory=list)  # applied BEFORE aggregation
    # Computed AFTER aggregation, from aggregation/group_by output columns —
    # e.g. a customer `segment` bucketed by summed Revenue (op case, column =
    # an aggregation's `name`). Use `derived` instead for anything computed
    # from raw (pre-aggregation) source columns.
    post_agg_derived: List[CleaningOp] = Field(default_factory=list)
    sort: List[SortPlan] = Field(default_factory=list)
    top_n: Optional[int] = None
    columns: List[str] = Field(default_factory=list)  # final projection; [] = all
    format: OutputFormat = OutputFormat.delta

    @field_validator("name")
    @classmethod
    def _norm_name(cls, v: str) -> str:
        return _snake(v, field="output name")


class SchedulePlan(BaseModel):
    frequency: str = "Day"  # Day | Hour | Minute | Week | Month
    interval: int = 1
    start_time: Optional[str] = None


class LogicalPipeline(BaseModel):
    """Pipeline ORGANISATION the BRD asks for — how the work is grouped into
    pipelines. This is a real requirement (a BRD may name PL_Landing_To_Bronze,
    PL_Bronze_To_Silver, a master, etc.), so the model captures it and the
    compiler builds exactly that many — no fixed count baked into code. Membership
    is by BUSINESS name (entities / output names), never ADF resource names; the
    compiler assigns every ADF name and dependency.

    A pipeline is EITHER a work stage (fill ingest/silver/gold) OR an orchestrator
    (fill `invokes` with the names of other logical pipelines, in run order) — a
    master. Leave `LogicalPlan.pipelines` empty when the BRD does not specify a
    structure; the compiler then builds a single pipeline."""

    name: str  # logical/BRD name, e.g. "PL_Bronze_To_Silver" (compiler normalizes)
    ingest: List[str] = Field(default_factory=list)  # entities to land (bronze copy)
    silver: List[str] = Field(default_factory=list)  # entities to clean (silver flow)
    gold: List[str] = Field(default_factory=list)  # OUTPUT names to build (gold flow)
    invokes: List[str] = Field(default_factory=list)  # child pipeline names, in run order


_AZ_CONTAINER = re.compile(r"[a-z0-9](?:[a-z0-9-]{1,61})[a-z0-9]")


class ContainerPlan(BaseModel):
    """WHERE each medallion layer lands — the BRD's container names ('copy into
    the raw container', 'write gold to analytics'). Defaults to the medallion
    convention; the compiler uses these names for every sink dataset and for
    container provisioning."""

    bronze: str = "bronze"
    silver: str = "silver"
    gold: str = "gold"

    @field_validator("bronze", "silver", "gold")
    @classmethod
    def _azure_name(cls, v: str) -> str:
        s = (v or "").strip().lower()
        if not _AZ_CONTAINER.fullmatch(s):
            raise ValueError(
                f"'{v}' is not a valid Azure container name (3-63 chars, "
                "lowercase letters/digits/hyphens, starts and ends alphanumeric)"
            )
        return s


class AzureTargetPlan(BaseModel):
    """Deploy target NAMED BY THE BRD (resource group / factory / storage).
    Advisory only: the UI pre-fills its Azure config from it and the USER
    confirms — BRD text is untrusted input and never silently picks where the
    agent deploys."""

    resource_group: Optional[str] = None
    factory_name: Optional[str] = None
    storage_account: Optional[str] = None

    def is_empty(self) -> bool:
        return not (self.resource_group or self.factory_name or self.storage_account)


class LogicalPlan(BaseModel):
    """Complete business description of the pipeline. NO ADF concepts."""

    project: str = "pipeline"  # short snake_case name for the master pipeline
    sources: List[LogicalSource]
    transforms: List[EntityTransform] = Field(default_factory=list)
    outputs: List[OutputPlan] = Field(default_factory=list)
    # Pipeline organisation the BRD specifies. Empty -> compiler builds ONE
    # pipeline. Populated -> compiler builds exactly these (any count the BRD
    # asks for, master + stages included).
    pipelines: List[LogicalPipeline] = Field(default_factory=list)
    schedule: Optional[SchedulePlan] = None
    # Requirements the model recognized but this system cannot build.
    # LOUD capability contract: each is one plain-English line, e.g.
    # "file-existence validation gates (GetMetadata/IfCondition) — not supported".
    unsupported: List[str] = Field(default_factory=list)
    assumptions: List[str] = Field(default_factory=list)
    clarifications: List[str] = Field(default_factory=list)
    # For each unsupported item the triage loop keeps, one proposed CLOSEST
    # SUPPORTED alternative the user can accept via chat (advisory only).
    suggestions: List[str] = Field(default_factory=list)
    # Deploy target the BRD names (if any) — UI pre-fill + user confirm only.
    azure_target: Optional[AzureTargetPlan] = None
    # Layer -> container names; defaults to bronze/silver/gold unless the BRD
    # names them ("copy into the raw container").
    containers: ContainerPlan = Field(default_factory=ContainerPlan)

    @field_validator("project")
    @classmethod
    def _norm_project(cls, v: str) -> str:
        s = re.sub(r"[^a-z0-9_]", "_", (v or "pipeline").strip().lower())
        s = re.sub(r"_+", "_", s).strip("_")
        return s or "pipeline"
