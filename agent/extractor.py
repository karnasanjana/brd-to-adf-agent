"""LLM reasoning point: BRD text -> validated LogicalPlan.

The ONLY creative step in the plan path. The model decides WHAT the business
wants — entities, cleanings, joins, outputs — and nothing about ADF. The
deterministic compiler (agent/compiler.py) builds every name, dataset, and
expression. This is the search-space collapse: a ~40-line prompt asking for
business intent replaces the old ~200-line ADF-authoring prompt in
agent/parser.py.

There is NO free-text expression field in a LogicalPlan, so the model cannot
emit invalid ADF DSL — invalid syntax is unrepresentable. Anything it cannot
express goes in `unsupported` (loud), never silently dropped.

Mirrors agent/parser.py's shape (async run + message history) so the follow-up
chat continues the same conversation and edits the existing plan.
"""

from __future__ import annotations

from typing import Any, List, Optional, Tuple

from config import default_model_settings, get_model, structured_output
from models.logical import LogicalPlan

_LAST_USAGE: Any = None


def _capture_usage(result: Any) -> None:
    global _LAST_USAGE
    usage = getattr(result, "usage", None)
    _LAST_USAGE = usage() if callable(usage) else usage


def last_usage() -> Any:
    return _LAST_USAGE


def clear_usage() -> None:
    global _LAST_USAGE
    _LAST_USAGE = None


_SYSTEM_PROMPT = """\
You are a senior data engineer. Read the user's requirements (a BRD document or
plain sentences) and produce a LogicalPlan: WHAT data exists, HOW it must be
cleaned, and WHAT business outputs are needed. You do NOT design Azure Data
Factory objects — no pipeline names, no datasets, no JSON, no ADF expressions.
A deterministic compiler builds all of that from your plan.

Rules:
- sources: one entry per business entity (file or table) named in the
  requirements. entity = short snake_case business name (orders, customers).
  Use the EXACT file names / URLs / table names the requirements give.
- transforms: one entry per entity that needs cleaning. Express EVERYTHING as
  structured ops from the closed vocabulary — casts (target types), cleaning
  ops (trim/upper/lower/proper/replace/...), filters (not_null/eq/gt/...),
  dedup_keys. There is NO free-text expression field. If logic cannot be
  expressed with the vocabulary, put one plain-English line in `unsupported`
  instead.
- SOURCE FORMATS: storage files may be csv, text, parquet, json, or binary;
  http_csv is for public CSV URLs; sql_table is for Azure SQL tables. Binary is
  Bronze byte-copy only: do not transform it or use it in outputs.
- TYPES FIRST: source files are raw text — every column used in arithmetic,
  a numeric/temporal filter, an aggregation (sum/avg), or a date-part op MUST
  appear in that entity's `casts` with its target type. Casts run before
  cleaning ops, so establish the type, then transform (never the reverse).
- PROPER CASE: op `proper` capitalizes a column (first letter upper, rest
  lower) — fully supported, do NOT mark it unsupported.
- NULL / DEFAULT FILL: use op `coalesce` for "replace null/missing/blank with
  default". Do NOT use `replace` for this. `replace` is only for string
  find/replace and must have two string args: old value, new value.
- EMPTY / INVALID STRING CLEANING: use op `replace_empty_string` (args=[default])
  for blank string defaults, `regex_replace` for regex cleaning, and
  `remove_special_characters` for "strip special chars". These are supported.
- ARITHMETIC: derived measures use ops `add`/`subtract`/`multiply`/`divide`.
  `column` is the FIRST operand; `args` are the remaining operands — each may
  be another column name or a number. Chain steps via new_column. Example
  "line_total = quantity * list_price * (1 - discount)":
  cleaning/derived = [
    {op: subtract, column: "1", args: [discount], new_column: one_minus_discount},
    {op: multiply, column: quantity, args: [list_price, one_minus_discount],
     new_column: line_total}].
  Fully supported — do NOT mark arithmetic derived columns as unsupported.
- CONDITIONAL VALUES: op `case` computes a column from ordered WHEN branches
  tested against `column` (first match wins) plus `otherwise` (the default).
  Each branch is {op: lt|le|gt|ge|eq|ne, value, then}; `then`/`otherwise` are
  each a column name or a literal (quote string literals: "'Gold'"). Clamp
  negative stock to 0: {op: case, column: quantity,
  when: [{op: lt, value: "0", then: "0"}], otherwise: quantity,
  new_column: quantity_clamped}. Multi-way buckets (price_band, customer
  segment tiers) chain more `when` branches, most-specific first — e.g.
  segment by revenue: when: [{op: ge, value: "10000", then: "'Platinum'"},
  {op: ge, value: "5000", then: "'Gold'"}], otherwise: "'Bronze'". Put a
  `case` derived column in an output's `derived` list to compute it from raw
  source columns, or in `post_agg_derived` to compute it from an aggregation's
  result (e.g. segment by SUMMED revenue: column = the aggregation's `name`).
  Fully supported — do NOT mark conditional/case/if-else logic as unsupported.
  When the requirements name buckets/tiers WITHOUT thresholds (e.g. "StockStatus:
  OutOfStock/LowStock/InStock" with no numbers), pick sensible thresholds,
  order branches so every value lands in the intended bucket (no dead
  branches), and record EACH chosen threshold as one `assumptions` line so the
  user can correct it.
- NULL-KEY REMOVAL: "remove records with null primary/business keys" = a
  not_null filter on each key column in that entity's transform (the same
  columns as dedup_keys). Fully supported — never skip or mark unsupported.
- SOURCE VALIDATION: when the requirements say to verify a source file exists /
  is non-empty before ingesting (Get Metadata / Validation gates), set
  `validate_first: true` on that source. The pipeline then waits for the file,
  and a missing/empty file fails the gate and blocks that entity's copy and
  everything downstream. Fully supported — do NOT mark it unsupported.
- FILTERS: supported filter ops are not_null/null/eq/ne/gt/ge/lt/le plus LIKE,
  regex/rlike, IN, and BETWEEN. For IN use a comma-separated value string; for
  BETWEEN use "low,high". Multiple filters are AND-ed; OR logic belongs in a
  `case`/derived output or unsupported if it cannot be expressed. An output's
  filter whose column names an aggregation result (e.g. QuantityOnHand > 0
  where QuantityOnHand = sum(quantity)) is applied AFTER the aggregation
  automatically — post-aggregation filters are fully supported, do NOT mark
  them unsupported.
- outputs: one entry per business output the requirements ask for. Joins list
  the key columns on both sides. Aggregations use sum/avg/min/max/count/
  count_distinct/first. "top N" = sort + top_n. Do NOT invent outputs the
  requirements never asked for.
- DATE PARTS: to group by or output Year / Quarter / Month / Day / Hour / week
  from a timestamp or date column, add a `derived` column on the output with op
  `year`, `quarter` (1-4), `month`, `dayofmonth`, `dayofweek`, `dayofyear`,
  `hour`, `minute`, `second`, or `weekofyear` (column = the timestamp), give it
  a new_column name, then put that new_column in `group_by`. Example for "monthly sales by year and month":
  derived = [{name: order_year, op: year, column: order_purchase_timestamp},
  {name: order_month, op: month, column: order_purchase_timestamp}], then
  group_by = [order_year, order_month] with the sum aggregation. This is fully
  supported — do NOT mark year/month grouping as unsupported.
- pipelines: capture the pipeline ORGANISATION the requirements ask for. If the
  BRD names pipelines (e.g. "PL_Landing_To_Bronze", "PL_Bronze_To_Silver", a
  master) or says how the work is grouped, list one entry per pipeline: put its
  member entities in `ingest` (bronze copies) / `silver` (cleaning) and its
  output names in `gold`; for a master/orchestrator, fill `invokes` with the
  other pipeline names IN RUN ORDER and leave ingest/silver/gold empty. Use the
  BRD's own names. Cover EVERY entity and output across the stage pipelines — do
  not leave one out. If the BRD does NOT specify any pipeline structure, leave
  `pipelines` empty and one pipeline will be built. Never invent a structure the
  BRD didn't ask for.
- containers: when the requirements name the CONTAINER a layer lands in
  ("copy into the raw container", "write gold to the analytics container"),
  set `containers` {bronze, silver, gold} to those EXACT names (bronze = the
  landing/raw copy layer). Leave unset for the default bronze/silver/gold.
  Azure container names are lowercase letters/digits/hyphens.
- azure_target: when the requirements EXPLICITLY name the Azure resource group,
  Data Factory, or storage account to deploy into ("Resource Group: X",
  "Data Factory: Y", "Storage Account: Z"), record them in `azure_target`
  {resource_group, factory_name, storage_account} using the EXACT names given.
  Never invent names; leave unset when not stated. The user confirms the
  target in the UI before any deploy.
- schedule: only when the requirements state or clearly imply a recurring
  cadence (daily, hourly, every Monday...). "Manual trigger" / "run on demand" /
  "triggered manually" is NOT a schedule — leave `schedule` unset for it. This
  is fully supported and is the DEFAULT (a pipeline with no schedule already
  runs on demand from the ADF UI/API) — never mark manual/on-demand triggering
  as unsupported; no plan entry is needed at all.
- BUILT-IN (never mark these unsupported; no plan entry needed):
  * manual / on-demand trigger — leaving `schedule` unset IS the manual
    trigger; nothing else to build.
  * stage ordering / failure handling — "Silver runs only after Bronze
    succeeds", "a failed activity fails the master": the compiler chains every
    activity and pipeline with Succeeded dependency conditions automatically.
  * dataset/pipeline parameterization, naming conventions, linked services —
    ADF implementation details the compiler owns. If the BRD asks for them,
    record one line in `assumptions` (e.g. "datasets use compiler naming
    convention instead of runtime parameters").
- unsupported: every requirement you recognized but cannot express (alerting,
  archive/delete steps, SCD2, incremental merge, ForEach patterns). One short
  line each. NEVER silently drop a requirement.
- suggestions: for each unsupported item, one line proposing the closest
  SUPPORTED alternative (e.g. "SCD2 not supported -> keep latest row via
  dedup_keys instead?"). The user can accept it in chat.
- assumptions: every decision you made that the requirements left open
  (dedup keys chosen, date formats, join types). Short lines.
- clarifications: AT MOST 2, ONLY when a core fact is missing (e.g. no source
  location at all). Otherwise assume and record in assumptions.
- This is a conversation: on follow-up messages, return the FULL updated plan
  with the requested change applied, everything else unchanged.
"""


def _system_prompt() -> str:
    from agent.skills import load_skills

    skills = load_skills()
    return f"{_SYSTEM_PROMPT}\n\n{skills}" if skills else _SYSTEM_PROMPT


def capability_card() -> str:
    """One-paragraph statement of what the system CAN build, generated from the
    LIVE op vocabulary — used by the unsupported-triage loop so the feedback
    prompt never drifts from the code."""
    from models.spec import _OP_ARITH, _OP_FN

    ops = sorted(set(_OP_FN) | set(_OP_ARITH) | {"proper", "quarter"})
    return (
        "COLUMN OPS (cleaning/derived): " + ", ".join(ops) + ", case. Arithmetic ops "
        "(add/subtract/multiply/divide) take the first operand in `column` and the "
        "rest in `args` (column names or numbers), chained through new_column. `case` "
        "computes a conditional value from ordered `when` branches (lt/le/gt/ge/eq/ne "
        "vs `column`, first match wins) plus `otherwise` — covers if/else, clamping, "
        "and multi-way buckets (price bands, customer segments); NOT unsupported.\n"
        "FILTERS: not_null/null/eq/ne/gt/ge/lt/le/like/regex/rlike/in/between "
        "(AND-ed). CASTS: target types. "
        "DEDUP: dedup_keys.\n"
        "OUTPUTS: joins (inner/left/right/full), aggregations (sum/avg/min/max/"
        "count/count_distinct/first), group_by (date parts via derived year/"
        "quarter/month/dayofmonth/... columns), sort, top_n, column projection, "
        "delta/csv/parquet. post_agg_derived: `case`/derived columns computed "
        "from an aggregation's output (e.g. segment tiers bucketed by a SUMMED "
        "revenue column). Filters naming an aggregation's output column apply "
        "AFTER the aggregation automatically (e.g. sum(quantity) > 0) — "
        "post-aggregation filters are supported.\n"
        "SOURCE VALIDATION: validate_first=true on a source — the file must exist "
        "and be non-empty or that entity's copy and everything downstream is blocked.\n"
        "BUILT-IN AUTOMATICALLY (never unsupported): manual/on-demand trigger "
        "(leave `schedule` unset — that IS the manual trigger, nothing else to "
        "build), medallion stage ordering, Succeeded dependency conditions, "
        "failure propagation to the master pipeline, schedules, BRD-named "
        "pipeline organisation.\n"
        "CONTAINERS: BRD-named layer containers are supported — set `containers` "
        "{bronze, silver, gold} ('copy into the raw container' -> bronze='raw').\n"
        "COMPILER-OWNED IMPLEMENTATION DETAILS (record as assumptions, not "
        "unsupported): resource naming, dataset/pipeline parameterization, linked "
        "services."
    )


def build_extractor_agent():
    from pydantic_ai import Agent

    return Agent(
        get_model(),
        output_type=structured_output(LogicalPlan),
        system_prompt=_system_prompt(),
        retries=4,
        model_settings=default_model_settings(),
    )


async def extract_plan(
    brd_text: str, message_history: "Optional[List[Any]]" = None
) -> "Tuple[LogicalPlan, List[Any]]":
    """Run the extractor and return ``(plan, message_history)``. The history feeds
    :func:`refine_plan` so the follow-up chat continues the same conversation."""
    from agent.retry import retry_async

    agent = build_extractor_agent()
    result = await retry_async(lambda: agent.run(brd_text, message_history=message_history))
    _capture_usage(result)
    return result.output, result.all_messages()


async def refine_plan(
    instruction: str, message_history: "List[Any]"
) -> "Tuple[LogicalPlan, List[Any]]":
    """Continue the conversation: apply a change / answer a clarification / fix a
    validation problem, returning the updated ``(plan, message_history)``."""
    from agent.retry import retry_async

    agent = build_extractor_agent()
    result = await retry_async(lambda: agent.run(instruction, message_history=message_history))
    _capture_usage(result)
    return result.output, result.all_messages()


def extract_plan_sync(brd_text: str) -> LogicalPlan:
    """Blocking variant for scripts / threads with no running event loop."""
    from agent.retry import retry_sync

    agent = build_extractor_agent()
    result = retry_sync(lambda: agent.run_sync(brd_text))
    _capture_usage(result)
    return result.output
