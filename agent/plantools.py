"""Tool-calling plan agent: BRD text -> LogicalPlan, built one tool call at a time.

The tools layer over agent/planhandlers.py — the pydantic-ai equivalent of
DataFactory.MCP's ``[McpServerTool]`` classes. Each tool is thin: describe the
call for the LLM, delegate to the session handler, return its ToolResult
(``{ok, errors, warnings, state}``) so the model corrects mistakes per call.

Same public surface as agent/extractor.py — ``extract_plan`` / ``refine_plan``
returning ``(plan, message_history)`` — so app.py swaps modes with one env var
(PLAN_MODE=tools|oneshot). The deterministic compiler/preflight path after the
plan is unchanged.

Why this beats one-shot extraction: the model makes ~10 small decisions with
instant deterministic feedback instead of one giant JSON emission that either
lands or doesn't. An invalid op, a misspelled column, an unconnected join
fails THAT call with an actionable message — not the whole plan.
"""

from typing import Any, Dict, List, Optional, Tuple

from pydantic_ai import Agent, RunContext

from config import default_model_settings, get_model
from agent.planhandlers import PlanSession
from models.logical import (
    AggregationPlan,
    CleaningOp,
    FilterRule,
    JoinPlan,
    LogicalColumn,
    LogicalPlan,
    SortPlan,
)

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


def _system_prompt() -> str:
    from agent.extractor import capability_card
    from agent.skills import load_skills

    prompt = """\
You are a senior data engineer. Read the user's requirements (a BRD document or
plain sentences) and BUILD a logical plan by calling tools — one source, one
transform, one output at a time. You never design Azure Data Factory objects
(no resource names, datasets, JSON, expressions); a deterministic compiler does
that from your plan.

WORKFLOW — follow this order:
1. add_source for every business entity (file/table) the requirements name.
   Use the EXACT file names / URLs / table names given. entity = short
   snake_case business name (orders, customers). Storage formats: csv, text,
   parquet, json, binary. Binary is Bronze byte-copy only; do not transform it
   or use it in outputs.
2. add_transform for every entity that needs cleaning.
3. add_output for every business output asked for. Do NOT invent outputs.
4. set_pipelines ONLY if the requirements name pipelines or say how work is
   grouped (use their names; cover EVERY entity and output). Otherwise skip —
   one pipeline is built automatically.
5. set_project (short snake_case project name); set_schedule only for a
   RECURRING cadence stated or clearly implied (daily, hourly, every Monday...).
   "Manual trigger" / "run on demand" / "triggered manually" is NOT a
   schedule — do NOT call set_schedule for it, and do NOT note it as
   unsupported. It is the DEFAULT: a pipeline with no schedule already runs on
   demand from the ADF UI/API. Nothing to build, nothing to note.
6. note anything unsupported / assumptions made.
7. finalize_plan. If it returns errors, fix them with more tool calls and
   finalize again. When it returns ok=true, reply with one short sentence
   summarizing the plan — no JSON.

EVERY tool returns {ok, errors, warnings, state}. On ok=false, READ the errors
and fix that call — correct the arguments and retry, or adjust earlier pieces.
`state` is the current draft; trust it over memory.

RULES:
- TYPES FIRST: source files are raw text — every column used in arithmetic, a
  numeric/temporal filter, an aggregation (sum/avg), or a date-part op MUST
  appear in that entity's `casts` with its target type.
- NULL / DEFAULT FILL: op `coalesce` replaces null/missing/blank with a
  default. `replace` is ONLY string find/replace (two string args: old, new).
- EMPTY / INVALID STRING CLEANING: use op `replace_empty_string` with args
  [default] for blank string defaults, `regex_replace` for regex cleaning, and
  `remove_special_characters` to strip punctuation/special chars.
- FILTERS: supported filter ops are not_null/null/eq/ne/gt/ge/lt/le plus LIKE,
  regex/rlike, IN, and BETWEEN. For IN pass a comma-separated value string; for
  BETWEEN pass "low,high". Multiple filters are AND-ed.
- ARITHMETIC: derived measures use add/subtract/multiply/divide. `column` is
  the FIRST operand; `args` the rest (column names or numbers). Chain through
  new_column, e.g. line_total = quantity * list_price * (1 - discount):
    {op: subtract, column: "1", args: ["discount"], new_column: "one_minus_discount"}
    {op: multiply, column: "quantity", args: ["list_price", "one_minus_discount"],
     new_column: "line_total"}
- DATE PARTS: to group by Year/Month/... derive them on the output first:
  derived=[{op: year, column: order_ts, new_column: order_year}] then
  group_by=["order_year"].
- CONDITIONAL VALUES: op `case` computes a column from ordered `when` branches
  tested against `column` (first match wins: {op: lt|le|gt|ge|eq|ne, value,
  then}) plus `otherwise` (the default). `then`/`otherwise` are each a column
  name or a literal (quote strings: "'Gold'"). Clamp negative stock to 0:
  {op: case, column: quantity, when: [{op: lt, value: "0", then: "0"}],
  otherwise: quantity, new_column: quantity_clamped}. Multi-way buckets
  (price_band, segment tiers) chain more `when` branches, most-specific
  first. Put it in `derived` for raw source columns, or `post_agg_derived`
  for a column computed from an aggregation's result (e.g. segment by SUMMED
  revenue: column = the aggregation's name). Fully supported — do NOT mark
  conditional/if-else/case logic as unsupported. When the requirements name
  buckets/tiers WITHOUT thresholds, pick sensible ones, order branches so no
  branch is dead, and note('assumption', ...) each chosen threshold.
- NULL-KEY REMOVAL: "remove records with null primary/business keys" = a
  not_null filter on each key column in that entity's transform (same columns
  as dedup_keys). Fully supported.
- SOURCE VALIDATION: "verify the file exists / is non-empty before ingesting"
  = validate_first=true on that source. Fully supported.
- top N = sort + top_n. PROPER CASE = op `proper`. All fully supported.
- unsupported: every requirement you cannot express as a tool call — one short
  note('unsupported', ...) line each, plus one note('suggestion', ...) with
  the closest supported alternative. NEVER silently drop a requirement.
- assumptions: every decision the requirements left open — note('assumption', ...).
- clarifications: AT MOST 2, ONLY when a core fact is missing (e.g. no source
  location at all). Otherwise assume and record an assumption.
- On follow-up messages, EDIT the existing draft with tool calls (state shows
  it) — change only what was asked, then finalize_plan again.

WHAT THE SYSTEM CAN BUILD:
"""
    return prompt + capability_card() + ("\n\n" + load_skills() if load_skills() else "")


def build_plan_agent():
    agent = Agent(
        get_model(),
        deps_type=PlanSession,
        output_type=str,
        system_prompt=_system_prompt(),
        retries=4,
        model_settings=default_model_settings(),
    )

    @agent.tool
    def add_source(
        ctx: RunContext[PlanSession],
        entity: str,
        format: str,
        container: Optional[str] = None,
        folder: Optional[str] = None,
        file_name: Optional[str] = None,
        http_url: Optional[str] = None,
        server: Optional[str] = None,
        database: Optional[str] = None,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        columns: Optional[List[LogicalColumn]] = None,
        validate_first: bool = False,
    ) -> Dict[str, Any]:
        """Add (or replace) one business entity to ingest. format: csv | text |
        parquet | json | binary | sql_table | http_csv. Storage sources need file_name (or
        folder); http_csv needs http_url; sql_table needs server, database,
        table_name. columns: known schema [{name, type}] when the requirements
        state it. validate_first: gate the copy on the file existing and being
        non-empty."""
        return ctx.deps.upsert_source({
            "entity": entity, "format": format, "container": container,
            "folder": folder, "file_name": file_name, "http_url": http_url,
            "server": server, "database": database, "schema_name": schema_name,
            "table_name": table_name,
            "columns": [c.model_dump() for c in (columns or [])],
            "validate_first": validate_first,
        })

    @agent.tool
    def add_transform(
        ctx: RunContext[PlanSession],
        entity: str,
        casts: Optional[List[LogicalColumn]] = None,
        cleaning: Optional[List[CleaningOp]] = None,
        filters: Optional[List[FilterRule]] = None,
        dedup_keys: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Add (or replace) the silver-layer cleaning for one entity. casts:
        target types [{name, type}] — run BEFORE cleaning ops, so cast every
        column arithmetic/filters/aggregations need. cleaning: structured ops
        [{op, column, args, new_column}]. filters are AND-ed. Non-empty
        dedup_keys deduplicates."""
        return ctx.deps.upsert_transform({
            "entity": entity,
            "casts": [c.model_dump() for c in (casts or [])],
            "cleaning": [c.model_dump() for c in (cleaning or [])],
            "filters": [f.model_dump() for f in (filters or [])],
            "dedup_keys": dedup_keys or [],
        })

    @agent.tool
    def add_output(
        ctx: RunContext[PlanSession],
        name: str,
        from_entities: List[str],
        joins: Optional[List[JoinPlan]] = None,
        derived: Optional[List[CleaningOp]] = None,
        group_by: Optional[List[str]] = None,
        aggregations: Optional[List[AggregationPlan]] = None,
        filters: Optional[List[FilterRule]] = None,
        post_agg_derived: Optional[List[CleaningOp]] = None,
        sort: Optional[List[SortPlan]] = None,
        top_n: Optional[int] = None,
        columns: Optional[List[str]] = None,
        format: str = "delta",
    ) -> Dict[str, Any]:
        """Add (or replace) one gold/business output. Multi-entity outputs need
        joins connecting every entity. derived runs BEFORE aggregation (date
        parts for grouping go here). post_agg_derived runs AFTER aggregation —
        use it for a column computed from an aggregation's result, e.g. a
        customer segment bucketed by SUMMED revenue ({op: case, column:
        <aggregation name>, when: [...], otherwise: ...}). Filters whose column
        names an aggregation result apply AFTER the aggregation automatically —
        post-aggregation filters (e.g. QuantityOnHand > 0) are supported.
        group_by needs aggregations. top_n needs sort. columns = final
        projection ([] = all). format: delta | csv | parquet."""
        return ctx.deps.upsert_output({
            "name": name, "from_entities": from_entities,
            "joins": [j.model_dump() for j in (joins or [])],
            "derived": [d.model_dump() for d in (derived or [])],
            "group_by": group_by or [],
            "aggregations": [a.model_dump() for a in (aggregations or [])],
            "filters": [f.model_dump() for f in (filters or [])],
            "post_agg_derived": [d.model_dump() for d in (post_agg_derived or [])],
            "sort": [s.model_dump() for s in (sort or [])],
            "top_n": top_n, "columns": columns or [], "format": format,
        })

    @agent.tool
    def set_pipelines(ctx: RunContext[PlanSession], pipelines: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Set the pipeline ORGANISATION the requirements ask for (replaces any
        previous). One entry per pipeline: {name, ingest: [entities], silver:
        [entities], gold: [output names], invokes: [child pipeline names]}. A
        pipeline is EITHER a work stage (ingest/silver/gold) OR a master
        (invokes, in run order). Every entity and output must be covered. Only
        call when the requirements specify a structure."""
        return ctx.deps.set_pipelines(pipelines)

    @agent.tool
    def set_project(ctx: RunContext[PlanSession], project: str) -> Dict[str, Any]:
        """Set the short snake_case project name (names the master pipeline)."""
        return ctx.deps.set_project(project)

    @agent.tool
    def set_containers(
        ctx: RunContext[PlanSession],
        bronze: Optional[str] = None,
        silver: Optional[str] = None,
        gold: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Set the storage CONTAINER each layer lands in when the requirements
        name them ('copy into the raw container' -> bronze='raw'). Unset fields
        keep the bronze/silver/gold defaults. Lowercase letters/digits/hyphens."""
        return ctx.deps.set_containers(bronze, silver, gold)

    @agent.tool
    def set_azure_target(
        ctx: RunContext[PlanSession],
        resource_group: Optional[str] = None,
        factory_name: Optional[str] = None,
        storage_account: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Record the Azure deploy target the requirements EXPLICITLY name
        (resource group / Data Factory / storage account). Use the exact names
        given; leave a field out if not stated. Do NOT invent names. The user
        confirms this target in the UI before any deploy."""
        return ctx.deps.set_azure_target(resource_group, factory_name, storage_account)

    @agent.tool
    def set_schedule(
        ctx: RunContext[PlanSession],
        frequency: str,
        interval: int = 1,
        start_time: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Set the trigger schedule. frequency: Day | Hour | Minute | Week |
        Month. Only when the requirements state or clearly imply one."""
        return ctx.deps.set_schedule(frequency, interval, start_time)

    @agent.tool
    def note(ctx: RunContext[PlanSession], kind: str, text: str) -> Dict[str, Any]:
        """Record one line. kind: unsupported (requirement the system cannot
        build) | assumption (decision made instead of asking) | clarification
        (max 2, only when a core fact is missing) | suggestion (closest
        supported alternative for an unsupported item)."""
        return ctx.deps.add_note(kind, text)

    @agent.tool
    def remove(ctx: RunContext[PlanSession], kind: str, name: str) -> Dict[str, Any]:
        """Remove a piece of the draft. kind: source | transform | output |
        pipeline | schedule | unsupported | assumption | clarification |
        suggestion. name: the entity/output/pipeline name (for notes: the exact
        text)."""
        return ctx.deps.remove_item(kind, name)

    @agent.tool
    def plan_status(ctx: RunContext[PlanSession]) -> Dict[str, Any]:
        """Current draft + full consistency check. Call when unsure what has
        been built or what is still missing."""
        return ctx.deps.status()

    @agent.tool
    def finalize_plan(ctx: RunContext[PlanSession]) -> Dict[str, Any]:
        """The gate: consistency + compile + dataflow typecheck. Call when the
        draft is complete. ok=false lists exactly what to fix; ok=true means
        the plan builds — then reply with a one-sentence summary."""
        return ctx.deps.finalize()

    return agent


async def _run(
    prompt: str,
    message_history: "Optional[List[Any]]",
    base_plan: "Optional[LogicalPlan]",
) -> "Tuple[LogicalPlan, List[Any]]":
    import os

    from pydantic_ai.usage import UsageLimits

    from agent.retry import retry_async

    # pydantic-ai defaults to 50 requests per run — a big BRD (or a small model
    # needing more correction rounds) hits that mid-build and dies with
    # UsageLimitExceeded. Tunable; 100 covers a 9-entity/8-output plan.
    limits = UsageLimits(
        request_limit=int(os.environ.get("PLAN_TOOL_REQUEST_LIMIT", "100"))
    )

    async def attempt() -> "Tuple[PlanSession, Any]":
        # Fresh session per attempt: a mid-run provider blip may have already
        # applied tool calls, and replaying onto that state would double them.
        session = PlanSession.from_plan(base_plan)
        agent = build_plan_agent()
        result = await agent.run(
            prompt, deps=session, message_history=message_history, usage_limits=limits
        )
        return session, result

    session, result = await retry_async(attempt)
    _capture_usage(result)
    return session.plan(), result.all_messages()


async def extract_plan(
    brd_text: str, message_history: "Optional[List[Any]]" = None
) -> "Tuple[LogicalPlan, List[Any]]":
    """BRD text -> (plan, message_history), plan built via tool calls."""
    return await _run(brd_text, message_history, None)


async def refine_plan(
    instruction: str,
    message_history: "List[Any]",
    base_plan: "Optional[LogicalPlan]" = None,
) -> "Tuple[LogicalPlan, List[Any]]":
    """Continue the conversation: the session is rebuilt from ``base_plan`` (the
    last accepted plan) and the model edits it with tool calls."""
    return await _run(instruction, message_history, base_plan)
