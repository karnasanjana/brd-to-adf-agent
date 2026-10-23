"""FastAPI orchestration + UI for the BRD -> ADF medallion agent.

Endpoints
---------
POST /plan           BRD (text, .txt/.md, or PDF upload) -> parse + build +
                     pre-flight. Returns run_id, summary, preflight, clarifications,
                     review, and the human-readable governance plan. No
                     Azure writes.
POST /chat           Continue the conversation for a run: answer the agent's
                     clarifying questions or request small changes. Re-parses with
                     full memory, rebuilds, re-runs pre-flight, returns the updated
                     review/plan. No Azure writes.
GET  /preview/{id}        review JSON.
GET  /preview/{id}/text   plain-text governance plan (the approval-gate artifact).
POST /approve        Requires a prior preview AND a passing pre-flight, THEN
                     connects to Azure (or the MOCK tenant), builds all objects,
                     and runs the master pipeline.
GET  /status/{id}    Live deploy + per-step run state + run IDs + errors + the
                     deploy verification report (intended vs actual factory counts,
                     prune results) + the self-heal audit log.
GET/POST /azure-config       Read / set the deploy target (subscription, resource
                             group, factory, storage) for this server session.
POST /azure-config/test      Read-only connectivity check of the target.
GET  /               The single-page UI.

The single agent reasons in a few tight places — parsing the BRD (/plan), refining
it on request (/chat), and self-healing a failed artifact during deploy/run
(/approve). Every reasoning step returns a validated PipelineSpec.
"""

from __future__ import annotations

import contextlib
import hmac
import json
import logging
import os
import shutil
import sys
import threading
import time
import uuid
from typing import Any, Dict, Optional

from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel

from agent import builder, preflight, review
from config import mock_mode
from models.spec import PipelineSpec
from ui import INDEX_HTML

app = FastAPI(title="BRD -> ADF Medallion Agent")

# In-memory run store, mirrored to <run_dir>/run_state.json after every status
# change so a server restart/reload does not orphan built or in-flight runs
# (see _persist_run/_get_run). Conversation history is process-local: /chat on a
# rehydrated run returns 409.
_RUNS: Dict[str, Dict[str, Any]] = {}


@app.middleware("http")
async def _require_api_token(request: Request, call_next):
    """Optional auth for non-localhost deployments: when API_TOKEN is set, every
    mutating (POST) endpoint requires a matching X-API-Token header. Unset =
    open (the default localhost/dev setup). GETs stay open — they expose no
    deploy controls."""
    token = os.environ.get("API_TOKEN", "").strip()
    if token and request.method == "POST":
        supplied = request.headers.get("x-api-token", "")
        if not hmac.compare_digest(supplied, token):
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing or invalid X-API-Token header."},
            )
    return await call_next(request)


class ApproveRequest(BaseModel):
    run_id: str
    # Required (true) to deploy a plan with `unsupported` requirements — the
    # explicit waiver that stops a known-incomplete pipeline slipping through.
    acknowledge_unsupported: bool = False


class ChatRequest(BaseModel):
    run_id: str
    message: str


# --------------------------------------------------------------------------- #
# Run-state persistence — a restart must not orphan a built/deployed run
# --------------------------------------------------------------------------- #

# JSON-safe subset of a run record (no message history, no threading.Event).
_PERSIST_KEYS = (
    "status", "run_dir", "spec", "preflight", "previewed", "mock",
    "clarifications", "containers_created", "deployed", "deploy_report",
    "run", "report", "heals", "heal_log", "error",
)
# In-flight statuses that cannot survive a restart (the background task died).
_LIVE_STATUSES = {"APPROVED", "PROVISIONING", "DEPLOYING", "RUNNING"}


def _persist_run(run_id: str) -> None:
    """Mirror the run's JSON-safe state next to its artifacts. Best-effort —
    persistence must never break the request that triggered it."""
    run = _RUNS.get(run_id)
    if not run or not run.get("run_dir") or not os.path.isdir(run["run_dir"]):
        return
    try:
        state = {k: run[k] for k in _PERSIST_KEYS if k in run}
        with open(os.path.join(run["run_dir"], "run_state.json"), "w", encoding="utf-8") as fh:
            json.dump(state, fh, indent=2, default=str)
    except Exception:  # noqa: BLE001
        pass


def _get_run(run_id: str) -> "Optional[Dict[str, Any]]":
    """The run record — from memory, else rehydrated from run_state.json (server
    restarted). A run that was mid-deploy/mid-run comes back as INTERRUPTED and
    can be re-approved (deploys are idempotent). The LLM conversation does not
    survive a restart, so /chat on a rehydrated run 409s."""
    run = _RUNS.get(run_id)
    if run is not None:
        return run
    path = os.path.join(builder.artifacts_dir(run_id), "run_state.json")
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            state = json.load(fh)
    except Exception:  # noqa: BLE001
        return None
    if state.get("status") in _LIVE_STATUSES:
        state["status"] = "INTERRUPTED"
        state["error"] = (
            "Server restarted while this run was in flight — approve again to "
            "redeploy (deploys are idempotent)."
        )
    _RUNS[run_id] = state
    return state


# --------------------------------------------------------------------------- #
# UI
# --------------------------------------------------------------------------- #


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return INDEX_HTML


@app.get("/health")
def health() -> Dict[str, Any]:
    from agent.validator import self_heal_enabled

    return {
        "ok": True,
        "mock": mock_mode(),
        "self_heal": self_heal_enabled(),
        "azure_configured": bool(
            os.environ.get("AZURE_SUBSCRIPTION_ID")
            and os.environ.get("AZURE_RESOURCE_GROUP")
            and os.environ.get("ADF_FACTORY_NAME")
            and os.environ.get("ADLS_ACCOUNT_NAME")
        ),
    }


# --------------------------------------------------------------------------- #
# Azure connection - set from environment or API, not hardcoded in the repo
# --------------------------------------------------------------------------- #

# UI field -> environment variable the rest of the system reads.
_AZ_FIELDS = {
    "subscription_id": "AZURE_SUBSCRIPTION_ID",
    "resource_group": "AZURE_RESOURCE_GROUP",
    "factory_name": "ADF_FACTORY_NAME",
    "storage_account": "ADLS_ACCOUNT_NAME",
    "storage_kind": "STORAGE_KIND",  # blob | adls
    "location": "AZURE_LOCATION",
}


class AzureConfigRequest(BaseModel):
    subscription_id: Optional[str] = None
    resource_group: Optional[str] = None
    factory_name: Optional[str] = None
    storage_account: Optional[str] = None
    storage_kind: Optional[str] = None
    location: Optional[str] = None


@app.get("/azure-config")
def get_azure_config() -> Dict[str, Any]:
    """Current deploy target (env or values set via POST). Nothing secret here —
    credentials stay in DefaultAzureCredential (az login / Managed Identity)."""
    return {field: os.environ.get(env, "") for field, env in _AZ_FIELDS.items()}


@app.post("/azure-config")
def set_azure_config(req: AzureConfigRequest) -> Dict[str, Any]:
    """Set the deploy target for this server session. Non-empty fields override the
    environment; the .env file is never modified. Clears a stale ADLS_ACCOUNT_URL
    when the storage account changes so the endpoint is re-derived."""
    for field, env in _AZ_FIELDS.items():
        value = (getattr(req, field) or "").strip()
        if value:
            os.environ[env] = value
            if env == "ADLS_ACCOUNT_NAME":
                os.environ.pop("ADLS_ACCOUNT_URL", None)
    return {"saved": True, **get_azure_config()}


@app.post("/azure-config/test")
def test_azure_config() -> Dict[str, Any]:
    """Verify the target before anyone approves a deploy: credential resolves, the
    Data Factory exists, the storage account answers. Read-only."""
    if mock_mode():
        return {"ok": True, "checks": [{"name": "mock_mode", "passed": True,
                                        "detail": "MOCK=true — no Azure calls"}]}
    from config import AzureConfig, get_datafactory_client

    checks = []
    try:
        cfg = AzureConfig.from_env()
        checks.append({"name": "config_present", "passed": True,
                       "detail": f"factory '{cfg.factory_name}' in '{cfg.resource_group}'"})
    except RuntimeError as exc:
        return {"ok": False, "checks": [{"name": "config_present", "passed": False,
                                         "detail": str(exc)}]}
    try:
        adf = get_datafactory_client(cfg)
        adf.factories.get(cfg.resource_group, cfg.factory_name)
        checks.append({"name": "data_factory_reachable", "passed": True,
                       "detail": cfg.factory_name})
    except Exception as exc:  # noqa: BLE001
        hint = _azure_error_hint(exc)
        checks.append({"name": "data_factory_reachable", "passed": False,
                       "detail": f"{type(exc).__name__}: {exc}" + (f" — {hint}" if hint else "")})
    try:
        from config import get_blob_service_client

        get_blob_service_client(cfg).get_account_information()
        checks.append({"name": "storage_reachable", "passed": True,
                       "detail": cfg.adls_account_name})
    except Exception as exc:  # noqa: BLE001
        blob = f"{exc}".lower()
        if "403" in blob or "authorizationpermissionmismatch" in blob:
            # Account exists; deploy identity just lacks a data-plane role (fine —
            # the ADF Managed Identity does the runtime reads/writes).
            checks.append({"name": "storage_reachable", "passed": True,
                           "detail": f"{cfg.adls_account_name} (exists; no data-plane role on deploy identity)"})
        else:
            checks.append({"name": "storage_reachable", "passed": False,
                           "detail": f"{type(exc).__name__}: {exc}"})
    return {"ok": all(c["passed"] for c in checks), "checks": checks}


# --------------------------------------------------------------------------- #
# /plan — parse + build + pre-flight (NO Azure writes)
# --------------------------------------------------------------------------- #


def _summary(spec: PipelineSpec) -> Dict[str, Any]:
    return {
        "name": spec.name,
        "source": spec.source.model_dump() if spec.source else None,
        "linked_services": [ls.name for ls in spec.linked_services],
        "datasets": [ds.name for ds in spec.datasets],
        "dataflows": [df.name for df in spec.dataflows],
        "pipelines": [pl.name for pl in spec.pipelines],
        "triggers": [tr.name for tr in spec.triggers],
    }


def _ground_during_plan() -> bool:
    """Whether /plan reads source-file HEADERS from storage to ground schemas.

    READ-ONLY (first 64 KB of files the BRD already names) — never a write, so
    the "nothing is created in your tenant before approval" contract holds. It
    is on by default because an ungrounded spec is the #1 runtime failure (the
    LLM guesses column lists, the flow dies with DF-EXPR-010). Set
    GROUND_PLAN=false for a fully offline plan phase — grounding then runs at
    /approve, just before the deploy that reads those files anyway."""
    return os.environ.get("GROUND_PLAN", "true").strip().lower() not in {"false", "0", "no", "off"}


def _store_and_respond(
    run_id: str, spec: PipelineSpec, messages: Any, phases: "Optional[_Phases]" = None
) -> Dict[str, Any]:
    """Persist conversation state and build the /plan|/chat response payload.

    Clarifications only BLOCK when the spec is genuinely unbuildable (no pipeline
    or no datasets). A buildable spec with open questions still builds, previews,
    and can be approved — the questions ride along as answerable notes. This is
    what stops "the agent asks a lot of questions and nothing happens".
    """
    if spec.clarifications and not spec.is_buildable():
        prev = _RUNS.get(run_id, {})
        _RUNS[run_id] = {
            **prev,
            "status": "NEEDS_CLARIFICATION",
            "clarifications": spec.clarifications[:2],  # hard cap: max 2 questions
            "messages": messages,
            # Drop any stale build — the spec isn't complete yet.
            "spec": spec.model_dump(),
            "previewed": False,
        }
        _RUNS[run_id].pop("run_dir", None)
        return {
            "run_id": run_id,
            "status": "NEEDS_CLARIFICATION",
            "clarifications": spec.clarifications[:2],
            "assumptions": spec.assumptions,
            "suggestions": spec.suggestions,
            "mock": mock_mode(),
        }

    # Ground schemas in REAL data before building: read file headers from storage
    # and stamp them into the spec, overriding whatever columns the LLM guessed
    # (or omitted). Read-only; never blocks — unreadable files keep the spec
    # as-is. Skipped entirely when GROUND_PLAN=false (offline plan phase).
    if _ground_during_plan():
        timer = phases.phase("grounding") if phases else contextlib.nullcontext()
        with timer:
            try:
                from agent.grounding import ground_spec

                for note in ground_spec(spec):
                    if note not in spec.assumptions:  # /chat re-runs this — don't duplicate
                        spec.assumptions.append(note)
            except Exception:  # noqa: BLE001 — grounding is best-effort by design
                pass

    # Rebuild from a clean directory so renamed/removed objects don't linger.
    run_dir = builder.artifacts_dir(run_id)
    if os.path.isdir(run_dir):
        shutil.rmtree(run_dir, ignore_errors=True)
    try:
        run_dir = builder.write_artifacts(spec, run_id)
        pf = preflight.validate(run_id)
    except Exception as exc:  # build error -> clean message, not a raw 500
        raise HTTPException(
            status_code=502, detail=f"Could not build pipelines from the spec: {exc}"
        ) from exc
    _RUNS[run_id] = {
        "status": "AWAITING_APPROVAL" if pf["passed"] else "PREFLIGHT_FAILED",
        "run_dir": run_dir,
        "spec": spec.model_dump(mode="json"),
        "messages": messages,
        "preflight": pf,
        "previewed": True,  # the response carries the full review
        "mock": mock_mode(),
    }
    _persist_run(run_id)
    return {
        "run_id": run_id,
        "status": _RUNS[run_id]["status"],
        "mock": mock_mode(),
        "summary": _summary(spec),
        "preflight": pf,
        # Non-blocking: the plan is buildable, these are optional questions the
        # user MAY answer via chat before approving.
        "clarifications": spec.clarifications[:2],
        "assumptions": spec.assumptions,
        "suggestions": spec.suggestions,
        # Requirements the system could NOT build (post-triage). Non-empty means
        # /approve will demand an explicit acknowledgement.
        "unsupported": spec.unsupported,
        "review": review.render(run_id),
        "plan_text": review.render_text(run_id),
        "plan_markdown": review.render_markdown(run_id),
    }


def _legacy_parser() -> bool:
    """DEPRECATED. The pre-LogicalPlan path: the LLM authored the whole ADF
    PipelineSpec (names, datasets, DSL expressions) in one shot. It has none of
    the deterministic guarantees the compiler path gives — no typecheck, no
    completeness critic, no closed op vocabulary — and exists only as an escape
    hatch while the new path proves out. Slated for removal; do not build on it."""
    on = os.environ.get("LEGACY_PARSER", "").strip().lower() in {"1", "true", "yes", "on"}
    if on:
        import warnings

        warnings.warn(
            "LEGACY_PARSER=true uses the deprecated LLM-authors-ADF-JSON path "
            "(no typecheck, no completeness critic). It will be removed — unset "
            "it to use the LogicalPlan compiler path.",
            DeprecationWarning,
            stacklevel=2,
        )
    return on


def _plan_mode() -> str:
    """'auto' (default): FIRST generation is one-shot (a single model call —
    the fast, cheap path); every repair/refine goes through the tool agent,
    which edits the existing plan precisely instead of re-emitting all of it.
    'tools': everything through validated tool calls (most robust, most turns).
    'oneshot': everything through whole-plan re-emission (legacy behavior)."""
    mode = os.environ.get("PLAN_MODE", "auto").strip().lower()
    if mode in {"oneshot", "one-shot", "extract"}:
        return "oneshot"
    if mode == "tools":
        return "tools"
    return "auto"


def _plan_fns():
    """(extract, refine) for the active PLAN_MODE, normalized to one signature:
    refine(instruction, messages, base_plan).

    auto: extract = one-shot with a TOOL-AGENT FALLBACK — a large BRD can defeat
    the single giant JSON emission ('Exceeded maximum output retries'), and the
    tool agent builds the same plan in small validated calls, so auto retries
    that way once before failing. refine = tool agent on a FRESH conversation
    whose context is the plan itself (the session is rebuilt from base_plan) —
    the one-shot history belongs to a different agent/system prompt and must
    not leak into the tool loop."""
    mode = _plan_mode()
    if mode == "tools":
        from agent.plantools import extract_plan, refine_plan

        return extract_plan, refine_plan
    if mode == "auto":
        from agent import extractor, plantools

        async def _extract(text, message_history=None):
            try:
                return await extractor.extract_plan(text, message_history)
            except Exception as exc:  # noqa: BLE001
                from pydantic_ai.exceptions import AgentRunError

                if not isinstance(exc, AgentRunError):
                    raise
                # One-shot emission failed on model output (transient HTTP was
                # already retried inside extract_plan) — build incrementally.
                return await plantools.extract_plan(text)

        async def _refine(instruction, messages, base_plan=None):
            return await plantools.refine_plan(instruction, None, base_plan)

        return _extract, _refine
    from agent.extractor import extract_plan, refine_plan

    async def _refine(instruction, messages, base_plan=None):
        return await refine_plan(instruction, messages)

    return extract_plan, _refine


_MAX_PLAN_REPAIRS = 3       # completeness-check repair rounds (before build)
_MAX_PREFLIGHT_REPAIRS = 2  # preflight repair rounds (after build)


class _Phases:
    """Wall-clock ms per phase (extract / repair / triage / compile / preflight /
    grounding) + the model-call count. Rides on the /plan|/chat response so a
    slow or chatty run is measurable instead of guessed at."""

    def __init__(self) -> None:
        self.ms: Dict[str, int] = {}
        self.llm_calls = 0
        self.usage: list = []

    @contextlib.contextmanager
    def phase(self, name: str, *, llm: bool = False):
        start = time.perf_counter()
        try:
            yield
        finally:
            self.ms[name] = self.ms.get(name, 0) + int((time.perf_counter() - start) * 1000)
            if llm:
                self.llm_calls += 1

    def report(self) -> Dict[str, Any]:
        out = {"ms": dict(self.ms), "total_ms": sum(self.ms.values()), "llm_calls": self.llm_calls}
        if self.usage:
            out["usage"] = self.usage
        return out

    def add_usage(self, phase: str, usage: Any) -> None:
        if usage is None:
            return
        if hasattr(usage, "model_dump"):
            payload = usage.model_dump()
        elif hasattr(usage, "__dict__"):
            payload = dict(usage.__dict__)
        else:
            payload = str(usage)
        self.usage.append({"phase": phase, "usage": payload})


def _latest_plan_usage() -> Any:
    for mod_name in ("agent.plantools", "agent.extractor"):
        mod = sys.modules.get(mod_name)
        if mod is None:
            continue
        getter = getattr(mod, "last_usage", None)
        if callable(getter):
            usage = getter()
            if usage is not None:
                return usage
    return None


def _clear_plan_usage() -> None:
    for mod_name in ("agent.plantools", "agent.extractor"):
        mod = sys.modules.get(mod_name)
        if mod is None:
            continue
        clear = getattr(mod, "clear_usage", None)
        if callable(clear):
            clear()


async def _run_logical(
    run_id: str,
    produce,
    cache_key: "Optional[str]" = None,
    requirements_text: "Optional[str]" = None,
) -> Dict[str, Any]:
    """New path: BRD/instruction -> LogicalPlan -> compile -> spec, with the model
    fixing its OWN plan against the deterministic checks. `produce` is an async
    callable returning (plan, messages) — extract_plan(text) or
    refine_plan(instruction, history).

    Two feedback loops close over the model, not the human:
      1. completeness.check() failures fed back before any build.
      2. preflight failures fed back after build (rare — compiler output is
         normally clean). Never deploys a failing spec; if neither loop
         converges the user sees the problems, exactly as the legacy path does.

    `cache_key` (first generation only) short-circuits the extract phase when
    the identical BRD was already planned by this model — zero LLM calls.
    """
    from agent.completeness import check
    from agent.compiler import compile_plan
    from agent import plancache

    _, refine_plan = _plan_fns()
    phases = _Phases()

    # The plan the human last reviewed (None on first generation) — diffed
    # against the new plan below so a refine shows exactly what changed.
    prev_plan = (_RUNS.get(run_id) or {}).get("logical_plan")

    cached = plancache.get(cache_key) if cache_key else None
    if cached is not None:
        plan, messages, phases.ms["extract_cached"] = cached, [], 0
    else:
        _clear_plan_usage()
        with phases.phase("extract", llm=True):
            plan, messages = await produce()
        phases.add_usage("extract", _latest_plan_usage())

    # Scope watermark: what the FIRST plan promised. A repair round can
    # satisfy the checker by DELETING the piece it was asked to fix — any
    # output that silently vanishes between here and the accepted plan is
    # flagged below, never dropped quietly.
    initial_outputs = {o.name for o in plan.outputs}

    def _ground(p):
        """Stamp real source schemas over whatever columns the model wrote.

        Every refine returns a FULL plan, so its sources carry freshly invented
        columns — grounding once after extract would leave repair rounds 2..n
        checking against hallucinated schemas. Head reads are memoized per
        (container, path), so re-grounding costs nothing after the first pass."""
        with phases.phase("ground_logical"):
            try:
                from agent.grounding import ground_logical_plan

                ground_logical_plan(p)
            except Exception:  # noqa: BLE001 - grounding helps checks; must not block planning
                pass
        try:
            # Deterministic name alignment (TenureBand vs tenure_band) — after
            # grounding so real schemas are in; each fix rides as an assumption.
            from agent.completeness import align_plan

            for note in align_plan(p):
                if note not in p.assumptions:
                    p.assumptions.append(note)
        except Exception:  # noqa: BLE001
            pass
        return p

    _ground(plan)

    def _with_requirements(instruction: str) -> str:
        if not requirements_text:
            return instruction
        return (
            "Original BRD / requirements:\n"
            f"{requirements_text}\n\n"
            f"{instruction}"
        )

    # Loop 1 — logical consistency, before building anything. (In tools mode
    # most problems are already caught per tool call; this is the safety net.)
    # Every problem a round FIXES is logged as a lesson candidate (repeated
    # model mistakes distill into skills/99-learned-lessons.md); whatever the
    # rounds CANNOT fix becomes questions for the user, not a dead-end error.
    from agent import triage

    # Repaired problems buffer here and are recorded as lesson candidates ONLY
    # if this run ends with a passing preflight — a run that ultimately failed
    # must not teach anything (guards against learning bad lessons).
    lesson_buffer: list = []

    for _ in range(_MAX_PLAN_REPAIRS):
        problems = check(plan)
        if not problems:
            break
        feedback = _with_requirements(
            "Your plan has problems. Fix ALL of them and return the full "
            "corrected plan:\n" + "\n".join(f"- {p}" for p in problems)
        )
        _clear_plan_usage()
        with phases.phase("repair_completeness", llm=True):
            plan, messages = await refine_plan(feedback, messages, plan)
        phases.add_usage("repair_completeness", _latest_plan_usage())
        _ground(plan)
        remaining = set(check(plan))
        lesson_buffer.extend(p for p in problems if p not in remaining)
    else:
        # Rounds exhausted — but the LAST refine may have converged (the loop
        # only re-checks at the top of an iteration). Route to the user only
        # when problems genuinely remain.
        problems = check(plan)
        if problems:
            # Last resort BEFORE a question wall: drop the unbuildable pieces
            # (transform ops / outputs referencing columns the data doesn't
            # have) and build the rest — each drop rides as a visible note.
            from agent.completeness import prune_unbuildable

            with phases.phase("prune_unbuildable"):
                prune_unbuildable(plan, problems)
            problems = check(plan)
        if problems:
            questions = triage.questions_for(problems)[:2]  # match the max-2 contract
            prev = _RUNS.get(run_id, {})
            _RUNS[run_id] = {
                **prev,
                "status": "NEEDS_CLARIFICATION",
                "clarifications": questions,
                "messages": messages,
                "logical_plan": plan,
                "previewed": False,
            }
            if requirements_text:
                _RUNS[run_id]["brd_text"] = requirements_text
            return {
                "run_id": run_id,
                "status": "NEEDS_CLARIFICATION",
                "clarifications": questions,
                "problems": problems,
                "mock": mock_mode(),
                "timings": phases.report(),
            }

    # Loop 1c — unsupported triage. Models over-mark: requirements land in
    # `unsupported` that the vocabulary CAN express (or that are built-in).
    # One feedback round with the capability card (generated from the live
    # vocabulary); whatever comes back still unsupported is a genuine gap —
    # it keeps a proposed alternative in `suggestions`, rides into the plan
    # review, and gates /approve until explicitly acknowledged.
    if plan.unsupported and cached is None:
        from agent.extractor import capability_card

        triage_prompt = _with_requirements(
            "You marked these requirements as unsupported:\n"
            + "\n".join(f"- {u}" for u in plan.unsupported)
            + "\n\nRe-check EACH against what the system CAN build:\n"
            + capability_card()
            + "\n\nReturn the FULL updated plan: express every item the "
            "vocabulary covers (and remove it from unsupported). For each item "
            "that genuinely cannot be built, keep its unsupported line AND add "
            "one `suggestions` line proposing the closest supported alternative "
            "the user could accept instead."
        )
        _clear_plan_usage()
        with phases.phase("triage_unsupported", llm=True):
            cand, cand_msgs = await refine_plan(triage_prompt, messages, plan)
        phases.add_usage("triage_unsupported", _latest_plan_usage())
        _ground(cand)
        if not check(cand):  # accept the triaged plan only if still consistent
            plan, messages = cand, cand_msgs

    # Scope guard: outputs the FIRST plan promised that no longer exist were
    # deleted by a repair round (deleting the offending piece satisfies the
    # checker). Never silent — each rides as a visible note the user can act on.
    for lost in sorted(initial_outputs - {o.name for o in plan.outputs}):
        if any(f"'{lost}'" in a and "not built" in a for a in plan.assumptions):
            continue  # prune_unbuildable already noted this drop
        note = (
            f"output '{lost}' was dropped during automatic repair — it could "
            "not be made consistent; ask in chat to restore or adjust it"
        )
        if note not in plan.assumptions:
            plan.assumptions.append(note)

    with phases.phase("compile"):
        spec = compile_plan(plan)
    resp = _store_and_respond(run_id, spec, messages, phases)

    # Loop 2 — compiled pipeline must pass preflight. Feed failures back.
    for _ in range(_MAX_PREFLIGHT_REPAIRS):
        if _RUNS[run_id].get("preflight", {}).get("passed"):
            break
        failed = [
            f"{c['name']}: {c['detail']}"
            for c in _RUNS[run_id].get("preflight", {}).get("checks", [])
            if not c["passed"]
        ]
        _clear_plan_usage()
        with phases.phase("repair_preflight", llm=True):
            plan, messages = await refine_plan(
                _with_requirements(
                    "The compiled pipeline failed validation. Adjust the plan to fix:\n"
                    + "\n".join(f"- {f}" for f in failed)
                ),
                messages,
                plan,
            )
        phases.add_usage("repair_preflight", _latest_plan_usage())
        _ground(plan)
        logical_problems = check(plan)
        if logical_problems:
            _clear_plan_usage()
            with phases.phase("repair_logical_after_preflight", llm=True):
                plan, messages = await refine_plan(
                    _with_requirements(
                        "Your previous preflight repair introduced logical plan "
                        "problems. Fix ALL of these first, then return the full "
                        "corrected plan:\n"
                        + "\n".join(f"- {p}" for p in logical_problems)
                    ),
                    messages,
                    plan,
                )
            phases.add_usage("repair_logical_after_preflight", _latest_plan_usage())
            _ground(plan)
        if check(plan):  # a repair that reintroduced a logical problem — spend the round
            continue
        with phases.phase("compile"):
            spec = compile_plan(plan)
        resp = _store_and_respond(run_id, spec, messages, phases)
        # A check that failed before this round and passes now = a repaired
        # model mistake -> lesson candidate (compare by check NAME; the detail
        # text changes between fail and pass).
        passing_now = {
            c["name"]
            for c in _RUNS[run_id].get("preflight", {}).get("checks", [])
            if c["passed"]
        }
        lesson_buffer.extend(
            f for f in failed if f.split(":", 1)[0] in passing_now
        )

    if not _RUNS[run_id].get("preflight", {}).get("passed"):
        # Preflight still failing after every repair round: route the residue
        # to the user as questions (the chat continues the same plan) instead
        # of leaving only a red FAIL panel.
        still_failed = [
            f"{c['name']}: {c['detail']}"
            for c in _RUNS[run_id].get("preflight", {}).get("checks", [])
            if not c["passed"]
        ]
        resp["clarifications"] = (
            list(resp.get("clarifications") or []) + triage.questions_for(still_failed)
        )[:2]  # match the max-2 contract everywhere
        _RUNS[run_id]["clarifications"] = resp["clarifications"]

    if _RUNS[run_id].get("preflight", {}).get("passed") and not mock_mode():
        # Green run: repaired mistakes become lesson candidates (deduped);
        # repeated signatures distill in the background. Never in MOCK.
        triage.record_candidates(dict.fromkeys(lesson_buffer))
        triage.spawn_distiller()

        # Semantic review — one advisory LLM pass comparing BRD vs plan.
        # Deterministic preflight proved the plan BUILDS; this asks whether it
        # says what the BRD says. Never gates approval; skipped on cache hits
        # (the cached plan was reviewed when first built).
        from agent import reviewer

        if reviewer.enabled() and requirements_text and cached is None:
            with phases.phase("plan_review", llm=True):
                findings = await reviewer.review_plan(requirements_text, plan)
            if findings:
                resp["review_findings"] = findings
                _RUNS[run_id]["review_findings"] = findings

    # Cache only a plan that survived every check — never a broken one.
    if cache_key and cached is None and _RUNS[run_id].get("preflight", {}).get("passed"):
        plancache.put(cache_key, plan)

    # Approval-gate transparency: on a refine, show the human exactly what the
    # model changed since the plan they last reviewed (deterministic diff — the
    # UI renders it before re-approval; empty on first generation).
    from agent.plandiff import diff as plan_diff

    resp["plan_changes"] = plan_diff(prev_plan, plan)
    _RUNS[run_id]["plan_changes"] = resp["plan_changes"]
    resp["timings"] = phases.report()

    # Deploy target the BRD names — UI pre-fills its Azure config from this and
    # the USER confirms; the server never applies it silently.
    if plan.azure_target and not plan.azure_target.is_empty():
        resp["azure_target"] = plan.azure_target.model_dump()

    # The accepted plan rides on the run (in-memory, like `messages`) so /chat
    # in tools mode can rebuild the agent's session from it.
    _RUNS[run_id]["logical_plan"] = plan
    if requirements_text:
        _RUNS[run_id]["brd_text"] = requirements_text

    # Audit: persist the logical plan next to the built artifacts.
    run_dir = _RUNS[run_id].get("run_dir")
    if run_dir:
        with open(os.path.join(run_dir, "logical_plan.json"), "w", encoding="utf-8") as fh:
            fh.write(plan.model_dump_json(indent=2))
    return resp


@app.post("/plan")
async def plan(
    brd: Optional[str] = Form(default=None),
    file: Optional[UploadFile] = File(default=None),
) -> Dict[str, Any]:
    from agent.ingest import extract_brd_text

    text = (brd or "").strip()
    if file is not None:
        data = await file.read()
        if data:
            try:
                text = extract_brd_text(file.filename or "", data)
            except RuntimeError as exc:  # unreadable PDF / missing pypdf
                raise HTTPException(status_code=422, detail=str(exc)) from exc
    if not text:
        raise HTTPException(status_code=422, detail="Provide BRD text or upload a file.")

    run_id = uuid.uuid4().hex[:12]
    try:
        if _legacy_parser():
            from agent.parser import parse_brd  # lazy: only the legacy path needs it

            spec, messages = await parse_brd(text)
            return _store_and_respond(run_id, spec, messages)
        from agent import plancache

        extract_plan, _ = _plan_fns()
        # Identical BRD + same mode + same model -> reuse the cached plan, no LLM.
        cache_key = plancache.key(text, _plan_mode())

        return await _run_logical(
            run_id, lambda: extract_plan(text), cache_key, requirements_text=text
        )
    except HTTPException:
        raise
    except Exception as exc:  # parse / model / config error — surface, don't 500
        raise _plan_http_error(exc, "BRD parse failed") from exc


def _plan_http_error(exc: Exception, prefix: str) -> HTTPException:
    """Map a plan-phase failure to the right status. pydantic-ai's AgentRunError
    subclasses RuntimeError, so a plain RuntimeError->503 mapping mislabelled
    'Exceeded maximum output retries' as 'service unavailable' — a model output
    problem, not an outage. Only config errors (missing key/endpoint) are 503."""
    from pydantic_ai.exceptions import AgentRunError

    if isinstance(exc, AgentRunError):
        # Cap exhaustion means the model churned instead of converging —
        # in practice the BRD is missing core facts (no source files named)
        # or asks mostly-unsupported things. Say so; a raw
        # 'UsageLimitExceeded' reads like an outage and helps nobody.
        return HTTPException(status_code=502, detail=(
            _describe(exc, prefix)
            + " — the model could not settle on a valid plan for this BRD. "
            "This usually means core facts are missing (source file names, "
            "container) or most of the requirements are outside what the "
            "system can build. Add specifics or split the BRD into smaller "
            "parts, then regenerate."
        ))
    if isinstance(exc, RuntimeError):  # missing Foundry / model config
        return HTTPException(status_code=503, detail=str(exc))
    return HTTPException(status_code=502, detail=_describe(exc, prefix))


def _describe(exc: Exception, prefix: str) -> str:
    """Error detail that never goes blank: some exceptions (TimeoutError,
    ReadError) stringify to '' and the UI showed 'BRD parse failed: ' with no
    clue. Always include the type; log the traceback for the server console."""
    logging.getLogger("app").exception("%s (run aborted)", prefix)
    msg = str(exc).strip() or repr(exc)
    return f"{prefix}: {type(exc).__name__}: {msg}"


@app.post("/chat")
async def chat(req: ChatRequest) -> Dict[str, Any]:
    """Refine an existing run: answer the agent's questions or request changes."""
    run = _get_run(req.run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Unknown run_id")
    if "messages" not in run:
        raise HTTPException(
            status_code=409,
            detail="This run has no conversation to continue (a server restart "
                   "drops chat history — submit the BRD again to refine it).",
        )
    message = (req.message or "").strip()
    if not message:
        raise HTTPException(status_code=422, detail="Type a change or an answer.")

    try:
        if _legacy_parser():
            from agent.parser import refine_spec

            spec, messages = await refine_spec(message, run["messages"])
            return _store_and_respond(req.run_id, spec, messages)
        _, refine_plan = _plan_fns()

        return await _run_logical(
            req.run_id,
            lambda: refine_plan(message, run["messages"], run.get("logical_plan")),
            requirements_text=run.get("brd_text"),
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise _plan_http_error(exc, "Refine failed") from exc


# --------------------------------------------------------------------------- #
# Preview (read-only)
# --------------------------------------------------------------------------- #


def _require_built(run_id: str) -> Dict[str, Any]:
    run = _get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Unknown run_id")
    if "run_dir" not in run:
        raise HTTPException(status_code=409, detail=f"Run is '{run['status']}'")
    return run


@app.get("/preview/{run_id}")
def preview(run_id: str) -> Dict[str, Any]:
    run = _require_built(run_id)
    run["previewed"] = True
    return {"run_id": run_id, "preflight": run["preflight"], "review": review.render(run_id)}


@app.get("/preview/{run_id}/text", response_class=PlainTextResponse)
def preview_text(run_id: str) -> str:
    _require_built(run_id)
    return review.render_text(run_id)


@app.get("/preview/{run_id}/markdown", response_class=PlainTextResponse)
def preview_markdown(run_id: str) -> str:
    """Human-readable architecture plan (Markdown, no raw JSON)."""
    _require_built(run_id)
    return review.render_markdown(run_id)


# --------------------------------------------------------------------------- #
# /approve — the gate: deploy + run (Azure or MOCK)
# --------------------------------------------------------------------------- #


def _azure_error_hint(exc: Exception) -> str:
    """Map a raw Azure SDK error to a one-line, actionable RBAC/config hint. Most
    deploy/run failures are missing role assignments, not code bugs — surface the
    fix instead of a bare stack trace."""
    blob = f"{type(exc).__name__}: {exc}".lower()
    # LLM provider errors FIRST — a ModelHTTPError 404 ('model no longer
    # available', gemini-2.5-flash retirement) otherwise falls into the Azure
    # 404 branch and sends the user auditing resource groups that are fine.
    if "modelhttperror" in blob or "model_name" in blob or "generativelanguage" in blob:
        return ("LLM provider error — check LLM_PROVIDER / LLM_MODEL in .env. A 404 "
                "usually means the pinned model was retired; list live models for "
                "your key and update LLM_MODEL.")
    if "authorizationpermissionmismatch" in blob or "authorizationfailure" in blob or "403" in blob:
        if "dfs.core.windows.net" in blob or "blob.core.windows.net" in blob or "filesystem" in blob or "container" in blob:
            return ("403 on storage — the DEPLOY identity needs 'Storage Blob Data "
                    "Contributor' on the storage account (control-plane roles don't grant data access).")
        return ("403 — identity lacks a role. Deploy identity needs 'Data Factory "
                "Contributor' on the resource group; the ADF Managed Identity needs "
                "'Storage Blob Data Contributor' on the storage account.")
    if "defaultazurecredential" in blob or "credentialunavailable" in blob or "no credential" in blob:
        return ("No Azure credential found — run 'az login', or set AZURE_CLIENT_ID / "
                "AZURE_TENANT_ID / AZURE_CLIENT_SECRET for a service principal.")
    if "notfound" in blob or "resourcenotfound" in blob or "404" in blob:
        return ("404 — check AZURE_RESOURCE_GROUP, ADF_FACTORY_NAME, and ADLS_ACCOUNT_NAME "
                "match real resources in AZURE_SUBSCRIPTION_ID.")
    if "storageaccountnotfound" in blob or "accountbeingcreated" in blob:
        return "Storage account not reachable — verify ADLS_ACCOUNT_NAME and STORAGE_KIND (blob vs adls)."
    if "blobstorageevents" in blob or "softdelete" in blob or "409" in blob:
        return ("409 from storage — likely a .dfs (ADLS/HNS) endpoint against a plain Blob "
                "account. Set STORAGE_KIND=blob (or enable hierarchical namespace).")
    return ""


def _source_file_checks(spec: PipelineSpec) -> list:
    """Read-only check that concrete Copy source files exist before deploy.

    A 403 is non-blocking because many setups let the app deploy ADF while the
    ADF managed identity owns the data-plane read. A real 404/missing file is
    blocked early instead of five minutes into an ADF run.
    """
    if mock_mode():
        return [{"name": "source_files", "passed": True, "detail": "MOCK=true"}]
    from config import AzureConfig, get_blob_service_client, get_datalake_client, storage_kind

    cfg = AzureConfig.from_env()
    ds_by_name = {d.name: d for d in spec.datasets}
    source_names = {
        act.source_dataset
        for pl in spec.pipelines
        for act in pl.activities
        if act.type == "Copy" and act.source_dataset
    }
    checks = []
    blob_service = None
    datalake_service = None
    for name in sorted(source_names):
        ds = ds_by_name.get(name)
        loc = ds.location if ds else None
        if loc is None or not loc.container or not loc.file_name:
            continue
        path = "/".join(p.strip("/") for p in (loc.folder, loc.file_name) if p)
        try:
            if storage_kind() == "blob":
                blob_service = blob_service or get_blob_service_client(cfg)
                blob_service.get_blob_client(loc.container, path).get_blob_properties()
            else:
                datalake_service = datalake_service or get_datalake_client(cfg)
                (
                    datalake_service
                    .get_file_system_client(loc.container)
                    .get_file_client(path)
                    .get_file_properties()
                )
            checks.append({
                "name": f"source_file:{name}",
                "passed": True,
                "detail": f"{loc.container}/{path}",
            })
        except Exception as exc:  # noqa: BLE001
            blob = f"{exc}".lower()
            if "403" in blob or "authorizationpermissionmismatch" in blob:
                checks.append({
                    "name": f"source_file:{name}",
                    "passed": True,
                    "detail": f"could not verify with deploy identity: {type(exc).__name__}",
                })
            else:
                checks.append({
                    "name": f"source_file:{name}",
                    "passed": False,
                    "detail": f"{loc.container}/{path}: {type(exc).__name__}: {exc}",
                })
    return checks or [{"name": "source_files", "passed": True, "detail": "no concrete file sources"}]


def _source_schema_checks(spec: PipelineSpec) -> list:
    """Declared dataflow source columns vs the REAL landing file header.

    Preflight/typecheck validate the spec against its OWN schema — if that
    schema was model-invented (file not yet uploaded when the plan was made),
    every offline check passes and the run dies ~5 minutes in with DF-EXPR-010
    'Column X used in expression is unavailable or invalid'. Approval is the
    moment the file MUST exist, so it is the last cheap place to compare.

    Each dataflow reads a bronze dataset that a Copy fills from landing; the
    copy is byte-for-byte, so the landing CSV header IS the flow's real input
    schema. Skips non-CSV/URL/Delta sources and unreadable files (existence is
    _source_file_checks' job) — this only ever blocks on a PROVEN mismatch.
    """
    if mock_mode():
        return []
    from agent.grounding import _header_columns, _read_head

    ds_by_name = {d.name: d for d in spec.datasets}
    landing_for = {
        act.sink_dataset: act.source_dataset
        for pl in spec.pipelines
        for act in pl.activities
        if act.type == "Copy" and act.source_dataset and act.sink_dataset
    }
    checks = []
    for df in spec.dataflows:
        declared = [c.name for c in df.source_columns]
        if not declared:
            continue
        ds = ds_by_name.get(landing_for.get(df.source.dataset, ""))
        loc = ds.location if ds else None
        if (
            loc is None or not loc.container or not loc.file_name
            or not loc.file_name.lower().endswith(".csv")
            or (ds.relative_url or "").strip()
        ):
            continue
        path = "/".join(p.strip("/") for p in (loc.folder, loc.file_name) if p)
        head = _read_head(loc.container, path)
        real = set(_header_columns(head)) if head else set()
        if not real:
            continue
        missing = [c for c in declared if c not in real]
        if missing:
            checks.append({
                "name": f"source_schema:{df.name}",
                "passed": False,
                "detail": (
                    f"flow expects column(s) {missing} that {loc.container}/{path} "
                    f"does not have (real header: {sorted(real)}) — re-plan against "
                    "the uploaded file or fix the file"
                ),
            })
        else:
            checks.append({
                "name": f"source_schema:{df.name}",
                "passed": True,
                "detail": f"{len(declared)} column(s) verified in {loc.container}/{path}",
            })
    return checks


def _deploy_and_run(run_id: str) -> None:
    from agent.deployer import deploy_artifacts, provision_containers
    from agent.validator import execute

    run = _RUNS[run_id]
    spec = PipelineSpec.model_validate(run["spec"])
    containers = (spec.storage.containers if spec.storage else None) or ["bronze", "silver", "gold"]

    # Live run state the UI polls.
    run["run"] = {"status": "Pending", "run_id": None, "layers": None}

    def on_update(rec: Dict[str, Any]) -> None:
        run["run"] = rec
        _persist_run(run_id)

    heals: list = []
    try:
        # GROUND_PLAN=false keeps the plan phase fully offline, so the schemas
        # are grounded HERE instead — still before any write, and the artifacts
        # are rebuilt from the grounded spec so what deploys is what was checked.
        if not _ground_during_plan():
            run["status"] = "GROUNDING"
            _persist_run(run_id)
            try:
                from agent.grounding import ground_spec

                ground_spec(spec)
                run["spec"] = spec.model_dump(mode="json")
                builder.write_artifacts(spec, run_id)
            except Exception:  # noqa: BLE001 — best-effort, same contract as /plan
                pass

        # The deploy + run create files grounding may have cached as absent.
        from agent.grounding import clear_caches

        clear_caches()

        run["status"] = "PROVISIONING"
        _persist_run(run_id)
        run["containers_created"] = provision_containers(containers)

        run["status"] = "DEPLOYING"
        _persist_run(run_id)
        run["deployed"] = deploy_artifacts(run["run_dir"])
        heals += list(getattr(deploy_artifacts, "last_heals", []) or [])
        # Verification: intended vs actual factory contents + what prune did.
        run["deploy_report"] = {
            "intended_counts": getattr(deploy_artifacts, "last_intended_counts", {}),
            "factory_counts": getattr(deploy_artifacts, "last_factory_counts", None),
            "pruned": getattr(deploy_artifacts, "last_pruned", []),
            # Stale agent-created objects found but NOT deleted (prune is off by
            # default — the agent never deletes Azure objects on its own).
            "stale": getattr(deploy_artifacts, "last_stale", []),
            "prune_error": getattr(deploy_artifacts, "last_prune_error", None),
        }

        stop_event = run.get("_stop_event")
        if stop_event is not None and stop_event.is_set():
            run["status"] = "STOPPED"
            return

        run["status"] = "RUNNING"
        _persist_run(run_id)
        report = execute(run["run_dir"], on_update=on_update, stop_event=stop_event)
        run["report"] = report
        # Collect any runtime artifact fixes from the self-heal loop.
        for att in report.get("attempts", []):
            if att.get("healed"):
                heals.append(att["healed"])
        if report["status"] == "Succeeded":
            run["status"] = "SUCCEEDED"
        elif report["status"] == "Cancelled":
            run["status"] = "STOPPED"
        else:
            run["status"] = "FAILED"
    except Exception as exc:  # surface deploy/runtime errors in /status
        run["status"] = "ERROR"
        hint = _azure_error_hint(exc)
        run["error"] = f"{type(exc).__name__}: {exc}" + (f"\n\nHINT: {hint}" if hint else "")
    finally:
        if heals:
            run["heals"] = heals
        # Full self-heal audit (deploy + runtime), with before/after bodies.
        try:
            from agent.validator import read_heal_log

            log = read_heal_log(run["run_dir"])
            if log:
                run["heal_log"] = log
        except Exception:  # noqa: BLE001 — audit must not break status
            pass
        _persist_run(run_id)


@app.post("/approve")
def approve(req: ApproveRequest, background: BackgroundTasks) -> Dict[str, Any]:
    run = _get_run(req.run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Unknown run_id")
    # INTERRUPTED = a restart killed the in-flight background task; deploys are
    # idempotent, so re-approval is the recovery path.
    if run["status"] not in {"AWAITING_APPROVAL", "INTERRUPTED"}:
        raise HTTPException(status_code=409, detail=f"Run is '{run['status']}', not approvable")
    # Gate: must have been previewed AND pre-flight must pass.
    if not run.get("previewed"):
        raise HTTPException(status_code=409, detail="Preview the plan before approving.")
    if not run.get("preflight", {}).get("passed"):
        raise HTTPException(status_code=409, detail="Pre-flight failed — cannot approve.")
    # Gate: BRD requirements the system could NOT build need an explicit waiver —
    # a known-incomplete pipeline must never deploy on a routine click.
    unsupported = (run.get("spec") or {}).get("unsupported") or []
    if unsupported and not req.acknowledge_unsupported:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "This plan has requirements that were NOT built. "
                           "Re-approve with acknowledge_unsupported=true to "
                           "deploy anyway.",
                "unsupported": unsupported,
            },
        )
    # Real deploys need a target — fail here with a clear message, not mid-deploy.
    if not mock_mode():
        missing = [
            env for env in ("AZURE_SUBSCRIPTION_ID", "AZURE_RESOURCE_GROUP",
                            "ADF_FACTORY_NAME", "ADLS_ACCOUNT_NAME")
            if not os.environ.get(env)
        ]
        if missing:
            raise HTTPException(
                status_code=409,
                detail="Azure connection not set - configure the server environment "
                       f"or POST /azure-config first (missing: {', '.join(missing)}).",
            )

        azure_test = test_azure_config()
        if not azure_test.get("ok"):
            failed = [
                f"{c['name']}: {c.get('detail', '')}"
                for c in azure_test.get("checks", [])
                if not c.get("passed")
            ]
            raise HTTPException(
                status_code=409,
                detail="Azure target check failed before deploy: " + "; ".join(failed),
            )
        spec_for_checks = PipelineSpec.model_validate(run["spec"])
        source_checks = _source_file_checks(spec_for_checks)
        failed_sources = [
            f"{c['name']}: {c.get('detail', '')}"
            for c in source_checks
            if not c.get("passed")
        ]
        if failed_sources:
            raise HTTPException(
                status_code=409,
                detail="Source file check failed before deploy: " + "; ".join(failed_sources),
            )
        # The landing file may have been uploaded AFTER planning — drop the
        # memoized "file absent" heads so the schema check reads fresh bytes.
        from agent.grounding import clear_caches

        clear_caches()
        failed_schemas = [
            f"{c['name']}: {c.get('detail', '')}"
            for c in _source_schema_checks(spec_for_checks)
            if not c.get("passed")
        ]
        if failed_schemas:
            raise HTTPException(
                status_code=409,
                detail="Source schema check failed before deploy: " + "; ".join(failed_schemas),
            )

    run["status"] = "APPROVED"
    run["_stop_event"] = threading.Event()
    _persist_run(req.run_id)
    background.add_task(_deploy_and_run, req.run_id)
    return {"run_id": req.run_id, "status": "APPROVED", "mock": mock_mode()}


@app.post("/stop/{run_id}")
def stop(run_id: str) -> Dict[str, Any]:
    """Abort a live run: stop the self-heal loop and cancel the in-flight ADF
    pipeline run (recursively, so child ExecutePipeline runs die too)."""
    run = _get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Unknown run_id")
    event = run.get("_stop_event")
    if event is None or run["status"] not in {"APPROVED", "PROVISIONING", "DEPLOYING", "RUNNING"}:
        raise HTTPException(status_code=409, detail=f"Run is '{run['status']}' — nothing to stop")
    event.set()
    return {"run_id": run_id, "status": run["status"], "stopping": True}


# --------------------------------------------------------------------------- #
# /status
# --------------------------------------------------------------------------- #


@app.get("/status/{run_id}")
def status(run_id: str) -> Dict[str, Any]:
    run = _get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Unknown run_id")
    keep = (
        "status",
        "mock",
        "containers_created",
        "deployed",
        "deploy_report",
        "run",
        "report",
        "heals",
        "heal_log",
        "error",
        "preflight",
        "clarifications",
    )
    return {"run_id": run_id, **{k: run[k] for k in keep if k in run}}
