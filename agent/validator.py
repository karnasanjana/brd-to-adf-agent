"""Validator: trigger the master pipeline, poll live ADF state, surface results.

On a failed run we surface the error clearly in the UI. The self-heal loop
(patch failing artifact -> redeploy -> retry, cap 3) is enabled by default and
can be disabled with SELF_HEAL=false when a tenant needs fail-loud behavior.

Per-layer status (Bronze -> Silver -> Gold) and run IDs are streamed back to the
caller via an ``on_update`` callback so the UI can show progress live. MOCK mode
simulates a run without touching Azure.
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import re
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

from agent.builder import DEPLOY_ORDER
from agent.deployer import deploy_object
from config import RETRY_CAP, AzureConfig, mock_mode

_TERMINAL = {"Succeeded", "Failed", "Cancelled"}
_MASTER = "pl_master_pipeline"
_LAYER_ORDER = ["bronze", "silver", "gold"]

UpdateFn = Callable[[Dict[str, Any]], None]


def self_heal_enabled() -> bool:
    return os.environ.get("SELF_HEAL", "true").strip().lower() not in {"0", "false", "no", "off"}


def _noop(_: Dict[str, Any]) -> None:
    pass


def _default_params() -> Dict[str, Any]:
    # Every generated pipeline parameter carries a defaultValue, so the master run
    # needs no explicit parameters — passing unknown ones (e.g. report_date) would
    # be rejected by ADF. Rely on the defaults baked into the pipeline.
    return {}


def _empty_layers() -> Dict[str, Dict[str, Any]]:
    return {layer: {"status": "Pending", "run_id": None, "error": None} for layer in _LAYER_ORDER}


def _pending_steps(run_dir: str) -> List[Dict[str, Any]]:
    """One Pending entry per activity of the pipeline that will run, in declared
    order — the generic (non-medallion) progress view the UI renders."""
    docs = _load_pipelines(run_dir)
    props = docs.get(_master_name(run_dir), {})
    return [
        {"name": a.get("name"), "status": "Pending", "run_id": None, "error": None}
        for a in props.get("activities", [])
    ]


def _steps_to_layers(steps: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Back-compat medallion view: map steps whose names mention a layer."""
    layers = _empty_layers()
    for step in steps:
        name = (step.get("name") or "").lower()
        layer = next((l for l in _LAYER_ORDER if l in name), None)
        if layer:
            layers[layer] = {
                "status": step["status"],
                "run_id": step.get("run_id"),
                "error": step.get("error"),
            }
    return layers


# --------------------------------------------------------------------------- #
# Master pipeline name discovery
# --------------------------------------------------------------------------- #


def _load_pipelines(run_dir: str) -> Dict[str, Dict[str, Any]]:
    pl_dir = os.path.join(run_dir, "pipelines")
    docs: Dict[str, Dict[str, Any]] = {}
    if os.path.isdir(pl_dir):
        for fname in sorted(os.listdir(pl_dir)):
            if fname.endswith(".json"):
                with open(os.path.join(pl_dir, fname), "r", encoding="utf-8") as fh:
                    doc = json.load(fh)
                docs[doc["name"]] = doc["properties"]
    return docs


def _master_name(run_dir: str) -> str:
    """The pipeline to run: the all-ExecutePipeline master when one exists,
    otherwise a ROOT pipeline (one no other pipeline invokes) — covers copy-only
    and single-pipeline specs that have no master at all."""
    docs = _load_pipelines(run_dir)
    for name, props in docs.items():
        acts = props.get("activities", [])
        if acts and all(a.get("type") == "ExecutePipeline" for a in acts):
            return name
    invoked = set()
    for props in docs.values():
        for act in props.get("activities", []):
            if act.get("type") == "ExecutePipeline":
                ref = act.get("typeProperties", {}).get("pipeline", {}).get("referenceName")
                if ref:
                    invoked.add(ref)
    roots = [n for n in docs if n not in invoked]
    if roots:
        return roots[0]
    return next(iter(docs), _MASTER)


# --------------------------------------------------------------------------- #
# Live status mapping
# --------------------------------------------------------------------------- #


def _step_status_real(
    adf, cfg: AzureConfig, run_id: str, pending: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Live status for every top-level activity of the running pipeline. Activities
    ADF hasn't scheduled yet stay Pending (from the declared activity list)."""
    now = _dt.datetime.utcnow()
    flt = {
        "lastUpdatedAfter": (now - _dt.timedelta(days=1)).isoformat() + "Z",
        "lastUpdatedBefore": (now + _dt.timedelta(minutes=5)).isoformat() + "Z",
    }
    resp = adf.activity_runs.query_by_pipeline_run(
        cfg.resource_group, cfg.factory_name, run_id, filter_parameters=flt
    )
    steps = {s["name"]: dict(s) for s in pending}
    for act in resp.value or []:
        child_run_id = None
        if isinstance(act.output, dict):
            child_run_id = act.output.get("pipelineRunId")
        err = None
        if isinstance(act.error, dict) and act.error.get("message"):
            err = act.error.get("message")
        steps[act.activity_name] = {
            "name": act.activity_name,
            "status": act.status,
            "run_id": child_run_id,
            "error": err,
        }
    # Preserve declared order; append any activity ADF reports that we didn't declare.
    ordered = [steps[s["name"]] for s in pending if s["name"] in steps]
    ordered += [v for k, v in steps.items() if k not in {s["name"] for s in pending}]
    return ordered


def _failed_activities(adf, cfg: AzureConfig, run_id: str) -> List[Tuple[str, str]]:
    now = _dt.datetime.utcnow()
    flt = {
        "lastUpdatedAfter": (now - _dt.timedelta(days=1)).isoformat() + "Z",
        "lastUpdatedBefore": (now + _dt.timedelta(minutes=5)).isoformat() + "Z",
    }
    resp = adf.activity_runs.query_by_pipeline_run(
        cfg.resource_group, cfg.factory_name, run_id, filter_parameters=flt
    )
    failures: List[Tuple[str, str]] = []
    for act in resp.value or []:
        if act.status == "Failed":
            msg = act.error.get("message", "") if isinstance(act.error, dict) else ""
            failures.append((act.activity_name, msg))
    return failures


# --------------------------------------------------------------------------- #
# Running the pipeline
# --------------------------------------------------------------------------- #


def _run_real(
    run_dir: str,
    cfg: AzureConfig,
    parameters: Dict[str, Any],
    on_update: UpdateFn,
    poll_seconds: int = 15,
    timeout_seconds: int = 1800,
    recover_from: "str | None" = None,
    stop_event: "Any | None" = None,
) -> Dict[str, Any]:
    from config import get_datafactory_client

    adf = get_datafactory_client(cfg)
    master = _master_name(run_dir)
    pending = _pending_steps(run_dir)
    if recover_from:
        # ADF-native recovery rerun: skip activities that succeeded in the referenced
        # run and restart at the failed ones (portal's "Rerun from failed activity").
        # A healed artifact was already redeployed, so the rerun picks it up.
        run = adf.pipelines.create_run(
            cfg.resource_group, cfg.factory_name, master, parameters=parameters,
            reference_pipeline_run_id=recover_from, is_recovery=True,
            start_from_failure=True,
        )
    else:
        run = adf.pipelines.create_run(
            cfg.resource_group, cfg.factory_name, master, parameters=parameters
        )
    run_id = run.run_id
    on_update({"run_id": run_id, "status": "InProgress", "steps": pending,
               "layers": _steps_to_layers(pending), "recovered_from": recover_from})

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if stop_event is not None and stop_event.is_set():
            try:
                adf.pipeline_runs.cancel(
                    cfg.resource_group, cfg.factory_name, run_id, is_recursive=True
                )
            except Exception:  # noqa: BLE001 — already-terminal runs reject cancel
                pass
            steps = _step_status_real(adf, cfg, run_id, pending)
            return {"run_id": run_id, "status": "Cancelled", "steps": steps,
                    "layers": _steps_to_layers(steps), "stopped_by_user": True}
        info = adf.pipeline_runs.get(cfg.resource_group, cfg.factory_name, run_id)
        steps = _step_status_real(adf, cfg, run_id, pending)
        layers = _steps_to_layers(steps)
        on_update({"run_id": run_id, "status": info.status, "steps": steps, "layers": layers})
        if info.status in _TERMINAL:
            result: Dict[str, Any] = {
                "run_id": run_id, "status": info.status, "steps": steps, "layers": layers,
            }
            if info.status != "Succeeded":
                result["failures"] = _failed_activities(adf, cfg, run_id)
                result["error"] = info.message or "; ".join(m for _, m in result["failures"])
            return result
        time.sleep(poll_seconds)

    steps = _step_status_real(adf, cfg, run_id, pending)
    return {"run_id": run_id, "status": "TimedOut", "steps": steps,
            "layers": _steps_to_layers(steps)}


def _run_mock(run_dir: str, on_update: UpdateFn) -> Dict[str, Any]:
    """Simulate the run step-by-step (no Azure)."""
    run_id = "mock-" + _dt.datetime.utcnow().strftime("%H%M%S")
    steps = _pending_steps(run_dir) or [
        {"name": l.capitalize(), "status": "Pending", "run_id": None, "error": None}
        for l in _LAYER_ORDER
    ]

    def emit(status: str) -> None:
        on_update({"run_id": run_id, "status": status,
                   "steps": [dict(s) for s in steps], "layers": _steps_to_layers(steps)})

    emit("InProgress")
    for step in steps:
        step.update(status="InProgress", run_id=f"{run_id}-{step['name']}")
        emit("InProgress")
        time.sleep(0.4)
        step.update(status="Succeeded")
        emit("InProgress")
    return {"run_id": run_id, "status": "Succeeded", "steps": steps,
            "layers": _steps_to_layers(steps), "mock": True}


# --------------------------------------------------------------------------- #
# Optional self-heal (ON by default; SELF_HEAL=false disables it)
# --------------------------------------------------------------------------- #


class HealedArtifact(BaseModel):
    explanation: str = Field(..., description="One-line summary of the fix.")
    properties: Dict[str, Any] = Field(..., description="Corrected ADF 'properties' body.")


_HEAL_PROMPT = """\
You are an Azure Data Factory expert. A single ADF artifact failed at runtime.
Given its current ARM 'properties' JSON and the runtime error, return the
corrected 'properties' body. Preserve names, references, and Managed Identity
auth. Change only what is necessary. Never add keys, SAS tokens, or connection
strings.

DIAGNOSE THE ROOT CAUSE BEFORE EDITING — follow this protocol:
1. Locate the exact expression/property the error points at (error codes like
   DF-DT-001 name the transformation and column).
2. Determine the column's ACTUAL type from the source/sink schemas provided
   below, and the type the failing expression or sink expects.
3. Decide the fix at the TYPE level: an explicit cast (toInteger/toDecimal/
   toDate/toTimestamp), iif(isNull(col), <typed default>, col), or fixing the
   sink mapping. Ask yourself: is there an implicit conversion? is coalesce()
   even the right function, or is iif()/a cast safer?
4. Regenerate the ENTIRE failing transformation correctly; leave every other
   line byte-identical.

NEVER silence a type error cosmetically (e.g. rewriting 0 as '0'): that turns
the column into a string, corrupts the sink schema, and the run fails again
one step later. Fix the type, not the symptom. coalesce() requires ALL
arguments to share one type — for a typed default on a numeric column use a
numeric literal, or cast the column before coalescing.

Data-flow 'scriptLines' are ADF data flow script DSL, NOT Spark SQL.
Window transform syntax:
  <in> window(over(<partition cols>), asc(<col>, true) | desc(<col>, true),
              <out> = <fn>()) ~> <name>
'partitionBy', 'orderBy' and 'row_number' do not exist in this DSL. For a
plain dedup, prefer aggregate(groupBy(<keys>), <col> = first(<col>), ...)
over a window + filter.
"""


def _schema_context(run_dir: str, category: str, name: str) -> str:
    """Source/sink column schemas for the artifact being healed — the healer
    must see real types to fix DF-DT-001-class errors at the root instead of
    requoting literals. Best-effort: empty string when the spec is unreadable."""
    try:
        from agent.review import load_spec

        spec = load_spec(run_dir)
        ds_by = {d.name: d for d in spec.datasets}

        def fmt_cols(label: str, cols) -> str:
            if not cols:
                return f"{label}: (schema unknown)"
            return f"{label}: " + ", ".join(f"{c.name} {c.type}" for c in cols)

        def fmt(ds_name: "str | None") -> str:
            ds = ds_by.get(ds_name or "")
            if ds is None or not ds.columns:
                return f"{ds_name}: (schema unknown)"
            return f"{ds_name}: " + ", ".join(f"{c.name} {c.type}" for c in ds.columns)

        if category == "dataflows":
            df = next((d for d in spec.dataflows if d.name == name), None)
            if df is None:
                return ""
            primary = df.source_columns or ds_by.get(df.source.dataset, None)
            primary_cols = primary if isinstance(primary, list) else getattr(primary, "columns", [])
            lines = [fmt_cols(f"Source {df.source.stream_name} ({df.source.dataset})", primary_cols)]
            for ep in df.additional_sources:
                cols = ep.columns or getattr(ds_by.get(ep.dataset), "columns", [])
                lines.append(fmt_cols(f"Source {ep.stream_name} ({ep.dataset})", cols))
            lines.append("Sink " + fmt(df.sink.dataset))
            return "\n".join(lines)
        if category == "pipelines":
            pl = next((p for p in spec.pipelines if p.name == name), None)
            if pl is None:
                return ""
            names: List[str] = []
            for act in pl.activities:
                names += [n for n in (act.source_dataset, act.sink_dataset) if n]
            return "\n".join(fmt(n) for n in dict.fromkeys(names))
    except Exception:  # noqa: BLE001 — context is an aid, never a blocker
        pass
    return ""


def _artifact_inventory(run_dir: str) -> Dict[str, str]:
    inventory: Dict[str, str] = {}
    for category in DEPLOY_ORDER:
        cat_dir = os.path.join(run_dir, category)
        if os.path.isdir(cat_dir):
            for fname in os.listdir(cat_dir):
                if fname.endswith(".json"):
                    inventory[fname[:-5]] = category
    return inventory


def _pick_target(
    error_message: str, inventory: Dict[str, str], run_dir: Optional[str] = None
) -> Optional[Tuple[str, str]]:
    # 1. Resolve the ACTIVITY chain first — it is the most reliable signal and the
    #    one that used to mis-fire. A runtime data-flow failure nests the targets
    #    outermost -> innermost:
    #      "Operation on target run_silver_x failed: Operation on target
    #       flow_silver_x failed: {DFExecutorUserError ... at Sink 'sinkX' ...}"
    #    The OUTER tokens are just the master's ExecutePipeline and the child's
    #    ExecuteDataFlow wrappers; the INNER one is the real failure. So walk the
    #    tokens innermost-first, and when a token is an ExecuteDataFlow activity,
    #    heal the DATAFLOW it runs (a DFExecutorUserError is a data-flow problem),
    #    NOT the pipeline that invokes it. Without this, a silver data-flow type
    #    error healed pl_master_pipeline twice and burned every retry.
    if run_dir:
        targets = re.findall(r"Operation on target (\S+) failed", error_message)
        pipelines = _load_pipelines(run_dir)
        for tok in reversed(targets):  # innermost first
            for pl_name, props in pipelines.items():
                for act in props.get("activities", []):
                    if act.get("name") != tok:
                        continue
                    if act.get("type") == "ExecuteDataFlow":
                        df = (
                            act.get("typeProperties", {})
                            .get("dataflow", {})
                            .get("referenceName")
                        )
                        if df and df in inventory:
                            return df, inventory[df]
                    elif act.get("type") != "ExecutePipeline":
                        # A Copy/GetMetadata/etc. failure — heal the pipeline that
                        # owns the activity (its dataset/LS refs live there).
                        if pl_name in inventory:
                            return pl_name, inventory[pl_name]
                    # ExecutePipeline wrapper: skip, keep unwinding to the inner target.

    # 2. The error names an artifact directly (e.g. a dataset in
    #    AdlsGen2InvalidResourceName). Prefer a data flow / dataset over a pipeline.
    matches = [(n, c) for n, c in inventory.items() if n and n in error_message]
    if matches:
        matches.sort(key=lambda nc: 0 if nc[1] in ("dataflows", "datasets") else 1)
        return matches[0]

    # 3. Fail loud. Healing an arbitrary artifact burns a full Azure retry and
    # can make a good artifact worse. If the runtime error does not identify a
    # target, surface the original failure instead of guessing.
    return None


def _log_heal(run_dir: str, record: Dict[str, Any]) -> None:
    """Append a full audit record to <run_dir>/heals.json so every self-heal is
    inspectable after the fact: what failed, what the model changed, and why."""
    path = os.path.join(run_dir, "heals.json")
    records: List[Dict[str, Any]] = []
    if os.path.isfile(path):
        try:
            with open(path, "r", encoding="utf-8") as fh:
                records = json.load(fh)
        except Exception:  # noqa: BLE001 — corrupt log never blocks a heal
            records = []
    records.append(record)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(records, fh, indent=2)


def read_heal_log(run_dir: str) -> List[Dict[str, Any]]:
    path = os.path.join(run_dir, "heals.json")
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:  # noqa: BLE001
        return []


def _lint_healed(name: str, properties: Dict[str, Any]) -> List[str]:
    """Lint a healed data flow's scriptLines before redeploy — a heal that
    trips the same parser checks would burn a full Azure run to learn what
    lint knows in milliseconds (runs 24756f0f5aa8 attempts 1-2)."""
    from agent.lint import lint_dataflow_script

    lines = ((properties.get("typeProperties") or {}).get("scriptLines")) or []
    if not lines:
        return []
    return lint_dataflow_script(name, "\n".join(lines))


def _run_llm(agent: "Any", user_msg: str, tries: int = 3) -> "Any":
    """agent.run_sync with exponential backoff on transient provider errors
    (shared policy — see agent/retry.py)."""
    from agent.retry import retry_sync

    return retry_sync(lambda: agent.run_sync(user_msg), tries=tries)


def heal_artifact(
    run_dir: str, category: str, name: str, error_message: str, phase: str = "runtime"
) -> str:
    from pydantic_ai import Agent

    from config import get_model, structured_output

    path = os.path.join(run_dir, category, f"{name}.json")
    with open(path, "r", encoding="utf-8") as fh:
        doc = json.load(fh)
    before = doc["properties"]

    from agent.skills import lessons_path

    heal_prompt = _HEAL_PROMPT
    try:  # lessons distilled from earlier heals — the healer benefits too
        with open(lessons_path(), "r", encoding="utf-8") as fh:
            heal_prompt += "\n\nLessons from previous incidents:\n" + fh.read()
    except OSError:
        pass
    agent = Agent(
        get_model(), output_type=structured_output(HealedArtifact), system_prompt=heal_prompt
    )
    schemas = _schema_context(run_dir, category, name)
    user_msg = (
        f"Artifact category: {category}\nArtifact name: {name}\n"
        f"Runtime error:\n{error_message}\n\n"
        + (f"Column schemas (ground truth — use these types):\n{schemas}\n\n" if schemas else "")
        + f"Current properties JSON:\n{json.dumps(before, indent=2)}"
    )
    healed = _run_llm(agent, user_msg).output
    lint_problems = _lint_healed(name, healed.properties) if category == "dataflows" else []
    if lint_problems:
        # One corrective round-trip: cheaper than discovering the same parse
        # error 5 minutes into the redeployed run.
        healed = _run_llm(
            agent,
            user_msg
            + "\n\nYour previous correction:\n"
            + json.dumps(healed.properties, indent=2)
            + "\n\nIt still fails the data-flow script linter:\n"
            + "\n".join(f"- {p}" for p in lint_problems)
            + "\n\nFix these problems and return the full corrected properties."
        ).output
        lint_problems = _lint_healed(name, healed.properties)
    doc["properties"] = healed.properties
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, indent=2)

    _log_heal(
        run_dir,
        {
            "time": _dt.datetime.utcnow().isoformat() + "Z",
            "phase": phase,  # 'deploy' (create_or_update rejected) or 'runtime' (run failed)
            "category": category,
            "name": name,
            "error": error_message,
            "fix": healed.explanation,
            "lint_after_heal": lint_problems,  # non-empty = heal shipped known-bad
            "before": before,
            "after": healed.properties,
        },
    )
    # Close the loop: distill this incident into a generic rule the planner
    # (and future heals) will see. Best-effort — never blocks the heal.
    from agent.skills import record_lesson

    record_lesson(error_message, healed.explanation)
    return healed.explanation


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


def execute(
    run_dir: str,
    cfg: "AzureConfig | None" = None,
    parameters: "Dict[str, Any] | None" = None,
    on_update: "UpdateFn | None" = None,
    stop_event: "Any | None" = None,
) -> Dict[str, Any]:
    """Run the master pipeline and report the outcome.

    Default: self-heal is enabled. When SELF_HEAL=false, run once and surface
    the error. With the default real backend, the app patches the failing
    artifact and retries up to RETRY_CAP. Retries are ADF recovery reruns:
    layers that already succeeded are skipped and the run restarts at the failed
    activity. `stop_event` (a threading.Event) aborts the loop and cancels the in-flight ADF run.
    """
    on_update = on_update or _noop
    parameters = parameters or _default_params()

    if mock_mode():
        report = _run_mock(run_dir, on_update)
        return {"status": report["status"], "attempts": [report]}

    cfg = cfg or AzureConfig.from_env()

    # Explicit fail-loud single attempt when SELF_HEAL=false.
    if not self_heal_enabled():
        report = _run_real(run_dir, cfg, parameters, on_update, stop_event=stop_event)
        return {"status": report["status"], "attempts": [report]}

    # Self-heal loop.
    inventory = _artifact_inventory(run_dir)
    attempts: List[Dict[str, Any]] = []
    prev_run_id: Optional[str] = None

    def _emit(rec: Dict[str, Any]) -> None:
        # Past attempts ride along on every live update, so the UI can keep
        # showing what failed (and how it was healed) while the retry runs —
        # otherwise each retry resets the steps and the error vanishes unread.
        if attempts:
            rec["attempt_history"] = [
                {"attempt": a.get("attempt"), "status": a.get("status"),
                 "run_id": a.get("run_id"), "error": a.get("error"),
                 "healed": a.get("healed")}
                for a in attempts
            ]
        on_update(rec)

    for attempt in range(1, RETRY_CAP + 1):
        report = _run_real(
            run_dir, cfg, parameters, _emit,
            recover_from=prev_run_id, stop_event=stop_event,
        )
        report["attempt"] = attempt
        if report["status"] == "Succeeded":
            attempts.append(report)
            return {"status": "Succeeded", "attempts": attempts}
        if report.get("stopped_by_user") or (stop_event is not None and stop_event.is_set()):
            report["healed"] = None
            attempts.append(report)
            return {"status": "Cancelled", "attempts": attempts}

        error_blob = report.get("error") or "; ".join(m for _, m in report.get("failures", []))
        target = _pick_target(error_blob or "", inventory, run_dir)
        if target is None or attempt == RETRY_CAP:
            report["healed"] = None
            attempts.append(report)
            break
        # Append BEFORE the (slow) heal call and emit, so the failure is readable
        # in the UI while the model works instead of vanishing into the retry.
        report["healed"] = None
        attempts.append(report)
        _emit({"run_id": report.get("run_id"), "status": "Healing",
               "steps": report.get("steps"), "layers": report.get("layers")})
        name, category = target
        try:
            explanation = heal_artifact(run_dir, category, name, error_blob or report["status"])
            deploy_object(run_dir, category, name, cfg)
        except Exception as exc:  # noqa: BLE001 — heal is best-effort: keep the
            # ADF failure readable instead of replacing it with an LLM/deploy
            # traceback (gemini 503 UNAVAILABLE killed a run at attempt 1).
            report["healed"] = {
                "category": category, "name": name,
                "fix": f"heal failed ({type(exc).__name__}): {exc}",
            }
            break
        # Mutates the dict already inside `attempts`, so history picks it up.
        report["healed"] = {"category": category, "name": name, "fix": explanation}
        # Next attempt resumes from this run's failure point instead of re-running
        # already-succeeded layers.
        prev_run_id = report.get("run_id")

    return {"status": "Failed", "attempts": attempts}
