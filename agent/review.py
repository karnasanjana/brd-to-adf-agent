"""Review rendering: all three medallion layers for human approval.

Bronze is rendered as its Copy Activity JSON labelled "raw landing, no
transformation" — there is intentionally NO script section (its absence is
correct). Silver and Gold are rendered as their Mapping Data Flow JSON plus the
generated script. Everything is read back from artifacts/<run_id>/ so the review
shows exactly what will be deployed.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from agent.builder import DEPLOY_ORDER
from models.spec import MedallionLayer, PipelineSpec

BRONZE_LABEL = "raw landing, no transformation"


# --------------------------------------------------------------------------- #
# Loading helpers (shared by review / preflight)
# --------------------------------------------------------------------------- #


def load_spec(run_dir: str) -> PipelineSpec:
    with open(os.path.join(run_dir, "spec.json"), "r", encoding="utf-8") as fh:
        return PipelineSpec.model_validate_json(fh.read())


def load_category(run_dir: str, category: str) -> Dict[str, Dict[str, Any]]:
    cat_dir = os.path.join(run_dir, category)
    objects: Dict[str, Dict[str, Any]] = {}
    if not os.path.isdir(cat_dir):
        return objects
    for fname in sorted(os.listdir(cat_dir)):
        if fname.endswith(".json"):
            with open(os.path.join(cat_dir, fname), "r", encoding="utf-8") as fh:
                doc = json.load(fh)
            objects[doc["name"]] = doc["properties"]
    return objects


def load_script(run_dir: str, dataflow_name: str) -> str:
    path = os.path.join(run_dir, "dataflows", f"{dataflow_name}.script")
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()
    return ""


def find_bronze_copy(spec: PipelineSpec):
    """Return (pipeline_name, copy_activity_obj) for the Bronze Copy activity."""
    # Prefer an explicit bronze-layer Copy; fall back to any Copy.
    for pl in spec.pipelines:
        for act in pl.activities:
            if act.type == "Copy" and act.layer == MedallionLayer.bronze:
                return pl.name, act
    for pl in spec.pipelines:
        for act in pl.activities:
            if act.type == "Copy":
                return pl.name, act
    return None, None


def _copy_activity_json(run_dir: str, pipeline_name: str, activity_name: str) -> Optional[Dict[str, Any]]:
    props = load_category(run_dir, "pipelines").get(pipeline_name)
    if not props:
        return None
    for act in props.get("activities", []):
        if act.get("name") == activity_name:
            return act
    return None


# --------------------------------------------------------------------------- #
# Structured review
# --------------------------------------------------------------------------- #


def render(run_id: str, root: str = "artifacts") -> Dict[str, Any]:
    run_dir = os.path.join(root, run_id)
    spec = load_spec(run_dir)

    pipelines_json = load_category(run_dir, "pipelines")
    dataflows_json = load_category(run_dir, "dataflows")

    # Bronze layer: Copy Activity only, no script.
    bronze_pl, bronze_act = find_bronze_copy(spec)
    bronze_block: Optional[Dict[str, Any]] = None
    if bronze_pl and bronze_act:
        bronze_block = {
            "label": BRONZE_LABEL,
            "pipeline": bronze_pl,
            "activity": bronze_act.name,
            "source_dataset": bronze_act.source_dataset,
            "sink_dataset": bronze_act.sink_dataset,
            "copy_activity": _copy_activity_json(run_dir, bronze_pl, bronze_act.name),
            "script": None,  # intentional: Bronze has no transformation
        }

    # Silver / Gold layers: data flow JSON + script. Non-medallion flows (layer=None)
    # don't join the layers block — they appear in the flat "dataflows" list below.
    layer_blocks: Dict[str, Any] = {}
    for df in spec.dataflows:
        if df.layer is None:
            continue
        layer_blocks[df.layer.value] = {
            "dataflow": df.name,
            "json": dataflows_json.get(df.name),
            "script": load_script(run_dir, df.name),
            "source_dataset": df.source.dataset,
            "sink_dataset": df.sink.dataset,
        }

    def _flatten(category: str) -> List[Dict[str, Any]]:
        return [{"name": n, "json": p} for n, p in load_category(run_dir, category).items()]

    return {
        "run_id": run_id,
        "spec_name": spec.name,
        "layers": {
            "bronze": bronze_block,
            "silver": layer_blocks.get("silver"),
            "gold": layer_blocks.get("gold"),
        },
        "linked_services": _flatten("linkedServices"),
        "datasets": _flatten("datasets"),
        "dataflows": _flatten("dataflows"),
        "pipelines": [{"name": n, "json": p} for n, p in pipelines_json.items()],
        # Schedule triggers come from the built artifacts; manual triggers are not
        # ADF resources (never built/deployed) but must still show in the overview,
        # so they're appended from the spec with an explanatory note.
        "triggers": _flatten("triggers") + [
            {
                "name": tr.name,
                "json": {
                    "type": "Manual (on demand)",
                    "pipeline": tr.pipeline,
                    "note": "Not deployed — ADF has no manual-trigger resource; "
                            "the pipeline is run on demand.",
                },
            }
            for tr in spec.triggers
            if tr.kind.value == "manual"
        ],
    }


# --------------------------------------------------------------------------- #
# Plain-text review (for terminals / logs)
# --------------------------------------------------------------------------- #


def render_text(run_id: str, root: str = "artifacts") -> str:
    data = render(run_id, root)
    out: List[str] = []

    def rule(title: str) -> None:
        out.append("\n" + "=" * 76)
        out.append(title)
        out.append("=" * 76)

    rule(f"REVIEW — {data['spec_name']}  (run {run_id})")

    bronze = data["layers"]["bronze"]
    if bronze:
        rule("BRONZE — Copy Activity (raw landing, no transformation)")
        out.append(f"pipeline: {bronze['pipeline']}   activity: {bronze['activity']}")
        out.append(f"source: {bronze['source_dataset']}  ->  sink: {bronze['sink_dataset']}")
        out.append("(no script section — Bronze performs no transformation)")
        out.append(json.dumps(bronze["copy_activity"], indent=2))

    for layer in ("silver", "gold"):
        block = data["layers"].get(layer)
        if block:
            rule(f"{layer.upper()} — Mapping Data Flow")
            out.append(f"data flow: {block['dataflow']}")
            out.append(f"source: {block['source_dataset']}  ->  sink: {block['sink_dataset']}")
            out.append("\n-- script --")
            out.append(block["script"])

    # Non-medallion data flows (not tagged with a layer).
    layered = {b["dataflow"] for b in data["layers"].values() if b and b.get("dataflow")}
    for df in data["dataflows"]:
        if df["name"] in layered:
            continue
        rule(f"DATA FLOW — {df['name']}")
        out.append(json.dumps(df["json"], indent=2))

    rule("PIPELINES")
    for pl in data["pipelines"]:
        out.append(f"\n# {pl['name']}")
        out.append(json.dumps(pl["json"], indent=2))

    return "\n".join(out)


# --------------------------------------------------------------------------- #
# Human-readable Markdown plan (no raw JSON) — what the architect proposes
# --------------------------------------------------------------------------- #


def _transform_english(t) -> str:
    """One plain-English line for a transform, derived from the typed spec."""
    from models.spec import TransformKind

    if t.kind in (TransformKind.cast, TransformKind.derived):
        cols = ", ".join(f"`{c.name}` = {c.expression}" for c in t.columns)
        verb = "cast" if t.kind == TransformKind.cast else "derive"
        return f"{verb} {cols}"
    if t.kind == TransformKind.filter:
        return f"filter rows where `{t.condition}`"
    if t.kind == TransformKind.aggregate:
        grp = ", ".join(f"`{g}`" for g in t.group_by)
        aggs = ", ".join(f"`{c.name}` = {c.expression}" for c in t.columns)
        if not aggs:  # aggregate with no measures = dedup on the group keys
            return f"dedup on {grp}"
        return f"aggregate by {grp} → {aggs}"
    if t.kind == TransformKind.join:
        cond = f" on `{t.condition}`" if t.condition else ""
        return f"{t.join_type} join `{t.right_stream}`{cond}"
    if t.kind == TransformKind.sort:
        keys = ", ".join(f"`{k.column}` {k.order}" for k in t.order_by)
        return f"sort by {keys}"
    if t.kind == TransformKind.select:
        cols = ", ".join(f"`{c.name}`" for c in t.columns)
        return f"select/rename {cols}"
    if t.kind == TransformKind.window:
        if t.partition_by:
            keys = ", ".join(f"`{p}`" for p in t.partition_by)
            outs = ", ".join(f"`{c.name}`" for c in t.columns)
            return f"window per {keys} → {outs}"
        return f"window `{t.condition or ''}`"
    if t.kind == TransformKind.limit:
        return f"keep top {t.take} rows" + (f" (skip {t.skip})" if t.skip else "")
    return t.kind.value


def render_markdown(run_id: str, root: str = "artifacts") -> str:
    """A human-readable architecture plan in Markdown — no raw JSON. This is the
    'agent proposes, you approve' view: what each layer does in English, the
    assumptions the agent made instead of asking, and any optional suggestions."""
    run_dir = os.path.join(root, run_id)
    spec = load_spec(run_dir)
    out: List[str] = [f"# Pipeline plan — {spec.name}", ""]

    # Source
    if spec.source is not None:
        src = spec.source
        kind = getattr(src, "kind", "?")
        where = getattr(src, "base_url", None) or getattr(src, "account_url", None) or ""
        out.append(f"**Source:** {kind} — {where}")
    if spec.storage is not None:
        out.append(
            f"**Storage:** `{spec.storage.account_name}` · containers "
            + ", ".join(f"`{c}`" for c in spec.storage.containers)
        )
    out.append("")

    # Steps. Bronze is one line (it's just a copy); Silver/Gold list the ACTUAL
    # operations one per bullet with the real expression — that's where runtime
    # errors live, so that's what the human verifies. No prose explanations.
    out.append("## Steps")
    # Sink formats ride on every line — the reviewer and the human must see
    # WHERE each stage lands and AS WHAT (Delta/CSV/Parquet), not infer it.
    ds_kind = {d.name: d.kind.value for d in spec.datasets}

    def _fmt(ds_name: str) -> str:
        kind = ds_kind.get(ds_name)
        return f" [{kind}]" if kind else ""

    _, bronze_act = find_bronze_copy(spec)
    if bronze_act is not None:
        out.append(
            f"- **Bronze**: copy `{bronze_act.source_dataset}` → "
            f"`{bronze_act.sink_dataset}`{_fmt(bronze_act.sink_dataset)} (raw, no transform)"
        )
    for pl in spec.pipelines:
        for act in pl.activities:
            if act.type == "Copy" and act is not bronze_act:
                out.append(
                    f"- **Copy** `{act.name}`: "
                    f"`{act.source_dataset}` → `{act.sink_dataset}`{_fmt(act.sink_dataset)}"
                )
    for df in spec.dataflows:
        label = df.layer.value.capitalize() if df.layer else "Transform"
        out.append(
            f"- **{label}** `{df.name}` (`{df.source.dataset}` → "
            f"`{df.sink.dataset}`{_fmt(df.sink.dataset)}):"
        )
        for i, t in enumerate(df.transforms, 1):
            out.append(f"  {i}. {_transform_english(t)}")
        if not df.transforms:
            out.append("  1. passthrough")
    out.append("")

    # Orchestration
    out.append("## Orchestration")
    master_lines = 0
    for pl in spec.pipelines:
        acts = [a for a in pl.activities if a.type == "ExecutePipeline"]
        if acts and len(acts) == len(pl.activities):
            chain = " -> ".join(a.invoked_pipeline for a in pl.activities)
            out.append(f"- **{pl.name}** (master): {chain} (each on Succeeded).")
            master_lines += 1
    if master_lines == 0:
        for pl in spec.pipelines:
            work = " -> ".join(a.name for a in pl.activities)
            if work:
                out.append(f"- **{pl.name}**: {work}.")
    for tr in spec.triggers:
        detail = "manual" if tr.kind.value == "manual" else f"{tr.frequency or 'Day'} schedule"
        out.append(f"- Trigger `{tr.name}`: {detail} → `{tr.pipeline}`.")
    if not spec.triggers:
        out.append("- No triggers — the pipeline runs on demand.")
    out.append("")

    out.append("## Cannot build")
    if spec.unsupported:
        out.extend(f"- {u}" for u in spec.unsupported)
    else:
        out.append("- Nothing identified as unsupported for this plan.")
    out.append("")

    # Only genuine blockers
    if spec.clarifications:
        out.append("## I need one thing before building")
        out.extend(f"- {c}" for c in spec.clarifications)
        out.append("")

    return "\n".join(out).rstrip() + "\n"
