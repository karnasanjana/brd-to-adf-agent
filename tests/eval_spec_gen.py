"""Eval harness: measure how reliably the CONFIGURED model turns a BRD into a
deployable spec. This is what makes "model-independent" a NUMBER instead of a
hope — run it before and after swapping LLM_MODEL / LLM_PROVIDER and compare.

For each golden BRD it runs the REAL parser N times (LLM calls — costs tokens),
then builds and pre-flights each spec offline (no Azure writes), and reports:

  parse    the model returned a valid PipelineSpec (within Pydantic AI retries)
  build    the deterministic builder produced ADF JSON from it
  preflight  static checks passed (cross-refs, naming, buildability)
  ops%     structured-op adoption: % of transform columns the model expressed
           as {op,...} instead of a raw DSL string (higher = less model risk)

Usage:
    python tests/eval_spec_gen.py [runs-per-brd]              # legacy path, default 3
    python tests/eval_spec_gen.py --logical [runs-per-brd]    # LogicalPlan path
    python tests/eval_spec_gen.py --logical 3 --mode tools    # tool-agent extractor

--mode oneshot (default) measures the single-call extractor; --mode tools
measures the tool-agent path (agent/plantools.py) on the SAME stages, so the
two PLAN_MODE strategies get compared with one number each.

Artifacts go under a temp directory and are deleted afterwards.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time
from typing import Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config  # noqa: E402, F401 — loads .env so the header reports the REAL model
from agent import builder, preflight  # noqa: E402
from samples import COVID_BRD  # noqa: E402

RETAIL_BRD = """\
Business Requirement: Retail order analytics on Azure Data Factory.
We receive olist_orders_dataset.csv in the 'landing' container of our data lake.
Build a medallion pipeline:
- Bronze: copy the file as-is from landing to the bronze container.
- Silver: remove rows with a missing order_id, trim order_status, and cast
  order_purchase_timestamp to a timestamp. Store silver as Delta.
- Gold: count orders per order_status into a CSV in the gold container.
Chain bronze -> silver -> gold in a master pipeline.
"""

COPY_ONLY_BRD = """\
Copy the public CSV file at
https://raw.githubusercontent.com/datasets/population/main/data/population.csv
into the 'raw' container of our ADLS account every night at 01:00 UTC.
No transformations - just land the file as population.csv.
"""

BRDS = {"retail-medallion": RETAIL_BRD, "covid-medallion": COVID_BRD, "copy-only": COPY_ONLY_BRD}

STAGES = ("extract", "check", "build", "preflight", "lint")


def _stage_line(
    rows: List[Dict[str, str]],
    brd: str,
    run: int,
    stage: str,
    passed: bool,
    reason: str = "",
) -> None:
    rows.append({
        "brd": brd,
        "run": str(run),
        "stage": stage,
        "status": "PASS" if passed else "FAIL",
        "reason": reason,
    })


def _print_stage_table(rows: List[Dict[str, str]]) -> None:
    print("\nStage failure table")
    print(f"{'brd':<18} {'run':>3}  {'stage':<10} {'status':<5}  reason")
    for r in rows:
        print(
            f"{r['brd']:<18} {r['run']:>3}  {r['stage']:<10} "
            f"{r['status']:<5}  {r['reason']}"
        )


def _preflight_reason(pf: dict) -> str:
    fails = [c for c in pf.get("checks", []) if not c.get("passed")]
    if not fails:
        return ""
    return "; ".join(f"{c['name']}: {c.get('detail', '')}" for c in fails[:3])


def _ops_adoption(spec) -> str:
    """percentage of transform columns expressed as structured ops."""
    total = structured = 0
    for df in spec.dataflows:
        for t in df.transforms:
            for c in t.columns:
                total += 1
                structured += 1 if c.op else 0
            if t.predicates:
                total += 1
                structured += 1
            elif t.condition:
                total += 1
    if total == 0:
        return "-"
    return f"{100 * structured // total}%"


def main() -> int:
    runs = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    root = tempfile.mkdtemp(prefix="adf_eval_")
    model = os.environ.get("LLM_MODEL", "<provider default>")
    provider = os.environ.get("LLM_PROVIDER", "gemini")  # mirror config.get_model()
    print(f"model={model} provider={provider} runs/brd={runs} artifacts={root}\n")
    print(f"{'brd':<18} {'run':>3}  {'parse':<5} {'build':<5} {'preflight':<9} "
          f"{'ops%':>4}  {'sec':>5}  notes")

    passed = total = 0
    for brd_name, brd in BRDS.items():
        for i in range(1, runs + 1):
            total += 1
            run_id = f"{brd_name}-{i}"
            t0 = time.time()
            note = ""
            parse_ok = build_ok = pf_ok = False
            ops = "-"
            try:
                from agent.parser import parse_brd_sync

                spec = parse_brd_sync(brd)
                parse_ok = True
                ops = _ops_adoption(spec)
                if spec.clarifications and not spec.is_buildable():
                    note = f"blocked on clarifications: {spec.clarifications[:1]}"
                else:
                    builder.write_artifacts(spec, run_id, root=root)
                    build_ok = True
                    pf = preflight.validate(run_id, root=root)
                    pf_ok = bool(pf.get("passed"))
                    if not pf_ok:
                        fails = [c["name"] for c in pf.get("checks", []) if not c.get("passed")]
                        note = f"preflight failed: {', '.join(fails[:3])}"
            except Exception as exc:  # noqa: BLE001 — a failure IS the measurement
                note = f"{type(exc).__name__}: {exc}"[:90]
            secs = time.time() - t0
            passed += 1 if pf_ok else 0
            mark = lambda b: "ok" if b else "FAIL"  # noqa: E731
            print(f"{brd_name:<18} {i:>3}  {mark(parse_ok):<5} {mark(build_ok):<5} "
                  f"{mark(pf_ok):<9} {ops:>4}  {secs:>5.1f}  {note}")

    print(f"\npass rate: {passed}/{total} ({100 * passed // max(total, 1)}%)")
    shutil.rmtree(root, ignore_errors=True)
    return 0 if passed == total else 1


def _raw_expression_count(spec) -> int:
    """Transform columns whose expression did NOT come from a structured op.
    For the logical path this MUST be 0 — every expression is compiler-rendered
    from an op or is a deterministic cast, never model-authored DSL."""
    n = 0
    for df in spec.dataflows:
        for t in df.transforms:
            for c in t.columns:
                # cast transforms carry a compiler-rendered expression with no op;
                # everything else with an expression-but-no-op on a NON-cast/select
                # transform would be model raw text. Compiler never emits that.
                if c.expression and not c.op and t.kind.value not in ("cast", "select"):
                    n += 1
    return n


def _parse_logical_args() -> "tuple[int, str]":
    """(runs, mode) from argv after '--logical'. mode: oneshot (default) | tools."""
    args = sys.argv[2:]
    runs, mode = 3, "oneshot"
    i = 0
    while i < len(args):
        if args[i] == "--mode" and i + 1 < len(args):
            mode = args[i + 1].strip().lower()
            i += 2
        elif args[i].isdigit():
            runs = int(args[i])
            i += 1
        else:
            i += 1
    if mode not in {"oneshot", "tools"}:
        raise SystemExit(f"--mode must be oneshot or tools, got '{mode}'")
    return runs, mode


def main_logical() -> int:
    """Logical-path eval: BRD -> extract_plan -> completeness -> compile ->
    build -> preflight -> lint. The gate for the LogicalPlan refactor."""
    runs, mode = _parse_logical_args()
    root = tempfile.mkdtemp(prefix="adf_eval_logical_")
    model = os.environ.get("LLM_MODEL", "<provider default>")
    print(f"[LOGICAL] model={model} mode={mode} runs/brd={runs} artifacts={root}\n")
    print(f"{'brd':<18} {'run':>3}  {'extract':<7} {'check':<6} {'build':<5} "
          f"{'preflt':<6} {'lint':<5} {'raw':>3}  {'sec':>5}  notes")

    try:
        from agent.completeness import check
        from agent.compiler import compile_plan
        from agent.grounding import ground_logical_plan
        from agent.lint import lint_dataflow_script
        from agent.review import load_script

        if mode == "tools":
            import asyncio

            from agent.plantools import extract_plan as _tools_extract

            def extract(brd_text):
                plan, _ = asyncio.run(_tools_extract(brd_text))
                return plan
        else:
            from agent.extractor import extract_plan_sync as extract
    except Exception as exc:  # noqa: BLE001
        print(f"cannot import logical pipeline: {exc}")
        return 2

    passed = total = 0
    stage_rows: List[Dict[str, str]] = []
    for brd_name, brd in BRDS.items():
        for i in range(1, runs + 1):
            total += 1
            run_id = f"logical-{brd_name}-{i}"
            t0 = time.time()
            note = ""
            ex_ok = chk_ok = build_ok = pf_ok = lint_ok = False
            raw = "-"
            try:
                plan = extract(brd)
                ex_ok = True
                ground_logical_plan(plan)
                problems = check(plan)
                chk_ok = not problems
                if not chk_ok:
                    note = f"check: {problems[:1]}"
                else:
                    spec = compile_plan(plan)
                    raw = str(_raw_expression_count(spec))
                    builder.write_artifacts(spec, run_id, root=root)
                    build_ok = True
                    pf = preflight.validate(run_id, root=root)
                    pf_ok = bool(pf.get("passed"))
                    if not pf_ok:
                        note = f"preflight: {_preflight_reason(pf)}"
                    lint_problems = []
                    for df in spec.dataflows:
                        lint_problems += lint_dataflow_script(
                            df.name, load_script(os.path.join(root, run_id), df.name)
                        )
                    lint_ok = not lint_problems
                    if not lint_ok:
                        note = (note + "; " if note else "") + f"lint: {lint_problems[:1]}"
            except Exception as exc:  # noqa: BLE001
                note = f"{type(exc).__name__}: {exc}"[:90]
            secs = time.time() - t0
            ok = ex_ok and chk_ok and build_ok and pf_ok and lint_ok
            passed += 1 if ok else 0
            mark = lambda b: "ok" if b else "FAIL"  # noqa: E731
            print(f"{brd_name:<18} {i:>3}  {mark(ex_ok):<7} {mark(chk_ok):<6} "
                  f"{mark(build_ok):<5} {mark(pf_ok):<6} {mark(lint_ok):<5} {raw:>3}  "
                  f"{secs:>5.1f}  {note}")
            _stage_line(stage_rows, brd_name, i, "extract", ex_ok, "" if ex_ok else note)
            _stage_line(
                stage_rows, brd_name, i, "check", chk_ok,
                "" if chk_ok else (note if note.startswith("check:") else "not reached" if not ex_ok else note),
            )
            _stage_line(
                stage_rows, brd_name, i, "build", build_ok,
                "" if build_ok else ("not reached" if not chk_ok else note),
            )
            _stage_line(
                stage_rows, brd_name, i, "preflight", pf_ok,
                "" if pf_ok else ("not reached" if not build_ok else note),
            )
            _stage_line(
                stage_rows, brd_name, i, "lint", lint_ok,
                "" if lint_ok else ("not reached" if not build_ok else note),
            )

    rate = 100 * passed // max(total, 1)
    print(f"\n[LOGICAL] pass rate: {passed}/{total} ({rate}%)  gate: >=95%")
    _print_stage_table(stage_rows)
    shutil.rmtree(root, ignore_errors=True)
    return 0 if rate >= 95 else 1


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--logical":
        raise SystemExit(main_logical())
    raise SystemExit(main())
