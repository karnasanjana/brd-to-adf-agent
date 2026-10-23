"""Failure triage + the plan-phase half of the learned-lessons loop.

Two jobs, one goal — stop dead-end errors and stop per-case code fixes:

1. QUESTIONS (user problems): when the repair loops exhaust without converging,
   the leftover problems are turned into plain answerable questions and routed
   to the chat (status NEEDS_CLARIFICATION) instead of a 422. The user answers;
   ``refine_plan`` continues the same conversation.

2. LESSON CANDIDATES (model problems): every problem the repair loop DID fix is
   evidence of a repeatable model mistake. Candidates are appended to
   ``artifacts/lesson_candidates.jsonl``; once the same *signature* has been
   seen ``_MIN_SEEN`` times the incident is distilled into one generic rule in
   ``skills/99-learned-lessons.md`` (the file every planning prompt already
   loads). Signatures and lessons are phrased in plan-vocabulary terms — never
   per-model — so they survive model swaps.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from typing import Any, Dict, Iterable, List, Optional

# --------------------------------------------------------------------------- #
# Questions — residual problems become chat questions, not errors
# --------------------------------------------------------------------------- #

_MAX_QUESTIONS = 5

# Problem shapes a USER can actually answer (they reference the BRD's data or
# business intent). Anything else is an internal defect — asking the user about
# it just moves the confusion; it gets an honest retry/report line instead.
_USER_ANSWERABLE = (
    "is not a column of",
    "unknown column",
    "not a source entity",
    "unsupported",
    "ambiguous",
    "can never match",  # case-branch thresholds are a business decision
)


def is_user_answerable(problem: str) -> bool:
    p = str(problem).lower()
    return any(marker in p for marker in _USER_ANSWERABLE)


def question_for(problem: str) -> str:
    """One residual checker/preflight problem -> one plain, answerable question."""
    p = str(problem).strip()
    if "is not a column of" in p:
        return (
            f"I couldn't resolve this after several attempts: {p} — which existing "
            "column should be used here, or how should it be derived?"
        )
    if "unsupported" in p.lower():
        return (
            f"I couldn't express this requirement: {p} — should I use the closest "
            "supported alternative, or drop it?"
        )
    return (
        f"I couldn't resolve this automatically: {p} — how should this be handled? "
        "Answer here and I'll update the plan."
    )


def questions_for(problems: Iterable[str]) -> List[str]:
    """User-answerable problems become questions; internal residue gets ONE
    honest line (retry / reword / report) instead of a fake question."""
    items = [str(p).strip() for p in problems if str(p).strip()]
    user_problems = [p for p in items if is_user_answerable(p)]
    internal = [p for p in items if not is_user_answerable(p)]
    out = [question_for(p) for p in user_problems[:_MAX_QUESTIONS]]
    if internal:
        shown = "; ".join(internal[:2])
        out.append(
            "The remaining issues look internal (not something you can answer): "
            f"{shown} — click Generate again to retry; if it persists, reword "
            "that requirement or report it."
        )
    return out


# --------------------------------------------------------------------------- #
# Lesson candidates — repaired problems, grouped by model-independent signature
# --------------------------------------------------------------------------- #

_QUOTED = re.compile(r"'[^']*'")
_BRACKETED = re.compile(r"\[[^\]]*\]")
_NUMBERS = re.compile(r"\b\d+\b")

# The artifacts root (same parent builder.artifacts_dir uses).
_ARTIFACTS = os.path.join(os.path.dirname(os.path.dirname(__file__)), "artifacts")
_MIN_SEEN = 2  # distill once the same mistake shape repeats


def signature(problem: str) -> str:
    """Collapse a problem message to its SHAPE: quoted names, bracketed lists,
    and numbers become placeholders, so \"output 'monthly_sales': 'order_year'
    is not a column of [...]\" and the same mistake on another output share one
    signature. No model name, no BRD specifics — model-independent by
    construction."""
    s = _QUOTED.sub("'<x>'", str(problem))
    s = _BRACKETED.sub("[...]", s)
    s = _NUMBERS.sub("<n>", s)
    return " ".join(s.split())


def candidates_path(root: str = _ARTIFACTS) -> str:
    return os.path.join(root, "lesson_candidates.jsonl")


def _distilled_path(root: str = _ARTIFACTS) -> str:
    return os.path.join(root, "lesson_distilled.json")


def record_candidates(
    resolved_problems: Iterable[str], root: str = _ARTIFACTS
) -> int:
    """Append one candidate per problem the repair loop just fixed. Best-effort:
    lesson bookkeeping must never break the plan request. Returns #recorded."""
    problems = [str(p).strip() for p in resolved_problems if str(p).strip()]
    if not problems:
        return 0
    try:
        os.makedirs(root, exist_ok=True)
        with open(candidates_path(root), "a", encoding="utf-8") as fh:
            for p in problems:
                fh.write(json.dumps({
                    "signature": signature(p),
                    "example": p,
                    "model": os.environ.get("LLM_MODEL", ""),  # audit only — never in the lesson
                    "ts": int(time.time()),
                }) + "\n")
        return len(problems)
    except Exception:  # noqa: BLE001
        return 0


def _load_candidates(root: str) -> List[Dict[str, Any]]:
    path = candidates_path(root)
    if not os.path.isfile(path):
        return []
    out: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except Exception:  # noqa: BLE001
                    continue
    return out


def _load_distilled(root: str) -> List[str]:
    path = _distilled_path(root)
    if not os.path.isfile(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        return list(data) if isinstance(data, list) else []
    except Exception:  # noqa: BLE001
        return []


def _save_distilled(sigs: List[str], root: str) -> None:
    with open(_distilled_path(root), "w", encoding="utf-8") as fh:
        json.dump(sigs, fh, indent=2)


def ready_groups(root: str = _ARTIFACTS) -> List[Dict[str, Any]]:
    """Signatures seen >= _MIN_SEEN times and not yet distilled, with their
    most recent example."""
    done = set(_load_distilled(root))
    groups: Dict[str, Dict[str, Any]] = {}
    for c in _load_candidates(root):
        sig = c.get("signature") or ""
        if not sig or sig in done:
            continue
        g = groups.setdefault(sig, {"signature": sig, "count": 0, "example": ""})
        g["count"] += 1
        g["example"] = c.get("example") or g["example"]
    return [g for g in groups.values() if g["count"] >= _MIN_SEEN]


def distill_ready(root: str = _ARTIFACTS, skills_dir: Optional[str] = None) -> List[str]:
    """Distill every ready signature into a lesson (one LLM call each) and mark
    it done. Returns the new rules. Never raises."""
    from agent.skills import record_plan_lesson

    rules: List[str] = []
    done = _load_distilled(root)
    for g in ready_groups(root):
        try:
            rule = (
                record_plan_lesson(g["example"], skills_dir=skills_dir)
                if skills_dir is not None
                else record_plan_lesson(g["example"])
            )
        except Exception:  # noqa: BLE001
            continue
        # Mark the signature handled either way — a 'covered' verdict (rule is
        # None) means the lessons file already prevents this class.
        done.append(g["signature"])
        if rule:
            rules.append(rule)
    try:
        os.makedirs(root, exist_ok=True)
        _save_distilled(done, root)
    except Exception:  # noqa: BLE001
        pass
    return rules


def distill_enabled() -> bool:
    return os.environ.get("LESSON_DISTILL", "true").strip().lower() not in {
        "0", "false", "no", "off",
    }


def spawn_distiller(root: str = _ARTIFACTS) -> None:
    """Fire-and-forget background distillation after a successful plan. Skipped
    in MOCK mode (dev/test runs must not spend LLM calls or learn from stubs)."""
    from config import mock_mode

    if mock_mode() or not distill_enabled():
        return
    if not ready_groups(root):
        return
    threading.Thread(target=distill_ready, args=(root,), daemon=True).start()
