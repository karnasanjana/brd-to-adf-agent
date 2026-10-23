"""PLAN_REVIEW: one LLM pass comparing the BRD against the finished plan.

Deterministic preflight proves the plan BUILDS; it cannot prove the plan says
what the BRD says. This reviewer reads both and reports semantic gaps —
requirements missing from the plan or contradicted by it. Findings are
ADVISORY: they render in the review panel next to the approval gate and never
block it (a second LLM opinion can itself be wrong; the deterministic checks
stay the hard floor).

Enabled by default; PLAN_REVIEW=false turns it off. Skipped in MOCK mode and
on cache hits (a cached plan was already reviewed when it was first built).
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

_PROMPT = """\
You are a strict, terse reviewer for a system that turns a BRD (business
requirements document) into an Azure Data Factory pipeline plan. You are given
the original requirements and the final logical plan (JSON).

Report ONLY genuine gaps:
- a requirement the plan does not implement at all (severity "missing"),
- a requirement the plan implements incorrectly/contradicts (severity "wrong").

Do NOT report: style; items already listed in the plan's `unsupported`,
`assumptions`, `suggestions`, or `clarifications`; implementation details the
compiler owns (naming, datasets, linked services, dependency wiring, manual
triggers). If the plan covers the BRD, return an empty findings list — most
plans should produce zero or very few findings.
"""


class ReviewFinding(BaseModel):
    requirement: str = Field(description="The BRD requirement, quoted or tightly paraphrased.")
    issue: str = Field(description="What the plan is missing or getting wrong, one sentence.")
    severity: str = Field(description="'missing' or 'wrong'.")


class PlanReview(BaseModel):
    findings: List[ReviewFinding] = Field(default_factory=list)


def enabled() -> bool:
    return os.environ.get("PLAN_REVIEW", "true").strip().lower() not in {
        "0", "false", "no", "off",
    }


async def review_plan(brd_text: str, plan: Any) -> List[Dict[str, str]]:
    """(BRD, LogicalPlan) -> advisory findings. Best-effort: any failure
    returns [] — the reviewer must never break a good plan."""
    if not brd_text:
        return []
    try:
        from pydantic_ai import Agent

        from agent.retry import retry_async
        from config import default_model_settings, get_model, structured_output

        agent = Agent(
            get_model(),
            output_type=structured_output(PlanReview),
            system_prompt=_PROMPT,
            model_settings=default_model_settings(),
        )
        prompt = (
            f"Requirements:\n{brd_text}\n\n"
            f"Final plan (JSON):\n{plan.model_dump_json(indent=1)}"
        )
        result = await retry_async(lambda: agent.run(prompt))
        return [f.model_dump() for f in result.output.findings]
    except Exception:  # noqa: BLE001
        return []
