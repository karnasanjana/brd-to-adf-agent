"""Skill loader: inject reusable Azure/ADF knowledge into the agent's prompt.

This mirrors the "Skills for Fabric" idea but for THIS agent's platform (ADF) and
THIS agent's LLM. A skill is a markdown file under ``skills/`` with optional
frontmatter (name/description). At session start the loader reads every ``.md``
file and concatenates the bodies into one block that the parser appends to its
system prompt — so the model "learns" the skills as in-context instructions.

Add a new skill by dropping a ``.md`` file into ``skills/`` — no code change needed.
Skills are static knowledge (they don't execute); the deterministic builder still
does the actual ADF JSON generation.
"""

from __future__ import annotations

import functools
import os
from typing import List, Optional, Tuple

# skills/ lives at the project root (one level up from this agent/ package).
_SKILLS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "skills")


def _split_frontmatter(text: str) -> Tuple[str, str]:
    """Return (name, body). Strips a leading ``---`` frontmatter block and pulls out
    the ``name:`` field if present."""
    name = ""
    body = text
    if text.lstrip().startswith("---"):
        parts = text.split("---", 2)
        if len(parts) == 3:
            front, body = parts[1], parts[2]
            for line in front.splitlines():
                if line.strip().lower().startswith("name:"):
                    name = line.split(":", 1)[1].strip()
    return name, body.strip()


def list_skills(skills_dir: str = _SKILLS_DIR) -> List[str]:
    """Names of the skill files that will be loaded (for logging / the UI)."""
    if not os.path.isdir(skills_dir):
        return []
    names: List[str] = []
    for fname in sorted(os.listdir(skills_dir)):
        if fname.endswith(".md"):
            with open(os.path.join(skills_dir, fname), "r", encoding="utf-8") as fh:
                name, _ = _split_frontmatter(fh.read())
            names.append(name or fname[:-3])
    return names


@functools.lru_cache(maxsize=1)
def load_skills(skills_dir: str = _SKILLS_DIR) -> str:
    """Concatenate every skill body into one prompt-ready block. Cached so files are
    read once per process. Returns '' when there are no skills."""
    if not os.path.isdir(skills_dir):
        return ""
    blocks: List[str] = []
    for fname in sorted(os.listdir(skills_dir)):
        if not fname.endswith(".md"):
            continue
        with open(os.path.join(skills_dir, fname), "r", encoding="utf-8") as fh:
            _, body = _split_frontmatter(fh.read())
        if body:
            blocks.append(body)
    if not blocks:
        return ""
    header = (
        "# Azure / ADF Skills\n"
        "Reusable, authoritative guidance for generating Azure Data Factory artifacts. "
        "Follow these exactly; they reflect real ADF behavior.\n"
    )
    return header + "\n\n---\n\n".join(blocks)


# --------------------------------------------------------------------------- #
# Learned lessons: the feedback half of the skills loop
# --------------------------------------------------------------------------- #
# Skills flow knowledge INTO the model at session start; this flows it BACK.
# When self-heal fixes a runtime failure, the incident is distilled into ONE
# generic rule and appended to skills/99-learned-lessons.md — which the loader
# above already picks up. The system grows its own knowledge; no developer
# writes per-case code.

_LESSONS_FILE = "99-learned-lessons.md"
_LESSONS_HEADER = """\
---
name: learned-lessons
description: Rules distilled automatically from self-heal incidents. Appended by the agent, not hand-written.
---
# Learned lessons (auto-appended from self-heal)
Rules below were learned from real runtime failures. Follow them when planning
transformations and writing data-flow expressions.
"""

_LESSON_PROMPT = """\
You maintain a lessons file for an agent that turns BRDs into Azure Data
Factory pipelines. A pipeline artifact failed at runtime and was fixed by a
self-heal. Distill the incident into ONE generic, reusable rule for FUTURE
pipeline generation so this class of error never happens again.

Rules for the rule:
- Generic, not incident-specific: no artifact names, dataset names, or run ids.
- Imperative, max 2 sentences. Include the ADF error code when present.
- It must be actionable while PLANNING transformations (choosing ops, args,
  types, expressions), not advice about fixing deployed JSON.
- If the existing lessons already cover this failure class, return covered=true
  and no lesson.
"""


_PLAN_LESSON_PROMPT = """\
You maintain a lessons file for an agent that turns BRDs into Azure Data
Factory pipelines. During planning, the model repeatedly made the same class of
mistake; a repair round fixed it each time. Distill the mistake into ONE
generic, reusable rule for FUTURE plan generation so it stops recurring.

Rules for the rule:
- MODEL-INDEPENDENT: describe how to use the plan vocabulary correctly (ops,
  casts, derived columns, group_by, joins, filters) — never mention a model
  name, a model behavior, or this incident's entity/column names.
- Imperative, max 2 sentences, phrased as an instruction the planner follows
  while building the plan.
- If the existing lessons already cover this mistake class, return covered=true
  and no lesson.
"""

# Keep the prompt-visible lessons bounded — past this, new rules are dropped
# and the file needs a human prune (quality of 30 rules beats quantity of 300).
_MAX_LESSONS = 30


def record_plan_lesson(problem_example: str, skills_dir: str = _SKILLS_DIR) -> Optional[str]:
    """Distill a repeatedly-repaired PLANNING mistake into a generic rule and
    append it to skills/99-learned-lessons.md. Returns the rule, or None when
    covered/capped. Never raises."""
    from pydantic import BaseModel, Field

    class Lesson(BaseModel):
        covered: bool = Field(description="True when existing lessons already cover this mistake class.")
        lesson: str = Field(default="", description="The single generic rule; empty when covered.")

    try:
        from pydantic_ai import Agent

        from config import get_model, structured_output

        path = lessons_path(skills_dir)
        existing = ""
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                existing = fh.read()
        if existing.count("\n- ") >= _MAX_LESSONS:
            return None

        agent = Agent(
            get_model(), output_type=structured_output(Lesson), system_prompt=_PLAN_LESSON_PROMPT
        )
        out = agent.run_sync(
            f"Repeated planning mistake (one example):\n{problem_example}\n\n"
            f"Existing lessons file:\n{existing or '(empty)'}"
        ).output
        rule = (out.lesson or "").strip()
        if out.covered or not rule or rule in existing:
            return None

        os.makedirs(skills_dir, exist_ok=True)
        with open(path, "a", encoding="utf-8") as fh:
            if not existing:
                fh.write(_LESSONS_HEADER + "\n")
            fh.write(f"- {rule}\n")
        load_skills.cache_clear()
        return rule
    except Exception:  # noqa: BLE001
        return None


def lessons_path(skills_dir: str = _SKILLS_DIR) -> str:
    return os.path.join(skills_dir, _LESSONS_FILE)


def record_lesson(
    error_message: str, fix_explanation: str, skills_dir: str = _SKILLS_DIR
) -> Optional[str]:
    """Distill a successful self-heal into a generic rule and append it to
    skills/99-learned-lessons.md. Returns the rule, or None when the existing
    lessons already cover the failure class. Never raises — a failed lesson
    must not break the heal that triggered it."""
    from pydantic import BaseModel, Field

    class Lesson(BaseModel):
        covered: bool = Field(description="True when existing lessons already cover this failure class.")
        lesson: str = Field(default="", description="The single generic rule; empty when covered.")

    try:
        from pydantic_ai import Agent

        from config import get_model, structured_output

        path = lessons_path(skills_dir)
        existing = ""
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as fh:
                existing = fh.read()

        agent = Agent(
            get_model(), output_type=structured_output(Lesson), system_prompt=_LESSON_PROMPT
        )
        out = agent.run_sync(
            f"Runtime error:\n{error_message}\n\n"
            f"What the self-heal changed:\n{fix_explanation}\n\n"
            f"Existing lessons file:\n{existing or '(empty)'}"
        ).output
        rule = (out.lesson or "").strip()
        if out.covered or not rule:
            return None
        if rule in existing:  # literal duplicate — belt and braces
            return None

        os.makedirs(skills_dir, exist_ok=True)
        with open(path, "a", encoding="utf-8") as fh:
            if not existing:
                fh.write(_LESSONS_HEADER + "\n")
            fh.write(f"- {rule}\n")
        load_skills.cache_clear()  # same-process parsers see the new rule
        return rule
    except Exception:
        return None
