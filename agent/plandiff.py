"""Deterministic LogicalPlan diff — the human side of the approval gate.

The LLM can change a plan on any /chat refine; the human should never have to
re-read the whole review to find out what moved. This module compares the
previously accepted plan against the new one and returns plain-English lines
("output 'monthly_sales': changed aggregations, sort") that ride on the /chat
response and render in the UI before re-approval.

Pattern borrowed from DataFactory.MCP's confirm-before-execute app resources:
show the human exactly what is about to be executed — here, exactly what the
model changed since the version they last reviewed. Pure code, no LLM.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from models.logical import LogicalPlan


def _by_key(items: List[Dict[str, Any]], key: str) -> Dict[str, Dict[str, Any]]:
    return {i[key]: i for i in items}


def _changed_fields(old: Dict[str, Any], new: Dict[str, Any]) -> List[str]:
    return sorted(k for k in set(old) | set(new) if old.get(k) != new.get(k))


def _section(
    lines: List[str],
    label: str,
    old_items: List[Dict[str, Any]],
    new_items: List[Dict[str, Any]],
    key: str,
) -> None:
    old_by, new_by = _by_key(old_items, key), _by_key(new_items, key)
    for name in sorted(new_by.keys() - old_by.keys()):
        lines.append(f"{label} '{name}': added")
    for name in sorted(old_by.keys() - new_by.keys()):
        lines.append(f"{label} '{name}': removed")
    for name in sorted(old_by.keys() & new_by.keys()):
        fields = _changed_fields(old_by[name], new_by[name])
        if fields:
            lines.append(f"{label} '{name}': changed {', '.join(fields)}")


def diff(old: "Optional[LogicalPlan]", new: LogicalPlan) -> List[str]:
    """Plain-English lines describing every difference; [] = identical."""
    if old is None:
        return []
    o, n = old.model_dump(mode="json"), new.model_dump(mode="json")
    lines: List[str] = []

    if o["project"] != n["project"]:
        lines.append(f"project: '{o['project']}' -> '{n['project']}'")
    _section(lines, "source", o["sources"], n["sources"], "entity")
    _section(lines, "transform", o["transforms"], n["transforms"], "entity")
    _section(lines, "output", o["outputs"], n["outputs"], "name")
    _section(lines, "pipeline", o["pipelines"], n["pipelines"], "name")
    if o["schedule"] != n["schedule"]:
        lines.append(f"schedule: {o['schedule']} -> {n['schedule']}")

    for field in ("unsupported", "assumptions", "clarifications", "suggestions"):
        added = [x for x in n[field] if x not in o[field]]
        removed = [x for x in o[field] if x not in n[field]]
        for x in added:
            lines.append(f"{field}: + {x}")
        for x in removed:
            lines.append(f"{field}: - {x}")
    return lines
