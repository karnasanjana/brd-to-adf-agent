"""Handlers for the tool-driven plan agent: validate -> mutate -> shape errors.

Architecture ported from microsoft/DataFactory.MCP (MIT), re-targeted from
Fabric REST to THIS project's classic-ADF pipeline:

    Tools (agent/plantools.py)  ->  Handlers (this file)  ->  Services
                                                              (completeness /
                                                               typecheck /
                                                               compiler)

Instead of one giant BRD -> LogicalPlan extraction that either lands or
doesn't, the model builds the plan INCREMENTALLY through small tools. Every
call is validated immediately (Pydantic + the deterministic completeness
critic) and returns a ToolResult — ``{ok, errors, warnings, state}`` — so the
model fixes each mistake the moment it makes it, not in a whole-plan repair
loop at the end.

This module is deliberately framework-agnostic (no pydantic-ai imports): the
handlers are plain methods on ``PlanSession`` returning dicts, so they can be
registered on any tool runtime — pydantic-ai today, a real MCP server later —
exactly like DataFactory.MCP's handler layer serves two MCP wrappers.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import ValidationError

from agent.completeness import check
from models.logical import (
    AzureTargetPlan,
    ContainerPlan,
    EntityTransform,
    LogicalPipeline,
    LogicalPlan,
    LogicalSource,
    OutputPlan,
    SchedulePlan,
)

_NOTE_KINDS = ("unsupported", "assumption", "clarification", "suggestion")


def _ok(state: Dict[str, Any], warnings: Optional[List[str]] = None) -> Dict[str, Any]:
    return {"ok": True, "errors": [], "warnings": warnings or [], "state": state}


def _err(errors: List[str], state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {"ok": False, "errors": errors, "warnings": [], "state": state or {}}


def _fmt_validation(exc: ValidationError) -> List[str]:
    """ValidationError -> short actionable lines the model can fix from."""
    out: List[str] = []
    for e in exc.errors():
        loc = ".".join(str(p) for p in e["loc"] if p != "__root__") or "value"
        out.append(f"{loc}: {e['msg']}")
    return out


class PlanSession:
    """Mutable draft of a LogicalPlan, built up across tool calls.

    The session is the tool-agent's state (DataFactory.MCP keeps auth state in
    a singleton service; we keep the draft plan here). ``plan()`` materializes
    the current draft as a validated LogicalPlan.
    """

    def __init__(self) -> None:
        self.project: str = "pipeline"
        self.sources: "Dict[str, LogicalSource]" = {}
        self.transforms: "Dict[str, EntityTransform]" = {}
        self.outputs: "Dict[str, OutputPlan]" = {}
        self.pipelines: "List[LogicalPipeline]" = []
        self.schedule: "Optional[SchedulePlan]" = None
        self.azure_target: "Optional[AzureTargetPlan]" = None
        self.containers: "Optional[ContainerPlan]" = None
        self.notes: "Dict[str, List[str]]" = {k: [] for k in _NOTE_KINDS}
        self.finalized: bool = False

    # ------------------------------------------------------------------ #
    # Construction / materialization
    # ------------------------------------------------------------------ #

    @classmethod
    def from_plan(cls, plan: "Optional[LogicalPlan]") -> "PlanSession":
        s = cls()
        if plan is None:
            return s
        s.project = plan.project
        s.sources = {src.entity: src for src in plan.sources}
        s.transforms = {t.entity: t for t in plan.transforms}
        s.outputs = {o.name: o for o in plan.outputs}
        s.pipelines = list(plan.pipelines)
        s.schedule = plan.schedule
        s.azure_target = plan.azure_target
        s.containers = plan.containers
        s.notes = {
            "unsupported": list(plan.unsupported),
            "assumption": list(plan.assumptions),
            "clarification": list(plan.clarifications),
            "suggestion": list(plan.suggestions),
        }
        return s

    def plan(self, *, include_pipelines: bool = True) -> LogicalPlan:
        return LogicalPlan(
            project=self.project,
            sources=list(self.sources.values()),
            transforms=list(self.transforms.values()),
            outputs=list(self.outputs.values()),
            pipelines=list(self.pipelines) if include_pipelines else [],
            schedule=self.schedule,
            azure_target=self.azure_target,
            containers=self.containers or ContainerPlan(),
            unsupported=list(self.notes["unsupported"]),
            assumptions=list(self.notes["assumption"]),
            clarifications=list(self.notes["clarification"]),
            suggestions=list(self.notes["suggestion"]),
        )

    def _state(self) -> Dict[str, Any]:
        """Compact draft summary returned with every ToolResult so the model
        always knows what it has built so far."""
        return {
            "project": self.project,
            "sources": sorted(self.sources),
            "transforms": sorted(self.transforms),
            "outputs": sorted(self.outputs),
            "pipelines": [p.name for p in self.pipelines],
            "schedule": self.schedule.model_dump() if self.schedule else None,
            "unsupported": list(self.notes["unsupported"]),
            "clarifications": list(self.notes["clarification"]),
        }

    # ------------------------------------------------------------------ #
    # Delta validation: problems INTRODUCED by one change, not the whole
    # draft's problems. Pipelines are excluded here (coverage is meaningless
    # mid-build); status()/finalize() check the full plan.
    # ------------------------------------------------------------------ #

    def _delta_problems(self, mutate) -> List[str]:
        before = set(check(self.plan(include_pipelines=False)))
        rollback = mutate()
        after = check(self.plan(include_pipelines=False))
        new = [p for p in after if p not in before]
        if new:
            rollback()
        return new

    # ------------------------------------------------------------------ #
    # Handlers (one per tool)
    # ------------------------------------------------------------------ #

    def upsert_source(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        try:
            src = LogicalSource.model_validate(fields)
        except ValidationError as exc:
            return _err(_fmt_validation(exc), self._state())

        def mutate():
            prev = self.sources.get(src.entity)
            self.sources[src.entity] = src

            def rollback():
                if prev is None:
                    self.sources.pop(src.entity, None)
                else:
                    self.sources[src.entity] = prev

            return rollback

        problems = self._delta_problems(mutate)
        if problems:
            return _err(problems, self._state())
        return _ok(self._state())

    def upsert_transform(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        try:
            t = EntityTransform.model_validate(fields)
        except ValidationError as exc:
            return _err(_fmt_validation(exc), self._state())
        if t.entity not in self.sources:
            return _err(
                [f"transform references unknown entity '{t.entity}' — add_source it first "
                 f"(known: {sorted(self.sources)})"],
                self._state(),
            )

        def mutate():
            prev = self.transforms.get(t.entity)
            self.transforms[t.entity] = t

            def rollback():
                if prev is None:
                    self.transforms.pop(t.entity, None)
                else:
                    self.transforms[t.entity] = prev

            return rollback

        problems = self._delta_problems(mutate)
        if problems:
            return _err(problems, self._state())
        return _ok(self._state())

    def upsert_output(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        try:
            out = OutputPlan.model_validate(fields)
        except ValidationError as exc:
            return _err(_fmt_validation(exc), self._state())
        unknown = [e for e in out.from_entities if e not in self.sources]
        if unknown:
            return _err(
                [f"output '{out.name}': unknown entities {unknown} — add_source them first "
                 f"(known: {sorted(self.sources)})"],
                self._state(),
            )

        def mutate():
            prev = self.outputs.get(out.name)
            self.outputs[out.name] = out

            def rollback():
                if prev is None:
                    self.outputs.pop(out.name, None)
                else:
                    self.outputs[out.name] = prev

            return rollback

        problems = self._delta_problems(mutate)
        if problems:
            return _err(problems, self._state())
        return _ok(self._state())

    def set_pipelines(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Replace the whole pipeline organisation at once (it is validated as a
        set: coverage + invokes only make sense together)."""
        try:
            pls = [LogicalPipeline.model_validate(i) for i in items]
        except ValidationError as exc:
            return _err(_fmt_validation(exc), self._state())
        prev = self.pipelines
        self.pipelines = pls
        problems = [
            p for p in check(self.plan())
            if p not in set(check(self.plan(include_pipelines=False)))
        ]
        if problems:
            self.pipelines = prev
            return _err(problems, self._state())
        return _ok(self._state())

    def set_project(self, project: str) -> Dict[str, Any]:
        self.project = project or "pipeline"
        # LogicalPlan's validator normalizes; materialize once to apply it.
        self.project = self.plan(include_pipelines=False).project
        return _ok(self._state())

    def set_schedule(
        self, frequency: str, interval: int = 1, start_time: Optional[str] = None
    ) -> Dict[str, Any]:
        try:
            self.schedule = SchedulePlan(
                frequency=frequency, interval=interval, start_time=start_time
            )
        except ValidationError as exc:
            return _err(_fmt_validation(exc), self._state())
        return _ok(self._state())

    def set_containers(
        self,
        bronze: Optional[str] = None,
        silver: Optional[str] = None,
        gold: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Container names the BRD gives for each layer; unset fields keep the
        medallion default."""
        base = self.containers or ContainerPlan()
        try:
            self.containers = ContainerPlan(
                bronze=(bronze or base.bronze),
                silver=(silver or base.silver),
                gold=(gold or base.gold),
            )
        except ValidationError as exc:
            return _err(_fmt_validation(exc), self._state())
        return _ok(self._state())

    def set_azure_target(
        self,
        resource_group: Optional[str] = None,
        factory_name: Optional[str] = None,
        storage_account: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Record the deploy target the BRD NAMES. Advisory: the user confirms
        it in the UI before any deploy — never applied silently."""
        target = AzureTargetPlan(
            resource_group=(resource_group or "").strip() or None,
            factory_name=(factory_name or "").strip() or None,
            storage_account=(storage_account or "").strip() or None,
        )
        self.azure_target = None if target.is_empty() else target
        return _ok(self._state())

    def add_note(self, kind: str, text: str) -> Dict[str, Any]:
        kind = (kind or "").strip().lower()
        if kind not in _NOTE_KINDS:
            return _err([f"unknown note kind '{kind}'. Allowed: {', '.join(_NOTE_KINDS)}"])
        text = (text or "").strip()
        if text and text not in self.notes[kind]:
            self.notes[kind].append(text)
        return _ok(self._state())

    def remove_item(self, kind: str, name: str) -> Dict[str, Any]:
        """Remove a source / transform / output / pipeline / schedule / note by name."""
        kind = (kind or "").strip().lower()
        name = (name or "").strip()
        if kind == "source":
            if self.sources.pop(name, None) is None:
                return _err([f"no source '{name}' (known: {sorted(self.sources)})"], self._state())
            self.transforms.pop(name, None)  # orphaned transform goes with it
        elif kind == "transform":
            if self.transforms.pop(name, None) is None:
                return _err([f"no transform '{name}'"], self._state())
        elif kind == "output":
            if self.outputs.pop(name, None) is None:
                return _err([f"no output '{name}' (known: {sorted(self.outputs)})"], self._state())
        elif kind == "pipeline":
            kept = [p for p in self.pipelines if p.name != name]
            if len(kept) == len(self.pipelines):
                return _err([f"no pipeline '{name}'"], self._state())
            self.pipelines = kept
        elif kind == "schedule":
            self.schedule = None
        elif kind in _NOTE_KINDS:
            self.notes[kind] = [n for n in self.notes[kind] if n != name]
        else:
            return _err(
                ["unknown kind. Allowed: source, transform, output, pipeline, schedule, "
                 + ", ".join(_NOTE_KINDS)]
            )
        return _ok(self._state())

    def status(self) -> Dict[str, Any]:
        """Full completeness check of the current draft — call any time."""
        problems = check(self.plan())
        if problems:
            return _err(problems, self._state())
        return _ok(self._state())

    def finalize(self) -> Dict[str, Any]:
        """The gate: completeness + compile + dataflow typecheck must all pass.

        Mirrors DataFactory.MCP's handler pattern of running validation BEFORE
        the service call — here the 'service' is the deterministic compiler, so
        a plan that finalizes cleanly is a plan that builds.
        """
        plan = self.plan()
        problems = check(plan)
        if problems:
            return _err(problems, self._state())
        try:
            from agent.compiler import compile_plan
            from agent.typecheck import check_spec

            spec = compile_plan(plan)
            problems = check_spec(spec)
        except Exception as exc:  # compiler raise = plan bug, surface verbatim
            return _err([f"compile failed: {exc}"], self._state())
        if problems:
            return _err(problems, self._state())
        self.finalized = True
        return _ok(self._state())
