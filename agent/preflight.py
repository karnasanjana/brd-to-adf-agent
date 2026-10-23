"""Pre-flight validation — the offline safety guard that gates approval.

Phase 1 of the three-phase plan ("Local dry-run"): JSON valid? spec correct?
naming ok? cross-references resolve? Bronze Copy present? Silver/Gold scripts
present? No Azure access required. Approval is blocked unless this passes.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List

from agent.builder import DEPLOY_ORDER
from agent.review import load_script, load_spec
from models.spec import PipelineSpec


def _check(name: str, passed: bool, detail: str = "") -> Dict[str, Any]:
    return {"name": name, "passed": bool(passed), "detail": detail}


def validate(run_id: str, root: str = "artifacts") -> Dict[str, Any]:
    run_dir = os.path.join(root, run_id)
    checks: List[Dict[str, Any]] = []

    # 1. spec.json present and re-validates against the contract.
    spec: PipelineSpec | None = None
    try:
        spec = load_spec(run_dir)
        checks.append(_check("spec_valid", True, "spec.json validates against PipelineSpec"))
    except Exception as exc:  # noqa: BLE001
        checks.append(_check("spec_valid", False, f"{type(exc).__name__}: {exc}"))
        return {"run_id": run_id, "passed": False, "checks": checks}

    # 2. Naming conventions.
    violations = spec.naming_violations()
    checks.append(
        _check("naming_ok", not violations, "; ".join(violations) or "ls_/ds_/df_/pl_ ok")
    )

    # 3. Cross-references resolve.
    xref = spec.cross_reference_errors()
    checks.append(
        _check("cross_references_resolve", not xref, "; ".join(xref) or "all references resolve")
    )

    # 4. Every artifact JSON file is well-formed.
    bad_json: List[str] = []
    for category in DEPLOY_ORDER:
        cat_dir = os.path.join(run_dir, category)
        if not os.path.isdir(cat_dir):
            continue
        for fname in os.listdir(cat_dir):
            if not fname.endswith(".json"):
                continue
            try:
                with open(os.path.join(cat_dir, fname), "r", encoding="utf-8") as fh:
                    json.load(fh)
            except Exception as exc:  # noqa: BLE001
                bad_json.append(f"{category}/{fname}: {exc}")
    checks.append(_check("json_well_formed", not bad_json, "; ".join(bad_json) or "all JSON parses"))

    # 5. At least one pipeline with real, executable work.
    work = [
        f"{pl.name}.{act.name}"
        for pl in spec.pipelines
        for act in pl.activities
        if act.type in ("Copy", "ExecuteDataFlow")
    ]
    checks.append(
        _check(
            "executable_work_present",
            bool(work),
            "; ".join(work[:5]) or "no Copy or ExecuteDataFlow activity anywhere",
        )
    )

    # 6. Every declared data flow has a non-trivial script with a sink, and the
    #    script LINTS clean — the same classes of problem ADF's Spark broker
    #    rejects at runtime (DF-DSL-001/DF-AGG-005) caught here in milliseconds.
    from agent.lint import lint_dataflow_script

    for df in spec.dataflows:
        script = load_script(run_dir, df.name)
        problems = lint_dataflow_script(df.name, script)
        checks.append(
            _check(
                f"script_{df.name}",
                not problems,
                "; ".join(problems) or f"{len(script.splitlines())} script lines, lint clean",
            )
        )

    # 6b. Container names are concrete and Azure-legal. A parameterized or
    #     underscore container fails at run time with AdlsGen2InvalidResourceName
    #     (run df2db30c73a5). The spec validator repairs inferable cases; this
    #     blocks whatever slips through.
    from models.spec import _AZ_CONTAINER_RE

    bad_containers = [
        f"{ds.name}: '{ds.location.container}'"
        for ds in spec.datasets
        if ds.location is not None
        and not _AZ_CONTAINER_RE.fullmatch((ds.location.container or "").strip())
    ]
    checks.append(
        _check(
            "container_names",
            not bad_containers,
            "; ".join(bad_containers) or "all dataset containers are concrete, valid names",
        )
    )

    # 7. Medallion wiring: a data flow must never write back onto its own input
    #    (in-place overwrite — silver stays empty, bronze gets clobbered), a
    #    silver/gold flow must land in its layer's container, and gold must not
    #    read bronze when a silver layer exists. The spec validator repairs the
    #    common cases; this is the backstop that blocks whatever slips through.
    wiring: List[str] = []
    ds_by_name = {d.name: d for d in spec.datasets}

    def _container(ds_name: str) -> str:
        ds = ds_by_name.get(ds_name)
        if ds is None or ds.location is None or not ds.location.container:
            return ""
        return ds.location.container.strip().lower()

    def _layer(df) -> "str | None":
        if df.layer is not None:
            return df.layer.value
        name = df.name.lower()
        return next((l for l in ("gold", "silver") if l in name), None)

    containers = {_container(d.name) for d in spec.datasets}
    silver_flows = [df for df in spec.dataflows if _layer(df) == "silver"]
    for df in spec.dataflows:
        if df.sink.dataset == df.source.dataset:
            wiring.append(
                f"{df.name}: sink dataset == source dataset ('{df.sink.dataset}') "
                f"— flow overwrites its own input"
            )
        layer = _layer(df)
        sink_container = _container(df.sink.dataset)
        if layer and layer in containers and sink_container and sink_container != layer:
            wiring.append(
                f"{df.name}: {layer} flow sinks to container '{sink_container}', "
                f"expected '{layer}'"
            )
        if layer == "gold" and silver_flows:
            for ep in (df.source, *df.additional_sources):
                if _container(ep.dataset) == "bronze":
                    wiring.append(
                        f"{df.name}: gold flow reads bronze dataset '{ep.dataset}' "
                        f"— must read the silver layer"
                    )
    checks.append(_check("medallion_wiring", not wiring, "; ".join(wiring) or "layer wiring ok"))

    # 7b. Expressions type-check against the (grounded) source schemas: walk each
    #     flow's transform chain tracking column types; catch arithmetic on uncast
    #     string columns, coalesce() mixing types, date parts of non-temporal
    #     columns, sum/avg on text, numeric comparisons on strings. BLOCKING —
    #     this is the DF-DT-001 class ADF only reports minutes into a run; the
    #     /plan repair loop feeds these lines back to the model, which adds the
    #     missing casts itself.
    from agent.typecheck import check_spec

    type_problems = check_spec(spec)
    checks.append(
        _check(
            "expression_types",
            not type_problems,
            "; ".join(type_problems) or "all expressions type-check against source schemas",
        )
    )

    # 8. No BRD requirement silently dropped: activities the builder can't render
    #    are reported by name. Informational (doesn't block) — the plan review
    #    shows the user exactly what was left out.
    from agent.builder import unsupported_activities

    dropped = unsupported_activities(spec)
    checks.append(
        _check(
            "all_activities_supported",
            True,  # informational: visible, never blocking
            ("NOT built (unsupported): " + "; ".join(dropped)) if dropped
            else "every activity type is supported",
        )
    )

    passed = all(c["passed"] for c in checks)
    return {"run_id": run_id, "passed": passed, "checks": checks}
