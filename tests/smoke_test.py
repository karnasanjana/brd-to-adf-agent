"""Offline 'local dry-run' spike - NO Foundry and NO Azure calls.

Proves the de-risked path: Claude only ever returns a TYPED, validated spec;
the deterministic builder turns that into deployable ADF JSON. Here we feed the
builder a hand-built spec (the shape Claude returns) and show it reliably
produces:

  (a) pre-flight PASS on the shipped COVID BRD,
  (b) the Bronze Copy Activity JSON (no script - correct),
  (c) the Silver and Gold Data Flow JSON + scripts,
  (d) the master pipeline Bronze->Silver->Gold dependsOn chaining.

Run:  python tests/smoke_test.py
"""

from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent import builder, preflight, review  # noqa: E402
from models.spec import validate_naming  # noqa: E402
from samples import sample_covid_spec  # noqa: E402

RUN_ID = "smoke"


def rule(title: str) -> None:
    print("\n" + "=" * 76)
    print(title)
    print("=" * 76)


def main() -> int:
    spec = sample_covid_spec()

    rule("NAMING VALIDATION (ls_/ds_/df_/pl_)")
    violations = validate_naming(spec)
    print("OK - no violations" if not violations else "\n".join(violations))
    assert not violations

    # Write artifacts the same way /plan does, then run pre-flight on them.
    run_dir = builder.write_artifacts(spec, RUN_ID)
    print("artifacts ->", run_dir)

    rule("(a) PRE-FLIGHT on the shipped COVID BRD")
    pf = preflight.validate(RUN_ID)
    for c in pf["checks"]:
        print(f"  [{'PASS' if c['passed'] else 'FAIL'}] {c['name']:24s} {c['detail']}")
    print("\nPRE-FLIGHT:", "PASS" if pf["passed"] else "FAIL")
    assert pf["passed"], "pre-flight must pass on the shipped BRD"

    rule("(b) BRONZE - Copy Activity JSON (raw landing, no transformation; no script)")
    rev = review.render(RUN_ID)
    bronze = rev["layers"]["bronze"]
    print(f"pipeline={bronze['pipeline']}  activity={bronze['activity']}  "
          f"source={bronze['source_dataset']} -> sink={bronze['sink_dataset']}")
    print(f"script section: {bronze['script']!r}  (None is correct for Bronze)")
    print(json.dumps(bronze["copy_activity"], indent=2))
    assert bronze["script"] is None

    rule("(c1) SILVER - Mapping Data Flow script DSL")
    print(rev["layers"]["silver"]["script"])
    rule("(c2) GOLD - Mapping Data Flow script DSL")
    print(rev["layers"]["gold"]["script"])

    # Show the deployable Data Flow JSON for Silver (the 'spike' deliverable).
    rule("(c3) SILVER - deployable MappingDataFlow JSON (what gets create_or_update'd)")
    silver_json = next(d["json"] for d in rev["dataflows"] if d["name"] == "df_silver_transform")
    print(json.dumps(silver_json, indent=2))

    rule("(d) pl_master_pipeline - dependsOn chaining")
    master = builder.build_pipeline(
        next(pl for pl in spec.pipelines if pl.name == "pl_master_pipeline"),
        builder._build_context(spec),
    )
    for act in master["activities"]:
        deps = [d["activity"] for d in act["dependsOn"]]
        conds = sorted({c for d in act["dependsOn"] for c in d["dependencyConditions"]})
        print(f"- {act['name']:7s} [{act['type']}]" + (f"  depends_on={deps} {conds}" if deps else "  (root)"))

    # Structural assertions so regressions fail loudly.
    by_name = {a["name"]: a for a in master["activities"]}
    assert by_name["Silver"]["dependsOn"][0]["activity"] == "Bronze"
    assert by_name["Silver"]["dependsOn"][0]["dependencyConditions"] == ["Succeeded"]
    assert by_name["Gold"]["dependsOn"][0]["activity"] == "Silver"
    assert silver_json["type"] == "MappingDataFlow"
    assert silver_json["typeProperties"]["sinks"][0]["dataset"]["referenceName"] == "ds_adls_silver"
    assert any("Dedup" in l for l in silver_json["typeProperties"]["scriptLines"])
    assert "case_fatality_rate" in rev["layers"]["gold"]["script"]
    rule("RESULT")
    print("Local dry-run passed - deployable ADF JSON generated with zero cloud calls.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
