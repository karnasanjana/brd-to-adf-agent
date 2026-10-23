"""Handler tests for the tool-driven plan agent — no LLM, no network.

Exercises agent/planhandlers.py the way the tool agent does: build a plan one
call at a time, assert every bad call returns ok=false with actionable errors
(and does NOT mutate the draft), and a clean session finalizes into a plan the
deterministic compiler accepts.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.planhandlers import PlanSession  # noqa: E402
from models.logical import LogicalPlan  # noqa: E402

ORDERS = {
    "entity": "orders",
    "format": "csv",
    "file_name": "orders.csv",
    "columns": [
        {"name": "order_id", "type": "string"},
        {"name": "customer_id", "type": "string"},
        {"name": "quantity", "type": "string"},
        {"name": "list_price", "type": "string"},
        {"name": "order_ts", "type": "string"},
    ],
}
CUSTOMERS = {
    "entity": "customers",
    "format": "csv",
    "file_name": "customers.csv",
    "columns": [
        {"name": "customer_id", "type": "string"},
        {"name": "customer_name", "type": "string"},
    ],
}


def _build_clean_session() -> PlanSession:
    s = PlanSession()
    assert s.upsert_source(ORDERS)["ok"]
    assert s.upsert_source(CUSTOMERS)["ok"]
    assert s.upsert_transform({
        "entity": "orders",
        "casts": [
            {"name": "quantity", "type": "integer"},
            {"name": "list_price", "type": "double"},
            {"name": "order_ts", "type": "timestamp"},
        ],
        "cleaning": [{"op": "trim", "column": "order_id"}],
        "filters": [{"op": "not_null", "column": "order_id"}],
        "dedup_keys": ["order_id"],
    })["ok"]
    assert s.upsert_output({
        "name": "sales_by_customer",
        "from_entities": ["orders", "customers"],
        "joins": [{
            "left_entity": "orders", "right_entity": "customers",
            "left_keys": ["customer_id"], "right_keys": ["customer_id"],
        }],
        "group_by": ["customer_name"],
        "aggregations": [{"name": "total_quantity", "op": "sum", "column": "quantity"}],
    })["ok"]
    return s


# --------------------------------------------------------------------------- #
# Per-call validation: bad input -> ok=false + errors, draft untouched
# --------------------------------------------------------------------------- #

def test_source_missing_location_rejected():
    s = PlanSession()
    r = s.upsert_source({"entity": "orders", "format": "csv"})
    assert not r["ok"] and r["errors"]
    assert s.sources == {}


def test_source_http_needs_url():
    s = PlanSession()
    r = s.upsert_source({"entity": "rates", "format": "http_csv"})
    assert not r["ok"]
    assert any("http_url" in e for e in r["errors"])


def test_transform_unknown_entity_rejected():
    s = PlanSession()
    r = s.upsert_transform({"entity": "ghost"})
    assert not r["ok"]
    assert "unknown entity 'ghost'" in r["errors"][0]


def test_transform_unknown_column_rejected_and_rolled_back():
    s = PlanSession()
    assert s.upsert_source(ORDERS)["ok"]
    r = s.upsert_transform({
        "entity": "orders",
        "casts": [{"name": "no_such_col", "type": "integer"}],
    })
    assert not r["ok"]
    assert any("no_such_col" in e for e in r["errors"])
    assert "orders" not in s.transforms  # bad call did not stick


def test_transform_unknown_op_rejected():
    s = PlanSession()
    assert s.upsert_source(ORDERS)["ok"]
    r = s.upsert_transform({
        "entity": "orders",
        "cleaning": [{"op": "frobnicate", "column": "order_id"}],
    })
    assert not r["ok"]
    assert any("unknown op" in e for e in r["errors"])


def test_output_unknown_entity_rejected():
    s = PlanSession()
    assert s.upsert_source(ORDERS)["ok"]
    r = s.upsert_output({"name": "x", "from_entities": ["orders", "ghost"]})
    assert not r["ok"]
    assert "ghost" in r["errors"][0]


def test_output_unconnected_entities_rejected():
    s = PlanSession()
    assert s.upsert_source(ORDERS)["ok"]
    assert s.upsert_source(CUSTOMERS)["ok"]
    r = s.upsert_output({"name": "x", "from_entities": ["orders", "customers"]})
    assert not r["ok"]
    assert any("not connected by joins" in e for e in r["errors"])
    assert "x" not in s.outputs


def test_output_group_by_without_aggregations_rejected():
    s = PlanSession()
    assert s.upsert_source(ORDERS)["ok"]
    r = s.upsert_output({
        "name": "x", "from_entities": ["orders"], "group_by": ["customer_id"],
    })
    assert not r["ok"]
    assert any("group_by without aggregations" in e for e in r["errors"])


def test_pipelines_must_cover_everything():
    s = _build_clean_session()
    r = s.set_pipelines([
        {"name": "pl_ingest", "ingest": ["orders"]},  # customers not covered
    ])
    assert not r["ok"]
    assert any("customers" in e for e in r["errors"])
    assert s.pipelines == []  # rolled back


def test_pipelines_valid_structure_accepted():
    s = _build_clean_session()
    r = s.set_pipelines([
        {"name": "pl_ingest", "ingest": ["orders", "customers"]},
        {"name": "pl_silver", "silver": ["orders"]},
        {"name": "pl_gold", "gold": ["sales_by_customer"]},
        {"name": "pl_master", "invokes": ["pl_ingest", "pl_silver", "pl_gold"]},
    ])
    assert r["ok"], r["errors"]
    assert [p.name for p in s.pipelines] == ["pl_ingest", "pl_silver", "pl_gold", "pl_master"]


def test_notes_and_remove():
    s = PlanSession()
    assert s.add_note("unsupported", "SCD2 merge")["ok"]
    assert s.add_note("suggestion", "keep latest row via dedup_keys instead")["ok"]
    assert not s.add_note("bogus", "x")["ok"]
    assert s.plan().unsupported == ["SCD2 merge"]
    assert s.remove_item("unsupported", "SCD2 merge")["ok"]
    assert s.plan().unsupported == []


def test_remove_source_drops_orphan_transform():
    s = PlanSession()
    assert s.upsert_source(ORDERS)["ok"]
    assert s.upsert_transform({"entity": "orders", "dedup_keys": ["order_id"]})["ok"]
    assert s.remove_item("source", "orders")["ok"]
    assert s.sources == {} and s.transforms == {}


# --------------------------------------------------------------------------- #
# The gate: finalize = completeness + compile + typecheck
# --------------------------------------------------------------------------- #

def test_finalize_empty_session_fails():
    s = PlanSession()
    r = s.finalize()
    assert not r["ok"]
    assert any("no sources" in e for e in r["errors"])


def test_finalize_clean_session_compiles():
    s = _build_clean_session()
    s.set_project("retail")
    r = s.finalize()
    assert r["ok"], r["errors"]
    plan = s.plan()
    assert plan.project == "retail"
    from agent.compiler import compile_plan

    spec = compile_plan(plan)  # must not raise
    assert spec.pipelines


def test_finalize_catches_type_error_sum_on_string():
    """quantity left as string -> sum(quantity) must fail the finalize gate
    with an actionable cast message (the typecheck service)."""
    s = PlanSession()
    assert s.upsert_source(ORDERS)["ok"]
    assert s.upsert_output({
        "name": "totals",
        "from_entities": ["orders"],
        "group_by": ["customer_id"],
        "aggregations": [{"name": "total_quantity", "op": "sum", "column": "quantity"}],
    })["ok"]
    r = s.finalize()
    assert not r["ok"]
    assert any("cast" in e.lower() for e in r["errors"])


# --------------------------------------------------------------------------- #
# Session round-trip (what /chat does in tools mode)
# --------------------------------------------------------------------------- #

def test_session_round_trips_through_plan():
    s = _build_clean_session()
    s.set_project("retail")
    s.set_schedule("Day", 1)
    s.add_note("assumption", "dedup by order_id")
    plan = s.plan()
    s2 = PlanSession.from_plan(plan)
    assert s2.plan() == plan


def test_state_summary_tracks_draft():
    s = _build_clean_session()
    state = s.status()["state"]
    assert state["sources"] == ["customers", "orders"]
    assert state["outputs"] == ["sales_by_customer"]


if __name__ == "__main__":
    import traceback

    failed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS {name}")
            except Exception:
                failed += 1
                print(f"FAIL {name}")
                traceback.print_exc()
    sys.exit(1 if failed else 0)
