"""Tests for agent/plandiff.py — the refine-transparency half of the approval gate."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.plandiff import diff  # noqa: E402
from models.logical import (  # noqa: E402
    AggregationPlan,
    LogicalPlan,
    LogicalSource,
    OutputPlan,
    SchedulePlan,
)


def _plan(**over):
    base = dict(
        project="retail",
        sources=[LogicalSource(entity="orders", format="csv", file_name="orders.csv")],
        outputs=[
            OutputPlan(
                name="totals",
                from_entities=["orders"],
                group_by=["customer_id"],
                aggregations=[AggregationPlan(name="n", op="count", column="order_id")],
            )
        ],
    )
    base.update(over)
    return LogicalPlan(**base)


def test_first_generation_no_diff():
    assert diff(None, _plan()) == []


def test_identical_plans_no_diff():
    assert diff(_plan(), _plan()) == []


def test_added_and_removed_source():
    old = _plan()
    new = _plan(sources=[
        LogicalSource(entity="customers", format="csv", file_name="customers.csv"),
    ])
    lines = diff(old, new)
    assert "source 'customers': added" in lines
    assert "source 'orders': removed" in lines


def test_changed_output_names_fields():
    new_out = OutputPlan(
        name="totals",
        from_entities=["orders"],
        group_by=["customer_id"],
        aggregations=[
            AggregationPlan(name="n", op="count", column="order_id"),
            AggregationPlan(name="revenue", op="sum", column="amount"),
        ],
        top_n=10,
        sort=[{"column": "revenue", "order": "desc"}],
    )
    lines = diff(_plan(), _plan(outputs=[new_out]))
    assert lines == ["output 'totals': changed aggregations, sort, top_n"]


def test_project_schedule_and_notes():
    old = _plan()
    new = _plan(
        project="retail_v2",
        schedule=SchedulePlan(frequency="Day"),
        unsupported=["Teams alert on failure"],
    )
    lines = diff(old, new)
    assert "project: 'retail' -> 'retail_v2'" in lines
    assert any(l.startswith("schedule: None ->") for l in lines)
    assert "unsupported: + Teams alert on failure" in lines


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
