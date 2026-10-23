"""Guards for the flow_gold_inventory_summary incident (DF-EXPR-010 +
EXE-0001): a filter on a column the REAL landing file never had, plus an
unrenderable predicate silently becoming an EMPTY filter().

Three layers, cheapest first:
  1. models.logical.FilterRule rejects a comparison filter with no value
     (extraction-time Pydantic retry).
  2. models.spec.Transform hard-errors on an unrenderable predicate instead
     of silently dropping the whole condition.
  3. app._source_schema_checks diffs each dataflow's declared source columns
     against the real landing CSV header at /approve time.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module
from models.logical import FilterRule
from models.spec import (
    Column,
    DataFlow,
    DataFlowEndpoint,
    Dataset,
    DatasetKind,
    DatasetLocation,
    Activity,
    PipelineDef,
    PipelineSpec,
    Predicate,
    Transform,
    TransformKind,
)


# --------------------------------------------------------------------------- #
# Layer 1: FilterRule
# --------------------------------------------------------------------------- #


def test_filter_rule_comparison_requires_value():
    with pytest.raises(ValueError, match="requires a value"):
        FilterRule(op="gt", column="QuantityOnHand")
    with pytest.raises(ValueError, match="requires a value"):
        FilterRule(op="eq", column="status", value="   ")


def test_filter_rule_null_tests_need_no_value():
    assert FilterRule(op="not_null", column="order_id").value is None
    assert FilterRule(op="null", column="ship_date").value is None
    assert FilterRule(op="gt", column="quantity", value="0").value == "0"


# --------------------------------------------------------------------------- #
# Layer 2: Transform predicate rendering
# --------------------------------------------------------------------------- #


def test_unrenderable_predicate_is_hard_error_not_empty_filter():
    with pytest.raises(ValueError, match="unrenderable predicate"):
        Transform(
            kind=TransformKind.filter,
            name="filterRows",
            predicates=[Predicate(op="gt", column="QuantityOnHand", value=None)],
        )


def test_renderable_predicates_still_join():
    t = Transform(
        kind=TransformKind.filter,
        name="filterRows",
        predicates=[
            Predicate(op="not_null", column="order_id"),
            Predicate(op="gt", column="quantity", value="0"),
        ],
    )
    assert t.condition == "!isNull(order_id) && quantity > 0"


# --------------------------------------------------------------------------- #
# Layer 3: /approve schema-vs-reality gate
# --------------------------------------------------------------------------- #


def _spec(declared_cols):
    return PipelineSpec(
        datasets=[
            Dataset(
                name="ds_landing_stocks",
                kind=DatasetKind.delimited_text,
                linked_service="ls_adls",
                location=DatasetLocation(
                    container="landing", file_name="stocks.csv"
                ),
            ),
            Dataset(
                name="ds_bronze_stocks",
                kind=DatasetKind.delimited_text,
                linked_service="ls_adls",
                location=DatasetLocation(container="bronze", file_name="stocks.csv"),
            ),
        ],
        dataflows=[
            DataFlow(
                name="df_gold_inventory_summary",
                source=DataFlowEndpoint(stream_name="srcStocks", dataset="ds_bronze_stocks"),
                sink=DataFlowEndpoint(stream_name="sinkOut", dataset="ds_gold_out"),
                source_columns=[Column(name=c) for c in declared_cols],
                transforms=[],
            )
        ],
        pipelines=[
            PipelineDef(
                name="pl_ingest",
                activities=[
                    Activity(
                        name="copy_stocks",
                        type="Copy",
                        source_dataset="ds_landing_stocks",
                        sink_dataset="ds_bronze_stocks",
                    )
                ],
            )
        ],
    )


def _run_checks(monkeypatch, declared_cols, real_header):
    monkeypatch.setenv("MOCK", "false")
    import agent.grounding as grounding

    monkeypatch.setattr(
        grounding, "_read_head", lambda container, path: real_header
    )
    return app_module._source_schema_checks(_spec(declared_cols))


def test_schema_gate_blocks_invented_column(monkeypatch):
    checks = _run_checks(
        monkeypatch,
        ["store_id", "QuantityOnHand"],
        b"store_id,product_id,quantity\n1,10,5\n",
    )
    failed = [c for c in checks if not c["passed"]]
    assert len(failed) == 1
    assert "QuantityOnHand" in failed[0]["detail"]
    assert "quantity" in failed[0]["detail"]  # real header named in the message


def test_schema_gate_passes_matching_columns(monkeypatch):
    checks = _run_checks(
        monkeypatch,
        ["store_id", "quantity"],
        b"store_id,product_id,quantity\n1,10,5\n",
    )
    assert checks and all(c["passed"] for c in checks)


def test_schema_gate_skips_unreadable_file(monkeypatch):
    # Existence is _source_file_checks' job; unreadable head must not block.
    checks = _run_checks(monkeypatch, ["store_id"], None)
    assert checks == []


def test_schema_gate_noop_in_mock(monkeypatch):
    monkeypatch.setenv("MOCK", "true")
    assert app_module._source_schema_checks(_spec(["anything"])) == []
