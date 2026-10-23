"""Acceptance tests for models/logical.py (Step 1)."""

import pytest
from pydantic import ValidationError

from models.logical import (
    AggregationPlan,
    CleaningOp,
    EntityTransform,
    JoinPlan,
    LogicalPlan,
    LogicalSource,
    OutputPlan,
)


def test_valid_plan():
    plan = LogicalPlan(
        project="retail",
        sources=[
            LogicalSource(entity="orders", format="csv", file_name="orders.csv"),
            LogicalSource(entity="customers", format="csv", file_name="customers.csv"),
        ],
        transforms=[
            EntityTransform(
                entity="orders",
                cleaning=[CleaningOp(op="trim", column="status")],
                dedup_keys=["order_id"],
            ),
        ],
        outputs=[
            OutputPlan(
                name="monthly_sales",
                from_entities=["orders", "customers"],
                joins=[
                    JoinPlan(
                        left_entity="orders",
                        right_entity="customers",
                        left_keys=["customer_id"],
                        right_keys=["customer_id"],
                    )
                ],
                group_by=["month"],
                aggregations=[
                    AggregationPlan(name="total", op="sum", column="amount"),
                    AggregationPlan(name="orders", op="count", column="order_id"),
                ],
            )
        ],
    )
    assert len(plan.sources) == 2
    assert plan.outputs[0].aggregations[0].name == "total"


def test_unknown_op_rejected():
    with pytest.raises(ValidationError):
        CleaningOp(op="partitionBy", column="x")


def test_join_needs_matching_keys():
    with pytest.raises(ValidationError):
        JoinPlan(left_entity="a", right_entity="b", left_keys=[], right_keys=[])


def test_entity_normalized_to_snake():
    src = LogicalSource(entity="Orders Data!", format="csv", file_name="o.csv")
    assert src.entity == "orders_data"
