"""Deterministic guards for the bike-store semantic defects (2026-07-11 run):

1. concat rendered bare column args as string literals -> "John last_name".
2. A quoted literal matching a column name slipped through every check.
3. StockStatus case had a dead branch (gt 10 / le 10 / gt 0) and 0 landed in
   the wrong bucket.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent.completeness import check
from agent.typecheck import check_dataflow
from models.logical import (
    CaseBranch,
    CleaningOp,
    EntityTransform,
    LogicalColumn,
    LogicalPlan,
    LogicalSource,
)
from models.spec import _render_op


# --------------------------------------------------------------------------- #
# 1. concat renders column args as columns, literals stay literals
# --------------------------------------------------------------------------- #

def test_concat_column_args_render_bare():
    expr = _render_op("concat", "first_name", ["' '", "last_name"])
    assert expr == "concat(first_name, ' ', last_name)"


def test_concat_prequoted_literal_stays_quoted():
    expr = _render_op("concat", "city", ["', '", "'USA'"])
    assert expr == "concat(city, ', ', 'USA')"


# --------------------------------------------------------------------------- #
# 2. quoted literal shadowing a column name is flagged
# --------------------------------------------------------------------------- #

def _flow_with_expression(expression):
    from models.spec import (
        Column, DataFlow, DataFlowEndpoint, DerivedColumn, MedallionLayer,
        Transform, TransformKind,
    )

    return DataFlow(
        name="df_silver_customers",
        layer=MedallionLayer.silver,
        source=DataFlowEndpoint(stream_name="src", dataset="ds_bronze_customers"),
        sink=DataFlowEndpoint(stream_name="sink", dataset="ds_silver_customers"),
        source_columns=[Column(name="first_name", type="string"),
                        Column(name="last_name", type="string")],
        transforms=[Transform(kind=TransformKind.derived, name="cleanColumns", columns=[
            DerivedColumn(name="CustomerName", expression=expression),
        ])],
    )


def test_literal_matching_column_is_flagged():
    problems = check_dataflow(_flow_with_expression("concat(first_name, ' ', 'last_name')"))
    assert any("string literal 'last_name' matches the column" in p for p in problems)


def test_legit_literal_not_flagged():
    problems = check_dataflow(_flow_with_expression("concat(first_name, ' ', 'Unknown')"))
    assert not any("string literal" in p for p in problems)


# --------------------------------------------------------------------------- #
# 3. dead case branches are reported by completeness
# --------------------------------------------------------------------------- #

def _plan_with_stock_case(when):
    return LogicalPlan(
        project="bikes",
        sources=[LogicalSource(entity="stocks", format="csv", file_name="stocks.csv",
                               columns=[LogicalColumn(name="product_id"),
                                        LogicalColumn(name="quantity", type="integer")])],
        transforms=[EntityTransform(entity="stocks", cleaning=[
            CleaningOp(op="case", column="quantity", new_column="StockStatus",
                       when=when, otherwise="'Out of Stock'"),
        ])],
    )


def test_dead_case_branch_is_reported():
    plan = _plan_with_stock_case([
        CaseBranch(op="gt", value="10", then="'In Stock'"),
        CaseBranch(op="le", value="10", then="'Low Stock'"),
        CaseBranch(op="gt", value="0", then="'Out of Stock'"),  # unreachable
    ])
    problems = check(plan)
    assert any("can never match" in p for p in problems)


def test_wellformed_case_has_no_dead_branch():
    plan = _plan_with_stock_case([
        CaseBranch(op="eq", value="0", then="'Out of Stock'"),
        CaseBranch(op="le", value="10", then="'Low Stock'"),
        CaseBranch(op="gt", value="10", then="'In Stock'"),
    ])
    problems = check(plan)
    assert not any("can never match" in p for p in problems)


def test_non_numeric_case_skipped():
    plan = _plan_with_stock_case([
        CaseBranch(op="eq", value="CA", then="'West'"),
        CaseBranch(op="eq", value="NY", then="'East'"),
    ])
    assert not any("can never match" in p for p in check(plan))


# --------------------------------------------------------------------------- #
# 4. transform-derived columns are available to outputs; near-miss names align
# --------------------------------------------------------------------------- #

def _hr_plan(group_by_name):
    from models.logical import AggregationPlan, OutputPlan

    return LogicalPlan(
        project="hr",
        sources=[LogicalSource(entity="employees", format="csv", file_name="e.csv",
                               columns=[LogicalColumn(name="employee_id"),
                                        LogicalColumn(name="hire_date")])],
        transforms=[EntityTransform(entity="employees", cleaning=[
            CleaningOp(op="case", column="hire_date", new_column="TenureBand",
                       when=[CaseBranch(op="lt", value="2015", then="'Veteran'")],
                       otherwise="'Recent'"),
        ])],
        outputs=[OutputPlan(
            name="tenure_distribution", from_entities=["employees"],
            group_by=[group_by_name],
            aggregations=[AggregationPlan(name="employee_count", op="count",
                                          column="employee_id")],
        )],
    )


def test_transform_derived_column_available_to_output():
    """Silver derives TenureBand -> gold may group by it. The old availability
    set omitted transform-derived columns and flagged CORRECT plans (held-out
    BRD 3: repair loop churned on a nonexistent problem)."""
    assert not any("TenureBand" in p for p in check(_hr_plan("TenureBand")))


def test_align_plan_fixes_case_mismatch():
    from agent.completeness import align_plan

    plan = _hr_plan("tenure_band")
    notes = align_plan(plan)
    assert plan.outputs[0].group_by == ["TenureBand"]
    assert any("'tenure_band' -> 'TenureBand'" in n for n in notes)
    assert not any("tenure_band" in p.lower() for p in check(plan))


def test_align_plan_leaves_ambiguous_and_unknown_names():
    from agent.completeness import align_plan

    plan = _hr_plan("completely_unknown_col")
    assert align_plan(plan) == []
    assert plan.outputs[0].group_by == ["completely_unknown_col"]
