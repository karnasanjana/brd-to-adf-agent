"""Conditional-value support (CleaningOp op=case, OutputPlan.post_agg_derived).

Regression for the stocks_and_products_analysis BRD (2026-07-09): the model
correctly marked negative-stock clamping, price_band, and revenue-based
customer segmentation as `unsupported` because the LogicalPlan vocabulary had
no conditional/if-else op — not model over-marking, a real capability gap
(models/logical.py deliberately has no free-text expression field). `case`
closes it: an ordered when/otherwise branch list that compiles to a native
ADF iif() chain.
"""

from agent.compiler import compile_plan
from agent.completeness import check
from agent.lint import lint_dataflow_script
from agent import builder
from agent.typecheck import check_spec
from models.logical import (
    AggregationPlan,
    CaseBranch,
    CleaningOp,
    EntityTransform,
    JoinPlan,
    LogicalColumn,
    LogicalPlan,
    LogicalSource,
    OutputPlan,
)
from pydantic import ValidationError
import pytest


def test_case_requires_when_and_otherwise():
    with pytest.raises(ValidationError):
        CleaningOp(op="case", column="quantity")  # no when/otherwise


def test_case_fields_are_case_only():
    with pytest.raises(ValidationError):
        CleaningOp(op="trim", column="status",
                   when=[CaseBranch(op="lt", value="0", then="0")], otherwise="x")


def test_case_branch_rejects_unknown_op():
    with pytest.raises(ValidationError):
        CaseBranch(op="startswith", value="0", then="0")


def _clamp_plan() -> LogicalPlan:
    return LogicalPlan(
        project="retail",
        sources=[LogicalSource(entity="stocks", format="csv", file_name="s.csv",
                               columns=[LogicalColumn(name="product_id"),
                                        LogicalColumn(name="quantity")])],
        transforms=[EntityTransform(
            entity="stocks",
            casts=[LogicalColumn(name="quantity", type="integer")],
            cleaning=[CleaningOp(
                op="case", column="quantity",
                when=[CaseBranch(op="lt", value="0", then="0")],
                otherwise="quantity", new_column="quantity",
            )],
        )],
    )


def test_clamp_negative_renders_iif_and_passes_checks():
    plan = _clamp_plan()
    assert check(plan) == []
    spec = compile_plan(plan)
    assert check_spec(spec) == []
    datasets = {d.name: d for d in spec.datasets}
    df = next(d for d in spec.dataflows if d.name == "df_silver_stocks")
    script = builder.build_dataflow_script(df, datasets)
    assert "iif(quantity < 0, 0, quantity)" in script
    assert lint_dataflow_script(df.name, script) == []


def test_price_band_multiway_bucket_chains_iif():
    plan = LogicalPlan(
        project="retail",
        sources=[LogicalSource(entity="products", format="csv", file_name="p.csv",
                               columns=[LogicalColumn(name="product_id"),
                                        LogicalColumn(name="list_price")])],
        transforms=[EntityTransform(
            entity="products",
            casts=[LogicalColumn(name="list_price", type="decimal(18,2)")],
            cleaning=[CleaningOp(
                op="case", column="list_price",
                when=[CaseBranch(op="lt", value="20", then="'Budget'"),
                      CaseBranch(op="lt", value="100", then="'Standard'")],
                otherwise="'Premium'", new_column="price_band",
            )],
        )],
    )
    spec = compile_plan(plan)
    assert check_spec(spec) == []
    datasets = {d.name: d for d in spec.datasets}
    df = next(d for d in spec.dataflows if d.name == "df_silver_products")
    script = builder.build_dataflow_script(df, datasets)
    assert ("price_band = iif(list_price < 20, 'Budget', "
            "iif(list_price < 100, 'Standard', 'Premium'))") in script
    assert lint_dataflow_script(df.name, script) == []


def test_segment_from_aggregated_revenue_post_agg_derived():
    """The genuinely tricky case: segment must be computed AFTER sum(revenue),
    not before — proves post_agg_derived orders itself between aggregate and
    sort/top_n/select in the rendered script."""
    plan = LogicalPlan(
        project="retail",
        sources=[
            LogicalSource(entity="customers", format="csv", file_name="c.csv",
                          columns=[LogicalColumn(name="customer_id")]),
            LogicalSource(entity="orders", format="csv", file_name="o.csv",
                          columns=[LogicalColumn(name="customer_id"),
                                   LogicalColumn(name="revenue")]),
        ],
        transforms=[EntityTransform(
            entity="orders", casts=[LogicalColumn(name="revenue", type="decimal(18,2)")])],
        outputs=[OutputPlan(
            name="customer_segments",
            from_entities=["customers", "orders"],
            joins=[JoinPlan(left_entity="customers", right_entity="orders",
                            left_keys=["customer_id"], right_keys=["customer_id"])],
            group_by=["customer_id"],
            aggregations=[AggregationPlan(name="total_revenue", op="sum", column="revenue")],
            post_agg_derived=[CleaningOp(
                op="case", column="total_revenue",
                when=[CaseBranch(op="ge", value="10000", then="'Platinum'"),
                      CaseBranch(op="ge", value="5000", then="'Gold'")],
                otherwise="'Bronze'", new_column="segment",
            )],
        )],
    )
    assert check(plan) == []
    spec = compile_plan(plan)
    assert check_spec(spec) == []
    gold = next(d for d in spec.dataflows if d.name.startswith("df_gold"))
    kinds = [t.kind.value for t in gold.transforms]
    assert kinds.index("aggregate") < kinds.index("derived")  # segment AFTER agg
    datasets = {d.name: d for d in spec.datasets}
    script = builder.build_dataflow_script(gold, datasets)
    assert ("segment = iif(total_revenue >= 10000, 'Platinum', "
            "iif(total_revenue >= 5000, 'Gold', 'Bronze'))") in script
    assert lint_dataflow_script(gold.name, script) == []


def test_post_agg_derived_output_usable_in_sort_and_columns():
    """completeness.py's column-existence check must know post_agg_derived
    names (e.g. 'segment') are valid — this used to false-positive-block a
    plan that sorts or projects by a post_agg_derived column."""
    plan = LogicalPlan(
        project="retail",
        sources=[
            LogicalSource(entity="customers", format="csv", file_name="c.csv",
                          columns=[LogicalColumn(name="customer_id")]),
            LogicalSource(entity="orders", format="csv", file_name="o.csv",
                          columns=[LogicalColumn(name="customer_id"),
                                   LogicalColumn(name="revenue")]),
        ],
        transforms=[EntityTransform(
            entity="orders", casts=[LogicalColumn(name="revenue", type="decimal(18,2)")])],
        outputs=[OutputPlan(
            name="customer_segments",
            from_entities=["customers", "orders"],
            joins=[JoinPlan(left_entity="customers", right_entity="orders",
                            left_keys=["customer_id"], right_keys=["customer_id"])],
            group_by=["customer_id"],
            aggregations=[AggregationPlan(name="total_revenue", op="sum", column="revenue")],
            post_agg_derived=[CleaningOp(
                op="case", column="total_revenue",
                when=[CaseBranch(op="ge", value="5000", then="'Gold'")],
                otherwise="'Bronze'", new_column="segment",
            )],
            columns=["customer_id", "total_revenue", "segment"],
        )],
    )
    assert not any("'segment' is not a column" in p for p in check(plan))
