"""prune_unbuildable: after repair rounds exhaust, unbuildable pieces are
dropped with notes and the REST builds — never a question wall that builds
nothing. Miniature of the BRD1 live run (CustomerAgeGroup needs birth_date,
which the real customers.csv does not have)."""

import shutil

from agent import builder, preflight
from agent.compiler import compile_plan
from agent.completeness import check, prune_unbuildable
from models.logical import (
    AggregationPlan, CaseBranch, CleaningOp, EntityTransform, JoinPlan,
    LogicalColumn, LogicalPlan, LogicalSource, OutputPlan,
)


def _plan():
    return LogicalPlan(
        project="bike_store",
        sources=[
            LogicalSource(entity="customers", format="csv", container="landing",
                          file_name="customers.csv",
                          columns=[LogicalColumn(name="customer_id", type="integer"),
                                   LogicalColumn(name="first_name"),
                                   LogicalColumn(name="city")]),
            LogicalSource(entity="orders", format="csv", container="landing",
                          file_name="orders.csv",
                          columns=[LogicalColumn(name="order_id", type="integer"),
                                   LogicalColumn(name="customer_id", type="integer"),
                                   LogicalColumn(name="amount", type="double")]),
        ],
        transforms=[EntityTransform(
            entity="customers",
            cleaning=[
                CleaningOp(op="trim", column="first_name"),  # buildable
                # BRD1 shape: age group derived from a column the data lacks
                CleaningOp(op="case", column="birth_date",
                           when=[CaseBranch(op="lt", value="1980", then="'Senior'")],
                           otherwise="'Adult'", new_column="customer_age_group"),
            ],
            dedup_keys=["customer_id"],
        )],
        outputs=[
            # healthy output — must survive and build
            OutputPlan(name="sales_by_city", from_entities=["customers", "orders"],
                       joins=[JoinPlan(left_entity="customers", right_entity="orders",
                                       left_keys=["customer_id"], right_keys=["customer_id"])],
                       group_by=["city"],
                       aggregations=[AggregationPlan(name="revenue", op="sum", column="amount")]),
            # poisoned output — groups by the column whose derivation is unbuildable
            OutputPlan(name="sales_by_age_group", from_entities=["customers", "orders"],
                       joins=[JoinPlan(left_entity="customers", right_entity="orders",
                                       left_keys=["customer_id"], right_keys=["customer_id"])],
                       group_by=["customer_age_group"],
                       aggregations=[AggregationPlan(name="revenue", op="sum", column="amount")]),
        ],
    )


def test_prune_builds_the_rest():
    plan = _plan()
    problems = check(plan)
    assert problems  # the poisoned pieces are real problems
    notes = prune_unbuildable(plan, problems)
    assert check(plan) == []  # what remains is consistent
    # the unbuildable transform op and output are gone, the healthy ones stay
    assert [o.name for o in plan.outputs] == ["sales_by_city"]
    ops = [op.new_column or op.column for op in plan.transforms[0].cleaning]
    assert ops == ["first_name"]
    # every drop is a visible note on the plan
    assert any("customer_age_group" in n for n in notes)
    assert any("sales_by_age_group" in n for n in notes)
    assert all(n in plan.assumptions for n in notes)
    # and the pruned plan compiles + passes preflight end to end
    spec = compile_plan(plan)
    run_id = "test_prune_builds"
    try:
        builder.write_artifacts(spec, run_id)
        assert preflight.validate(run_id)["passed"]
    finally:
        shutil.rmtree(builder.artifacts_dir(run_id), ignore_errors=True)


def test_prune_leaves_ungrounded_entities_alone():
    plan = _plan()
    plan.sources[0].columns = []  # customers schema unknown -> nothing provable
    before = len(plan.transforms[0].cleaning)
    prune_unbuildable(plan, [])
    assert len(plan.transforms[0].cleaning) == before


def test_prune_clears_pipeline_gold_refs():
    from models.logical import LogicalPipeline
    plan = _plan()
    plan.pipelines = [
        LogicalPipeline(name="PL_Bronze", ingest=["customers", "orders"]),
        LogicalPipeline(name="PL_Silver", silver=["customers"]),
        LogicalPipeline(name="PL_Gold", gold=["sales_by_city", "sales_by_age_group"]),
        LogicalPipeline(name="PL_Master", invokes=["PL_Bronze", "PL_Silver", "PL_Gold"]),
    ]
    prune_unbuildable(plan, check(plan))
    assert check(plan) == []
    gold_pl = next(p for p in plan.pipelines if p.name == "PL_Gold")
    assert gold_pl.gold == ["sales_by_city"]
