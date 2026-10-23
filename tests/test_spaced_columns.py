"""Stress-suite defect classes (2026-07-11 run):

1. Spaced/non-identifier column names must brace-escape in data-flow script
   ({Country Name}) — population.csv gold flow died at runtime on bare names.
2. folder == container must collapse (landing/landing/transactions.csv 404'd
   the source gate).
"""

import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent import builder, preflight
from agent.compiler import compile_plan
from models.logical import (
    AggregationPlan,
    EntityTransform,
    LogicalColumn,
    LogicalPlan,
    LogicalSource,
    OutputPlan,
)
from models.spec import Predicate, dsl_name


def test_dsl_name_escapes_non_identifiers():
    assert dsl_name("Country Name") == "{Country Name}"
    assert dsl_name("unit-price") == "{unit-price}"
    assert dsl_name("plain_col") == "plain_col"
    assert dsl_name("{Already Braced}") == "{Already Braced}"


def test_predicate_render_escapes_spaced_column():
    assert Predicate(op="ge", column="Country Name", value="'x'").render().startswith(
        "{Country Name}"
    )
    assert Predicate(op="not_null", column="Country Name").render() == "!isNull({Country Name})"


def test_spaced_columns_flow_end_to_end(monkeypatch):
    """population.csv shape: 'Country Name'/'Country Code'/'Year'/'Value'.
    Cast + dedup + group-by on spaced names must produce a script with braces
    and pass preflight."""
    monkeypatch.setenv("MOCK", "true")
    plan = LogicalPlan(
        project="population",
        sources=[LogicalSource(
            entity="population", format="csv", file_name="population.csv",
            columns=[LogicalColumn(name="Country Name"),
                     LogicalColumn(name="Country Code"),
                     LogicalColumn(name="Year"),
                     LogicalColumn(name="Value")])],
        transforms=[EntityTransform(
            entity="population",
            casts=[LogicalColumn(name="Year", type="integer"),
                   LogicalColumn(name="Value", type="long")],
            dedup_keys=["Country Name", "Year"],
        )],
        outputs=[OutputPlan(
            name="country_totals",
            from_entities=["population"],
            group_by=["Country Name"],
            aggregations=[AggregationPlan(name="total_value", op="sum", column="Value")],
        )],
    )
    spec = compile_plan(plan)
    run_id = "test_spaced_cols"
    run_dir = builder.artifacts_dir(run_id)
    try:
        builder.write_artifacts(spec, run_id)
        # Silver: source projection keeps the FILE header (brace-escaped),
        # then sanitizeNames renames to Delta-safe columns.
        silver = next(df for df in spec.dataflows if df.name == "df_silver_population")
        s_script = builder.build_dataflow_script(silver)
        assert "{Country Name} as string" in s_script
        assert "Country_Name = {Country Name}" in s_script  # sanitize select
        # Gold: everything downstream uses the Delta-safe names — Delta
        # rejects spaced names at the sink (DF-SINK-005).
        gold = next(df for df in spec.dataflows if df.name == "df_gold_country_totals")
        g_script = builder.build_dataflow_script(gold)
        assert "groupBy(Country_Name)" in g_script
        assert "{Country Name}" not in g_script
        assert any("renamed to 'Country_Name'" in a for a in spec.assumptions)
        pf = preflight.validate(run_id)
        assert pf["passed"], [c for c in pf["checks"] if not c["passed"]]
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def test_left_join_renders_dsl_join_type():
    """Stress BRD 10: the DSL takes joinType [outer,left,cross,inner,right] —
    'left_outer' fails at runtime with DF-ARG-005. A BRD 'left join' must
    render joinType:'left'."""
    from models.logical import JoinPlan

    plan = LogicalPlan(
        project="shop",
        sources=[
            LogicalSource(entity="web_orders", format="csv", file_name="w.csv",
                          columns=[LogicalColumn(name="order_id"),
                                   LogicalColumn(name="order_total", type="decimal(18,2)")]),
            LogicalSource(entity="returns", format="csv", file_name="r.csv",
                          columns=[LogicalColumn(name="return_id"),
                                   LogicalColumn(name="order_id")]),
        ],
        outputs=[OutputPlan(
            name="net_revenue",
            from_entities=["web_orders", "returns"],
            joins=[JoinPlan(left_entity="web_orders", right_entity="returns",
                            left_keys=["order_id"], right_keys=["order_id"],
                            type="left")],
            group_by=["order_id"],
            aggregations=[AggregationPlan(name="total", op="sum", column="order_total")],
        )],
    )
    spec = compile_plan(plan)
    gold = next(df for df in spec.dataflows if df.name == "df_gold_net_revenue")
    script = builder.build_dataflow_script(gold)
    assert "joinType:'left'" in script
    assert "left_outer" not in script
    # Full outer maps to the DSL's 'outer'.
    plan.outputs[0].joins[0].type = "full"
    spec = compile_plan(plan)
    gold = next(df for df in spec.dataflows if df.name == "df_gold_net_revenue")
    assert "joinType:'outer'" in builder.build_dataflow_script(gold)


def test_folder_equal_to_container_collapses():
    plan = LogicalPlan(
        project="bank",
        sources=[LogicalSource(entity="transactions", format="csv",
                               container="landing", folder="landing",
                               file_name="transactions.csv",
                               columns=[LogicalColumn(name="transaction_id")])],
    )
    spec = compile_plan(plan)
    landing = next(d for d in spec.datasets if d.name == "ds_landing_transactions")
    assert landing.location.container == "landing"
    assert landing.location.folder is None