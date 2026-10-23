"""Acceptance tests for agent/compiler.py (Step 2)."""

import os
import shutil

import pytest

from agent import builder, preflight
from agent.compiler import compile_plan
from models.logical import (
    AggregationPlan,
    CleaningOp,
    EntityTransform,
    FilterRule,
    JoinPlan,
    LogicalColumn,
    LogicalPlan,
    LogicalSource,
    OutputPlan,
    SortPlan,
)


def _plan():
    return LogicalPlan(
        project="retail",
        sources=[
            LogicalSource(
                entity="orders",
                format="csv",
                file_name="orders.csv",
                columns=[
                    LogicalColumn(name="order_id"),
                    LogicalColumn(name="customer_id"),
                    LogicalColumn(name="amount", type="decimal(18,2)"),
                    LogicalColumn(name="month"),
                    LogicalColumn(name="status"),
                ],
            ),
            LogicalSource(
                entity="customers",
                format="csv",
                file_name="customers.csv",
                columns=[LogicalColumn(name="customer_id"), LogicalColumn(name="region")],
            ),
        ],
        transforms=[
            EntityTransform(
                entity="orders",
                casts=[LogicalColumn(name="amount", type="decimal(18,2)")],
                cleaning=[CleaningOp(op="trim", column="status")],
                filters=[FilterRule(op="not_null", column="order_id")],
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
                    AggregationPlan(name="total_amount", op="sum", column="amount"),
                    AggregationPlan(name="order_count", op="count", column="order_id"),
                ],
                sort=[SortPlan(column="total_amount", order="desc")],
                top_n=10,
            )
        ],
    )


def test_compile_clean_spec():
    spec = compile_plan(_plan())
    assert spec.naming_violations() == []
    assert spec.cross_reference_errors() == []
    storage_ls = [ls for ls in spec.linked_services if ls.name == "ls_storage"]
    assert len(storage_ls) == 1
    # 2 landing + 2 bronze + 2 silver (orders + customers pass-through) + 1 gold
    assert len(spec.datasets) == 7
    # ONE pipeline, not a master + children.
    assert len(spec.pipelines) == 1
    pl = spec.pipelines[0]
    assert pl.name == "pl_retail"
    kinds = [a.type for a in pl.activities]
    assert kinds.count("Copy") == 2  # bronze copies
    assert kinds.count("ExecuteDataFlow") == 3  # 2 silver + 1 gold
    assert "ExecutePipeline" not in kinds  # no child-pipeline wrappers
    copy_orders = next(a for a in pl.activities if a.name == "copy_orders")
    assert copy_orders.depends_on == []
    silver_orders = next(a for a in pl.activities if a.name == "flow_silver_orders")
    assert silver_orders.depends_on == ["copy_orders"]
    gold = next(a for a in pl.activities if a.name == "flow_gold_monthly_sales")
    assert gold.depends_on == ["flow_silver_orders", "flow_silver_customers"]


def test_delta_sink_overwrites_stale_table():
    """Delta sinks must use the docs-valid `overwrite` table action so a run's
    schema replaces whatever a previous run (or project) left in the same
    silver/gold folder. `recreate` is a SQL-sink property Delta sinks silently
    ignore — it caused 'Failed to merge incompatible data types' at runtime."""
    spec = compile_plan(_plan())
    datasets = {d.name: d for d in spec.datasets}
    delta_sink_scripts = [
        builder.build_dataflow_script(df, datasets)
        for df in spec.dataflows
    ]
    assert delta_sink_scripts
    for script in delta_sink_scripts:
        assert "overwrite: true," in script
        assert "recreate" not in script
        assert "mergeSchema" not in script


def test_top_n_renders_rank_not_limit():
    """ADF has no limit() transform — DF-FLO-005 'limit' not registered killed
    flow_gold_top_customers. Top-N must lower to rank(<sort keys>) + filter +
    drop of the helper column, with the final stream keeping the limit's name."""
    spec = compile_plan(_plan())
    datasets = {d.name: d for d in spec.datasets}
    gold = next(df for df in spec.dataflows if df.name.startswith("df_gold"))
    script = builder.build_dataflow_script(gold, datasets)
    assert "limit(" not in script
    assert "rank(desc(total_amount)" in script  # keys from the preceding sort
    assert "xRowRank <= 10" in script
    assert "match(name != 'xRowRank')" in script
    # DF-DSL-001: ADF rejects identifiers starting with double underscore
    assert "__" not in script
    assert "~> limitRows\n" in script or script.rstrip().endswith("~> limitRows") \
        or "limitRows sink(" in script

    from agent.lint import lint_dataflow_script
    assert lint_dataflow_script(gold.name, script) == []


def test_lint_rejects_limit_transform():
    from agent.lint import lint_dataflow_script
    script = (
        "source(output(a as string)) ~> src\n"
        "src limit(0,\n    10) ~> limitRows\n"
        "limitRows sink(skipDuplicateMapInputs: true) ~> sinkOut"
    )
    problems = lint_dataflow_script("df_x", script)
    assert any("limit(" in p and "rank" in p for p in problems)


def test_compile_deterministic():
    a = compile_plan(_plan()).model_dump()
    b = compile_plan(_plan()).model_dump()
    assert a == b


def test_aggregation_dup_group_by_raises():
    plan = _plan()
    plan.outputs[0].aggregations[0].name = "month"  # collides with group_by
    with pytest.raises(ValueError):
        compile_plan(plan)


def test_brd_specified_pipeline_structure(monkeypatch):
    """When the plan carries pipelines (any BRD may), the compiler builds exactly
    those — master + stages — not 1 and not per-entity."""
    monkeypatch.setenv("MOCK", "true")
    from agent.completeness import check
    from models.logical import LogicalPipeline, SchedulePlan

    plan = LogicalPlan(
        project="retail",
        sources=[
            LogicalSource(entity="orders", format="csv", file_name="o.csv",
                          columns=[LogicalColumn(name="order_id"),
                                   LogicalColumn(name="amount", type="decimal(18,2)")]),
            LogicalSource(entity="payments", format="csv", file_name="p.csv",
                          columns=[LogicalColumn(name="order_id"),
                                   LogicalColumn(name="amount", type="decimal(18,2)")]),
        ],
        transforms=[EntityTransform(entity="orders", dedup_keys=["order_id"])],
        outputs=[OutputPlan(name="monthly_sales", from_entities=["orders", "payments"],
                            joins=[JoinPlan(left_entity="orders", right_entity="payments",
                                            left_keys=["order_id"], right_keys=["order_id"])],
                            aggregations=[AggregationPlan(name="rev", op="sum", column="amount")])],
        pipelines=[
            LogicalPipeline(name="PL_Master", invokes=["PL_Bronze", "PL_Silver", "PL_Gold"]),
            LogicalPipeline(name="PL_Bronze", ingest=["orders", "payments"]),
            LogicalPipeline(name="PL_Silver", silver=["orders", "payments"]),
            LogicalPipeline(name="PL_Gold", gold=["monthly_sales"]),
        ],
        schedule=SchedulePlan(),
    )
    assert check(plan) == []
    spec = compile_plan(plan)
    names = [p.name for p in spec.pipelines]
    assert names == ["pl_master", "pl_bronze", "pl_silver", "pl_gold"]
    master = next(p for p in spec.pipelines if p.name == "pl_master")
    assert [a.type for a in master.activities] == ["ExecutePipeline"] * 3
    # master chains the stages in order
    assert master.activities[1].depends_on == ["run_bronze"]
    assert spec.triggers[0].pipeline == "pl_master"
    run_id = "test_brd_pipes"
    run_dir = builder.artifacts_dir(run_id)
    try:
        builder.write_artifacts(spec, run_id)
        assert preflight.validate(run_id)["passed"]
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def test_no_pipelines_specified_falls_back_to_one(monkeypatch):
    monkeypatch.setenv("MOCK", "true")
    plan = LogicalPlan(
        project="simple",
        sources=[LogicalSource(entity="orders", format="csv", file_name="o.csv",
                               columns=[LogicalColumn(name="order_id")])],
        transforms=[EntityTransform(entity="orders", dedup_keys=["order_id"])],
    )
    spec = compile_plan(plan)
    assert len(spec.pipelines) == 1
    assert spec.pipelines[0].name == "pl_simple"


def test_gold_date_part_grouping(monkeypatch):
    """Year/Month grouping from a timestamp must build a real gold flow — not be
    dropped as unsupported (the monthly_sales gap the user hit)."""
    monkeypatch.setenv("MOCK", "true")
    plan = LogicalPlan(
        project="retail_sales",
        sources=[
            LogicalSource(entity="orders", format="csv", file_name="o.csv",
                          columns=[LogicalColumn(name="order_id"),
                                   LogicalColumn(name="order_purchase_timestamp")]),
            LogicalSource(entity="payments", format="csv", file_name="p.csv",
                          columns=[LogicalColumn(name="order_id"),
                                   LogicalColumn(name="payment_value", type="decimal(18,2)")]),
        ],
        transforms=[EntityTransform(entity="orders",
                                    casts=[LogicalColumn(name="order_purchase_timestamp", type="timestamp")])],
        outputs=[OutputPlan(
            name="monthly_sales",
            from_entities=["orders", "payments"],
            joins=[JoinPlan(left_entity="orders", right_entity="payments",
                            left_keys=["order_id"], right_keys=["order_id"])],
            derived=[CleaningOp(op="year", column="order_purchase_timestamp", new_column="order_year"),
                     CleaningOp(op="month", column="order_purchase_timestamp", new_column="order_month")],
            group_by=["order_year", "order_month"],
            aggregations=[AggregationPlan(name="total_sales", op="sum", column="payment_value")],
        )],
    )
    spec = compile_plan(plan)
    gold = next(df for df in spec.dataflows if df.name == "df_gold_monthly_sales")
    exprs = [c.expression for t in gold.transforms for c in t.columns]
    assert "year(order_purchase_timestamp)" in exprs
    assert "month(order_purchase_timestamp)" in exprs
    run_id = "test_gold_dateparts"
    run_dir = builder.artifacts_dir(run_id)
    try:
        builder.write_artifacts(spec, run_id)
        assert preflight.validate(run_id)["passed"]
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def test_gold_quarter_grouping(monkeypatch):
    """OrderQuarter from a timestamp: ADF's DSL has no quarter() function, so
    the op renders the floor((month-1)/3)+1 composite — not marked unsupported
    (the bike-store BRD gap)."""
    monkeypatch.setenv("MOCK", "true")
    plan = LogicalPlan(
        project="bike_store",
        sources=[
            LogicalSource(entity="orders", format="csv", file_name="o.csv",
                          columns=[LogicalColumn(name="order_id"),
                                   LogicalColumn(name="order_date"),
                                   LogicalColumn(name="amount", type="decimal(18,2)")]),
        ],
        transforms=[EntityTransform(entity="orders",
                                    casts=[LogicalColumn(name="order_date", type="timestamp")])],
        outputs=[OutputPlan(
            name="quarterly_sales",
            from_entities=["orders"],
            derived=[CleaningOp(op="year", column="order_date", new_column="order_year"),
                     CleaningOp(op="quarter", column="order_date", new_column="order_quarter")],
            group_by=["order_year", "order_quarter"],
            aggregations=[AggregationPlan(name="revenue", op="sum", column="amount")],
        )],
    )
    spec = compile_plan(plan)
    gold = next(df for df in spec.dataflows if df.name == "df_gold_quarterly_sales")
    exprs = [c.expression for t in gold.transforms for c in t.columns]
    assert "(floor((month(order_date) - 1) / 3) + 1)" in exprs
    run_id = "test_gold_quarter"
    run_dir = builder.artifacts_dir(run_id)
    try:
        builder.write_artifacts(spec, run_id)
        assert preflight.validate(run_id)["passed"]
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def test_proper_case_and_arithmetic(monkeypatch):
    """The two 'unsupported' asks from the retail BRD: Proper Case cleaning and
    a derived measure line_total = quantity * list_price * (1 - discount)."""
    monkeypatch.setenv("MOCK", "true")
    from agent.completeness import check

    plan = LogicalPlan(
        project="retail",
        sources=[LogicalSource(
            entity="order_items", format="csv", file_name="oi.csv",
            columns=[LogicalColumn(name="order_id"),
                     LogicalColumn(name="first_name"),
                     LogicalColumn(name="quantity", type="integer"),
                     LogicalColumn(name="list_price", type="decimal(18,2)"),
                     LogicalColumn(name="discount", type="decimal(18,2)")])],
        transforms=[EntityTransform(
            entity="order_items",
            cleaning=[
                CleaningOp(op="proper", column="first_name"),
                CleaningOp(op="subtract", column="1", args=["discount"],
                           new_column="one_minus_discount"),
                CleaningOp(op="multiply", column="quantity",
                           args=["list_price", "one_minus_discount"],
                           new_column="line_total"),
            ],
        )],
    )
    assert check(plan) == []  # arithmetic operands are not missing columns
    spec = compile_plan(plan)
    df = next(d for d in spec.dataflows if d.name == "df_silver_order_items")
    exprs = {c.name: c.expression for t in df.transforms for c in t.columns}
    assert exprs["first_name"] == (
        "concat(upper(left(first_name, 1)), lower(substring(first_name, 2)))"
    )
    # toDouble: written Delta type must match the projected 'double' (gold
    # source schemas) — bare int*decimal arithmetic writes decimal(38,x).
    assert exprs["one_minus_discount"] == "toDouble(1 - discount)"
    assert exprs["line_total"] == "toDouble(quantity * list_price * one_minus_discount)"
    # chained op referencing one_minus_discount lands in a LATER transform —
    # sibling derived columns can't see each other inside one ADF transform.
    derived = [t for t in df.transforms if t.columns]
    idx_of = {c.name: i for i, t in enumerate(derived) for c in t.columns}
    assert idx_of["line_total"] > idx_of["one_minus_discount"]
    run_id = "test_proper_arith"
    try:
        builder.write_artifacts(spec, run_id)
        assert preflight.validate(run_id)["passed"]
    finally:
        shutil.rmtree(builder.artifacts_dir(run_id), ignore_errors=True)


def test_coalesce_column_fallback_renders_bare_not_quoted():
    """Regression for a real deploy failure: coalesce(shipped_date, order_date)
    was rendered as coalesce(shipped_date, 'order_date') — the compiler's
    generic op/args renderer has no column context, so it force-quoted the
    fallback column into a string literal (DF-DT-001 at runtime: timestamp vs
    string). 'order_date' names a real column of the same entity, so it must
    render bare — a genuine default literal ('N/A') must still be quoted."""
    plan = LogicalPlan(
        project="retail",
        sources=[LogicalSource(
            entity="orders", format="csv", file_name="orders.csv",
            columns=[LogicalColumn(name="order_id"),
                     LogicalColumn(name="order_date", type="timestamp"),
                     LogicalColumn(name="shipped_date", type="timestamp"),
                     LogicalColumn(name="status")])],
        transforms=[EntityTransform(
            entity="orders",
            cleaning=[
                CleaningOp(op="coalesce", column="shipped_date", args=["order_date"]),
                CleaningOp(op="coalesce", column="status", args=["N/A"]),
            ],
        )],
    )
    spec = compile_plan(plan)
    df = next(d for d in spec.dataflows if d.name == "df_silver_orders")
    exprs = {c.name: c.expression for t in df.transforms for c in t.columns}
    assert exprs["shipped_date"] == "coalesce(shipped_date, order_date)"  # bare column
    assert exprs["status"] == "coalesce(status, 'N/A')"  # still a quoted literal


def test_validate_first_gates_the_copy(monkeypatch):
    """validate_first on a source emits a Validation activity the copy depends
    on — and the gate survives BRD-specified pipeline grouping."""
    monkeypatch.setenv("MOCK", "true")
    from models.logical import LogicalPipeline

    plan = LogicalPlan(
        project="retail",
        sources=[
            LogicalSource(entity="orders", format="csv", file_name="o.csv",
                          validate_first=True,
                          columns=[LogicalColumn(name="order_id")]),
            LogicalSource(entity="payments", format="csv", file_name="p.csv",
                          columns=[LogicalColumn(name="order_id")]),
        ],
    )
    spec = compile_plan(plan)
    pl = spec.pipelines[0]
    validate = next(a for a in pl.activities if a.name == "validate_orders")
    assert validate.type == "Validation"
    assert validate.source_dataset == "ds_landing_orders"
    copy = next(a for a in pl.activities if a.name == "copy_orders")
    assert copy.depends_on == ["validate_orders"]
    assert next(a for a in pl.activities if a.name == "copy_payments").depends_on == []

    # BRD pipeline structure: gate + copy land together, dependency intact.
    plan.pipelines = [
        LogicalPipeline(name="PL_Master", invokes=["PL_Bronze"]),
        LogicalPipeline(name="PL_Bronze", ingest=["orders", "payments"]),
    ]
    spec = compile_plan(plan)
    bronze = next(p for p in spec.pipelines if p.name == "pl_bronze")
    names = [a.name for a in bronze.activities]
    assert "validate_orders" in names and "copy_orders" in names
    copy = next(a for a in bronze.activities if a.name == "copy_orders")
    assert copy.depends_on == ["validate_orders"]
    run_id = "test_validate_first"
    try:
        builder.write_artifacts(spec, run_id)
        pf = preflight.validate(run_id)
        assert pf["passed"], [c for c in pf["checks"] if not c["passed"]]
    finally:
        shutil.rmtree(builder.artifacts_dir(run_id), ignore_errors=True)


def test_unsupported_surfaces_on_spec():
    plan = _plan()
    plan.unsupported = ["send a Teams alert on failure"]
    spec = compile_plan(plan)
    assert spec.unsupported == ["send a Teams alert on failure"]
    assert any("NOT BUILT" in a for a in spec.assumptions)


def test_filter_on_aggregation_alias_runs_after_aggregate():
    """Regression for df_gold_inventory_summary: QuantityOnHand is created by
    aggregateRows, so a filter on it must not run on the raw srcStocks stream."""
    plan = LogicalPlan(
        project="retail",
        sources=[LogicalSource(
            entity="stocks",
            format="csv",
            file_name="stocks.csv",
            columns=[
                LogicalColumn(name="store_id", type="integer"),
                LogicalColumn(name="product_id", type="integer"),
                LogicalColumn(name="quantity", type="integer"),
            ],
        )],
        transforms=[EntityTransform(
            entity="stocks",
            casts=[LogicalColumn(name="quantity", type="integer")],
        )],
        outputs=[OutputPlan(
            name="inventory_summary",
            from_entities=["stocks"],
            group_by=["store_id", "product_id"],
            aggregations=[AggregationPlan(name="QuantityOnHand", op="sum", column="quantity")],
            filters=[FilterRule(op="gt", column="QuantityOnHand", value="0")],
        )],
    )
    spec = compile_plan(plan)
    df = next(d for d in spec.dataflows if d.name == "df_gold_inventory_summary")
    names = [t.name for t in df.transforms]
    assert names.index("filterAfterAgg") > names.index("aggregateRows")
    script = builder.build_dataflow_script(df, {d.name: d for d in spec.datasets})
    assert "aggregateRows filter(QuantityOnHand > 0) ~> filterAfterAgg" in script
    assert "srcStocks filter(QuantityOnHand > 0)" not in script


def test_build_and_preflight(monkeypatch):
    monkeypatch.setenv("MOCK", "true")
    run_id = "test_compile_run"
    run_dir = builder.artifacts_dir(run_id)
    try:
        spec = compile_plan(_plan())
        builder.write_artifacts(spec, run_id)
        pf = preflight.validate(run_id)
        assert pf["passed"], [c for c in pf["checks"] if not c["passed"]]
    finally:
        shutil.rmtree(run_dir, ignore_errors=True)


def test_coalesce_default_matches_column_type():
    """DF-DT-001 regression (df_silver_products): coalesce(stringCol, 0) fails at
    runtime because the bare-numeric default disagrees with the column type. The
    compiler must quote numeric defaults for string columns and unquote quoted
    numerics for numeric columns; unknown columns stay untouched."""
    plan = LogicalPlan(
        project="retail",
        sources=[LogicalSource(
            entity="products", format="csv", file_name="products.csv",
            columns=[LogicalColumn(name="product_id"),
                     LogicalColumn(name="product_name"),
                     LogicalColumn(name="weight_g")])],
        transforms=[EntityTransform(
            entity="products",
            casts=[LogicalColumn(name="weight_g", type="integer")],
            cleaning=[
                # string column (source default type), numeric default -> quoted
                CleaningOp(op="coalesce", column="product_name", args=["0"]),
                # integer column (cast), quoted default -> bare numeric
                CleaningOp(op="coalesce", column="weight_g", args=["'0'"]),
                # unknown column -> args untouched
                CleaningOp(op="coalesce", column="mystery", args=["0"]),
            ],
        )],
    )
    spec = compile_plan(plan)
    df = next(d for d in spec.dataflows if d.name == "df_silver_products")
    exprs = {c.name: c.expression for t in df.transforms for c in t.columns}
    assert exprs["product_name"] == "coalesce(product_name, '0')"
    assert exprs["weight_g"] == "coalesce(weight_g, 0)"
    assert exprs["mystery"] == "coalesce(mystery, 0)"


def test_cosmetic_string_op_on_numeric_dropped():
    """BRD2/BRD3 live incident: 'trim all string columns' makes the model trim
    zip_code (grounded integer) -> typecheck fails preflight and only a lucky
    LLM repair saves the run. The compiler must drop the op deterministically
    (with an assumption note) and keep trims on real string columns."""
    plan = LogicalPlan(
        project="trim_int",
        sources=[LogicalSource(
            entity="customers", format="csv", container="landing",
            file_name="customers.csv",
            columns=[LogicalColumn(name="customer_id", type="integer"),
                     LogicalColumn(name="first_name", type="string"),
                     LogicalColumn(name="zip_code", type="integer")])],
        transforms=[EntityTransform(
            entity="customers",
            cleaning=[
                CleaningOp(op="trim", column="first_name"),   # keep: string
                CleaningOp(op="trim", column="zip_code"),     # drop: integer
                CleaningOp(op="upper", column="zip_code"),    # drop: integer
            ],
        )],
    )
    spec = compile_plan(plan)
    df = next(d for d in spec.dataflows if d.name == "df_silver_customers")
    exprs = {c.name: c.expression for t in df.transforms for c in t.columns}
    assert exprs["first_name"] == "trim(first_name)"
    assert "zip_code" not in exprs  # both numeric-target ops dropped
    dropped_notes = [a for a in spec.assumptions if "zip_code" in a and "skipped" in a]
    assert len(dropped_notes) == 2
    # and the spec passes preflight instead of dying on expression_types
    run_id = "test_trim_int_drop"
    try:
        builder.write_artifacts(spec, run_id)
        assert preflight.validate(run_id)["passed"]
    finally:
        shutil.rmtree(builder.artifacts_dir(run_id), ignore_errors=True)


def test_join_emits_dedup_select():
    """After a join both streams carry the join key (order_id twice) — a later
    bare-name reference (countDistinct(order_id)) is ambiguous and ADF Studio
    flags the aggregate. One select-all dedup with skipDuplicate flags must sit
    between the last join and everything downstream."""
    spec = compile_plan(_plan())
    datasets = {d.name: d for d in spec.datasets}
    gold = next(df for df in spec.dataflows if df.name.startswith("df_gold"))
    script = builder.build_dataflow_script(gold, datasets)
    assert "dedupJoinColumns" in script
    assert "skipDuplicateMapInputs: true" in script
    # dedup comes AFTER the join and BEFORE the aggregate
    assert script.index("join1") < script.index("dedupJoinColumns") < script.index("aggregateRows")
    # joins still use qualified src@col conditions (select must not precede them)
    assert "srcOrders@customer_id == srcCustomers@customer_id" in script
