"""Type inference (grounding) + deterministic expression type-checking.

The failure class under test: DF-DT-001-style runtime errors — arithmetic on
uncast text columns, coalesce() mixing types — must be caught in preflight
(milliseconds, fed back to the model) instead of minutes into an ADF run.
"""

import shutil

from agent import builder, preflight
from agent.compiler import compile_plan
from agent.grounding import (
    _classify,
    _flow_output_columns,
    _infer_column_types,
    ground_logical_plan,
)
from agent.typecheck import check_dataflow, check_spec
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
)
from models.spec import (
    Column,
    DataFlow,
    DataFlowEndpoint,
    DerivedColumn,
    Predicate,
    Transform,
    TransformKind,
)


# --------------------------------------------------------------------------- #
# Grounding: type inference from sampled rows
# --------------------------------------------------------------------------- #


def test_classify_values():
    assert _classify("42") == "integer"
    assert _classify("99999999999") == "long"
    assert _classify("3.14") == "double"
    assert _classify("true") == "boolean"
    assert _classify("2024-01-31") == "date"
    assert _classify("2024-01-31 08:15:00") == "timestamp"
    assert _classify("hello") == "string"
    assert _classify("") == "empty"


def test_infer_column_types_from_sample():
    head = (
        b"order_id,amount,order_date,status\n"
        b"1,9.99,2024-01-01,shipped\n"
        b"2,12.50,2024-01-02,pending\n"
        b"3,,2024-01-03,shipped\n"  # empty amount ignored, not string
        b"4,7.00,2024-01-04,ship"  # truncated tail row -> discarded
    )
    types = _infer_column_types(head, ["order_id", "amount", "order_date", "status"])
    assert types["order_id"] == "integer"
    assert types["amount"] == "double"
    assert types["order_date"] == "date"
    assert "status" not in types  # string columns stay default


def test_ground_logical_plan_reads_mock_source_schema(tmp_path, monkeypatch):
    root = tmp_path / "tenant"
    stocks = root / "storage" / "landing"
    stocks.mkdir(parents=True)
    (stocks / "stocks.csv").write_text(
        "store_id,product_id,quantity,updated_at\n"
        "1,10,5,2026-07-09\n"
        "2,11,7,2026-07-10\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("MOCK", "true")
    import agent.grounding as grounding

    monkeypatch.setattr(grounding, "MOCK_TENANT_DIR", str(root))
    plan = LogicalPlan(
        project="retail",
        sources=[LogicalSource(entity="stocks", format="csv", file_name="stocks.csv")],
    )
    notes = ground_logical_plan(plan)
    cols = {c.name: c.type for c in plan.sources[0].columns}
    assert notes
    assert cols == {
        "store_id": "integer",
        "product_id": "integer",
        "quantity": "integer",
        "updated_at": "date",
    }


def test_mixed_observations_degrade_safely():
    head = b"a,b\n1,5\n2.5,x\n9"  # trailing partial row is discarded
    types = _infer_column_types(head, ["a", "b"])
    assert types.get("a") == "double"  # int + float -> double
    assert "b" not in types  # int + text -> string


# --------------------------------------------------------------------------- #
# Type-checker over compiled flows
# --------------------------------------------------------------------------- #


def _items_plan(casts, cleaning, filters=()):
    return LogicalPlan(
        project="retail",
        sources=[LogicalSource(
            entity="order_items", format="csv", file_name="oi.csv",
            columns=[LogicalColumn(name="order_id"),
                     LogicalColumn(name="quantity"),
                     LogicalColumn(name="list_price"),
                     LogicalColumn(name="discount"),
                     LogicalColumn(name="order_date")])],
        transforms=[EntityTransform(entity="order_items", casts=list(casts),
                                    cleaning=list(cleaning), filters=list(filters))],
    )


_ARITH = [
    CleaningOp(op="subtract", column="1", args=["discount"], new_column="one_minus_discount"),
    CleaningOp(op="multiply", column="quantity", args=["list_price", "one_minus_discount"],
               new_column="line_total"),
]


def test_arithmetic_on_uncast_text_is_flagged():
    spec = compile_plan(_items_plan(casts=[], cleaning=_ARITH))
    problems = check_spec(spec)
    assert any("quantity" in p and "cast" in p for p in problems)
    assert any("list_price" in p for p in problems)


def test_cast_first_then_arithmetic_is_clean():
    casts = [LogicalColumn(name="quantity", type="integer"),
             LogicalColumn(name="list_price", type="decimal(18,2)"),
             LogicalColumn(name="discount", type="decimal(18,2)")]
    spec = compile_plan(_items_plan(casts=casts, cleaning=_ARITH))
    assert check_spec(spec) == []


def test_coalesce_defaults_repaired_or_flagged():
    # numeric default on a text column — the DF-DT-001 classic. The compiler
    # auto-repairs it deterministically (quotes the default to match the type).
    spec = compile_plan(_items_plan(
        casts=[], cleaning=[CleaningOp(op="coalesce", column="quantity", args=["0"])]))
    exprs = [c.expression for df in spec.dataflows for t in df.transforms for c in t.columns]
    assert "coalesce(quantity, '0')" in exprs
    assert check_spec(spec) == []
    # cast first -> numeric default stays numeric, clean
    spec = compile_plan(_items_plan(
        casts=[LogicalColumn(name="quantity", type="integer")],
        cleaning=[CleaningOp(op="coalesce", column="quantity", args=["0"])]))
    assert check_spec(spec) == []
    # text default on a NUMERIC column: no safe auto-repair -> flagged
    spec = compile_plan(_items_plan(
        casts=[LogicalColumn(name="quantity", type="integer")],
        cleaning=[CleaningOp(op="coalesce", column="quantity", args=["unknown"])]))
    problems = check_spec(spec)
    assert any("coalesce" in p and "quantity" in p for p in problems)


def test_date_part_needs_temporal():
    op = CleaningOp(op="year", column="order_date", new_column="order_year")
    spec = compile_plan(_items_plan(casts=[], cleaning=[op]))
    assert any("order_date" in p and "timestamp" in p for p in check_spec(spec))
    spec = compile_plan(_items_plan(
        casts=[LogicalColumn(name="order_date", type="date")], cleaning=[op]))
    assert check_spec(spec) == []


def test_numeric_filter_on_text_flagged():
    spec = compile_plan(_items_plan(
        casts=[], cleaning=[], filters=[FilterRule(op="gt", column="quantity", value="0")]))
    assert any("quantity" in p and "cast" in p for p in check_spec(spec))


def test_type_errors_block_preflight(monkeypatch):
    """The repair-loop contract: a type-broken plan must FAIL preflight (which
    /plan feeds back to the model) — never reach approval."""
    monkeypatch.setenv("MOCK", "true")
    spec = compile_plan(_items_plan(casts=[], cleaning=_ARITH))
    run_id = "test_typecheck_block"
    try:
        builder.write_artifacts(spec, run_id)
        pf = preflight.validate(run_id)
        assert not pf["passed"]
        failing = next(c for c in pf["checks"] if c["name"] == "expression_types")
        assert not failing["passed"]
        assert "cast" in failing["detail"]
    finally:
        shutil.rmtree(builder.artifacts_dir(run_id), ignore_errors=True)


def test_grounding_preserves_arithmetic_derived_type():
    """Regression for gold sum(line_total): grounding used to classify rendered
    arithmetic expressions as string, so silver line_total reached gold as text."""
    casts = [LogicalColumn(name="quantity", type="integer"),
             LogicalColumn(name="list_price", type="decimal(18,2)"),
             LogicalColumn(name="discount", type="decimal(18,2)")]
    spec = compile_plan(_items_plan(casts=casts, cleaning=_ARITH))
    df = next(d for d in spec.dataflows if d.name == "df_silver_order_items")
    source = [Column(name="order_id"),
              Column(name="quantity"),
              Column(name="list_price"),
              Column(name="discount"),
              Column(name="order_date")]
    out = {c.name: c.type for c in _flow_output_columns(df, source)}
    assert out["one_minus_discount"] == "double"
    assert out["line_total"] == "double"


def test_select_missing_column_is_flagged():
    spec = compile_plan(_items_plan(
        casts=[LogicalColumn(name="quantity", type="integer")],
        cleaning=[],
    ))
    df = next(d for d in spec.dataflows if d.name == "df_silver_order_items")
    df.transforms.append(
        Transform(
            kind=TransformKind.select,
            name="selectFinal",
            columns=[DerivedColumn(name="missing_out", expression="does_not_exist")],
        )
    )
    problems = check_spec(spec)
    assert any("does_not_exist" in p and "not available" in p for p in problems)


def test_replace_on_non_string_is_flagged():
    spec = compile_plan(_items_plan(
        casts=[LogicalColumn(name="quantity", type="integer")],
        cleaning=[CleaningOp(op="replace", column="quantity", args=["0"])],
    ))
    problems = check_spec(spec)
    assert any("replace" in p and "quantity" in p and "coalesce" in p for p in problems)


def test_filter_missing_column_is_flagged():
    df = DataFlow(
        name="df_gold_inventory_summary",
        source=DataFlowEndpoint(stream_name="srcStocks", dataset="ds_silver_stocks"),
        sink=DataFlowEndpoint(stream_name="sinkInventorySummary", dataset="ds_gold_inventory_summary"),
        source_columns=[
            Column(name="store_id", type="integer"),
            Column(name="product_id", type="integer"),
            Column(name="quantity", type="integer"),
        ],
        transforms=[
            Transform(
                kind=TransformKind.filter,
                name="filterRows",
                predicates=[Predicate(op="gt", column="QuantityOnHand", value="0")],
            ),
            Transform(
                kind=TransformKind.aggregate,
                name="aggregateRows",
                group_by=["store_id", "product_id"],
                columns=[DerivedColumn(name="QuantityOnHand", op="sum", column="quantity")],
            ),
        ],
    )
    problems = check_dataflow(df)
    assert any("QuantityOnHand" in p and "not available" in p for p in problems)


def test_boolean_filter_renders_boolean_literal():
    df = DataFlow(
        name="df_silver_staffs",
        source=DataFlowEndpoint(stream_name="srcStaffs", dataset="ds_bronze_staffs"),
        sink=DataFlowEndpoint(stream_name="sinkStaffs", dataset="ds_silver_staffs"),
        source_columns=[Column(name="active", type="boolean")],
        transforms=[
            Transform(
                kind=TransformKind.filter,
                name="filterRows",
                predicates=[Predicate(op="eq", column="active", value="true")],
            )
        ],
    )
    assert df.transforms[0].condition == "active == true"
    assert "active == 'true'" not in df.transforms[0].condition


# --------------------------------------------------------------------------- #
# Join key types (DF-FN-0015 'equals argument 1 of StringType and argument 2
# of IntegerType should be similar' — run flow_gold_top_customers)
# --------------------------------------------------------------------------- #


def _join_flow(cust_type: str, ord_type: str) -> DataFlow:
    return DataFlow(
        name="df_gold_top_customers",
        source=DataFlowEndpoint(stream_name="srcCustomers", dataset="ds_c"),
        sink=DataFlowEndpoint(stream_name="sinkTopCustomers", dataset="ds_g"),
        source_columns=[Column(name="customer_id", type=cust_type),
                        Column(name="region", type="string")],
        additional_sources=[DataFlowEndpoint(
            stream_name="srcOrders", dataset="ds_o",
            columns=[Column(name="customer_id", type=ord_type),
                     Column(name="amount", type="double")])],
        transforms=[Transform(
            kind=TransformKind.join, name="join1", right_stream="srcOrders",
            condition="srcCustomers@customer_id == srcOrders@customer_id")],
    )


def test_join_key_type_mismatch_flagged():
    problems = check_dataflow(_join_flow("string", "integer"))
    assert any("join1" in p and "customer_id" in p and "cast" in p.lower()
               for p in problems)


def test_join_key_matching_types_clean():
    assert check_dataflow(_join_flow("integer", "integer")) == []
    assert check_dataflow(_join_flow("string", "string")) == []


def test_chained_join_mismatch_flagged_through_compile():
    """Regression for run 564483b2 (flow_gold_sales_by_category): the SECOND
    join of a chain compared integer products.category_id with string
    categories.category_id — DF-FN-0015 in Azure. Must be flagged at preflight
    on the compiled spec."""
    plan = LogicalPlan(
        project="retail",
        sources=[
            LogicalSource(entity="order_items", format="csv", file_name="oi.csv",
                          columns=[LogicalColumn(name="product_id"),
                                   LogicalColumn(name="quantity")]),
            LogicalSource(entity="products", format="csv", file_name="p.csv",
                          columns=[LogicalColumn(name="product_id"),
                                   LogicalColumn(name="category_id")]),
            LogicalSource(entity="categories", format="csv", file_name="c.csv",
                          columns=[LogicalColumn(name="category_id"),
                                   LogicalColumn(name="category_name")]),
        ],
        transforms=[
            EntityTransform(entity="products",
                            casts=[LogicalColumn(name="product_id", type="integer"),
                                   LogicalColumn(name="category_id", type="integer")]),
            EntityTransform(entity="order_items",
                            casts=[LogicalColumn(name="product_id", type="integer"),
                                   LogicalColumn(name="quantity", type="integer")]),
        ],
        outputs=[OutputPlan(
            name="sales_by_category",
            from_entities=["order_items", "products", "categories"],
            joins=[
                JoinPlan(left_entity="order_items", right_entity="products",
                         left_keys=["product_id"], right_keys=["product_id"]),
                JoinPlan(left_entity="products", right_entity="categories",
                         left_keys=["category_id"], right_keys=["category_id"]),
            ],
            group_by=["category_name"],
            aggregations=[AggregationPlan(name="total_qty", op="sum",
                                          column="quantity")],
        )],
    )
    problems = check_spec(compile_plan(plan))
    assert any("join2" in p and "category_id" in p for p in problems)
    # fixing the cast clears it
    plan.transforms.append(EntityTransform(
        entity="categories",
        casts=[LogicalColumn(name="category_id", type="integer")]))
    problems = check_spec(compile_plan(plan))
    assert not any("join2" in p for p in problems)


def test_join_checked_even_without_primary_schema():
    """The join check must not hide behind the ungrounded-flow early return."""
    df = _join_flow("string", "integer")
    df.source_columns = []
    # primary side now unknown -> only a mismatch between typed extra sources
    # is checkable; swap the join to compare two extra-source columns.
    df.additional_sources.append(DataFlowEndpoint(
        stream_name="srcItems", dataset="ds_i",
        columns=[Column(name="customer_id", type="string")]))
    df.transforms[0].condition = "srcItems@customer_id == srcOrders@customer_id"
    problems = check_dataflow(df)
    assert any("join1" in p for p in problems)
