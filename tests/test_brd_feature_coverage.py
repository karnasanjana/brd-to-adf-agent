"""Regression coverage for common BRD wording/classes.

These tests stay below the LLM: they prove that once extraction maps user
phrasing into the LogicalPlan contract, the deterministic compiler/build path
supports the feature or rejects it loudly.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent import builder  # noqa: E402
from agent.compiler import compile_plan  # noqa: E402
from agent.completeness import check  # noqa: E402
from agent.lint import lint_dataflow_script  # noqa: E402
from agent.typecheck import check_spec  # noqa: E402
from models.logical import (  # noqa: E402
    AggregationPlan,
    CleaningOp,
    EntityTransform,
    FilterRule,
    LogicalColumn,
    LogicalPlan,
    LogicalSource,
    OutputPlan,
)


def test_folder_source_uses_recursive_wildcard_copy_settings(monkeypatch):
    """A BRD that names a folder instead of a concrete file should copy all files
    recursively instead of failing ADF's MissingFileName validation."""
    monkeypatch.setenv("MOCK", "true")
    plan = LogicalPlan(
        project="folder_ingest",
        sources=[
            LogicalSource(
                entity="orders",
                format="csv",
                container="landing",
                folder="incoming/orders",
                columns=[LogicalColumn(name="order_id")],
            )
        ],
    )
    spec = compile_plan(plan)
    built = builder.build_all(spec)
    act = built["pipelines"]["pl_folder_ingest"]["activities"][0]
    settings = act["typeProperties"]["source"]["storeSettings"]
    assert settings["recursive"] is True
    assert settings["wildcardFileName"] == "*"
    assert settings["wildcardFolderPath"] == "incoming/orders"


def test_binary_source_is_bronze_copy_only(monkeypatch):
    monkeypatch.setenv("MOCK", "true")
    plan = LogicalPlan(
        project="binary_landing",
        sources=[
            LogicalSource(
                entity="images",
                format="binary",
                container="landing",
                folder="raw/images",
            )
        ],
    )
    assert check(plan) == []
    spec = compile_plan(plan)
    built = builder.build_all(spec)
    assert built["datasets"]["ds_landing_images"]["type"] == "Binary"
    assert built["datasets"]["ds_bronze_images"]["type"] == "Binary"
    act = built["pipelines"]["pl_binary_landing"]["activities"][0]
    assert act["type"] == "Copy"
    assert act["typeProperties"]["source"]["type"] == "BinarySource"
    assert act["typeProperties"]["sink"]["type"] == "BinarySink"


def test_binary_source_cannot_feed_transforms_or_outputs():
    plan = LogicalPlan(
        project="bad_binary",
        sources=[
            LogicalSource(entity="images", format="binary", file_name="x.bin"),
        ],
        transforms=[EntityTransform(entity="images", cleaning=[CleaningOp(op="trim", column="body")])],
        outputs=[OutputPlan(name="image_summary", from_entities=["images"])],
    )
    problems = check(plan)
    assert any("binary sources support byte-for-byte Bronze copy only" in p for p in problems)
    assert any("cannot feed a data flow output" in p for p in problems)


def test_like_in_between_and_regex_filters_render_and_typecheck(monkeypatch):
    monkeypatch.setenv("MOCK", "true")
    plan = LogicalPlan(
        project="filter_coverage",
        sources=[
            LogicalSource(
                entity="orders",
                format="csv",
                file_name="orders.csv",
                columns=[
                    LogicalColumn(name="order_id"),
                    LogicalColumn(name="status"),
                    LogicalColumn(name="region"),
                    LogicalColumn(name="amount"),
                ],
            )
        ],
        transforms=[
            EntityTransform(
                entity="orders",
                casts=[LogicalColumn(name="amount", type="decimal(18,2)")],
                filters=[
                    FilterRule(op="like", column="status", value="COMP%"),
                    FilterRule(op="in", column="region", value="US,CA,MX"),
                    FilterRule(op="between", column="amount", value="10,100"),
                    FilterRule(op="regex", column="order_id", value="^ORD-[0-9]+$"),
                ],
            )
        ],
    )
    spec = compile_plan(plan)
    assert check_spec(spec) == []
    df = next(d for d in spec.dataflows if d.name == "df_silver_orders")
    script = builder.build_dataflow_script(df, {d.name: d for d in spec.datasets})
    assert "like(status, 'COMP%')" in script
    assert "in(['US', 'CA', 'MX'], region)" in script
    assert "between(amount, 10, 100)" in script
    assert "regexMatch(order_id, '^ORD-[0-9]+$')" in script
    assert lint_dataflow_script(df.name, script) == []


def test_regex_remove_special_and_empty_string_cleaning(monkeypatch):
    monkeypatch.setenv("MOCK", "true")
    plan = LogicalPlan(
        project="cleaning_coverage",
        sources=[
            LogicalSource(
                entity="customers",
                format="csv",
                file_name="customers.csv",
                columns=[
                    LogicalColumn(name="customer_id"),
                    LogicalColumn(name="phone"),
                    LogicalColumn(name="nickname"),
                ],
            )
        ],
        transforms=[
            EntityTransform(
                entity="customers",
                cleaning=[
                    CleaningOp(op="regex_replace", column="phone", args=["[^0-9]", ""]),
                    CleaningOp(op="remove_special_characters", column="nickname"),
                    CleaningOp(op="replace_empty_string", column="nickname", args=["Unknown"]),
                ],
            )
        ],
        outputs=[
            OutputPlan(
                name="customer_count",
                from_entities=["customers"],
                aggregations=[AggregationPlan(name="row_count", op="count", column="customer_id")],
            )
        ],
    )
    spec = compile_plan(plan)
    assert check_spec(spec) == []
    df = next(d for d in spec.dataflows if d.name == "df_silver_customers")
    script = builder.build_dataflow_script(df, {d.name: d for d in spec.datasets})
    assert "phone = regexReplace(phone, '[^0-9]', '')" in script
    assert "nickname = regexReplace(nickname, '[^A-Za-z0-9 ]', '')" in script
    assert "iif(isNull(nickname) || trim(nickname) == '', 'Unknown', nickname)" in script
    assert lint_dataflow_script(df.name, script) == []
