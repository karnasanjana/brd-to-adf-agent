"""Shared data-flow IR: every ExecuteDataFlow must reference ir-dataflow
(TTL cluster) instead of AutoResolve — one ~5-min Spark startup per RUN
instead of one per FLOW."""

import json
import os
import shutil
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agent import builder
from agent.builder import DATAFLOW_IR_NAME
from models.spec import (
    Activity,
    Column,
    DataFlow,
    DataFlowEndpoint,
    Dataset,
    DatasetKind,
    DatasetLocation,
    PipelineDef,
    PipelineSpec,
)


def _spec():
    return PipelineSpec(
        datasets=[
            Dataset(
                name="ds_bronze_orders",
                kind=DatasetKind.delimited_text,
                linked_service="ls_adls",
                location=DatasetLocation(container="bronze", file_name="orders.csv"),
            ),
            Dataset(
                name="ds_silver_orders",
                kind=DatasetKind.parquet,
                linked_service="ls_adls",
                location=DatasetLocation(container="silver", file_name="orders.parquet"),
            ),
        ],
        dataflows=[
            DataFlow(
                name="df_silver_orders",
                source=DataFlowEndpoint(stream_name="srcOrders", dataset="ds_bronze_orders"),
                sink=DataFlowEndpoint(stream_name="sinkOrders", dataset="ds_silver_orders"),
                source_columns=[Column(name="order_id")],
                transforms=[],
            )
        ],
        pipelines=[
            PipelineDef(
                name="pl_silver",
                activities=[
                    Activity(
                        name="flow_silver_orders",
                        type="ExecuteDataFlow",
                        data_flow="df_silver_orders",
                    )
                ],
            )
        ],
    )


def test_execute_dataflow_references_shared_ir(monkeypatch):
    monkeypatch.delenv("DATAFLOW_TTL_MINUTES", raising=False)
    built = builder.build_all(_spec())
    assert DATAFLOW_IR_NAME in built["integrationRuntimes"]
    ir = built["integrationRuntimes"][DATAFLOW_IR_NAME]
    dfp = ir["typeProperties"]["computeProperties"]["dataFlowProperties"]
    assert dfp["timeToLive"] == 10 and dfp["cleanup"] is False
    act = built["pipelines"]["pl_silver"]["activities"][0]
    assert act["typeProperties"]["integrationRuntime"]["referenceName"] == DATAFLOW_IR_NAME


def test_ttl_zero_falls_back_to_autoresolve(monkeypatch):
    monkeypatch.setenv("DATAFLOW_TTL_MINUTES", "0")
    built = builder.build_all(_spec())
    assert built["integrationRuntimes"] == {}
    act = built["pipelines"]["pl_silver"]["activities"][0]
    assert "integrationRuntime" not in act["typeProperties"]


def test_ir_artifact_written(monkeypatch):
    monkeypatch.delenv("DATAFLOW_TTL_MINUTES", raising=False)
    root = tempfile.mkdtemp(prefix="dataflow_ir_")
    try:
        builder.write_artifacts(_spec(), "r1", root=root)
        path = os.path.join(root, "r1", "integrationRuntimes", f"{DATAFLOW_IR_NAME}.json")
        assert os.path.isfile(path)
        with open(path, encoding="utf-8") as fh:
            assert json.load(fh)["properties"]["type"] == "Managed"
    finally:
        shutil.rmtree(root, ignore_errors=True)
