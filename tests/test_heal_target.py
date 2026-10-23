"""Regression for _pick_target: a nested runtime data-flow error must heal the
DATAFLOW, not the master pipeline that wraps it (the bug observed on silver
order_items, where self-heal patched pl_master_pipeline twice and burned every
retry)."""

import json
import os

from agent.validator import _pick_target

# The exact shape the user hit: master -> child -> data flow, DFExecutorUserError.
ERROR = (
    "Operation on target run_silver_order_items failed: "
    "Operation on target flow_silver_order_items failed: "
    '{"StatusCode":"DFExecutorUserError","Message":"Job failed ... at Sink '
    "'sinkOrderItems': Failed to merge fields 'order_item_id' and "
    "'order_item_id'. Failed to merge incompatible data types StringType and "
    'IntegerType"}'
)


def _write(run_dir, category, name, props):
    d = os.path.join(run_dir, category)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, f"{name}.json"), "w", encoding="utf-8") as fh:
        json.dump({"name": name, "properties": props}, fh)


def test_nested_dataflow_error_targets_dataflow(tmp_path):
    run_dir = str(tmp_path)
    _write(run_dir, "pipelines", "pl_master_pipeline", {
        "activities": [
            {"name": "run_silver_order_items", "type": "ExecutePipeline",
             "typeProperties": {"pipeline": {"referenceName": "pl_silver_order_items"}}}
        ]
    })
    _write(run_dir, "pipelines", "pl_silver_order_items", {
        "activities": [
            {"name": "flow_silver_order_items", "type": "ExecuteDataFlow",
             "typeProperties": {"dataflow": {"referenceName": "df_silver_order_items"}}}
        ]
    })
    _write(run_dir, "dataflows", "df_silver_order_items", {"type": "MappingDataFlow"})

    inventory = {
        "pl_master_pipeline": "pipelines",
        "pl_silver_order_items": "pipelines",
        "df_silver_order_items": "dataflows",
    }
    target = _pick_target(ERROR, inventory, run_dir)
    assert target == ("df_silver_order_items", "dataflows"), target


def test_copy_error_targets_owning_pipeline(tmp_path):
    run_dir = str(tmp_path)
    _write(run_dir, "pipelines", "pl_ingest_orders", {
        "activities": [{"name": "copy_orders", "type": "Copy"}]
    })
    inventory = {"pl_ingest_orders": "pipelines"}
    err = "Operation on target copy_orders failed: some copy error"
    assert _pick_target(err, inventory, run_dir) == ("pl_ingest_orders", "pipelines")


def test_unidentified_error_does_not_guess_dataflow(tmp_path):
    run_dir = str(tmp_path)
    _write(run_dir, "dataflows", "df_good_flow", {"type": "MappingDataFlow"})
    inventory = {"df_good_flow": "dataflows"}
    err = "DFExecutorUserError without artifact or activity name"
    assert _pick_target(err, inventory, run_dir) is None
