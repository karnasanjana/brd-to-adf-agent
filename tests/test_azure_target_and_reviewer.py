"""BRD-named deploy target (advisory, user-confirmed) + PLAN_REVIEW gating."""

import os
import shutil
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module
from agent.planhandlers import PlanSession
from models.logical import (
    AggregationPlan,
    AzureTargetPlan,
    EntityTransform,
    LogicalColumn,
    LogicalPlan,
    LogicalSource,
    OutputPlan,
)


@pytest.fixture(autouse=True)
def _mock_env(monkeypatch):
    monkeypatch.setenv("MOCK", "true")
    monkeypatch.setenv("PLAN_MODE", "oneshot")
    monkeypatch.setenv("PLAN_CACHE", "false")
    monkeypatch.delenv("LEGACY_PARSER", raising=False)
    yield
    for rid in list(app_module._RUNS):
        rd = app_module._RUNS[rid].get("run_dir")
        if rd:
            shutil.rmtree(rd, ignore_errors=True)
        app_module._RUNS.pop(rid, None)


def _plan(azure_target=None):
    return LogicalPlan(
        project="bikes",
        sources=[LogicalSource(
            entity="orders", format="csv", file_name="orders.csv",
            columns=[LogicalColumn(name="order_id"), LogicalColumn(name="month"),
                     LogicalColumn(name="amount", type="decimal(18,2)")])],
        transforms=[EntityTransform(entity="orders", dedup_keys=["order_id"])],
        outputs=[OutputPlan(
            name="monthly_sales", from_entities=["orders"], group_by=["month"],
            aggregations=[AggregationPlan(name="revenue", op="sum", column="amount")])],
        azure_target=azure_target,
    )


def test_session_roundtrips_azure_target():
    target = AzureTargetPlan(resource_group="RetailProject",
                             factory_name="RetailADFFF",
                             storage_account="retailproject3001")
    s = PlanSession.from_plan(_plan(azure_target=target))
    assert s.plan().azure_target == target


def test_set_azure_target_handler_normalizes_empty():
    s = PlanSession()
    s.set_azure_target(resource_group="  ", factory_name="", storage_account=None)
    assert s.azure_target is None
    s.set_azure_target(resource_group="RG1")
    assert s.azure_target.resource_group == "RG1"
    assert s.azure_target.factory_name is None


def test_plan_response_carries_azure_target_but_never_applies_it(monkeypatch):
    """The response advertises the BRD-named target for UI confirm; the server
    env must be untouched (no silent deploy-target switch)."""
    async def fake_extract(text, message_history=None):
        return _plan(azure_target=AzureTargetPlan(
            resource_group="RetailProject", factory_name="RetailADFFF",
            storage_account="retailproject3001")), []

    from agent import extractor
    monkeypatch.setattr(extractor, "extract_plan", fake_extract)
    monkeypatch.delenv("AZURE_RESOURCE_GROUP", raising=False)

    client = TestClient(app_module.app)
    r = client.post("/plan", data={"brd": "Resource Group: RetailProject ..."})
    assert r.status_code == 200
    assert r.json()["azure_target"] == {
        "resource_group": "RetailProject",
        "factory_name": "RetailADFFF",
        "storage_account": "retailproject3001",
    }
    assert os.environ.get("AZURE_RESOURCE_GROUP") is None  # not applied


def test_plan_response_omits_azure_target_when_brd_names_none(monkeypatch):
    async def fake_extract(text, message_history=None):
        return _plan(), []

    from agent import extractor
    monkeypatch.setattr(extractor, "extract_plan", fake_extract)

    client = TestClient(app_module.app)
    r = client.post("/plan", data={"brd": "plain brd"})
    assert r.status_code == 200
    assert "azure_target" not in r.json()


def test_reviewer_enabled_flag(monkeypatch):
    from agent import reviewer

    monkeypatch.delenv("PLAN_REVIEW", raising=False)
    assert reviewer.enabled()  # default on
    monkeypatch.setenv("PLAN_REVIEW", "false")
    assert not reviewer.enabled()


def test_reviewer_skipped_in_mock(monkeypatch):
    """MOCK runs must spend zero reviewer LLM calls."""
    calls = []

    async def fake_review(brd, plan):
        calls.append(1)
        return [{"requirement": "x", "issue": "y", "severity": "missing"}]

    from agent import reviewer
    monkeypatch.setattr(reviewer, "review_plan", fake_review)

    async def fake_extract(text, message_history=None):
        return _plan(), []

    from agent import extractor
    monkeypatch.setattr(extractor, "extract_plan", fake_extract)

    client = TestClient(app_module.app)
    r = client.post("/plan", data={"brd": "plain brd"})
    assert r.status_code == 200
    assert calls == []  # gated off by MOCK
    assert "review_findings" not in r.json()
