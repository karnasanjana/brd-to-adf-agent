"""Plan cache + PLAN_MODE=auto wiring — no LLM, no network.

Proves the two efficiency claims: an identical BRD costs zero model calls the
second time, and `auto` mode uses the cheap one-shot extractor for the first
generation while routing repairs through the tool agent.
"""

import shutil

import pytest
from fastapi.testclient import TestClient

import app as app_module
from agent import plancache
from models.logical import (
    AggregationPlan,
    EntityTransform,
    LogicalColumn,
    LogicalPlan,
    LogicalSource,
    OutputPlan,
)

_BRD = "cache me: orders.csv, dedup order_id, total amount per month"


@pytest.fixture(autouse=True)
def _env(monkeypatch, tmp_path):
    monkeypatch.setenv("MOCK", "true")
    monkeypatch.delenv("LEGACY_PARSER", raising=False)
    monkeypatch.setenv("LLM_PROVIDER", "gemini")
    monkeypatch.setenv("LLM_MODEL", "test-model")
    monkeypatch.setattr(plancache, "_CACHE_DIR", str(tmp_path / "plan_cache"))
    yield
    for rid in list(app_module._RUNS):
        rd = app_module._RUNS[rid].get("run_dir")
        if rd:
            shutil.rmtree(rd, ignore_errors=True)
        app_module._RUNS.pop(rid, None)


def _good_plan():
    return LogicalPlan(
        project="retail",
        sources=[LogicalSource(
            entity="orders", format="csv", file_name="orders.csv",
            columns=[LogicalColumn(name="order_id"), LogicalColumn(name="month"),
                     LogicalColumn(name="amount", type="decimal(18,2)")])],
        transforms=[EntityTransform(entity="orders", dedup_keys=["order_id"])],
        outputs=[OutputPlan(
            name="monthly_sales", from_entities=["orders"], group_by=["month"],
            aggregations=[AggregationPlan(name="total", op="sum", column="amount")])],
    )


# --------------------------------------------------------------------------- #
# Key derivation
# --------------------------------------------------------------------------- #

def test_key_is_stable_and_ignores_surrounding_whitespace():
    assert plancache.key(_BRD, "auto") == plancache.key(f"  {_BRD}\n", "auto")


def test_key_changes_with_mode_model_and_brd(monkeypatch):
    base = plancache.key(_BRD, "auto")
    assert plancache.key(_BRD, "tools") != base           # different agent path
    assert plancache.key(_BRD + " and top 10", "auto") != base  # different BRD
    monkeypatch.setenv("LLM_MODEL", "other-model")
    assert plancache.key(_BRD, "auto") != base            # different model


def test_key_changes_with_claude_model_when_llm_model_unset(monkeypatch):
    monkeypatch.delenv("LLM_MODEL", raising=False)
    monkeypatch.setenv("LLM_PROVIDER", "anthropic")
    monkeypatch.setenv("CLAUDE_MODEL", "claude-a")
    base = plancache.key(_BRD, "auto")
    monkeypatch.setenv("CLAUDE_MODEL", "claude-b")
    assert plancache.key(_BRD, "auto") != base


def test_get_miss_returns_none():
    assert plancache.get(plancache.key("never planned", "auto")) is None


def test_put_then_get_round_trips():
    k = plancache.key(_BRD, "auto")
    plancache.put(k, _good_plan())
    assert plancache.get(k) == _good_plan()


def test_disabled_cache_never_reads_or_writes(monkeypatch):
    monkeypatch.setenv("PLAN_CACHE", "false")
    k = plancache.key(_BRD, "auto")
    plancache.put(k, _good_plan())
    assert plancache.get(k) is None


# --------------------------------------------------------------------------- #
# /plan integration: second identical BRD costs zero LLM calls
# --------------------------------------------------------------------------- #

def test_second_identical_brd_skips_the_model(monkeypatch):
    calls = {"n": 0}

    async def fake_extract(text, message_history=None):
        calls["n"] += 1
        return _good_plan(), []

    monkeypatch.setenv("PLAN_MODE", "oneshot")
    from agent import extractor
    monkeypatch.setattr(extractor, "extract_plan", fake_extract)

    client = TestClient(app_module.app)
    first = client.post("/plan", data={"brd": _BRD})
    assert first.status_code == 200, first.text
    assert calls["n"] == 1
    assert first.json()["timings"]["llm_calls"] == 1

    second = client.post("/plan", data={"brd": _BRD})
    assert second.status_code == 200, second.text
    assert calls["n"] == 1, "identical BRD must not call the model again"
    body = second.json()
    assert body["preflight"]["passed"]
    assert body["timings"]["llm_calls"] == 0
    assert "extract_cached" in body["timings"]["ms"]


def test_failing_plan_is_not_cached(monkeypatch):
    """Only a plan that passed every check may be reused. An unprunable plan
    (no sources) stays NEEDS_CLARIFICATION and must NOT enter the cache."""
    from models.logical import LogicalPlan

    async def fake_extract(text, message_history=None):
        return LogicalPlan(project="empty", sources=[]), []

    async def fake_refine(instruction, message_history):
        return LogicalPlan(project="empty", sources=[]), message_history

    monkeypatch.setenv("PLAN_MODE", "oneshot")
    from agent import extractor
    monkeypatch.setattr(extractor, "extract_plan", fake_extract)
    monkeypatch.setattr(extractor, "refine_plan", fake_refine)

    client = TestClient(app_module.app)
    r = client.post("/plan", data={"brd": _BRD})
    assert r.status_code == 200
    assert r.json()["status"] == "NEEDS_CLARIFICATION"
    assert plancache.get(plancache.key(_BRD, "oneshot")) is None


# --------------------------------------------------------------------------- #
# PLAN_MODE=auto: one-shot extract, tool-agent refine
# --------------------------------------------------------------------------- #

def test_auto_mode_extracts_oneshot_and_refines_with_tools(monkeypatch):
    """auto: one-shot first (cheap), tool agent as extraction fallback + refine."""
    import asyncio

    monkeypatch.setenv("PLAN_MODE", "auto")
    assert app_module._plan_mode() == "auto"

    calls = []

    async def fake_oneshot(text, message_history=None):
        calls.append("oneshot")
        return _good_plan(), []

    async def fake_tools(text, message_history=None):
        calls.append("tools")
        return _good_plan(), []

    from agent import extractor, plantools
    monkeypatch.setattr(extractor, "extract_plan", fake_oneshot)
    monkeypatch.setattr(plantools, "extract_plan", fake_tools)

    extract, refine = app_module._plan_fns()
    asyncio.run(extract("brd"))
    assert calls == ["oneshot"]  # tools NOT called when one-shot succeeds
    assert refine is not plantools.refine_plan  # wrapped, but backed by the tools agent


def test_auto_mode_falls_back_to_tools_on_output_failure(monkeypatch):
    """A large BRD can defeat the single giant JSON emission ('Exceeded maximum
    output retries') — auto must retry via the tool agent instead of failing."""
    import asyncio

    from pydantic_ai.exceptions import UnexpectedModelBehavior

    monkeypatch.setenv("PLAN_MODE", "auto")
    calls = []

    async def failing_oneshot(text, message_history=None):
        calls.append("oneshot")
        raise UnexpectedModelBehavior("Exceeded maximum output retries (4)")

    async def fake_tools(text, message_history=None):
        calls.append("tools")
        return _good_plan(), []

    from agent import extractor, plantools
    monkeypatch.setattr(extractor, "extract_plan", failing_oneshot)
    monkeypatch.setattr(plantools, "extract_plan", fake_tools)

    extract, _ = app_module._plan_fns()
    plan, _msgs = asyncio.run(extract("huge brd"))
    assert calls == ["oneshot", "tools"]
    assert plan.project == "retail"


def test_auto_mode_does_not_swallow_config_errors(monkeypatch):
    """Only model-output failures trigger the fallback; a missing API key must
    still surface as the original error."""
    import asyncio

    monkeypatch.setenv("PLAN_MODE", "auto")

    async def config_error(text, message_history=None):
        raise RuntimeError("Set GEMINI_API_KEY in .env")

    from agent import extractor
    monkeypatch.setattr(extractor, "extract_plan", config_error)

    extract, _ = app_module._plan_fns()
    try:
        asyncio.run(extract("brd"))
        raise AssertionError("expected RuntimeError")
    except RuntimeError as exc:
        assert "GEMINI_API_KEY" in str(exc)


def test_mode_defaults_to_auto(monkeypatch):
    monkeypatch.delenv("PLAN_MODE", raising=False)
    assert app_module._plan_mode() == "auto"


def test_timings_report_phases(monkeypatch):
    async def fake_extract(text, message_history=None):
        return _good_plan(), []

    monkeypatch.setenv("PLAN_MODE", "oneshot")
    monkeypatch.setenv("PLAN_CACHE", "false")
    from agent import extractor
    monkeypatch.setattr(extractor, "extract_plan", fake_extract)

    client = TestClient(app_module.app)
    t = client.post("/plan", data={"brd": _BRD}).json()["timings"]
    assert {"extract", "compile"} <= set(t["ms"])
    assert t["total_ms"] >= 0 and t["llm_calls"] == 1
