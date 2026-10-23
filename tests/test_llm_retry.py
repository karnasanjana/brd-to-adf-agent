"""Transient provider errors retry with backoff instead of killing the call —
self-heal (validator._run_llm) AND the plan path (agent/retry.py). Original
incident: gemini-2.5-flash 503 UNAVAILABLE mid-heal died at attempt 1 with the
ADF failure unhealed; same 503 later killed /plan outright."""

import asyncio
import time

import pytest
from pydantic_ai.exceptions import ModelHTTPError

from agent.retry import retry_async
from agent.validator import _run_llm


def _transient_error(status: int) -> ModelHTTPError:
    return ModelHTTPError(
        status_code=status,
        model_name="gemini-2.5-flash",
        body={"error": {"code": status, "status": "UNAVAILABLE"}},
    )


class _FlakyAgent:
    def __init__(self, failures: int, status: int = 503):
        self.failures = failures
        self.status = status
        self.calls = 0

    def run_sync(self, msg: str) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise _transient_error(self.status)
        return "ok"

    async def run(self, msg: str) -> str:
        return self.run_sync(msg)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda s: None)

    async def _instant(_s):
        return None

    monkeypatch.setattr(asyncio, "sleep", _instant)


def test_transient_503_retried():
    agent = _FlakyAgent(failures=2)
    assert _run_llm(agent, "msg") == "ok"
    assert agent.calls == 3


def test_gives_up_after_cap():
    agent = _FlakyAgent(failures=99)
    with pytest.raises(ModelHTTPError):
        _run_llm(agent, "msg")
    assert agent.calls == 3


def test_non_transient_not_retried():
    agent = _FlakyAgent(failures=99, status=401)
    with pytest.raises(ModelHTTPError):
        _run_llm(agent, "msg")
    assert agent.calls == 1


def test_async_transient_503_retried():
    agent = _FlakyAgent(failures=2)
    assert asyncio.run(retry_async(lambda: agent.run("msg"))) == "ok"
    assert agent.calls == 3


def test_async_gives_up_after_cap():
    agent = _FlakyAgent(failures=99)
    with pytest.raises(ModelHTTPError):
        asyncio.run(retry_async(lambda: agent.run("msg")))
    assert agent.calls == 3


def test_async_non_transient_not_retried():
    agent = _FlakyAgent(failures=99, status=401)
    with pytest.raises(ModelHTTPError):
        asyncio.run(retry_async(lambda: agent.run("msg")))
    assert agent.calls == 1


def test_async_network_error_retried():
    # BRD1 incident: httpx.ReadError('') (remote dropped mid-read) killed /plan
    # outright — network transport errors and timeouts must retry like 503s.
    import httpx

    calls = []

    async def flaky():
        calls.append(1)
        if len(calls) < 3:
            raise httpx.ReadError("")
        return "ok"

    assert asyncio.run(retry_async(flaky)) == "ok"
    assert len(calls) == 3


def test_async_timeout_retried():
    calls = []

    async def flaky():
        calls.append(1)
        if len(calls) < 2:
            raise TimeoutError()
        return "ok"

    assert asyncio.run(retry_async(flaky)) == "ok"
    assert len(calls) == 2


def test_async_real_bug_not_retried():
    # A genuine code bug (KeyError, ValueError...) must NOT be absorbed.
    async def broken():
        raise KeyError("real bug")

    with pytest.raises(KeyError):
        asyncio.run(retry_async(broken))


def test_async_fn_rebuilt_each_attempt():
    # The plan-tools path passes a factory that must run from scratch per
    # attempt (fresh PlanSession) — verify the factory is re-invoked.
    built = []
    agent = _FlakyAgent(failures=1)

    async def attempt():
        built.append(object())
        return await agent.run("msg")

    assert asyncio.run(retry_async(attempt)) == "ok"
    assert len(built) == 2
