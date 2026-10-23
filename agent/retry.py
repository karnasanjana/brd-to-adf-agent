"""Backoff for transient LLM provider errors — shared by every model call.

gemini-2.5-flash sporadically returns 503 UNAVAILABLE / 429 under load. One
blip must not kill /plan, /chat, or a self-heal attempt: transient statuses
retry with exponential backoff; everything else (401, 404, ...) raises
immediately so real config errors stay loud.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Awaitable, Callable

# Provider blips worth absorbing with a short backoff.
TRANSIENT_LLM_STATUSES = {408, 429, 500, 502, 503, 504}

_TRIES = 3
_FIRST_DELAY = 5.0


def _is_transient_network(exc: BaseException) -> bool:
    """Timeouts AND connection-level failures are transient: the per-request
    LLM_TIMEOUT in config.py turns a stalled connection into an exception, and
    httpx raises ReadError/ConnectError (subclasses of TransportError) when the
    remote end drops mid-request — a fresh attempt usually succeeds (observed:
    ReadError('') killed /plan twice on BRD1). Match by type NAME so no
    provider-specific import (httpx, google-genai, anthropic) is needed."""
    for klass in type(exc).__mro__:
        name = klass.__name__.lower()
        if "timeout" in name or name == "transporterror":
            return True
    return isinstance(exc, (TimeoutError, ConnectionError))


def retry_sync(fn: "Callable[[], Any]", tries: int = _TRIES) -> Any:
    """``fn()`` with exponential backoff on transient provider errors."""
    from pydantic_ai.exceptions import ModelHTTPError

    delay = _FIRST_DELAY
    for attempt in range(tries):
        try:
            return fn()
        except ModelHTTPError as exc:
            if exc.status_code not in TRANSIENT_LLM_STATUSES or attempt == tries - 1:
                raise
            time.sleep(delay)
            delay *= 2
        except Exception as exc:  # noqa: BLE001 — only transient network errors retry
            if not _is_transient_network(exc) or attempt == tries - 1:
                raise
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("unreachable")


async def retry_async(fn: "Callable[[], Awaitable[Any]]", tries: int = _TRIES) -> Any:
    """``await fn()`` with the same backoff. ``fn`` is re-invoked from scratch
    each attempt, so it must rebuild any per-attempt state (e.g. a fresh
    PlanSession) rather than close over a partially mutated one."""
    from pydantic_ai.exceptions import ModelHTTPError

    delay = _FIRST_DELAY
    for attempt in range(tries):
        try:
            return await fn()
        except ModelHTTPError as exc:
            if exc.status_code not in TRANSIENT_LLM_STATUSES or attempt == tries - 1:
                raise
            await asyncio.sleep(delay)
            delay *= 2
        except Exception as exc:  # noqa: BLE001 — only transient network errors retry
            if not _is_transient_network(exc) or attempt == tries - 1:
                raise
            await asyncio.sleep(delay)
            delay *= 2
    raise RuntimeError("unreachable")
