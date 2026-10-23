"""Content-addressed LogicalPlan cache: the same BRD never pays for the LLM twice.

Keyed by sha256(brd_text + plan_mode + model id + vocabulary fingerprint), so a
cache hit is only ever returned for a plan produced by the same requirements
under the same agent, the same model, AND the same plan vocabulary. Stored as
JSON on disk (artifacts/.plan_cache/) so it survives the server restarts that
already drop in-memory run state.

Only the FIRST generation is cached — a /chat refine depends on conversation
context the cache cannot key on, and its result is a different plan for the
same BRD. Disable with PLAN_CACHE=false (e.g. when evaluating model changes).
"""

from __future__ import annotations

import functools
import hashlib
import json
import os
from typing import Optional

from models.logical import LogicalPlan

_CACHE_DIR = os.path.join("artifacts", ".plan_cache")


def enabled() -> bool:
    return os.environ.get("PLAN_CACHE", "true").strip().lower() not in {"false", "0", "no", "off"}


def _model_id() -> str:
    provider = os.environ.get("LLM_PROVIDER", "gemini").strip().lower()
    model = (
        os.environ.get("LLM_MODEL")
        or os.environ.get("CLAUDE_MODEL")
        or ""
    ).strip().lower()
    return f"{provider}:{model}"


@functools.lru_cache(maxsize=1)
def _vocab_fingerprint() -> str:
    """Hash of the LogicalPlan JSON schema — the closed vocabulary the model
    plans against. WITHOUT this, extending the vocabulary (adding op `case`,
    `top_n`, `post_agg_derived`, ...) silently keeps serving plans generated
    when the system could NOT express the new thing: the same BRD replays its
    old `unsupported` list forever and the new capability is unreachable.
    Observed 2026-07-09 — a BRD kept reporting conditional logic as NOT BUILT
    for an hour after `case` shipped. Old entries are never read again (their
    key is unreachable) and cost only disk."""
    schema = json.dumps(LogicalPlan.model_json_schema(), sort_keys=True)
    return hashlib.sha256(schema.encode("utf-8")).hexdigest()[:12]


def key(brd_text: str, plan_mode: str) -> str:
    """Cache key: requirements + the agent path + the model that will answer +
    the vocabulary it plans against. Changing any of the four is a different
    plan, so it must be a different key."""
    blob = f"{plan_mode}\x00{_model_id()}\x00{_vocab_fingerprint()}\x00{brd_text.strip()}"
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:32]


def get(cache_key: str) -> "Optional[LogicalPlan]":
    """The cached plan, or None on a miss / unreadable or stale-schema entry."""
    if not enabled():
        return None
    path = os.path.join(_CACHE_DIR, f"{cache_key}.json")
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return LogicalPlan.model_validate_json(fh.read())
    except Exception:  # noqa: BLE001 — corrupt entry or LogicalPlan schema changed
        return None


def put(cache_key: str, plan: LogicalPlan) -> None:
    """Store a plan. Best-effort: a cache write must never break a good plan."""
    if not enabled():
        return
    try:
        os.makedirs(_CACHE_DIR, exist_ok=True)
        with open(os.path.join(_CACHE_DIR, f"{cache_key}.json"), "w", encoding="utf-8") as fh:
            fh.write(plan.model_dump_json(indent=2))
    except Exception:  # noqa: BLE001
        pass
