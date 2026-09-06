"""Per-org task concurrency limiter using Redis.

Tracks running tasks per org_id. Before starting a task, call
``acquire(org_id)`` — returns True if a slot is available, False if the
org has hit its limit.  Call ``release(org_id)`` when the task finishes.
"""

import logging

import redis

from datanika.config import settings
from datanika.hooks import emit

logger = logging.getLogger(__name__)

_REDIS_PREFIX = "datanika:concurrency:"
# TTL on each counter as a safety net (if a worker dies without releasing).
_KEY_TTL_SECONDS = 3600 * 6  # 6 hours

# The limit a core-edition instance keeps. It has no plans and no subscriptions, no
# handler is registered for ``concurrency.get_limit``, and ``emit`` returns with the
# context untouched — so this value is the whole answer for a self-hoster.
#
# ⚠️ There is deliberately **no per-slug table here.** The authoritative per-slug values
# are ``PUBLISHED_MAX_PARALLEL_RUNS`` in migration ``f6a7b8c9d0e1`` and the ``plans`` rows
# it writes; cloud resolves them per org and writes the answer into the hook context.
# A copy in this module was read by nothing and audited by everybody — core#780's scope
# was drawn from it and drawn wrong (core#915).
DEFAULT_MAX_PARALLEL = 5


def _redis_client() -> redis.Redis:
    return redis.from_url(settings.redis_url, decode_responses=True)


def _key(org_id: int) -> str:
    return f"{_REDIS_PREFIX}{org_id}"


def get_max_parallel(org_id: int) -> int:
    """Return the concurrency limit for an org.

    Cloud plugin can override via ``concurrency.get_limit`` hook by
    mutating ``context["max_parallel"]``.

    A failing handler degrades to ``DEFAULT_MAX_PARALLEL`` rather than blocking the run —
    the safe direction for a limiter — but it is **logged**. This was
    ``contextlib.suppress(Exception)``, and a silent degrade makes "every org fell back to
    the default" indistinguishable from "the plan tiers are being honoured", which is the
    state core#780 sat in for five months.
    """
    context = {"org_id": org_id, "max_parallel": DEFAULT_MAX_PARALLEL}
    try:
        emit("concurrency.get_limit", context=context)
    except Exception:
        # Deliberately NOT `return DEFAULT_MAX_PARALLEL`. `suppress` left whatever the
        # context held, and this must stay behaviour-identical: with a handler that
        # mutates and then raises, the mutation is cloud's resolved answer and discarding
        # it would be a real change hiding inside an observability fix.
        logger.warning(
            "Concurrency limit lookup failed for org %s; using %s",
            org_id,
            context["max_parallel"],
            exc_info=True,
        )
    return context["max_parallel"]


def running_count(org_id: int) -> int:
    """Return number of currently running tasks for the org."""
    r = _redis_client()
    val = r.get(_key(org_id))
    return int(val) if val else 0


def acquire(org_id: int) -> bool:
    """Try to acquire a concurrency slot.  Returns True if granted."""
    limit = get_max_parallel(org_id)
    r = _redis_client()
    key = _key(org_id)

    # Atomic increment + check
    pipe = r.pipeline(transaction=True)
    pipe.incr(key)
    pipe.expire(key, _KEY_TTL_SECONDS)
    results = pipe.execute()
    current = results[0]

    if current > limit:
        # Over limit — roll back the increment
        r.decr(key)
        logger.info(
            "Concurrency limit reached for org %s (%s/%s)",
            org_id,
            current - 1,
            limit,
        )
        return False

    return True


def release(org_id: int) -> None:
    """Release a concurrency slot after task completion."""
    r = _redis_client()
    key = _key(org_id)
    val = r.decr(key)
    # Don't go below zero
    if val is not None and int(val) < 0:
        r.set(key, 0, ex=_KEY_TTL_SECONDS)
