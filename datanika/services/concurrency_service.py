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

# Default limits per plan slug — overridable via hook from datanika-cloud.
DEFAULT_MAX_PARALLEL = 5
PLAN_DEFAULTS = {
    "free": 2,
    "pro-monthly": 5,
    "enterprise-monthly": 20,
}


def _redis_client() -> redis.Redis:
    return redis.from_url(settings.redis_url, decode_responses=True)


def _key(org_id: int) -> str:
    return f"{_REDIS_PREFIX}{org_id}"


def get_max_parallel(org_id: int) -> int:
    """Return the concurrency limit for an org.

    Cloud plugin can override via ``concurrency.get_limit`` hook by
    mutating ``context["max_parallel"]``.
    """
    import contextlib

    context = {"org_id": org_id, "max_parallel": DEFAULT_MAX_PARALLEL}
    with contextlib.suppress(Exception):
        emit("concurrency.get_limit", context=context)
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
