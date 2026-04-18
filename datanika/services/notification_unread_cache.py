"""Redis-backed cache for per-user unread-notification counts.

The ``/api/v1/notifications/unread-count`` endpoint is called on every
bell-icon render and was pushing p95 to 265ms at 100 VUs (k6 Run 5),
above the 200ms target. The underlying query is cheap — two predicates
on an indexed table — but round-trip + connection checkout + ORM
materialization add up. Caching the scalar count in Redis drops the
warm-path to a single round-trip.

Design notes:

* **Key shape**: ``notif_unread:{org_id}:{user_id}``. The count depends
  on both because the UI query unions user-specific rows (``user_id==U``)
  and org-wide rows (``user_id IS NULL``) — two users in the same org
  can have different totals if one has unread user-specific notifications.
* **TTL**: 60s safety net. Writes invalidate explicitly, TTL just
  prevents unbounded drift if an invalidation is lost (Redis flush,
  network partition, subscriber bug).
* **Invalidation granularity**: writes that affect state the cache can
  observe (``create``, ``mark_read``, ``mark_all_read``, ``dismiss``)
  invalidate the org's entire prefix. Orgs are small (<20 users by
  design), so SCAN+DEL is cheap in practice.
* **Redis unavailable** → fall through to DB. A broken cache must not
  break the endpoint.
"""

from __future__ import annotations

import logging

import redis

from datanika.config import settings

logger = logging.getLogger(__name__)

_KEY_PREFIX = "notif_unread:"
_TTL_SECONDS = 60


def _redis() -> redis.Redis:
    # Short timeouts so a down/misconfigured Redis degrades fast to the DB
    # fallback — the endpoint must stay responsive even with a bad cache.
    return redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=1,
        socket_timeout=1,
    )


def _user_key(org_id: int, user_id: int) -> str:
    return f"{_KEY_PREFIX}{org_id}:{user_id}"


def _org_pattern(org_id: int) -> str:
    return f"{_KEY_PREFIX}{org_id}:*"


def get(org_id: int, user_id: int) -> int | None:
    """Return the cached count for ``(org, user)`` or ``None`` on miss/error."""
    try:
        raw = _redis().get(_user_key(org_id, user_id))
    except Exception:
        logger.warning("notif_unread cache read failed", exc_info=True)
        return None
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def set(org_id: int, user_id: int, count: int) -> None:
    """Store the freshly-computed count. Silent on Redis error."""
    try:
        _redis().set(_user_key(org_id, user_id), count, ex=_TTL_SECONDS)
    except Exception:
        logger.warning("notif_unread cache write failed", exc_info=True)


def invalidate_user(org_id: int, user_id: int) -> None:
    """Drop a single user's cached count (e.g. after ``mark_all_read``)."""
    try:
        _redis().delete(_user_key(org_id, user_id))
    except Exception:
        logger.warning("notif_unread cache invalidate_user failed", exc_info=True)


def invalidate_org(org_id: int) -> None:
    """Drop every user's cached count in an org.

    Used when we don't know which users are affected (notification
    created with ``user_id=None`` → org-wide; a single ``mark_read``
    where we don't have the user context). Cheap because orgs are small.
    """
    try:
        client = _redis()
        # SCAN with a prefix match returns batches; COUNT is a hint. For
        # small orgs a single SCAN iteration covers everything.
        keys = list(client.scan_iter(match=_org_pattern(org_id), count=100))
        if keys:
            client.delete(*keys)
    except Exception:
        logger.warning("notif_unread cache invalidate_org failed", exc_info=True)
