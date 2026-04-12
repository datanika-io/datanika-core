"""Idempotency-Key support for POST endpoints.

When an agent retries a ``POST`` request with the same
``Idempotency-Key`` header, we return the cached response instead of
creating a duplicate resource. Keys are scoped to ``org_id`` so
different orgs can reuse the same key safely.

Storage: Redis with a 24-hour TTL per key. The stored value is the
full JSON response body (serialized as a string) + the original HTTP
status code.

Usage in the API middleware:

    key = get_idempotency_key(request, api_key.org_id)
    if key:
        cached = get_cached_response(key)
        if cached:
            return cached  # return the original response, skip handler
    # ... call handler ...
    if key:
        cache_response(key, response)
"""

from __future__ import annotations

import json
import logging

import redis
from starlette.requests import Request
from starlette.responses import JSONResponse

from datanika.config import settings

logger = logging.getLogger(__name__)

_KEY_PREFIX = "idem:"
_TTL_SECONDS = 86400  # 24 hours


def _redis() -> redis.Redis:
    return redis.from_url(settings.redis_url, decode_responses=True)


def _cache_key(org_id: int, idempotency_key: str) -> str:
    return f"{_KEY_PREFIX}{org_id}:{idempotency_key}"


def get_idempotency_key(request: Request, org_id: int) -> str | None:
    """Extract the ``Idempotency-Key`` header from a request.

    Returns ``None`` if the header is absent (idempotency is opt-in).
    """
    raw = request.headers.get("idempotency-key")
    if not raw or not raw.strip():
        return None
    return _cache_key(org_id, raw.strip())


def get_cached_response(cache_key: str) -> JSONResponse | None:
    """Return a cached response for the key, or None if not found."""
    try:
        r = _redis()
        raw = r.get(cache_key)
    except Exception:
        logger.warning("Idempotency cache read failed", exc_info=True)
        return None
    if raw is None:
        return None
    try:
        data = json.loads(raw)
        return JSONResponse(data["body"], status_code=data["status"])
    except Exception:
        return None


def cache_response(cache_key: str, response: JSONResponse) -> None:
    """Store the response body + status code under the key with TTL."""
    try:
        body = json.loads(response.body.decode())
        data = json.dumps({"status": response.status_code, "body": body})
        r = _redis()
        r.set(cache_key, data, ex=_TTL_SECONDS)
    except Exception:
        logger.warning("Idempotency cache write failed", exc_info=True)
