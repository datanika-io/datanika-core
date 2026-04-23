"""API middleware — authentication + rate limiting for /api/v1/ routes.

E12 — sync/async handler detection. Handlers declared as ``def`` (not
``async def``) are executed inside ``asyncio.to_thread`` so their sync
DB work doesn't block the event loop. Async handlers keep the original
path for backward compatibility.

Before E12, every handler held the event loop for the duration of its
sync SQLAlchemy session.execute()/commit(), serializing all requests on
a Granian async worker. k6 Run 4 (2026-04-18) measured 60 req/s at 100
VUs with 4 workers — theoretical max was 97. Wrapping sync handlers in
``to_thread`` unlocks threadpool-level concurrency (~40 threads/worker).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse

from datanika.config import settings
from datanika.db import get_sync_session
from datanika.services.api_key_service import ApiKeyService
from datanika.services.rate_limit_service import RateLimitService

logger = logging.getLogger(__name__)

_api_key_svc = ApiKeyService()
_rate_limit_svc = RateLimitService()


def _get_session():
    return get_sync_session()


def _error(status: int, message: str, headers: dict[str, str] | None = None) -> JSONResponse:
    resp = JSONResponse({"error": message}, status_code=status)
    if headers:
        for k, v in headers.items():
            resp.headers[k] = v
    return resp


def api_endpoint(
    required_scope: str | None = None,
) -> Callable:
    """Decorator for API route handlers that enforces auth + rate limiting.

    Handlers can be declared as ``async def`` (original behavior — awaited
    on the event loop) or plain ``def`` (E12 — executed in ``asyncio.to_thread``
    so sync SQLAlchemy operations don't block the event loop).

    Usage::

        # Async handler (unchanged)
        @api_endpoint(required_scope="pipeline:read")
        async def list_pipelines(request, api_key, session):
            ...

        # Sync handler (E12 — threaded, recommended for DB-bound endpoints)
        @api_endpoint(required_scope="pipeline:read")
        def list_pipelines(request, api_key, session):
            ...
    """

    def decorator(
        handler: Callable[..., Coroutine[Any, Any, JSONResponse] | JSONResponse],
    ) -> Callable[..., Coroutine[Any, Any, JSONResponse]]:
        is_async_handler = asyncio.iscoroutinefunction(handler)

        async def wrapper(request: Request) -> JSONResponse:
            # 1. Extract API key (async-safe, no DB)
            auth_header = request.headers.get("authorization", "")
            if not auth_header.startswith("Bearer "):
                return _error(401, "Missing or invalid Authorization header")

            raw_key = auth_header[7:]
            if not raw_key:
                return _error(401, "Missing API key")

            # 2. Pre-consume body for POST so the handler can read it in
            # the sync thread without touching the outer event loop's receive.
            if request.method in ("POST", "PUT", "PATCH"):
                await request.body()

            # 3. Auth + rate-limit + handler + commit. E12: run in threadpool
            # for sync handlers to unblock the event loop. Async handlers
            # keep the original inline path.
            if is_async_handler:
                return await _run_async_handler(handler, request, raw_key, required_scope)
            return await asyncio.to_thread(
                _run_sync_handler, handler, request, raw_key, required_scope
            )

        wrapper.__name__ = handler.__name__
        wrapper.__qualname__ = handler.__qualname__
        return wrapper

    return decorator


async def _run_async_handler(
    handler: Callable, request: Request, raw_key: str, required_scope: str | None
) -> JSONResponse:
    """Original async-handler path — kept for backward compat."""
    with _get_session() as session:
        api_key = _api_key_svc.authenticate_api_key(session, raw_key, required_scope=required_scope)
        if api_key is None:
            return _error(401, "Invalid or expired API key")
        # Release the auth-read txn before Redis rate-limit/idempotency
        # work and before the handler runs (#292). Keeps `idle in
        # transaction` out of pg_stat_activity across the Redis/Python
        # gap; handler re-opens a fresh txn on its first query.
        session.commit()

        limit_rpm = _rate_limit_svc.get_limit_for_org(api_key.org_id)
        result = _rate_limit_svc.check_rate_limit(
            api_key_id=api_key.id,
            org_id=api_key.org_id,
            limit_rpm=limit_rpm,
            burst_per_sec=settings.api_rate_limit_burst,
        )

        if not result.allowed:
            return _error(
                429,
                f"Rate limit exceeded ({limit_rpm} requests/minute). "
                f"Retry after {result.retry_after} seconds.",
                headers=result.headers(),
            )

        from datanika.services.idempotency import (
            cache_response,
            get_cached_response,
            get_idempotency_key,
        )

        idem_key = None
        if request.method == "POST":
            idem_key = get_idempotency_key(request, api_key.org_id)
            if idem_key:
                cached = get_cached_response(idem_key)
                if cached is not None:
                    for k, v in result.headers().items():
                        cached.headers[k] = v
                    return cached

        try:
            response = await handler(request, api_key=api_key, session=session)
            session.commit()
        except Exception:
            logger.exception("API handler error")
            session.rollback()
            return _error(500, "Internal server error")

        if idem_key:
            cache_response(idem_key, response)

        for k, v in result.headers().items():
            response.headers[k] = v

        return response


def _run_sync_handler(
    handler: Callable, request: Request, raw_key: str, required_scope: str | None
) -> JSONResponse:
    """E12 — synchronous path, invoked via asyncio.to_thread from wrapper.

    Same logic as the async path but all calls stay sync. SQLAlchemy sessions
    are not thread-safe; we create one per invocation and close on exit.
    """
    with _get_session() as session:
        api_key = _api_key_svc.authenticate_api_key(session, raw_key, required_scope=required_scope)
        if api_key is None:
            return _error(401, "Invalid or expired API key")
        # Release the auth-read txn before Redis rate-limit/idempotency
        # work and before the handler runs (#292).
        session.commit()

        limit_rpm = _rate_limit_svc.get_limit_for_org(api_key.org_id)
        result = _rate_limit_svc.check_rate_limit(
            api_key_id=api_key.id,
            org_id=api_key.org_id,
            limit_rpm=limit_rpm,
            burst_per_sec=settings.api_rate_limit_burst,
        )

        if not result.allowed:
            return _error(
                429,
                f"Rate limit exceeded ({limit_rpm} requests/minute). "
                f"Retry after {result.retry_after} seconds.",
                headers=result.headers(),
            )

        from datanika.services.idempotency import (
            cache_response,
            get_cached_response,
            get_idempotency_key,
        )

        idem_key = None
        if request.method == "POST":
            idem_key = get_idempotency_key(request, api_key.org_id)
            if idem_key:
                cached = get_cached_response(idem_key)
                if cached is not None:
                    for k, v in result.headers().items():
                        cached.headers[k] = v
                    return cached

        try:
            response = handler(request, api_key=api_key, session=session)
            session.commit()
        except Exception:
            logger.exception("API handler error")
            session.rollback()
            return _error(500, "Internal server error")

        if idem_key:
            cache_response(idem_key, response)

        for k, v in result.headers().items():
            response.headers[k] = v

        return response
