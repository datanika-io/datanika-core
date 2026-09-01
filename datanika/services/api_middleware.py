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
import contextlib
import logging
from collections.abc import Callable, Coroutine
from typing import Any

from sqlalchemy import event
from sqlalchemy.orm import Session as SASession
from starlette.requests import Request
from starlette.responses import JSONResponse

from datanika.config import settings
from datanika.db import get_sync_session
from datanika.services.api_key_service import ApiKeyService
from datanika.services.client_ip import resolve_client_ip
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


# ---------------------------------------------------------------------------
# Load shedding, in front of everything that costs a database session (#774)
# ---------------------------------------------------------------------------
#
# Ordering was the defect. Authentication ran first, so a request with an
# invalid key was answered 401 having already spent a session checkout, a
# sha256 and an indexed SELECT — and was never counted, so there was nothing
# for it to exceed (40 of 40 measured unthrottled on production). A valid key
# over its limit was worse: it paid *two* sessions and 9 Redis commands to be
# told no, which is exactly what a 200 costs. A limiter that does not shed load
# amplifies it.
#
# Everything below is advisory and suppresses Redis errors. That is deliberate:
# it is an optimisation in front of `check_rate_limit`, which is authoritative
# and still fails **closed**. When Redis is unavailable this layer disappears
# and behaviour is what it was before this change — it must never be the reason
# a request is refused or 500s.


def _client_bucket(request: Request) -> str:
    """The caller's address bucket, or "" when we cannot name them.

    "" means *skip the address bucket entirely*. It must not degrade into a
    placeholder bucket like ``apiauth:ip:``: in production the socket peer is
    always 127.0.0.1 (Cloudflare → Apache → :8000), so one shared bucket is the
    whole internet, and the tenth failure from anyone would lock out everyone.
    See services/client_ip.py, which exists to answer exactly this and to refuse
    when it cannot.
    """
    try:
        headers = dict(request.headers)
        if request.client is not None:
            headers["asgi-scope-client"] = request.client.host
        address = resolve_client_ip(headers)
    except Exception:
        return ""
    return RateLimitService.client_bucket(address) if address else ""


def _shed_before_auth(raw_key: str, request: Request) -> JSONResponse | None:
    """Refuse, from Redis alone, anything we already know we will refuse.

    Returns a 429 response when the request must not reach the database, or
    ``None`` to proceed. The response carries only ``Retry-After``: pre-auth we
    have not identified the caller, so we must not describe an entitlement.
    """
    try:
        result = _rate_limit_svc.preauth_check(
            credential=RateLimitService.credential_bucket(raw_key),
            client=_client_bucket(request),
            window_seconds=settings.api_auth_failure_window_seconds,
            credential_failure_limit=settings.api_auth_failure_limit,
            client_failure_limit=settings.api_auth_failure_ip_limit,
        )
        if result.allowed:
            return None
        retry_after = max(int(result.retry_after), 1)
    except Exception:
        return None
    return _error(
        429,
        f"Too many requests. Retry after {retry_after} seconds.",
        headers={"Retry-After": str(retry_after)},
    )


def _record_auth_failure(raw_key: str, request: Request) -> None:
    """Count a rejected credential so the next one can be refused for free."""
    with contextlib.suppress(Exception):
        _rate_limit_svc.record_auth_failure(
            credential=RateLimitService.credential_bucket(raw_key),
            client=_client_bucket(request),
            window_seconds=settings.api_auth_failure_window_seconds,
        )


def _mark_refused(raw_key: str, retry_after: int) -> None:
    """Arm the pre-auth refusal for a key that just exceeded its own limit."""
    with contextlib.suppress(Exception):
        _rate_limit_svc.mark_refused(
            credential=RateLimitService.credential_bucket(raw_key),
            retry_after=retry_after,
        )


# ---------------------------------------------------------------------------
# What a non-2xx response does to the transaction (#790)
# ---------------------------------------------------------------------------
#
# This decorator used to commit whenever the handler **returned** and roll back
# only when it **raised** — and `_error(400, ...)` is a return. So a rejected
# `PUT` kept whatever the service had already assigned above the validator that
# rejected it: `update_transformation` and `update_pipeline` assign as they go,
# so a 400 durably renamed the row it refused to update. QA's AST audit put the
# exposed surface at 26 non-2xx returns across 20 of the 54 handlers.
#
# ⚠️ The guarantee a middleware can deliver is *"nothing since the handler's own
# last commit"*, not *"nothing at all"*. `trigger_upload`, `trigger_pipeline`
# and `trigger_transformation` commit mid-request so the Celery task can see the
# run row before `.delay()`; for those three the two sentences differ, and the
# run row is meant to survive.


def _is_rejection(response: JSONResponse) -> bool:
    """True when the API refused the request, so its writes must not persist.

    One predicate for both handler paths on purpose — a transaction rule that
    applies to only one of the two is not a rule. No handler answers 3xx.
    """
    return response.status_code >= 400


class _HandlerCommitWatch:
    """Records whether the handler committed durable state of its own.

    Needed for the idempotency cache rather than for the rollback. The three
    `trigger_*` handlers answer **408** (still running at the timeout) or
    **422** (terminal, not success), which `_trigger_and_maybe_wait` documents
    as *results about the run*, not transport rejections — and the run row they
    describe is already committed. Dropping those from the cache because they
    are non-2xx would start a **second warehouse run** on the caller's retry,
    which is the duplication an `Idempotency-Key` exists to prevent.

    Watching `after_commit` answers "did this handler commit?" mechanically,
    instead of hardcoding a status list that goes stale as handlers change. A
    target carrying no SQLAlchemy events (a test double) reports ``False``; in
    production `session` is always a real ``Session``.
    """

    def __init__(self, session) -> None:
        self._session = session
        self._armed = isinstance(session, SASession)
        self.committed = False

    def _record(self, _session) -> None:
        self.committed = True

    def __enter__(self) -> _HandlerCommitWatch:
        if self._armed:
            event.listen(self._session, "after_commit", self._record)
        return self

    def __exit__(self, *_exc: object) -> bool:
        if self._armed:
            event.remove(self._session, "after_commit", self._record)
        return False


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
    # #774: before the session, not after. Nothing below this line runs for a
    # caller we have already decided to refuse.
    shed = _shed_before_auth(raw_key, request)
    if shed is not None:
        return shed

    with _get_session() as session:
        api_key = _api_key_svc.authenticate_api_key(session, raw_key, required_scope=required_scope)
        if api_key is None:
            _record_auth_failure(raw_key, request)
            return _error(401, "Invalid or expired API key")
        # Release the auth-read txn before Redis rate-limit/idempotency
        # work and before the handler runs (#292). Keeps `idle in
        # transaction` out of pg_stat_activity across the Redis/Python
        # gap; handler re-opens a fresh txn on its first query.
        session.commit()

        limit_rpm = _rate_limit_svc.get_limit_for_org(api_key.org_id)
        result = _rate_limit_svc.check_rate_limit(
            bucket=f"{api_key.id}",
            org_id=api_key.org_id,
            limit_rpm=limit_rpm,
            burst_per_sec=settings.api_rate_limit_burst,
        )

        if not result.allowed:
            _mark_refused(raw_key, result.retry_after)
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

        watch = _HandlerCommitWatch(session)
        try:
            with watch:
                response = await handler(request, api_key=api_key, session=session)
            if _is_rejection(response):
                session.rollback()
            else:
                session.commit()
        except Exception:
            logger.exception("API handler error")
            session.rollback()
            return _error(500, "Internal server error")

        if idem_key and (not _is_rejection(response) or watch.committed):
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
    # #774: before the session, not after. Kept identical to the async path —
    # a shedding rule that applies to only one of the two is not a rule.
    shed = _shed_before_auth(raw_key, request)
    if shed is not None:
        return shed

    with _get_session() as session:
        api_key = _api_key_svc.authenticate_api_key(session, raw_key, required_scope=required_scope)
        if api_key is None:
            _record_auth_failure(raw_key, request)
            return _error(401, "Invalid or expired API key")
        # Release the auth-read txn before Redis rate-limit/idempotency
        # work and before the handler runs (#292).
        session.commit()

        limit_rpm = _rate_limit_svc.get_limit_for_org(api_key.org_id)
        result = _rate_limit_svc.check_rate_limit(
            bucket=f"{api_key.id}",
            org_id=api_key.org_id,
            limit_rpm=limit_rpm,
            burst_per_sec=settings.api_rate_limit_burst,
        )

        if not result.allowed:
            _mark_refused(raw_key, result.retry_after)
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

        watch = _HandlerCommitWatch(session)
        try:
            with watch:
                response = handler(request, api_key=api_key, session=session)
            if _is_rejection(response):
                session.rollback()
            else:
                session.commit()
        except Exception:
            logger.exception("API handler error")
            session.rollback()
            return _error(500, "Internal server error")

        if idem_key and (not _is_rejection(response) or watch.committed):
            cache_response(idem_key, response)

        for k, v in result.headers().items():
            response.headers[k] = v

        return response
