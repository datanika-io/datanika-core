"""API middleware — authentication + rate limiting for /api/v1/ routes."""

import logging
from collections.abc import Callable, Coroutine
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from starlette.requests import Request
from starlette.responses import JSONResponse

from datanika.config import settings
from datanika.services.api_key_service import ApiKeyService
from datanika.services.rate_limit_service import RateLimitService

logger = logging.getLogger(__name__)

_engine = create_engine(settings.database_url_sync)
_api_key_svc = ApiKeyService()
_rate_limit_svc = RateLimitService()


def _get_session() -> Session:
    return Session(_engine)


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

    Usage::

        @api_endpoint(required_scope="pipeline:read")
        async def list_pipelines(request, api_key, session):
            ...
            return JSONResponse({"items": [...]})
    """

    def decorator(
        handler: Callable[..., Coroutine[Any, Any, JSONResponse]],
    ) -> Callable[..., Coroutine[Any, Any, JSONResponse]]:
        async def wrapper(request: Request) -> JSONResponse:
            # 1. Extract API key from Authorization header
            auth_header = request.headers.get("authorization", "")
            if not auth_header.startswith("Bearer "):
                return _error(401, "Missing or invalid Authorization header")

            raw_key = auth_header[7:]  # Strip "Bearer "
            if not raw_key:
                return _error(401, "Missing API key")

            # 2. Authenticate
            with _get_session() as session:
                api_key = _api_key_svc.authenticate_api_key(
                    session, raw_key, required_scope=required_scope
                )
                if api_key is None:
                    return _error(401, "Invalid or expired API key")

                # 3. Rate limiting
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

                # 4. Call the actual handler
                try:
                    response = await handler(request, api_key=api_key, session=session)
                    session.commit()
                except Exception:
                    logger.exception("API handler error")
                    session.rollback()
                    return _error(500, "Internal server error")

                # 5. Attach rate limit headers to successful responses
                for k, v in result.headers().items():
                    response.headers[k] = v

                return response

        wrapper.__name__ = handler.__name__
        wrapper.__qualname__ = handler.__qualname__
        return wrapper

    return decorator
