"""Health check endpoints — /healthz (liveness) and /readyz (readiness)."""

import logging

from sqlalchemy import create_engine, text
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from datanika.config import settings

logger = logging.getLogger(__name__)

_engine = create_engine(settings.database_url_sync, pool_pre_ping=True)


async def healthz(request: Request) -> JSONResponse:
    """Liveness probe — always returns 200 if the process is alive."""
    return JSONResponse({"status": "ok"})


async def readyz(request: Request) -> JSONResponse:
    """Readiness probe — checks database and Redis connectivity."""
    checks = {}

    # Database
    try:
        with _engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as exc:
        logger.warning("Readiness: database check failed: %s", exc)
        checks["database"] = "error"

    # Redis
    try:
        import redis

        r = redis.from_url(settings.redis_url, socket_timeout=2)
        r.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        logger.warning("Readiness: redis check failed: %s", exc)
        checks["redis"] = "error"

    all_ok = all(v == "ok" for v in checks.values())
    status_code = 200 if all_ok else 503
    return JSONResponse({"status": "ok" if all_ok else "degraded", "checks": checks}, status_code)


health_routes = [
    Route("/healthz", healthz, methods=["GET"]),
    Route("/readyz", readyz, methods=["GET"]),
]
