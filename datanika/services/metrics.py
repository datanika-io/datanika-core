"""Application-level Prometheus metrics.

Exposes:
- HTTP request metrics (count, latency) via ASGI middleware
- Celery task metrics (count by state, duration) via signals
- Queue length gauge (polled from Redis)
- /metrics endpoint for Prometheus scraping
"""

import logging
import time

from prometheus_client import (
    REGISTRY,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
from starlette.types import ASGIApp, Receive, Scope, Send

_log = logging.getLogger(__name__)

# --- HTTP metrics ---

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "path", "status"],
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "path"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# --- Celery metrics ---

celery_tasks_total = Counter(
    "celery_tasks_total",
    "Total Celery tasks by name and state",
    ["task", "state"],
)

celery_task_duration_seconds = Histogram(
    "celery_task_duration_seconds",
    "Celery task execution duration in seconds",
    ["task"],
    buckets=(0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0),
)

celery_queue_length = Gauge(
    "celery_queue_length",
    "Number of tasks waiting in the Celery queue",
    ["queue"],
)

# --- Volume metering (V2 P1, per plans/infra/SPEC_GB_THROUGHPUT_METRICS.md §3.2) ---
# Operational histogram. Billing-neutral — deliberately NO `org_id` label
# (per-tenant semantics live in cloud-owned counters to keep core open-source).
# Fed from `run.*_completed` hook handlers once Engineering's V2 P1 lands.
# Until then this series stays empty; the 7-bucket layout is cardinality-safe
# (2 modes x 3 run_kinds x 7 buckets = 42 series).
bytes_processed_by_run = Histogram(
    "datanika_bytes_processed_by_run",
    "Bytes processed by a single run",
    ["mode", "run_kind"],
    buckets=(
        1_000_000,  # 1 MB
        10_000_000,  # 10 MB
        100_000_000,  # 100 MB
        1_000_000_000,  # 1 GB
        10_000_000_000,  # 10 GB
        100_000_000_000,  # 100 GB
        1_000_000_000_000,  # 1 TB
    ),
)


# --- ASGI Middleware ---

# Paths to skip metering (high-cardinality or internal)
_SKIP_PREFIXES = ("/_next/", "/static/", "/metrics", "/healthz", "/readyz")


class PrometheusMiddleware:
    """ASGI middleware that records request count and latency."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if any(path.startswith(p) for p in _SKIP_PREFIXES):
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "GET")
        status_code = 500
        start = time.perf_counter()

        async def send_wrapper(message: dict) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration = time.perf_counter() - start
            # Normalize dynamic path segments to reduce cardinality
            normalized = _normalize_path(path)
            http_requests_total.labels(method, normalized, str(status_code)).inc()
            http_request_duration_seconds.labels(method, normalized).observe(duration)


def _normalize_path(path: str) -> str:
    """Replace dynamic segments (IDs, UUIDs) with placeholders."""
    parts = path.strip("/").split("/")
    normalized = []
    for part in parts:
        if part.isdigit():
            normalized.append(":id")
        else:
            normalized.append(part)
    return "/" + "/".join(normalized) if normalized else "/"


# --- Celery signal handlers ---


def setup_celery_metrics(celery_app) -> None:
    """Register Celery signal handlers for task metrics."""
    from celery.signals import task_failure, task_postrun, task_prerun, task_retry

    _task_start_times: dict[str, float] = {}

    @task_prerun.connect
    def on_task_prerun(sender=None, task_id=None, **kwargs):
        _task_start_times[task_id] = time.perf_counter()

    @task_postrun.connect
    def on_task_postrun(sender=None, task_id=None, state=None, **kwargs):
        task_name = sender.name if sender else "unknown"
        celery_tasks_total.labels(task_name, state or "SUCCESS").inc()
        start = _task_start_times.pop(task_id, None)
        if start is not None:
            celery_task_duration_seconds.labels(task_name).observe(time.perf_counter() - start)

    @task_failure.connect
    def on_task_failure(sender=None, task_id=None, **kwargs):
        task_name = sender.name if sender else "unknown"
        celery_tasks_total.labels(task_name, "FAILURE").inc()
        _task_start_times.pop(task_id, None)

    @task_retry.connect
    def on_task_retry(sender=None, **kwargs):
        task_name = sender.name if sender else "unknown"
        celery_tasks_total.labels(task_name, "RETRY").inc()


# --- /metrics endpoint ---


async def metrics_endpoint(request: Request) -> Response:
    """Expose Prometheus metrics."""
    # Update queue length before responding
    try:
        import redis as redis_lib

        from datanika.config import settings

        r = redis_lib.from_url(settings.redis_url, socket_timeout=2)
        length = r.llen("celery")
        celery_queue_length.labels("celery").set(length)
    except Exception:
        # Serving /metrics must not depend on Redis being reachable -- a scrape
        # that 500s takes every other metric down with it. But the swallow has a
        # sharp edge worth naming (core#723): `celery_queue_length` is a Gauge,
        # so on failure it KEEPS ITS LAST VALUE rather than going absent, and a
        # frozen gauge reads exactly like a steady queue. This log line is the
        # only thing that distinguishes them.
        _log.warning("celery_queue_length not refreshed; the gauge is now stale", exc_info=True)

    body = generate_latest(REGISTRY)
    return Response(body, media_type="text/plain; version=0.0.4; charset=utf-8")


metrics_routes = [
    Route("/metrics", metrics_endpoint, methods=["GET"]),
]
