"""Application-level Prometheus metrics.

Exposes:
- HTTP request metrics (count, latency) via ASGI middleware
- Celery task metrics (count by state, duration) via signals
- Queue length gauge (polled from Redis)
- /metrics endpoint for Prometheus scraping
"""

import logging
import time
from collections.abc import Mapping

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

# Every request that matched no route shares this one label value.
UNMATCHED_PATH_LABEL = "<other>"

# Appended to a mount's prefix. A Mount matches, then hands an arbitrary tail to
# a sub-application that reports nothing about what it matched, so the tail is
# caller-controlled and cannot become part of a label value.
MOUNTED_TAIL_LABEL = "<mounted>"


class PrometheusMiddleware:
    """ASGI middleware that records request count and latency.

    The ``path`` label is derived from what the **router matched**, never from
    what the caller sent — see :func:`_normalize_path`. This middleware sits
    outside the router, so the routing facts it reads are only present by the
    time the ``finally`` block runs. That is already where the metering call is;
    do not move it earlier.
    """

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
        # Snapshot before routing. Starlette's ``Mount`` EXTENDS ``root_path``,
        # so a change across the call is how we learn that a sub-application
        # served the request. Comparing against "" instead would mislabel every
        # request whenever the server itself is mounted under a prefix.
        root_path_before = scope.get("root_path") or ""

        async def send_wrapper(message: dict) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            duration = time.perf_counter() - start
            normalized = _normalize_path(path, scope, root_path_before)
            http_requests_total.labels(method, normalized, str(status_code)).inc()
            http_request_duration_seconds.labels(method, normalized).observe(duration)


def _normalize_path(path: str, scope: Scope, root_path_before: str = "") -> str:
    """Reduce a request path to the route template the router matched.

    Every path parameter becomes a placeholder (IDs, UUIDs, ULIDs, slugs), a
    request that matched no route becomes ``<other>``, and a request served by a
    mounted sub-application becomes that mount's prefix plus ``<mounted>``.

    The ``path`` label is unauthenticated input from the public internet, so
    every value a caller can invent would mint a permanent Prometheus time
    series — 290 of the 298 values observed on production were vulnerability
    scanners, 31% of the whole TSDB (core#896). The value therefore has to come
    from what the router *matched* and never from the shape of what was *sent*.
    Shape-guessing is what the previous implementation did, and it is why
    ``/api/auth/sso/login/{org_slug}`` — a **matched** route taking a segment
    invented by an unauthenticated caller — stayed unbounded: no shape test
    distinguishes ``acme`` from ``login``.

    ⚠️ Starlette 0.52.1 does **not** put the matched route in the scope;
    ``scope["route"]`` is unset for matched and unmatched requests alike, so
    reading it buckets *everything* into ``<other>`` — which satisfies every
    cardinality criterion and blinds every SLI in ``docs/slo_instruments.yml``.
    What the router does set is ``endpoint`` (presence means matched) and
    ``path_params`` (the values to redact).
    """
    if "endpoint" not in scope:
        return UNMATCHED_PATH_LABEL

    path_params = scope.get("path_params") or {}
    root_path_after = scope.get("root_path") or ""

    if root_path_after != root_path_before:
        # A Mount matched: only its own prefix is bounded. Redacting the prefix
        # too costs nothing and covers a parameterised mount, which nothing
        # declares today.
        prefix, _ = _redact_path_params(root_path_after, path_params)
        return f"{prefix.rstrip('/')}/{MOUNTED_TAIL_LABEL}"

    redacted, unambiguous = _redact_path_params(path, path_params)
    # A parameter we could not locate is a value we did not redact; a parameter we
    # could locate in more than one place is a template we would be guessing at.
    # Bounded and imprecise beats precise and unbounded.
    return redacted if unambiguous else UNMATCHED_PATH_LABEL


def _redact_path_params(path: str, path_params: Mapping[str, object]) -> tuple[str, bool]:
    """Replace each matched parameter's value with ``:<name>``, left to right.

    Returns the redacted path and whether that placement was the **only** one
    possible — see the ambiguity note below.

    ``path_params`` originates in ``re.Match.groupdict()``, so it is ordered by
    the parameter's position in the route template — the same order the values
    appear in the path. Consuming it in that order is what stops
    ``/api/v1/orgs/{org_id}/members`` redacting the literal segment ``members``
    when a caller sets ``org_id=members``.

    ⚠️ **Order alone is not enough, because the router does not tell us the
    template** (``scope["route"]`` is unset — see :func:`_normalize_path`). A
    single parameter whose value equals a literal appearing *earlier in its own
    route* is genuinely ambiguous: ``/api/auth/sso/login/login`` is consistent
    with ``/api/auth/sso/login/{org_slug}`` and with ``/api/auth/sso/{org_slug}/
    login``, and left-to-right picked the second — minting a rotated template
    that is not a route, in a series a caller chooses (core#1020, four extra
    values per affected route).

    **No fixed search direction fixes it.** Rightmost-first would resolve that
    case and break the ``org_id=members`` case in the paragraph above. So the
    placement is *counted* rather than trusted: when more than one in-order
    assignment of all parameters fits the path, this reports ambiguity and the
    caller buckets the request. That costs precision for the handful of values
    that collide with a literal of their own route, and it never emits a
    template that does not exist.
    """
    pending = [(name, str(value)) for name, value in path_params.items() if str(value)]
    segments = path.split("/")
    out: list[str] = []
    remaining = list(pending)
    index = 0

    while index < len(segments):
        if remaining:
            name, value = remaining[0]
            value_segments = value.split("/")
            if _segments_match(segments[index : index + len(value_segments)], value_segments):
                out.append(f":{name}")
                index += len(value_segments)
                remaining.pop(0)
                continue
        out.append(segments[index])
        index += 1

    redacted = "/".join(out) or "/"
    if remaining:
        return redacted, False
    return redacted, _placement_count(segments, pending, 0, limit=2) == 1


def _placement_count(
    segments: list[str],
    pending: list[tuple[str, str]],
    start: int,
    limit: int,
) -> int:
    """How many in-order placements of ``pending`` fit ``segments[start:]``, capped at ``limit``.

    Only "one" and "more than one" matter, so the search stops at ``limit``.
    Counting *complete* assignments rather than per-value occurrences is what
    keeps a legitimate ``/orgs/5/members/5`` unambiguous: the two parameters
    share a value, and there is still exactly one way to place both in order.
    """
    if not pending:
        return 1
    _, value = pending[0]
    value_segments = value.split("/")
    total = 0
    for index in range(start, len(segments) - len(value_segments) + 1):
        if _segments_match(segments[index : index + len(value_segments)], value_segments):
            total += _placement_count(
                segments, pending[1:], index + len(value_segments), limit - total
            )
            if total >= limit:
                return total
    return total


def _segments_match(actual: list[str], expected: list[str]) -> bool:
    """Whether ``actual`` is the wire form of a converted parameter value.

    Exact equality covers the ``str`` convertor. The ``int`` convertor
    normalises, so ``/pipelines/007`` converts to ``7`` and ``str(7)`` is no
    longer the segment the caller sent; reconciling numerically keeps a real
    route in its own series instead of dropping it into ``<other>``. This is not
    shape-guessing — the router has already told us this segment *is* the
    parameter; we are only reconciling two spellings of its value.
    """
    if actual == expected:
        return True
    if len(actual) != 1 or len(expected) != 1:
        return False
    return actual[0].isdigit() and expected[0].isdigit() and int(actual[0]) == int(expected[0])


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
