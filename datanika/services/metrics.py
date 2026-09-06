"""Application-level Prometheus metrics.

Exposes:
- HTTP request metrics (count, latency) via ASGI middleware
- Celery task metrics (count by state, duration) via signals
- Queue length gauge (polled from Redis)
- /metrics endpoint for Prometheus scraping
"""

import logging
import re
import time
from collections.abc import Iterable, Mapping, Sequence

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

# --- Volume metering (V2 P1, per datanika-cloud/docs/specs/SPEC_GB_THROUGHPUT_METRICS.md §3.2) ---
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

# How far to follow the ASGI wrapper chain looking for the route table. Starlette's
# own stack is three deep; the bound stops a cycle or a self-referential wrapper
# from spinning, and reaching it simply means no index (the search still runs).
_MAX_WRAPPER_DEPTH = 12

# `{name}` -> `:name`, matching what `_redact_path_params` emits so a template and a
# redacted path are the same label value for the same route.
_PARAM_IN_TEMPLATE = re.compile(r"\{([^}:]+)(?::[^}]+)?\}")


def _find_route_table(app: object) -> Sequence[object]:
    """Follow ``.app`` until something exposes a route table.

    🚨 **The obvious one-liner — ``getattr(app, "routes", ())`` — is empty in
    production**, and full in every direct-construction test, which is the worst
    possible combination. ``datanika/datanika.py`` installs this middleware with
    ``app._api.add_middleware(PrometheusMiddleware)``, and Starlette's stack is
    ``ServerErrorMiddleware -> user middleware -> ExceptionMiddleware -> Router``. So
    the object handed to ``__init__`` is an ``ExceptionMiddleware``, which has no
    ``routes``; the ``Router`` that has them is one ``.app`` further in. Measured:

    ``AFTER stack build: ExceptionMiddleware has .routes = False len = 0``
    ``wrapped.app -> Router has .routes = True len = 2``

    Walking the chain covers both shapes with one rule, rather than making the test
    harness lie about how the middleware is installed.
    """
    node = app
    seen: set[int] = set()
    for _ in range(_MAX_WRAPPER_DEPTH):
        routes = getattr(node, "routes", None)
        if routes:
            return routes
        node = getattr(node, "app", None)
        if node is None or id(node) in seen:
            break
        seen.add(id(node))
    return ()


def _build_endpoint_index(routes: Iterable[object]) -> dict[int, str]:
    """``id(endpoint)`` -> route template, for endpoints serving exactly one route.

    Keyed on identity because an endpoint need not be hashable, and safe to key that
    way because the middleware holds the app, the app holds the router, and the router
    holds the routes — so nothing here is collected while the index is alive.

    **``Mount`` is deliberately not indexed and not descended into.** A ``Mount``
    extends ``root_path``, which :func:`_normalize_path` detects *before* it reaches
    the lookup and answers with ``<mounted>`` — because a mount serves a
    caller-controlled tail and reports nothing about what it matched. Indexing routes
    underneath one would build entries no request can ever reach, and an unreachable
    branch is worse than an absent one.

    An endpoint serving **more than one** template is dropped rather than resolved to
    whichever route was seen last: either template would be a guess, and one of them
    names a route the request did not match. Two routes on one handler is ordinary
    Starlette — measured on the real app 2026-09-04, ``app._api`` carries **84 routes:
    83 ``Route`` on 83 distinct endpoints, plus 1 ``Mount``**, so the index resolves 83
    and nothing falls back. That is a measurement, not a law, and this branch is what
    keeps it from becoming one. (core#1035 recorded 78/78; the table has grown since.)
    """
    templates: dict[int, set[str]] = {}
    for route in routes:
        if not isinstance(route, Route):
            continue
        endpoint = getattr(route, "endpoint", None)
        path_format = getattr(route, "path_format", "")
        if endpoint is None or not path_format:
            continue
        templates.setdefault(id(endpoint), set()).add(_PARAM_IN_TEMPLATE.sub(r":\1", path_format))
    return {key: next(iter(seen)) for key, seen in templates.items() if len(seen) == 1}


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
        # Built once, here, because Starlette builds its middleware stack **lazily on
        # the first request** — so by the time this runs, every route appended at
        # import time (including `/mcp` and the OAuth AS routes, which
        # `datanika/datanika.py` appends *after* `add_middleware`) is already in the
        # table. A route added after the first request would simply be absent, and an
        # absent entry falls back to the search rather than mislabelling anything.
        self._templates = _build_endpoint_index(_find_route_table(app))

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
            normalized = _normalize_path(path, scope, root_path_before, self._templates)
            http_requests_total.labels(method, normalized, str(status_code)).inc()
            http_request_duration_seconds.labels(method, normalized).observe(duration)


def _normalize_path(
    path: str,
    scope: Scope,
    root_path_before: str = "",
    templates: Mapping[int, str] | None = None,
) -> str:
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

    🆕 **core#1035: ``endpoint`` is enough to recover the template exactly**, without
    the search. :func:`_build_endpoint_index` inverts the route table once, so a
    matched request looks its template up instead of reconstructing it — and a lookup
    has no search direction to get wrong. ``templates`` is that index; when it does
    not contain the endpoint (two routes sharing one handler, or a route added after
    the middleware was constructed) the search below still runs, unchanged.
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

    # core#1035. The router already knows which template it matched; ask it, rather
    # than reconstructing the answer by searching the path for the parameter's value.
    # A lookup has no search direction to get wrong, so the whole ambiguity class
    # below simply does not arise for an indexed endpoint.
    if templates:
        template = templates.get(id(scope["endpoint"]))
        if template is not None:
            # `scope["path"]` is the full path including `root_path` (ASGI spec),
            # while a route's template is relative to its router. They differ only
            # when the server itself is mounted under a prefix — in which case the
            # search-based branch below would have kept that prefix, so keep it here.
            return f"{root_path_before.rstrip('/')}{template}"

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

    🆕 **Since core#1035 this is the FALLBACK, not the primary path.** An indexed
    endpoint never reaches here, so the precision cost above is now paid only where
    two routes share one handler — zero routes on today's table. Everything in this
    docstring stays true of the cases that do reach it, which is why it stays.
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
