"""Prometheus series emitted by the dedicated scheduler process (core#1199).

Why these do not live in ``datanika/services/metrics.py``
---------------------------------------------------------
That module is the **app** tier's surface: its counters are incremented by ASGI middleware
and Celery signal handlers, and it owns the ``/metrics`` Starlette route that ``app`` and
``app_b`` serve. The scheduler is a third process (core#648) with no HTTP surface of its own
and no Starlette app mounted.

That distinction is the whole of core#704. ``celery_tasks_total`` is incremented in the
**worker** while ``/metrics`` is served by the **app**, so those samples were collected into
a registry nothing ever read — and ``celery-task-failures`` sat green for the life of the
project because it was filtering a metric with zero series. Putting the scheduler's numbers
in ``metrics.py`` would reproduce that exactly, and would look just as correct.

So this module owns both halves: the series, and :func:`start_metrics_server`, which is how
the emitting process exposes them. ``monitoring/prometheus.yml`` scrapes it, and
``tests/test_deploy/test_scheduler_metrics_scrape.py`` asserts that those two agree — the
port is named in three files that do not import one another.

What each series is for
-----------------------
``datanika_scheduler_reconcile_last_success_timestamp_seconds``
    The one that answers core#1199's question. ``time() - <this>`` is large exactly when the
    reconcile has been raising, which is the state where the container reads ``Up``, already
    known schedules keep firing, and nothing new is ever learned again.

``datanika_scheduler_reconcile_consecutive_failures``
    Distinguishes "one bad row, recovered" from "raising since Tuesday". Reset on success —
    a streak that only climbs cannot tell a recovered scheduler from a broken one.

``datanika_scheduler_reconcile_total{outcome}``
    Rate of attempts, and the denominator for a failure ratio.

``datanika_scheduler_dispatch_total{target_type}``
    core#1199 item 2, and the series that would have made core#648 visible **from the
    outside**: five dispatches per firing is obvious in a counter and was obvious in nothing
    else. ``runs`` carries no ``schedule_id`` to group duplicates by — both ``Run`` and
    ``Schedule`` associate through ``target_type``/``target_id`` — so there was no query that
    would have found it after the fact either.

⚠️ A note for whoever writes the alert
--------------------------------------
Every series here vanishes when the scheduler container dies, and a filtering expression over
a vanished series yields no series, which under Grafana's ``noDataState: OK`` reads healthy.
That case belongs to ``container-down``, whose selector carries ``datanika-scheduler`` since
core#648. These two are a PAIR: neither covers the other's failure, and an alert built on
this module alone is blind to the process being gone.
"""

from __future__ import annotations

import logging
import time

from prometheus_client import Counter, Gauge, start_http_server

_log = logging.getLogger(__name__)

scheduler_reconcile_total = Counter(
    "datanika_scheduler_reconcile_total",
    "Scheduler reconcile attempts by outcome",
    ["outcome"],
)

scheduler_reconcile_last_success_timestamp_seconds = Gauge(
    "datanika_scheduler_reconcile_last_success_timestamp_seconds",
    "Unix timestamp of the last reconcile that completed without raising",
)

scheduler_reconcile_consecutive_failures = Gauge(
    "datanika_scheduler_reconcile_consecutive_failures",
    "Reconcile failures since the last success. Reset to 0 on success.",
)

scheduler_dispatch_total = Counter(
    "datanika_scheduler_dispatch_total",
    "Scheduled runs dispatched by the scheduler process, by target type",
    ["target_type"],
)


def record_reconcile_success() -> None:
    """Mark a reconcile as having completed. Advances the freshness gauge."""
    scheduler_reconcile_total.labels(outcome="success").inc()
    scheduler_reconcile_last_success_timestamp_seconds.set(time.time())
    scheduler_reconcile_consecutive_failures.set(0)


def record_reconcile_failure() -> None:
    """Mark a reconcile as having raised.

    Deliberately does NOT touch the last-success gauge. A freshness gauge that moved on
    failure would read healthy for precisely the failure it exists to catch.
    """
    scheduler_reconcile_total.labels(outcome="failure").inc()
    scheduler_reconcile_consecutive_failures.inc()


def record_dispatch(target_type: str) -> None:
    """Count one scheduled run handed to Celery."""
    scheduler_dispatch_total.labels(target_type=str(target_type)).inc()


def start_metrics_server(port: int) -> None:
    """Expose this process's registry over HTTP.

    Called from :func:`datanika.scheduler_main.main` **before** ``bootstrap()``, because
    ``bootstrap()`` runs a reconcile immediately and the boot reconcile is a perfectly
    ordinary one to fail. Exposing afterwards would guarantee that the single most
    interesting failure is the one with nothing listening.

    Binds ``0.0.0.0`` inside the container so Prometheus can reach it over the compose
    network; ``docker-compose.yml`` publishes it only on ``127.0.0.1``.
    """
    start_http_server(port)
    _log.info("Scheduler metrics exposed", extra={"port": port})
