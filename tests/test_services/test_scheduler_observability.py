"""core#1199 — a failing reconcile must be distinguishable from a healthy one.

The failure this exists to make visible
---------------------------------------
Observed directly while building core#648's probe, not reasoned about. A reconcile given a
bad enum value raised ``LookupError`` every 5 seconds for the life of the process, and
APScheduler did what it is supposed to do: caught it, logged it, and **kept the job
scheduled**. The resulting state is

* container ``Up``;
* already-known schedules still dispatching;
* nothing new ever learned again;
* and no signal anywhere that says so.

After core#648 ``reconcile()`` is the *only* path by which a schedule created in the UI
reaches APScheduler. So a persistently failing reconcile means the user creates a schedule,
the row is written, the UI shows it active, and it never runs.

⚠️ Why the metric may not live on the app's ``/metrics``
-------------------------------------------------------
This is core#704 verbatim, and it is the trap this file exists to keep shut.
``celery_tasks_total`` was incremented by signal handlers in the **worker** process while
``/metrics`` was a Starlette route in the **app** process, so those counters were collected
and discarded — which is why ``celery-task-failures`` had never once been able to fire.

The scheduler is a third process with no HTTP surface at all. A ``Counter`` incremented here
and served from ``app`` would be exactly the same defect wearing a new hat, and it would look
just as green. So the process that emits these numbers must also expose them, and something
must scrape that exposition — see ``tests/test_deploy/test_scheduler_metrics_scrape.py`` for
the other half of that joint.

⚠️ What this deliberately does NOT claim
----------------------------------------
Alert rules are Infra's, read off the Grafana API. This file asserts only that the
application *emits something that discriminates*. A rule that filters on these series still
has the ``noDataState: OK`` hazard: if the scheduler container dies the series vanishes and a
filtering expression reads healthy. That case is covered by ``container-down``, which carries
``datanika-scheduler`` since core#648 — the two are a pair, and neither covers the other's
failure.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from prometheus_client import REGISTRY

from datanika import scheduler_main
from datanika.services import scheduler_metrics


def _sample(name: str, labels: dict[str, str] | None = None) -> float:
    """Read a series from the default registry, or 0.0 when it is absent.

    Absent-reads-as-zero is fine *here* because every assertion below is a DELTA across a
    known action. It would not be fine in an alert, which is the whole of core#1199.
    """
    value = REGISTRY.get_sample_value(name, labels or {})
    return 0.0 if value is None else value


@pytest.fixture
def counts():
    """Snapshot the series this module touches, so assertions are deltas.

    prometheus_client metrics are process-global and these tests share a process with the
    rest of the suite. Absolute values would couple these tests to execution order.
    """
    return {
        "success": _sample("datanika_scheduler_reconcile_total", {"outcome": "success"}),
        "failure": _sample("datanika_scheduler_reconcile_total", {"outcome": "failure"}),
    }


class TestAHealthyReconcileIsRecorded:
    def test_it_advances_the_last_success_timestamp(self, counts):
        before = _sample("datanika_scheduler_reconcile_last_success_timestamp_seconds")
        with (
            patch.object(scheduler_main, "scheduler_integration") as sched,
            patch.object(scheduler_main, "get_sync_session"),
        ):
            sched.reconcile.return_value = {"synced": 3, "removed": 0}
            scheduler_main.reconcile_once()
        after = _sample("datanika_scheduler_reconcile_last_success_timestamp_seconds")
        assert after > before, (
            "a successful reconcile did not advance "
            "datanika_scheduler_reconcile_last_success_timestamp_seconds. That gauge is the "
            "only thing that distinguishes a scheduler still reading the table from one "
            "that has been raising for a week (core#1199)."
        )

    def test_it_counts_the_success(self, counts):
        with (
            patch.object(scheduler_main, "scheduler_integration") as sched,
            patch.object(scheduler_main, "get_sync_session"),
        ):
            sched.reconcile.return_value = {"synced": 0, "removed": 0}
            scheduler_main.reconcile_once()
        assert (
            _sample("datanika_scheduler_reconcile_total", {"outcome": "success"})
            == counts["success"] + 1
        )

    def test_it_clears_the_failure_streak(self):
        scheduler_metrics.scheduler_reconcile_consecutive_failures.set(7)
        with (
            patch.object(scheduler_main, "scheduler_integration") as sched,
            patch.object(scheduler_main, "get_sync_session"),
        ):
            sched.reconcile.return_value = {"synced": 0, "removed": 0}
            scheduler_main.reconcile_once()
        assert _sample("datanika_scheduler_reconcile_consecutive_failures") == 0, (
            "the consecutive-failure gauge did not reset after a success. A streak that "
            "only ever climbs makes a recovered scheduler indistinguishable from a broken one."
        )


class TestAFailingReconcileIsRecorded:
    def test_it_does_NOT_advance_the_last_success_timestamp(self, counts):
        """The load-bearing one. A gauge that moves on failure records nothing."""
        before = _sample("datanika_scheduler_reconcile_last_success_timestamp_seconds")
        with (
            patch.object(scheduler_main, "scheduler_integration") as sched,
            patch.object(scheduler_main, "get_sync_session"),
        ):
            sched.reconcile.side_effect = LookupError(
                "'upload' is not among the defined enum values"
            )
            with pytest.raises(LookupError):
                scheduler_main.reconcile_once()
        after = _sample("datanika_scheduler_reconcile_last_success_timestamp_seconds")
        assert after == before, (
            "a FAILING reconcile advanced the last-success gauge. Every alert built on this "
            "series would then read healthy for exactly the failure it was built to catch."
        )

    def test_it_counts_the_failure(self, counts):
        with (
            patch.object(scheduler_main, "scheduler_integration") as sched,
            patch.object(scheduler_main, "get_sync_session"),
        ):
            sched.reconcile.side_effect = LookupError("boom")
            with pytest.raises(LookupError):
                scheduler_main.reconcile_once()
        assert (
            _sample("datanika_scheduler_reconcile_total", {"outcome": "failure"})
            == counts["failure"] + 1
        )

    def test_it_advances_the_failure_streak(self):
        scheduler_metrics.scheduler_reconcile_consecutive_failures.set(0)
        with (
            patch.object(scheduler_main, "scheduler_integration") as sched,
            patch.object(scheduler_main, "get_sync_session"),
        ):
            sched.reconcile.side_effect = LookupError("boom")
            for _ in range(3):
                with pytest.raises(LookupError):
                    scheduler_main.reconcile_once()
        assert _sample("datanika_scheduler_reconcile_consecutive_failures") == 3

    def test_the_exception_still_propagates(self):
        """Recording must not become swallowing.

        APScheduler logs the traceback and keeps the job scheduled. Both are wanted: the
        log is the only place the *reason* appears, and continuing to run is what keeps one
        bad row from killing the dispatcher. A helper that caught-and-returned would delete
        the traceback and leave the metric as the sole witness.
        """
        with (
            patch.object(scheduler_main, "scheduler_integration") as sched,
            patch.object(scheduler_main, "get_sync_session"),
        ):
            sched.reconcile.side_effect = LookupError("boom")
            with pytest.raises(LookupError):
                scheduler_main.reconcile_once()

    def test_the_session_is_closed_even_when_reconcile_raises(self):
        with (
            patch.object(scheduler_main, "scheduler_integration") as sched,
            patch.object(scheduler_main, "get_sync_session") as session_factory,
        ):
            sched.reconcile.side_effect = LookupError("boom")
            with pytest.raises(LookupError):
                scheduler_main.reconcile_once()
        session_factory.return_value.close.assert_called_once()


class TestTheseAssertionsCanActuallyFail:
    """Negative controls. Each drives the real helpers and shows the series MOVES.

    Without these, every assertion above is also satisfied by a `_sample` that always
    returns 0.0 — which is precisely how a check with one possible answer passes forever
    (ENGINEERING_RULES §20, §43).
    """

    def test_the_success_helper_moves_the_series(self):
        before = _sample("datanika_scheduler_reconcile_total", {"outcome": "success"})
        scheduler_metrics.record_reconcile_success()
        assert _sample("datanika_scheduler_reconcile_total", {"outcome": "success"}) == before + 1

    def test_the_failure_helper_moves_the_series(self):
        before = _sample("datanika_scheduler_reconcile_total", {"outcome": "failure"})
        scheduler_metrics.record_reconcile_failure()
        assert _sample("datanika_scheduler_reconcile_total", {"outcome": "failure"}) == before + 1

    def test_the_reader_returns_none_as_zero_for_a_name_that_does_not_exist(self):
        assert _sample("datanika_scheduler_no_such_series_total") == 0.0

    def test_the_reader_discriminates_between_labels(self):
        """`_sample` must not be reading the same series under two different labels."""
        scheduler_metrics.scheduler_reconcile_total.labels(outcome="success").inc(0)
        scheduler_metrics.scheduler_reconcile_total.labels(outcome="failure").inc(0)
        before_s = _sample("datanika_scheduler_reconcile_total", {"outcome": "success"})
        before_f = _sample("datanika_scheduler_reconcile_total", {"outcome": "failure"})
        scheduler_metrics.record_reconcile_failure()
        assert _sample("datanika_scheduler_reconcile_total", {"outcome": "success"}) == before_s
        assert _sample("datanika_scheduler_reconcile_total", {"outcome": "failure"}) == before_f + 1


class TestTheDispatchCounter:
    """core#1199 item 2 — the one that would have caught core#648 from the outside.

    Five dispatches per firing is visible in a counter and was visible in nothing else: the
    duplicated runs looked like ordinary runs, and `runs` carries no `schedule_id` to group
    them by (verified on the models — `run.py:23-26` and `schedule.py:12-15` both associate
    through `target_type`/`target_id`).
    """

    def test_a_dispatch_is_counted_by_target_type(self):
        before = _sample("datanika_scheduler_dispatch_total", {"target_type": "pipeline"})
        scheduler_metrics.record_dispatch("pipeline")
        assert (
            _sample("datanika_scheduler_dispatch_total", {"target_type": "pipeline"}) == before + 1
        )

    def test_target_types_do_not_share_a_series(self):
        before_u = _sample("datanika_scheduler_dispatch_total", {"target_type": "upload"})
        scheduler_metrics.record_dispatch("transformation")
        assert _sample("datanika_scheduler_dispatch_total", {"target_type": "upload"}) == before_u


class TestTheProcessExposesItsOwnMetrics:
    """core#704's lesson, applied before it can happen a second time.

    A counter incremented in a process that serves no HTTP is collected and discarded. The
    scheduler container publishes no port and mounts no Starlette app, so without this the
    numbers above would be perfectly correct and permanently unreadable.
    """

    def _run_main(self, **patches):
        scheduler_main._stop.set()
        try:
            with (
                patch.object(scheduler_main, "scheduler_integration"),
                patch.object(scheduler_main, "get_sync_session"),
                patch.object(scheduler_main, "start_metrics_server") as start,
                patch.object(scheduler_main, "bootstrap") as boot,
            ):
                scheduler_main.main()
            return start, boot
        finally:
            scheduler_main._stop.clear()

    def test_main_starts_an_exposition_server(self):
        start, _ = self._run_main()
        start.assert_called_once()

    def test_it_binds_the_configured_port(self):
        from datanika.config import settings

        start, _ = self._run_main()
        assert start.call_args.args[0] == settings.scheduler_metrics_port, (
            "the exposition server must bind the port named in settings — that value is "
            "also what monitoring/prometheus.yml scrapes, and the two are asserted equal "
            "in tests/test_deploy/test_scheduler_metrics_scrape.py"
        )

    def test_the_server_starts_BEFORE_the_first_reconcile(self):
        """Order is load-bearing: the boot reconcile can be the one that fails.

        `bootstrap()` runs a reconcile immediately. If exposition started after it, a
        process whose very first reconcile raised would expose nothing at all — the
        failure most worth seeing would be the one guaranteed to be invisible.
        """
        calls = []
        scheduler_main._stop.set()
        try:
            with (
                patch.object(scheduler_main, "scheduler_integration"),
                patch.object(scheduler_main, "get_sync_session"),
                patch.object(
                    scheduler_main,
                    "start_metrics_server",
                    side_effect=lambda *a, **k: calls.append("expose"),
                ),
                patch.object(
                    scheduler_main, "bootstrap", side_effect=lambda *a, **k: calls.append("boot")
                ),
            ):
                scheduler_main.main()
        finally:
            scheduler_main._stop.clear()
        assert calls == ["expose", "boot"], f"expected exposition before bootstrap, got {calls}"
