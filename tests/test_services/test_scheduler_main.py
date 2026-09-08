"""core#648 — the dedicated scheduler process entrypoint.

The property that is invisible and expensive to get wrong
---------------------------------------------------------
Once the scheduler runs in its own process, ``add_job`` from the web tier never reaches it.
That is not a design opinion, it is how APScheduler 3.11.2 is built, measured against real
Postgres:

* ``BaseScheduler.add_job`` calls ``wakeup()`` on **its own instance only**;
* ``BlockingScheduler._main_loop`` waits ``_process_jobs()``'s return value, which is
  ``TIMEOUT_MAX`` — **4294967 s, 49.7 days** — when the store holds no due job.

Measured: a job written by a second scheduler instance, due in 2 s, produced **0** dispatches
at t+3 s, t+6 s and t+10 s. A single explicit ``wakeup()`` then dispatched it immediately,
which is the control proving it was only ever asleep rather than broken.

So the scheduler process must wake itself on a **bounded** interval. The reconcile heartbeat
is that wakeup and the DB poll at once: it re-reads ``schedules`` (picking up whatever the
web tier wrote) and, by existing in the job set at all, caps ``_process_jobs()``'s wait.

``test_the_reconcile_interval_is_bounded`` is therefore not a style assertion. Without a
finite, small interval this process starts, logs "Scheduler started", reports healthy, and
never runs a schedule created after boot. Every signal we have reads green.
"""

from __future__ import annotations

from unittest.mock import patch

from datanika import scheduler_main


class TestTheEntrypointArmsExactlyOneScheduler:
    def test_it_starts_the_scheduler_unpaused(self):
        with (
            patch.object(scheduler_main, "scheduler_integration") as sched,
            patch.object(scheduler_main, "get_sync_session"),
        ):
            scheduler_main.bootstrap()
        sched.start.assert_called_once()
        # A paused scheduler writes the jobstore but never dispatches — measured. This
        # process is the one that must dispatch, so it may not be started paused.
        assert sched.start.call_args.kwargs.get("paused", False) is False, (
            "the dedicated scheduler process started PAUSED: it would keep the jobstore "
            "in step and dispatch nothing at all (core#648)"
        )

    def test_it_reconciles_once_at_boot(self):
        with (
            patch.object(scheduler_main, "scheduler_integration") as sched,
            patch.object(scheduler_main, "get_sync_session") as session_factory,
        ):
            scheduler_main.bootstrap()
        sched.reconcile.assert_called_once_with(session_factory.return_value)

    def test_it_closes_the_boot_session(self):
        with (
            patch.object(scheduler_main, "scheduler_integration"),
            patch.object(scheduler_main, "get_sync_session") as session_factory,
        ):
            scheduler_main.bootstrap()
        session_factory.return_value.close.assert_called_once()


class TestItWakesItselfUp:
    def test_it_registers_a_repeating_reconcile(self):
        with (
            patch.object(scheduler_main, "scheduler_integration") as sched,
            patch.object(scheduler_main, "get_sync_session"),
        ):
            scheduler_main.bootstrap()
        sched.schedule_internal.assert_called_once()
        kwargs = sched.schedule_internal.call_args.kwargs
        assert kwargs["job_id"] == scheduler_main.RECONCILE_JOB_ID

    def test_the_reconcile_interval_is_bounded(self):
        """See the module docstring. An unbounded wait is a 49.7-day sleep."""
        with (
            patch.object(scheduler_main, "scheduler_integration") as sched,
            patch.object(scheduler_main, "get_sync_session"),
        ):
            scheduler_main.bootstrap()
        seconds = sched.schedule_internal.call_args.kwargs["seconds"]
        assert 0 < seconds <= 300, (
            f"reconcile interval is {seconds}s. It bounds two things: how long a newly "
            "created schedule stays invisible to this process, and how long APScheduler "
            "sleeps between wakeups (TIMEOUT_MAX = 49.7 days with an empty store). "
            "It must be finite and small."
        )

    def test_the_repeating_reconcile_actually_reconciles(self):
        """The registered callable must be the reconcile, not a bare no-op wakeup.

        A no-op would still bound the sleep and still let this suite pass every other
        assertion here — while schedules created in the UI never loaded.
        """
        with (
            patch.object(scheduler_main, "scheduler_integration") as sched,
            patch.object(scheduler_main, "get_sync_session") as session_factory,
        ):
            scheduler_main.bootstrap()
            sched.reconcile.reset_mock()
            scheduler_main.scheduler_integration.schedule_internal.call_args.kwargs["func"]()
        sched.reconcile.assert_called_once_with(session_factory.return_value)
