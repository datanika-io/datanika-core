"""core#648 — the scheduler process reconciles the jobstore from `schedules`.

Why this method has to exist at all
-----------------------------------
Moving the scheduler out of the web tier severs the call that used to keep APScheduler in
step with the database: ``ScheduleService`` ran ``sync_schedule()`` on a scheduler living in
its own process. Once the scheduler is a separate deployment unit that call has nowhere to
land, so the ``schedules`` table becomes the sole source of truth and the scheduler process
must *pull* from it.

``sync_all()`` is not that method, and the gap is not cosmetic.
``TestSyncAllAloneIsNotEnough`` below is a **characterisation test of the existing code**:
it pins the fact that ``sync_all`` only ever *adds*. A schedule the user deactivates or
deletes keeps its job in the store and keeps firing — forever, since nothing else removes
it. Under today's architecture that is masked, because ``ScheduleService`` removes the job
in-process at the moment of the edit. Under the new one nothing would, so shipping the move
without ``reconcile`` would convert "deactivate" into a no-op: the UI would show the
schedule off while it went on running. Silent, customer-discovered, and billed.

That test is written against ``sync_all`` deliberately rather than deleted once ``reconcile``
exists. It is the evidence that the new method is *needed*, and if someone later "simplifies"
``reconcile`` back into ``sync_all`` it goes red.

Real scheduler, not a mock
--------------------------
These use a real ``BackgroundScheduler`` over a ``MemoryJobStore``, started **paused** so it
never dispatches. A ``MagicMock`` scheduler would agree with any assertion made about it,
including the wrong ones — and the property under test is precisely what APScheduler does
with the job set, not what we asked it to do.
"""

from __future__ import annotations

import pytest
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.background import BackgroundScheduler

from datanika.models.dependency import NodeType
from datanika.models.schedule import Schedule
from datanika.services.scheduler_integration import SchedulerIntegrationService


@pytest.fixture
def svc():
    """A real, paused scheduler over an in-memory jobstore."""
    scheduler = BackgroundScheduler(
        jobstores={"default": MemoryJobStore()},
        job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 300},
    )
    scheduler.start(paused=True)
    service = SchedulerIntegrationService.__new__(SchedulerIntegrationService)
    service._scheduler = scheduler
    try:
        yield service
    finally:
        scheduler.shutdown(wait=False)


def _schedule(db_session, *, org_id=1, target_id=10, is_active=True, deleted_at=None):
    row = Schedule(
        org_id=org_id,
        target_type=NodeType.UPLOAD,
        target_id=target_id,
        cron_expression="*/5 * * * *",
        timezone="UTC",
        is_active=is_active,
    )
    if deleted_at is not None:
        row.deleted_at = deleted_at
    db_session.add(row)
    db_session.flush()
    return row


def _job_ids(svc) -> set[str]:
    return {job.id for job in svc._scheduler.get_jobs()}


class TestReconcileMakesTheJobstoreMatchTheTable:
    def test_it_adds_a_job_for_an_active_schedule(self, svc, db_session):
        row = _schedule(db_session)
        svc.reconcile(db_session)
        assert f"schedule_{row.id}" in _job_ids(svc)

    def test_it_removes_the_job_of_a_deactivated_schedule(self, svc, db_session):
        """The whole reason `reconcile` is not `sync_all`.

        The user switches a schedule off in the UI. The web tier writes the row and
        nothing else — so if this does not remove the job, the pipeline keeps running
        on a schedule the UI shows as inactive.
        """
        row = _schedule(db_session)
        svc.reconcile(db_session)
        assert f"schedule_{row.id}" in _job_ids(svc), "arming check: the job was never there"

        row.is_active = False
        db_session.flush()
        svc.reconcile(db_session)
        assert f"schedule_{row.id}" not in _job_ids(svc), (
            "a deactivated schedule kept its APScheduler job; it will go on firing "
            "while the UI shows it off (core#648)"
        )

    def test_it_removes_the_job_of_a_soft_deleted_schedule(self, svc, db_session):
        from datetime import datetime

        row = _schedule(db_session)
        svc.reconcile(db_session)
        assert f"schedule_{row.id}" in _job_ids(svc), "arming check: the job was never there"

        row.deleted_at = datetime(2026, 9, 8, 12, 0, 0)
        db_session.flush()
        svc.reconcile(db_session)
        assert f"schedule_{row.id}" not in _job_ids(svc), (
            "a soft-deleted schedule kept its APScheduler job"
        )

    def test_it_leaves_jobs_it_does_not_own_alone(self, svc, db_session):
        """The scheduler process keeps its own reconcile heartbeat in this scheduler.

        A reconcile that removed every job it did not recognise would delete the very
        job that calls it — once, silently, and then never reconcile again.
        """
        svc._scheduler.add_job(lambda: None, "interval", seconds=3600, id="datanika_reconcile")
        _schedule(db_session)
        svc.reconcile(db_session)
        assert "datanika_reconcile" in _job_ids(svc), (
            "reconcile removed a job that is not a schedule; it must only manage ids "
            "under its own prefix"
        )

    def test_it_reports_what_it_did(self, svc, db_session):
        keep = _schedule(db_session, target_id=10)
        drop = _schedule(db_session, target_id=11)
        svc.reconcile(db_session)
        drop.is_active = False
        db_session.flush()

        result = svc.reconcile(db_session)
        assert result == {"synced": 1, "removed": 1}, (
            f"reconcile must report its effect for the scheduler log; got {result}"
        )
        assert f"schedule_{keep.id}" in _job_ids(svc)


class TestSyncAllAloneIsNotEnough:
    """Characterisation of the *existing* method, and the reason `reconcile` exists.

    Not a bug in `sync_all` as it is used today — `ScheduleService` removes the job
    in-process at edit time. It becomes one the moment the scheduler moves out of the
    web tier, which is exactly what core#648 does.
    """

    def test_sync_all_never_removes_a_stale_job(self, svc, db_session):
        row = _schedule(db_session)
        svc.sync_all(db_session)
        assert f"schedule_{row.id}" in _job_ids(svc)

        row.is_active = False
        db_session.flush()
        svc.sync_all(db_session)
        assert f"schedule_{row.id}" in _job_ids(svc), (
            "if sync_all has learned to remove stale jobs, reconcile may be redundant "
            "— but check the prefix-scoping test above before deleting it"
        )


class TestASavedScheduleStillReachesTheScheduler:
    """The guarantee core#648 moved, asserted end to end where it now lives.

    ``tests/test_ui/test_run_dispatch.py`` used to hold this: it checked that the web tier
    handed ``ScheduleService`` the in-process scheduler singleton, because there was once a
    bug where it did not and saved schedules never ran. The fix for core#648 inverts that
    assertion — the web tier must NOT own a scheduler now — and an inverted regression test
    whose guarantee is not re-established somewhere else is just a deleted one.

    So: write the row the way the UI writes it, reconcile the way the scheduler process
    does, and require the job to exist. This spans the seam the fix created, which the old
    single-call-site assertion never did.
    """

    def _service(self):
        from unittest.mock import MagicMock

        from datanika.services.schedule_service import ScheduleService

        return ScheduleService(MagicMock(), MagicMock(), pipeline_service=MagicMock())

    def test_a_schedule_created_through_the_service_is_dispatchable_after_reconcile(
        self, svc, db_session
    ):
        schedule_svc = self._service()
        created = schedule_svc.create_schedule(
            db_session,
            org_id=1,
            target_type=NodeType.UPLOAD,
            target_id=10,
            cron_expression="*/5 * * * *",
            timezone="UTC",
        )
        db_session.flush()

        assert _job_ids(svc) == set(), (
            "arming check: creating the schedule must NOT have reached APScheduler by "
            "itself, or this test would pass without reconcile doing anything"
        )

        svc.reconcile(db_session)
        assert f"schedule_{created.id}" in _job_ids(svc), (
            "a schedule saved through ScheduleService never reached the scheduler. The web "
            "tier writes the row and nothing else, so if reconcile does not pick it up the "
            "user's schedule is saved, shown as active, and never runs (core#648)."
        )

    def test_deleting_it_through_the_service_removes_the_job(self, svc, db_session):
        """The same seam in the other direction, which is the one that keeps billing honest."""
        schedule_svc = self._service()
        created = schedule_svc.create_schedule(
            db_session,
            org_id=1,
            target_type=NodeType.UPLOAD,
            target_id=10,
            cron_expression="*/5 * * * *",
            timezone="UTC",
        )
        db_session.flush()
        svc.reconcile(db_session)
        assert f"schedule_{created.id}" in _job_ids(svc)

        schedule_svc.delete_schedule(db_session, org_id=1, schedule_id=created.id)
        db_session.flush()
        svc.reconcile(db_session)
        assert f"schedule_{created.id}" not in _job_ids(svc), (
            "a schedule deleted in the UI kept its job and goes on running pipelines"
        )
