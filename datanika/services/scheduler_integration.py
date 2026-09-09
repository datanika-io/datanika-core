"""SchedulerIntegrationService — bridges Schedule DB records to APScheduler jobs."""

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.errors import UserFacingError
from datanika.models.dependency import NodeType
from datanika.models.schedule import Schedule
from datanika.services.execution_service import ExecutionService
from datanika.tasks.pipeline_tasks import run_pipeline_task
from datanika.tasks.transformation_tasks import run_transformation_task
from datanika.tasks.upload_tasks import run_upload_task

#: Every APScheduler job this service owns is ``schedule_<Schedule.id>``.
#: ``reconcile()`` only ever removes ids under this prefix, so the scheduler
#: process's own reconcile heartbeat survives its own reconcile pass (core#648).
JOB_ID_PREFIX = "schedule_"

#: Process-local jobs (the reconcile heartbeat) live here rather than in the shared
#: Postgres store. They are machinery, not user data: persisting them would leave rows
#: in ``apscheduler_jobs``, which is the table every core#648 diagnostic counts to decide
#: whether any schedule exists at all.
INTERNAL_JOBSTORE = "internal"


def _job_id(schedule_id: int) -> str:
    return f"{JOB_ID_PREFIX}{schedule_id}"


class SchedulerIntegrationService:
    """Bridges Schedule DB records to APScheduler jobs."""

    def __init__(self, database_url_sync: str):
        self._scheduler = BackgroundScheduler(
            jobstores={"default": SQLAlchemyJobStore(url=database_url_sync)},
            job_defaults={
                "coalesce": True,
                "max_instances": 1,
                "misfire_grace_time": 300,
            },
        )

    def start(self, *, paused: bool = False) -> None:
        """Start the APScheduler background scheduler.

        ``paused=True`` starts a scheduler that still writes ``add_job``/``remove_job``
        through to the shared jobstore but never dispatches. Measured on apscheduler
        3.11.2 against real Postgres, both directions: the row appears and no dispatch
        happens 2.5s past the run time, and a separate unpaused scheduler then picks the
        same job up and runs it.
        """
        self._scheduler.start(paused=paused)

    def shutdown(self) -> None:
        """Gracefully shut down the scheduler."""
        self._scheduler.shutdown()

    @property
    def running(self) -> bool:
        """Whether the scheduler is currently running."""
        return self._scheduler.running

    def sync_schedule(self, schedule) -> str:
        """Add or update an APScheduler job for this Schedule.

        If is_active: add/replace CronTrigger job.
        If not is_active: remove job if exists.
        Returns the APScheduler job_id.
        """
        job_id = _job_id(schedule.id)

        if not schedule.is_active:
            existing = self._scheduler.get_job(job_id)
            if existing:
                self._scheduler.remove_job(job_id)
            return job_id

        # Active: remove existing then add fresh
        existing = self._scheduler.get_job(job_id)
        if existing:
            self._scheduler.remove_job(job_id)

        trigger = self._build_cron_trigger(schedule.cron_expression, schedule.timezone)
        self._scheduler.add_job(
            self._dispatch_target,
            trigger=trigger,
            id=job_id,
            args=[schedule.org_id, schedule.target_type.value, schedule.target_id],
            replace_existing=True,
        )
        return job_id

    def remove_schedule(self, schedule_id: int) -> bool:
        """Remove an APScheduler job. Returns True if job existed."""
        job_id = _job_id(schedule_id)
        existing = self._scheduler.get_job(job_id)
        if existing:
            self._scheduler.remove_job(job_id)
            return True
        return False

    def sync_all(self, session: Session) -> int:
        """Load all active schedules from DB and sync to APScheduler.

        Returns the count of jobs synced.

        ⚠️ **Use :meth:`reconcile` instead. This only ADDS.**

        A schedule that was deactivated or deleted keeps its APScheduler job here, so it
        goes on firing under a UI that shows it off. That was harmless while the web tier
        removed the job in-process at edit time; after core#648 nothing does, and
        ``reconcile`` is the only thing that keeps the job set honest.

        Kept, rather than deleted, because
        ``TestSyncAllAloneIsNotEnough::test_sync_all_never_removes_a_stale_job``
        characterises this behaviour on the real code — that test is the evidence that
        ``reconcile`` is necessary rather than merely nicer, and it needs a subject.
        Nothing in production calls this.
        """
        stmt = select(Schedule).where(
            Schedule.is_active.is_(True),
            Schedule.deleted_at.is_(None),
        )
        schedules = session.execute(stmt).scalars().all()
        count = 0
        for schedule in schedules:
            self.sync_schedule(schedule)
            count += 1
        return count

    def schedule_internal(self, func, *, seconds: int, job_id: str) -> None:
        """Register a repeating process-local job in a memory jobstore.

        Used by the dedicated scheduler process for its reconcile heartbeat. Two jobs in
        one: it re-reads ``schedules``, and its mere presence in the job set caps how long
        ``BaseScheduler._process_jobs()`` tells the main loop to sleep. With an empty store
        that wait is ``TIMEOUT_MAX`` — 49.7 days — and ``add_job`` from another process
        does not shorten it, because ``wakeup()`` is called only on the adding instance.
        """
        if not getattr(self, "_internal_jobstore_added", False):
            self._scheduler.add_jobstore(MemoryJobStore(), INTERNAL_JOBSTORE)
            self._internal_jobstore_added = True
        self._scheduler.add_job(
            func,
            "interval",
            seconds=seconds,
            id=job_id,
            jobstore=INTERNAL_JOBSTORE,
            replace_existing=True,
        )

    def reconcile(self, session: Session) -> dict[str, int]:
        """Make the jobstore match the ``schedules`` table. Returns ``{synced, removed}``.

        This is the scheduler process's only input. Once the scheduler leaves the web
        tier, ``ScheduleService`` writes the row and nothing else — no cross-process call
        exists to tell the scheduler about it — so every change reaches APScheduler here
        or not at all.

        The half ``sync_all`` does not do is the removal. ``sync_all`` adds active
        schedules and leaves everything else in place, which is correct while the web
        tier deletes the job in-process at edit time and silently wrong the moment it
        stops: a deactivated schedule would keep firing under a UI that shows it off.
        See ``TestSyncAllAloneIsNotEnough``.

        Only ids under :data:`JOB_ID_PREFIX` are removed, so the caller's own heartbeat
        job survives — a reconcile that deleted the job which invokes it would run
        exactly once.
        """
        stmt = select(Schedule).where(
            Schedule.is_active.is_(True),
            Schedule.deleted_at.is_(None),
        )
        wanted: set[str] = set()
        for schedule in session.execute(stmt).scalars().all():
            wanted.add(self.sync_schedule(schedule))

        removed = 0
        for job in self._scheduler.get_jobs():
            if job.id.startswith(JOB_ID_PREFIX) and job.id not in wanted:
                self._scheduler.remove_job(job.id)
                removed += 1

        return {"synced": len(wanted), "removed": removed}

    def get_job(self, schedule_id: int):
        """Get APScheduler job for a schedule. Returns None if not found."""
        return self._scheduler.get_job(_job_id(schedule_id))

    @staticmethod
    def _build_cron_trigger(cron_expression: str, timezone: str) -> CronTrigger:
        """Parse 5-field cron into APScheduler CronTrigger."""
        fields = cron_expression.strip().split()
        if len(fields) != 5:
            raise UserFacingError(f"Expected 5 cron fields, got {len(fields)}")
        return CronTrigger(
            minute=fields[0],
            hour=fields[1],
            day=fields[2],
            month=fields[3],
            day_of_week=fields[4],
            timezone=timezone,
        )

    @staticmethod
    def _dispatch_target(org_id: int, target_type: str, target_id: int) -> None:
        """Callback for APScheduler: create Run + dispatch Celery task."""
        from datanika.db import get_sync_session

        session = get_sync_session()

        try:
            exec_svc = ExecutionService()
            node_type = NodeType(target_type)
            run = exec_svc.create_run(session, org_id, node_type, target_id)
            session.commit()

            if target_type == "upload":
                run_upload_task.delay(run_id=run.id, org_id=org_id, scheduled=True)
            elif target_type == "transformation":
                run_transformation_task.delay(run_id=run.id, org_id=org_id, scheduled=True)
            elif target_type == "pipeline":
                run_pipeline_task.delay(run_id=run.id, org_id=org_id, scheduled=True)
        finally:
            session.close()
