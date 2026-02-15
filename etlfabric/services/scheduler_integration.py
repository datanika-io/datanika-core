"""SchedulerIntegrationService — bridges Schedule DB records to APScheduler jobs."""

from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.orm import Session as SyncSession

from etlfabric.models.dependency import NodeType
from etlfabric.models.schedule import Schedule
from etlfabric.services.execution_service import ExecutionService
from etlfabric.tasks.pipeline_tasks import run_pipeline_task
from etlfabric.tasks.transformation_tasks import run_transformation_task


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

    def start(self) -> None:
        """Start the APScheduler background scheduler."""
        self._scheduler.start()

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
        job_id = f"schedule_{schedule.id}"

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
        job_id = f"schedule_{schedule_id}"
        existing = self._scheduler.get_job(job_id)
        if existing:
            self._scheduler.remove_job(job_id)
            return True
        return False

    def sync_all(self, session: Session) -> int:
        """Load all active schedules from DB and sync to APScheduler.

        Called on startup. Returns count of jobs synced.
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

    def get_job(self, schedule_id: int):
        """Get APScheduler job for a schedule. Returns None if not found."""
        return self._scheduler.get_job(f"schedule_{schedule_id}")

    @staticmethod
    def _build_cron_trigger(cron_expression: str, timezone: str) -> CronTrigger:
        """Parse 5-field cron into APScheduler CronTrigger."""
        fields = cron_expression.strip().split()
        if len(fields) != 5:
            raise ValueError(f"Expected 5 cron fields, got {len(fields)}")
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
        from etlfabric.config import settings

        engine = create_engine(settings.database_url_sync)
        session = SyncSession(engine)

        try:
            exec_svc = ExecutionService()
            node_type = NodeType(target_type)
            run = exec_svc.create_run(session, org_id, node_type, target_id)
            session.commit()

            if target_type == "pipeline":
                run_pipeline_task.delay(run_id=run.id, org_id=org_id)
            elif target_type == "transformation":
                run_transformation_task.delay(run_id=run.id, org_id=org_id)
        finally:
            session.close()
