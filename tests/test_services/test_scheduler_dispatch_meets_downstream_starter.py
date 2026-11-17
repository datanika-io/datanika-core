"""Scheduler dispatch and the downstream starter, on one database (core#1673).

Dispatch opens ``get_sync_session``. The starter runs in the session that
finished the upstream. Those are different sessions, so this file gives both
the same factory.

A dispatch run left pending would also stop a second start. This test
finishes that run before the upstream succeeds, so the active schedule is
the reason the scheduled transformation stays at one run. The unscheduled
sibling, started by the same success, is how the test knows the starter ran.
The scheduler stays paused; the test calls the job the reconcile registered.
"""

from unittest.mock import patch

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import select

from datanika.models.dependency import Dependency, NodeType
from datanika.models.run import Run, RunStatus
from datanika.models.schedule import Schedule
from datanika.models.user import Organization
from datanika.services.execution_service import ExecutionService
from datanika.services.scheduler_integration import SchedulerIntegrationService

SCHEDULED_TARGET = 7
UNSCHEDULED_TARGET = 8
UPLOAD_TARGET = 1


def _runs(session, org_id, target_id):
    return list(
        session.execute(
            select(Run)
            .where(
                Run.org_id == org_id,
                Run.target_type == NodeType.TRANSFORMATION,
                Run.target_id == target_id,
            )
            .order_by(Run.id)
        )
        .scalars()
        .all()
    )


def _jobs_for(service, target_id):
    return [
        job
        for job in service._scheduler.get_jobs()
        if list(job.args)[1:] == ["transformation", target_id]
    ]


def test_a_scheduled_run_comes_from_dispatch_and_an_unscheduled_one_from_the_upstream(
    production_session_factory, monkeypatch
):
    monkeypatch.setattr("datanika.db.get_sync_session", production_session_factory)
    factory = production_session_factory
    with factory() as session:
        org = Organization(name="Meet", slug="scheduler-starter-meet")
        session.add(org)
        session.flush()
        schedule = Schedule(
            org_id=org.id,
            target_type=NodeType.TRANSFORMATION,
            target_id=SCHEDULED_TARGET,
            cron_expression="0 * * * *",
            timezone="UTC",
            is_active=True,
        )
        session.add(schedule)
        for target_id in (SCHEDULED_TARGET, UNSCHEDULED_TARGET):
            session.add(
                Dependency(
                    org_id=org.id,
                    upstream_type=NodeType.UPLOAD,
                    upstream_id=UPLOAD_TARGET,
                    downstream_type=NodeType.TRANSFORMATION,
                    downstream_id=target_id,
                )
            )
        session.flush()
        org_id = org.id
        schedule_id = schedule.id
        session.commit()

    scheduler = BackgroundScheduler(
        jobstores={"default": MemoryJobStore()},
        job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 300},
    )
    scheduler.start(paused=True)
    service = SchedulerIntegrationService.__new__(SchedulerIntegrationService)
    service._scheduler = scheduler
    try:
        with (
            patch("datanika.tasks.upload_tasks.run_upload_task.delay") as upload_delay,
            patch(
                "datanika.tasks.transformation_tasks.run_transformation_task.delay"
            ) as transformation_delay,
            patch("datanika.tasks.pipeline_tasks.run_pipeline_task.delay"),
        ):
            with factory() as session:
                assert service.reconcile(session)["synced"] == 1
            scheduled_job = service.get_job(schedule_id)
            assert scheduled_job is not None
            assert list(scheduled_job.args) == [org_id, "transformation", SCHEDULED_TARGET]
            assert _jobs_for(service, UNSCHEDULED_TARGET) == []

            scheduled_job.func(*scheduled_job.args)

            with factory() as session:
                dispatched = _runs(session, org_id, SCHEDULED_TARGET)
                assert len(dispatched) == 1
                assert dispatched[0].status == RunStatus.PENDING
                assert dispatched[0].caused_by_run_id is None
                dispatch_run_id = dispatched[0].id
                # Terminal before the upstream success, so a non-terminal run is
                # not what keeps the starter from writing a second row.
                ExecutionService().complete_run(
                    session, org_id, dispatch_run_id, rows_loaded=1, logs="dispatched"
                )
                session.commit()
            transformation_delay.assert_called_once_with(
                run_id=dispatch_run_id, org_id=org_id, scheduled=True
            )

            with factory() as session:
                upload = ExecutionService().create_run(
                    session, org_id, NodeType.UPLOAD, UPLOAD_TARGET
                )
                ExecutionService().complete_run(
                    session, org_id, upload.id, rows_loaded=1, logs="upstream"
                )
                upload_id = upload.id
                session.commit()

            with factory() as session:
                scheduled_runs = _runs(session, org_id, SCHEDULED_TARGET)
                assert [run.id for run in scheduled_runs] == [dispatch_run_id]
                assert scheduled_runs[0].status == RunStatus.SUCCESS
                assert scheduled_runs[0].caused_by_run_id is None
                started = _runs(session, org_id, UNSCHEDULED_TARGET)
                assert len(started) == 1
                assert started[0].status == RunStatus.PENDING
                assert started[0].caused_by_run_id == upload_id
                started_id = started[0].id

            assert transformation_delay.call_count == 2
            transformation_delay.assert_any_call(run_id=started_id, org_id=org_id, scheduled=True)
            upload_delay.assert_not_called()
            assert _jobs_for(service, UNSCHEDULED_TARGET) == []
            assert service.get_job(schedule_id) is not None
    finally:
        scheduler.shutdown(wait=False)
