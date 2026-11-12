"""One starter for a node with no active schedule (SPEC_DBT_PROJECT_AND_DOWNSTREAM).

A direct upstream SUCCESS starts that node once. A node with an active schedule
stays on its cron. Successes that land during the in-flight run collapse into
one follow-up, and the success that caused the in-flight run does not.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import select

from datanika.models.dependency import Dependency, NodeType
from datanika.models.run import NON_TERMINAL_RUN_STATUSES, Run, RunStatus
from datanika.models.schedule import Schedule
from datanika.models.user import Organization
from datanika.services.execution_service import ExecutionService
from datanika.tasks.dependency_helpers import DEPENDENCY_MAX_RETRIES, DEPENDENCY_RETRY_COUNTDOWN


@pytest.fixture
def org(db_session):
    row = Organization(name="Downstream", slug="downstream-starter")
    db_session.add(row)
    db_session.flush()
    return row


@pytest.fixture
def dispatched():
    with (
        patch("datanika.tasks.upload_tasks.run_upload_task.delay") as upload,
        patch(
            "datanika.tasks.transformation_tasks.run_transformation_task.delay"
        ) as transformation,
        patch("datanika.tasks.pipeline_tasks.run_pipeline_task.delay") as pipeline,
    ):
        yield {
            "upload": upload,
            "transformation": transformation,
            "pipeline": pipeline,
        }


def _edge(session, org_id, upstream_type, upstream_id, downstream_type, downstream_id):
    session.add(
        Dependency(
            org_id=org_id,
            upstream_type=upstream_type,
            upstream_id=upstream_id,
            downstream_type=downstream_type,
            downstream_id=downstream_id,
        )
    )
    session.flush()


def _runs(session, org_id, target_type, target_id):
    return list(
        session.execute(
            select(Run)
            .where(
                Run.org_id == org_id,
                Run.target_type == target_type,
                Run.target_id == target_id,
            )
            .order_by(Run.id)
        )
        .scalars()
        .all()
    )


def _succeed(session, org_id, run):
    ExecutionService().complete_run(session, org_id, run.id, rows_loaded=1, logs="ok")
    session.commit()


def _backdate(session, run, hours=1):
    run.created_at = datetime.now(UTC) - timedelta(hours=hours)
    session.flush()


class TestDownstreamStarter:
    def test_the_in_run_wait_stays_sixty_seconds_and_five_retries(self):
        assert DEPENDENCY_RETRY_COUNTDOWN == 60
        assert DEPENDENCY_MAX_RETRIES == 5

    def test_a_finished_upload_starts_an_unscheduled_model_once(self, db_session, org, dispatched):
        _edge(db_session, org.id, NodeType.UPLOAD, 1, NodeType.TRANSFORMATION, 7)
        upload = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, 1)
        _succeed(db_session, org.id, upload)

        started = _runs(db_session, org.id, NodeType.TRANSFORMATION, 7)
        assert len(started) == 1
        assert started[0].status == RunStatus.PENDING
        assert started[0].caused_by_run_id == upload.id
        dispatched["transformation"].assert_called_once_with(
            run_id=started[0].id, org_id=org.id, scheduled=True
        )

        _succeed(db_session, org.id, started[0])
        assert len(_runs(db_session, org.id, NodeType.TRANSFORMATION, 7)) == 1

    def test_a_finished_upload_does_not_start_a_scheduled_model(self, db_session, org, dispatched):
        _edge(db_session, org.id, NodeType.UPLOAD, 1, NodeType.TRANSFORMATION, 7)
        db_session.add(
            Schedule(
                org_id=org.id,
                target_type=NodeType.TRANSFORMATION,
                target_id=7,
                cron_expression="0 * * * *",
                is_active=True,
            )
        )
        db_session.add(
            Schedule(
                org_id=org.id,
                target_type=NodeType.TRANSFORMATION,
                target_id=7,
                cron_expression="30 * * * *",
                is_active=True,
            )
        )
        db_session.flush()
        upload = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, 1)
        _succeed(db_session, org.id, upload)
        assert _runs(db_session, org.id, NodeType.TRANSFORMATION, 7) == []
        dispatched["transformation"].assert_not_called()

    def test_an_inactive_or_deleted_schedule_is_not_the_starter(self, db_session, org, dispatched):
        _edge(db_session, org.id, NodeType.UPLOAD, 1, NodeType.TRANSFORMATION, 8)
        db_session.add(
            Schedule(
                org_id=org.id,
                target_type=NodeType.TRANSFORMATION,
                target_id=8,
                cron_expression="0 * * * *",
                is_active=False,
            )
        )
        deleted = Schedule(
            org_id=org.id,
            target_type=NodeType.TRANSFORMATION,
            target_id=9,
            cron_expression="0 * * * *",
            is_active=True,
        )
        deleted.deleted_at = datetime.now(UTC)
        db_session.add(deleted)
        _edge(db_session, org.id, NodeType.UPLOAD, 1, NodeType.TRANSFORMATION, 9)
        db_session.flush()
        upload = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, 1)
        _succeed(db_session, org.id, upload)
        assert len(_runs(db_session, org.id, NodeType.TRANSFORMATION, 8)) == 1
        assert len(_runs(db_session, org.id, NodeType.TRANSFORMATION, 9)) == 1

    def test_failed_cancelled_and_still_working_start_nothing(self, db_session, org, dispatched):
        _edge(db_session, org.id, NodeType.UPLOAD, 1, NodeType.TRANSFORMATION, 7)
        svc = ExecutionService()
        failed = svc.create_run(db_session, org.id, NodeType.UPLOAD, 1)
        svc.fail_run(db_session, org.id, failed.id, error_message="no", logs="no")
        db_session.commit()
        assert _runs(db_session, org.id, NodeType.TRANSFORMATION, 7) == []

        cancelling = svc.create_run(db_session, org.id, NodeType.UPLOAD, 1)
        cancelling.status = RunStatus.CANCELLING
        db_session.flush()
        svc.complete_run(db_session, org.id, cancelling.id, rows_loaded=0, logs="stopped")
        db_session.commit()
        assert cancelling.status == RunStatus.CANCELLED
        assert _runs(db_session, org.id, NodeType.TRANSFORMATION, 7) == []

        working = svc.create_run(db_session, org.id, NodeType.UPLOAD, 1)
        svc.start_run(db_session, org.id, working.id)
        db_session.commit()
        assert working.status == RunStatus.RUNNING
        assert _runs(db_session, org.id, NodeType.TRANSFORMATION, 7) == []
        dispatched["transformation"].assert_not_called()

    def test_two_successes_that_land_together_start_one_run(self, db_session, org, dispatched):
        _edge(db_session, org.id, NodeType.UPLOAD, 1, NodeType.TRANSFORMATION, 7)
        _edge(db_session, org.id, NodeType.UPLOAD, 2, NodeType.TRANSFORMATION, 7)
        svc = ExecutionService()
        first = svc.create_run(db_session, org.id, NodeType.UPLOAD, 1)
        second = svc.create_run(db_session, org.id, NodeType.UPLOAD, 2)
        _succeed(db_session, org.id, first)
        _succeed(db_session, org.id, second)
        assert len(_runs(db_session, org.id, NodeType.TRANSFORMATION, 7)) == 1
        assert dispatched["transformation"].call_count == 1

    def test_a_success_during_a_non_terminal_run_does_not_start_another(
        self, db_session, org, dispatched
    ):
        _edge(db_session, org.id, NodeType.UPLOAD, 1, NodeType.TRANSFORMATION, 7)
        in_flight = ExecutionService().create_run(db_session, org.id, NodeType.TRANSFORMATION, 7)
        in_flight.status = RunStatus.RUNNING
        db_session.flush()
        upload = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, 1)
        _succeed(db_session, org.id, upload)
        runs = _runs(db_session, org.id, NodeType.TRANSFORMATION, 7)
        assert [run.id for run in runs] == [in_flight.id]
        assert in_flight.status == RunStatus.RUNNING
        dispatched["transformation"].assert_not_called()

    def test_successes_during_the_run_collapse_into_one_follow_up(
        self, db_session, org, dispatched
    ):
        _edge(db_session, org.id, NodeType.UPLOAD, 1, NodeType.TRANSFORMATION, 7)
        _edge(db_session, org.id, NodeType.UPLOAD, 2, NodeType.TRANSFORMATION, 7)
        _edge(db_session, org.id, NodeType.UPLOAD, 3, NodeType.TRANSFORMATION, 7)
        svc = ExecutionService()
        cause = svc.create_run(db_session, org.id, NodeType.UPLOAD, 1)
        _succeed(db_session, org.id, cause)
        in_flight = _runs(db_session, org.id, NodeType.TRANSFORMATION, 7)[0]
        _backdate(db_session, in_flight)
        for upload_id in (2, 3):
            upstream = svc.create_run(db_session, org.id, NodeType.UPLOAD, upload_id)
            _succeed(db_session, org.id, upstream)
        assert len(_runs(db_session, org.id, NodeType.TRANSFORMATION, 7)) == 1
        _succeed(db_session, org.id, in_flight)
        runs = _runs(db_session, org.id, NodeType.TRANSFORMATION, 7)
        assert len(runs) == 2
        assert runs[0].status == RunStatus.SUCCESS
        assert runs[1].status == RunStatus.PENDING
        assert sum(run.status in NON_TERMINAL_RUN_STATUSES for run in runs) == 1

    def test_the_cause_alone_does_not_arm_a_follow_up(self, db_session, org, dispatched):
        _edge(db_session, org.id, NodeType.UPLOAD, 1, NodeType.TRANSFORMATION, 7)
        cause = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, 1)
        _succeed(db_session, org.id, cause)
        in_flight = _runs(db_session, org.id, NodeType.TRANSFORMATION, 7)[0]
        _backdate(db_session, in_flight)
        _succeed(db_session, org.id, in_flight)
        assert len(_runs(db_session, org.id, NodeType.TRANSFORMATION, 7)) == 1

    def test_a_schedule_that_exists_when_the_run_ends_blocks_the_follow_up(
        self, db_session, org, dispatched
    ):
        _edge(db_session, org.id, NodeType.UPLOAD, 1, NodeType.TRANSFORMATION, 7)
        _edge(db_session, org.id, NodeType.UPLOAD, 2, NodeType.TRANSFORMATION, 7)
        svc = ExecutionService()
        cause = svc.create_run(db_session, org.id, NodeType.UPLOAD, 1)
        _succeed(db_session, org.id, cause)
        in_flight = _runs(db_session, org.id, NodeType.TRANSFORMATION, 7)[0]
        _backdate(db_session, in_flight)
        later = svc.create_run(db_session, org.id, NodeType.UPLOAD, 2)
        _succeed(db_session, org.id, later)
        db_session.add(
            Schedule(
                org_id=org.id,
                target_type=NodeType.TRANSFORMATION,
                target_id=7,
                cron_expression="0 * * * *",
                is_active=True,
            )
        )
        db_session.flush()
        _succeed(db_session, org.id, in_flight)
        assert len(_runs(db_session, org.id, NodeType.TRANSFORMATION, 7)) == 1

    def test_a_self_edge_starts_nothing(self, db_session, org, dispatched):
        _edge(db_session, org.id, NodeType.UPLOAD, 1, NodeType.UPLOAD, 1)
        upload = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, 1)
        _succeed(db_session, org.id, upload)
        assert len(_runs(db_session, org.id, NodeType.UPLOAD, 1)) == 1
        dispatched["upload"].assert_not_called()

    def test_a_later_hop_starts_when_that_downstream_succeeds(self, db_session, org, dispatched):
        _edge(db_session, org.id, NodeType.UPLOAD, 1, NodeType.TRANSFORMATION, 7)
        _edge(db_session, org.id, NodeType.TRANSFORMATION, 7, NodeType.PIPELINE, 4)
        upload = ExecutionService().create_run(db_session, org.id, NodeType.UPLOAD, 1)
        _succeed(db_session, org.id, upload)
        assert _runs(db_session, org.id, NodeType.PIPELINE, 4) == []
        model = _runs(db_session, org.id, NodeType.TRANSFORMATION, 7)[0]
        _succeed(db_session, org.id, model)
        started = _runs(db_session, org.id, NodeType.PIPELINE, 4)
        assert len(started) == 1
        dispatched["pipeline"].assert_called_once_with(
            run_id=started[0].id, org_id=org.id, scheduled=True
        )

    def test_an_edge_with_no_timeframe_still_counts(self, db_session, org, dispatched):
        _edge(db_session, org.id, NodeType.PIPELINE, 3, NodeType.UPLOAD, 9)
        pipeline = ExecutionService().create_run(db_session, org.id, NodeType.PIPELINE, 3)
        _succeed(db_session, org.id, pipeline)
        assert len(_runs(db_session, org.id, NodeType.UPLOAD, 9)) == 1
        dispatched["upload"].assert_called_once()
