"""TDD tests for SchedulerIntegrationService — APScheduler bridge."""

from unittest.mock import MagicMock, patch

import pytest
from apscheduler.triggers.cron import CronTrigger

from datanika.services.scheduler_integration import SchedulerIntegrationService


@pytest.fixture
def mock_scheduler():
    """Return a mock BackgroundScheduler."""
    with patch("datanika.services.scheduler_integration.BackgroundScheduler") as mock_cls:
        scheduler_instance = mock_cls.return_value
        scheduler_instance.running = False
        scheduler_instance.get_job.return_value = None
        yield scheduler_instance


@pytest.fixture
def svc(mock_scheduler):
    """Create a SchedulerIntegrationService with a mocked scheduler."""
    service = SchedulerIntegrationService.__new__(SchedulerIntegrationService)
    service._scheduler = mock_scheduler
    return service


def _make_schedule(
    schedule_id=1,
    org_id=1,
    target_type_value="upload",
    target_id=10,
    cron_expression="0 * * * *",
    timezone="UTC",
    is_active=True,
):
    """Create a mock Schedule object."""
    schedule = MagicMock()
    schedule.id = schedule_id
    schedule.org_id = org_id
    schedule.target_type.value = target_type_value
    schedule.target_id = target_id
    schedule.cron_expression = cron_expression
    schedule.timezone = timezone
    schedule.is_active = is_active
    return schedule


# ---------------------------------------------------------------------------
# _build_cron_trigger
# ---------------------------------------------------------------------------
class TestBuildCronTrigger:
    def test_every_minute_trigger(self):
        trigger = SchedulerIntegrationService._build_cron_trigger("* * * * *", "UTC")
        assert isinstance(trigger, CronTrigger)

    def test_specific_time(self):
        trigger = SchedulerIntegrationService._build_cron_trigger("30 14 * * *", "UTC")
        assert isinstance(trigger, CronTrigger)

    def test_day_of_week(self):
        trigger = SchedulerIntegrationService._build_cron_trigger("0 9 * * 1-5", "UTC")
        assert isinstance(trigger, CronTrigger)

    def test_invalid_cron_raises(self):
        with pytest.raises(ValueError):
            SchedulerIntegrationService._build_cron_trigger("bad cron", "UTC")


# ---------------------------------------------------------------------------
# sync_schedule
# ---------------------------------------------------------------------------
class TestSyncSchedule:
    def test_active_schedule_adds_job(self, svc, mock_scheduler):
        schedule = _make_schedule(is_active=True)
        job_id = svc.sync_schedule(schedule)
        assert job_id == "schedule_1"
        mock_scheduler.add_job.assert_called_once()

    def test_inactive_schedule_removes_job(self, svc, mock_scheduler):
        schedule = _make_schedule(is_active=False)
        mock_scheduler.get_job.return_value = MagicMock()  # job exists
        svc.sync_schedule(schedule)
        mock_scheduler.remove_job.assert_called_once_with("schedule_1")

    def test_update_replaces_job(self, svc, mock_scheduler):
        schedule = _make_schedule(is_active=True)
        mock_scheduler.get_job.return_value = MagicMock()  # existing job
        svc.sync_schedule(schedule)
        mock_scheduler.remove_job.assert_called_once_with("schedule_1")
        mock_scheduler.add_job.assert_called_once()

    def test_returns_correct_job_id_format(self, svc, mock_scheduler):
        schedule = _make_schedule(schedule_id=42)
        job_id = svc.sync_schedule(schedule)
        assert job_id == "schedule_42"

    def test_handles_toggle(self, svc, mock_scheduler):
        # Active -> adds job
        schedule_active = _make_schedule(is_active=True)
        svc.sync_schedule(schedule_active)
        mock_scheduler.add_job.assert_called_once()

        mock_scheduler.reset_mock()
        mock_scheduler.get_job.return_value = MagicMock()

        # Inactive -> removes job
        schedule_inactive = _make_schedule(is_active=False)
        svc.sync_schedule(schedule_inactive)
        mock_scheduler.remove_job.assert_called_once()


# ---------------------------------------------------------------------------
# remove_schedule
# ---------------------------------------------------------------------------
class TestRemoveSchedule:
    def test_removes_existing_job(self, svc, mock_scheduler):
        mock_scheduler.get_job.return_value = MagicMock()
        result = svc.remove_schedule(1)
        assert result is True
        mock_scheduler.remove_job.assert_called_once_with("schedule_1")

    def test_returns_false_for_nonexistent(self, svc, mock_scheduler):
        mock_scheduler.get_job.return_value = None
        result = svc.remove_schedule(999)
        assert result is False

    def test_idempotent(self, svc, mock_scheduler):
        mock_scheduler.get_job.return_value = None
        result1 = svc.remove_schedule(1)
        result2 = svc.remove_schedule(1)
        assert result1 is False
        assert result2 is False


# ---------------------------------------------------------------------------
# sync_all
# ---------------------------------------------------------------------------
class TestSyncAll:
    def test_loads_all_active_schedules(self, svc, mock_scheduler):
        mock_session = MagicMock()
        s1 = _make_schedule(schedule_id=1, is_active=True)
        s2 = _make_schedule(schedule_id=2, is_active=True)
        mock_session.execute.return_value.scalars.return_value.all.return_value = [s1, s2]

        count = svc.sync_all(mock_session)
        assert count == 2
        assert mock_scheduler.add_job.call_count == 2

    def test_skips_inactive(self, svc, mock_scheduler):
        mock_session = MagicMock()
        # sync_all only queries active schedules, so inactive ones won't be returned
        s1 = _make_schedule(schedule_id=1, is_active=True)
        mock_session.execute.return_value.scalars.return_value.all.return_value = [s1]

        count = svc.sync_all(mock_session)
        assert count == 1

    def test_returns_correct_count(self, svc, mock_scheduler):
        mock_session = MagicMock()
        schedules = [_make_schedule(schedule_id=i, is_active=True) for i in range(5)]
        mock_session.execute.return_value.scalars.return_value.all.return_value = schedules

        count = svc.sync_all(mock_session)
        assert count == 5


# ---------------------------------------------------------------------------
# get_job
# ---------------------------------------------------------------------------
class TestGetJob:
    def test_returns_job_for_existing(self, svc, mock_scheduler):
        mock_job = MagicMock()
        mock_scheduler.get_job.return_value = mock_job
        assert svc.get_job(1) == mock_job

    def test_returns_none_for_nonexistent(self, svc, mock_scheduler):
        mock_scheduler.get_job.return_value = None
        assert svc.get_job(999) is None


# ---------------------------------------------------------------------------
# _dispatch_target
# ---------------------------------------------------------------------------
class TestDispatchTarget:
    @patch("datanika.services.scheduler_integration.run_upload_task")
    @patch("datanika.services.scheduler_integration.ExecutionService")
    def test_creates_run_for_upload(self, mock_exec_cls, mock_upload_task):
        mock_svc = mock_exec_cls.return_value
        mock_run = MagicMock()
        mock_run.id = 1
        mock_svc.create_run.return_value = mock_run

        with (
            patch("datanika.services.scheduler_integration.create_engine"),
            patch("datanika.services.scheduler_integration.SyncSession"),
        ):
            SchedulerIntegrationService._dispatch_target(1, "upload", 10)

        mock_svc.create_run.assert_called_once()
        mock_upload_task.delay.assert_called_once()
        call_kwargs = mock_upload_task.delay.call_args[1]
        assert call_kwargs["scheduled"] is True

    @patch("datanika.services.scheduler_integration.run_transformation_task")
    @patch("datanika.services.scheduler_integration.ExecutionService")
    def test_creates_run_for_transformation(self, mock_exec_cls, mock_transform_task):
        mock_svc = mock_exec_cls.return_value
        mock_run = MagicMock()
        mock_run.id = 2
        mock_svc.create_run.return_value = mock_run

        with (
            patch("datanika.services.scheduler_integration.create_engine"),
            patch("datanika.services.scheduler_integration.SyncSession"),
        ):
            SchedulerIntegrationService._dispatch_target(1, "transformation", 20)

        mock_svc.create_run.assert_called_once()
        mock_transform_task.delay.assert_called_once()
        call_kwargs = mock_transform_task.delay.call_args[1]
        assert call_kwargs["scheduled"] is True

    @patch("datanika.services.scheduler_integration.run_upload_task")
    @patch("datanika.services.scheduler_integration.ExecutionService")
    def test_dispatches_correct_celery_task(self, mock_exec_cls, mock_upload_task):
        mock_svc = mock_exec_cls.return_value
        mock_run = MagicMock()
        mock_run.id = 5
        mock_svc.create_run.return_value = mock_run

        with (
            patch("datanika.services.scheduler_integration.create_engine"),
            patch("datanika.services.scheduler_integration.SyncSession"),
        ):
            SchedulerIntegrationService._dispatch_target(3, "upload", 7)

        mock_upload_task.delay.assert_called_once_with(run_id=5, org_id=3, scheduled=True)

    @patch("datanika.services.scheduler_integration.run_pipeline_task")
    @patch("datanika.services.scheduler_integration.ExecutionService")
    def test_creates_run_for_pipeline(self, mock_exec_cls, mock_pipeline_task):
        mock_svc = mock_exec_cls.return_value
        mock_run = MagicMock()
        mock_run.id = 3
        mock_svc.create_run.return_value = mock_run

        with (
            patch("datanika.services.scheduler_integration.create_engine"),
            patch("datanika.services.scheduler_integration.SyncSession"),
        ):
            SchedulerIntegrationService._dispatch_target(1, "pipeline", 30)

        mock_svc.create_run.assert_called_once()
        mock_pipeline_task.delay.assert_called_once_with(run_id=3, org_id=1, scheduled=True)
