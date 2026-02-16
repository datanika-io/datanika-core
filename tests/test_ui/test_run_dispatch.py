"""Regression tests: run_pipeline must dispatch Celery task, not just create a Run."""

import asyncio
from unittest.mock import MagicMock, patch

import pytest

from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus


class TestPipelineRunDispatchesCeleryTask:
    """Bug: PipelineState.run_pipeline() created a PENDING run but never dispatched
    the Celery task, leaving runs stuck in pending forever."""

    @pytest.mark.asyncio
    async def test_run_pipeline_calls_celery_delay(self):
        """After creating a run, run_pipeline must call run_pipeline_task.delay()."""
        from datanika.ui.state.pipeline_state import PipelineState

        # Get the underlying function from the Reflex EventHandler
        fn = PipelineState.run_pipeline.fn

        # Build a fake state with the _get_org_id coroutine
        state = MagicMock()

        async def fake_get_org_id():
            return 1

        state._get_org_id = fake_get_org_id

        # Mock the sync session and ExecutionService
        mock_run = MagicMock(spec=Run)
        mock_run.id = 42
        mock_run.status = RunStatus.PENDING

        mock_exec_svc = MagicMock()
        mock_exec_svc.create_run.return_value = mock_run

        with (
            patch(
                "datanika.ui.state.pipeline_state.get_sync_session"
            ) as mock_get_session,
            patch(
                "datanika.ui.state.pipeline_state.ExecutionService",
                return_value=mock_exec_svc,
            ),
            patch(
                "datanika.ui.state.pipeline_state.run_pipeline_task"
            ) as mock_task,
        ):
            mock_session = MagicMock()
            mock_get_session.return_value.__enter__ = MagicMock(
                return_value=mock_session
            )
            mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

            await fn(state, pipeline_id=5)

        # Verify run was created
        mock_exec_svc.create_run.assert_called_once_with(
            mock_session, 1, NodeType.PIPELINE, 5
        )
        # THE KEY ASSERTION: Celery task must be dispatched
        mock_task.delay.assert_called_once_with(run_id=42, org_id=1)


class TestScheduleStateUsesSchedulerIntegration:
    """Bug: ScheduleState._get_service() created ScheduleService without
    scheduler_integration, so schedules were never synced to APScheduler."""

    def test_get_service_passes_scheduler_integration(self):
        """_get_service() must pass the scheduler_integration singleton."""
        from datanika.ui.state.schedule_state import ScheduleState

        state = MagicMock()

        with (
            patch("datanika.scheduler.scheduler_integration") as mock_sched,
            patch("datanika.ui.state.schedule_state.EncryptionService"),
        ):
            svc = ScheduleState._get_service(state)

        assert svc._scheduler is mock_sched
