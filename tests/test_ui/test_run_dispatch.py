"""Regression tests: run_upload must dispatch Celery task, not just create a Run."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datanika.models.dependency import NodeType
from datanika.models.run import Run, RunStatus


class TestUploadRunDispatchesCeleryTask:
    """Bug: UploadState.run_upload() created a PENDING run but never dispatched
    the Celery task, leaving runs stuck in pending forever."""

    @pytest.mark.asyncio
    async def test_run_upload_calls_celery_delay(self):
        """After creating a run, run_upload must call run_upload_task.delay()."""
        from datanika.ui.state.upload_state import UploadState

        # Get the underlying function from the Reflex EventHandler
        fn = UploadState.run_upload.fn

        # Build a fake state with the auth-state pattern
        state = MagicMock()

        # Mock _check_role to return True
        state._check_role = AsyncMock(return_value=True)
        # core#872 — the handler now yields a translated toast. A bare
        # MagicMock attribute is not awaitable, and `await` on one raises
        # inside the handler rather than reporting anything about dispatch.
        state._saved_toast = AsyncMock(return_value=None)

        # Mock get_state(AuthState) to return a fake auth state
        mock_auth = MagicMock()
        mock_auth.current_org.id = 1
        mock_auth.current_user.id = 10
        state.get_state = AsyncMock(return_value=mock_auth)

        # Mock the sync session and ExecutionService
        mock_run = MagicMock(spec=Run)
        mock_run.id = 42
        mock_run.status = RunStatus.PENDING

        mock_exec_svc = MagicMock()
        mock_exec_svc.create_run.return_value = mock_run

        # #93 — run_upload now instantiates Encryption/Connection/Upload
        # services to consume the template first-run latch. Mock them so
        # the template path no-ops and Celery dispatch is the only signal
        # this test asserts on.
        mock_conn_svc = MagicMock()
        mock_conn_svc.consume_template_first_run.return_value = None

        mock_upload = MagicMock()
        mock_upload.source_connection_id = 101
        mock_upload.destination_connection_id = 202
        mock_upload_svc = MagicMock()
        mock_upload_svc.get_upload.return_value = mock_upload

        with (
            patch("datanika.ui.state.upload_state.get_sync_session") as mock_get_session,
            patch(
                "datanika.ui.state.upload_state.ExecutionService",
                return_value=mock_exec_svc,
            ),
            patch("datanika.ui.state.upload_state.EncryptionService"),
            patch(
                "datanika.ui.state.upload_state.ConnectionService",
                return_value=mock_conn_svc,
            ),
            patch(
                "datanika.ui.state.upload_state.UploadService",
                return_value=mock_upload_svc,
            ),
            patch("datanika.ui.state.upload_state.run_upload_task") as mock_task,
            patch("datanika.ui.state.base_state.BaseState._audit"),
        ):
            mock_session = MagicMock()
            mock_get_session.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_get_session.return_value.__exit__ = MagicMock(return_value=False)

            async for _ in fn(state, upload_id=5):
                pass

        # Verify run was created
        mock_exec_svc.create_run.assert_called_once_with(mock_session, 1, NodeType.UPLOAD, 5)
        # THE KEY ASSERTION: Celery task must be dispatched
        mock_task.delay.assert_called_once_with(run_id=42, org_id=1)


class TestScheduleStateUsesSchedulerIntegration:
    """Bug: ScheduleState._get_schedule_service() created ScheduleService without
    scheduler_integration, so schedules were never synced to APScheduler."""

    def test_get_service_passes_scheduler_integration(self):
        """_get_schedule_service() must pass the scheduler_integration singleton."""
        from datanika.ui.state.schedule_state import ScheduleState

        state = MagicMock()

        with (
            patch("datanika.scheduler.scheduler_integration") as mock_sched,
            patch("datanika.ui.state.schedule_state.EncryptionService"),
        ):
            svc = ScheduleState._get_schedule_service(state)

        assert svc._scheduler is mock_sched
