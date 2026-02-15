"""Dashboard state — stats and recent runs."""

from pydantic import BaseModel

from etlfabric.config import settings
from etlfabric.models.run import RunStatus
from etlfabric.services.connection_service import ConnectionService
from etlfabric.services.encryption import EncryptionService
from etlfabric.services.execution_service import ExecutionService
from etlfabric.services.pipeline_service import PipelineService
from etlfabric.services.schedule_service import ScheduleService
from etlfabric.services.transformation_service import TransformationService
from etlfabric.ui.state.base_state import BaseState, get_sync_session
from etlfabric.ui.state.run_state import RunItem


class DashboardStats(BaseModel):
    total_pipelines: int = 0
    total_transformations: int = 0
    total_schedules: int = 0
    recent_runs_success: int = 0
    recent_runs_failed: int = 0
    recent_runs_total: int = 0


class DashboardState(BaseState):
    stats: DashboardStats = DashboardStats()
    recent_runs: list[RunItem] = []

    async def load_dashboard(self):
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        pipe_svc = PipelineService(conn_svc)
        transform_svc = TransformationService()
        schedule_svc = ScheduleService(pipe_svc, transform_svc)
        exec_svc = ExecutionService()

        with get_sync_session() as session:
            pipelines = pipe_svc.list_pipelines(session, org_id)
            transformations = transform_svc.list_transformations(session, org_id)
            schedules = schedule_svc.list_schedules(session, org_id)

            recent = exec_svc.list_runs(session, org_id, limit=10)
            success_count = sum(1 for r in recent if r.status == RunStatus.SUCCESS)
            failed_count = sum(1 for r in recent if r.status == RunStatus.FAILED)

            self.stats = DashboardStats(
                total_pipelines=len(pipelines),
                total_transformations=len(transformations),
                total_schedules=len(schedules),
                recent_runs_success=success_count,
                recent_runs_failed=failed_count,
                recent_runs_total=len(recent),
            )
            self.recent_runs = [
                RunItem(
                    id=r.id,
                    target_type=r.target_type.value,
                    target_id=r.target_id,
                    status=r.status.value,
                    started_at=str(r.started_at) if r.started_at else "",
                    finished_at=str(r.finished_at) if r.finished_at else "",
                    rows_loaded=r.rows_loaded or 0,
                    error_message=r.error_message or "",
                )
                for r in recent
            ]
        self.error_message = ""
