"""Dashboard state — stats and recent runs."""

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.run import RunStatus
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.pipeline_service import PipelineService
from datanika.services.schedule_service import ScheduleService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService
from datanika.ui.state.base_state import BaseState, get_sync_session
from datanika.ui.state.run_state import RunItem


class DashboardStats(BaseModel):
    total_uploads: int = 0
    total_transformations: int = 0
    total_pipelines: int = 0
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
        upload_svc = UploadService(conn_svc)
        transform_svc = TransformationService()
        pipeline_svc = PipelineService()
        schedule_svc = ScheduleService(upload_svc, transform_svc)
        exec_svc = ExecutionService()

        with get_sync_session() as session:
            uploads = upload_svc.list_uploads(session, org_id)
            transformations = transform_svc.list_transformations(session, org_id)
            pipelines = pipeline_svc.list_pipelines(session, org_id)
            schedules = schedule_svc.list_schedules(session, org_id)

            upload_names = {u.id: u.name for u in uploads}
            trans_names = {t.id: t.name for t in transformations}
            pipeline_names = {p.id: p.name for p in pipelines}

            recent = exec_svc.list_runs(session, org_id, limit=10)
            success_count = sum(1 for r in recent if r.status == RunStatus.SUCCESS)
            failed_count = sum(1 for r in recent if r.status == RunStatus.FAILED)

            self.stats = DashboardStats(
                total_uploads=len(uploads),
                total_transformations=len(transformations),
                total_pipelines=len(pipelines),
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
                    target_name=self._resolve_target_name(
                        r.target_type.value,
                        r.target_id,
                        upload_names,
                        trans_names,
                        pipeline_names,
                    ),
                    status=r.status.value,
                    started_at=str(r.started_at) if r.started_at else "",
                    finished_at=str(r.finished_at) if r.finished_at else "",
                    rows_loaded=r.rows_loaded or 0,
                    error_message=r.error_message or "",
                )
                for r in recent
            ]
        self.error_message = ""

    @staticmethod
    def _resolve_target_name(
        target_type: str,
        target_id: int,
        upload_names: dict,
        trans_names: dict,
        pipeline_names: dict | None = None,
    ) -> str:
        if target_type == "upload":
            name = upload_names.get(target_id, f"#{target_id}")
            return f"upload: {name}"
        if target_type == "pipeline":
            name = (pipeline_names or {}).get(target_id, f"#{target_id}")
            return f"pipeline: {name}"
        name = trans_names.get(target_id, f"#{target_id}")
        return f"transformation: {name}"
