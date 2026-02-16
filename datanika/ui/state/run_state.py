"""Run state for Reflex UI."""

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.pipeline_service import PipelineService
from datanika.services.transformation_service import TransformationService
from datanika.ui.state.base_state import BaseState, get_sync_session


class RunItem(BaseModel):
    id: int = 0
    target_type: str = ""
    target_id: int = 0
    target_name: str = ""
    status: str = ""
    started_at: str = ""
    finished_at: str = ""
    rows_loaded: int = 0
    error_message: str = ""


class RunState(BaseState):
    runs: list[RunItem] = []
    filter_status: str = ""
    filter_target_type: str = ""

    async def load_runs(self):
        org_id = await self._get_org_id()
        svc = ExecutionService()
        status_filter = RunStatus(self.filter_status) if self.filter_status else None
        target_type_filter = NodeType(self.filter_target_type) if self.filter_target_type else None

        # Build name lookups
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        pipe_svc = PipelineService(conn_svc)
        transform_svc = TransformationService()

        with get_sync_session() as session:
            pipelines = pipe_svc.list_pipelines(session, org_id)
            pipe_names = {p.id: p.name for p in pipelines}
            transformations = transform_svc.list_transformations(session, org_id)
            trans_names = {t.id: t.name for t in transformations}

            rows = svc.list_runs(
                session,
                org_id,
                status=status_filter,
                target_type=target_type_filter,
                limit=100,
            )
            self.runs = [
                RunItem(
                    id=r.id,
                    target_type=r.target_type.value,
                    target_id=r.target_id,
                    target_name=self._resolve_target_name(
                        r.target_type.value, r.target_id, pipe_names, trans_names
                    ),
                    status=r.status.value,
                    started_at=str(r.started_at) if r.started_at else "",
                    finished_at=str(r.finished_at) if r.finished_at else "",
                    rows_loaded=r.rows_loaded or 0,
                    error_message=r.error_message or "",
                )
                for r in rows
            ]
        self.error_message = ""

    @staticmethod
    def _resolve_target_name(
        target_type: str, target_id: int, pipe_names: dict, trans_names: dict
    ) -> str:
        if target_type == "pipeline":
            name = pipe_names.get(target_id, f"#{target_id}")
            return f"pipeline: {name}"
        name = trans_names.get(target_id, f"#{target_id}")
        return f"transformation: {name}"

    async def set_filter(self, status: str):
        self.filter_status = status
        await self.load_runs()

    async def set_target_type_filter(self, target_type: str):
        self.filter_target_type = target_type
        await self.load_runs()
