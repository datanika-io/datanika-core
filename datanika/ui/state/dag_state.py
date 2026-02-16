"""DAG (dependency) state for Reflex UI."""

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.dependency import NodeType
from datanika.services.connection_service import ConnectionService
from datanika.services.dependency_service import DependencyService
from datanika.services.encryption import EncryptionService
from datanika.services.pipeline_service import PipelineService
from datanika.services.transformation_service import TransformationService
from datanika.ui.state.base_state import BaseState, get_sync_session


class DependencyItem(BaseModel):
    id: int = 0
    upstream_type: str = ""
    upstream_id: int = 0
    upstream_name: str = ""
    downstream_type: str = ""
    downstream_id: int = 0
    downstream_name: str = ""


class DagState(BaseState):
    dependencies: list[DependencyItem] = []
    form_upstream_type: str = "pipeline"
    form_upstream_id: str = ""
    form_downstream_type: str = "transformation"
    form_downstream_id: str = ""

    def set_form_upstream_type(self, value: str):
        self.form_upstream_type = value

    def set_form_upstream_id(self, value: str):
        self.form_upstream_id = value

    def set_form_downstream_type(self, value: str):
        self.form_downstream_type = value

    def set_form_downstream_id(self, value: str):
        self.form_downstream_id = value

    def _get_service(self) -> DependencyService:
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        pipe_svc = PipelineService(conn_svc)
        transform_svc = TransformationService()
        return DependencyService(pipe_svc, transform_svc)

    @staticmethod
    def _resolve_node_name(
        node_type: str, node_id: int, pipe_names: dict, trans_names: dict
    ) -> str:
        if node_type == "pipeline":
            name = pipe_names.get(node_id, f"#{node_id}")
            return f"pipeline: {name}"
        name = trans_names.get(node_id, f"#{node_id}")
        return f"transformation: {name}"

    async def load_dependencies(self):
        org_id = await self._get_org_id()
        svc = self._get_service()

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

            rows = svc.list_dependencies(session, org_id)
            self.dependencies = [
                DependencyItem(
                    id=d.id,
                    upstream_type=d.upstream_type.value,
                    upstream_id=d.upstream_id,
                    upstream_name=self._resolve_node_name(
                        d.upstream_type.value, d.upstream_id, pipe_names, trans_names
                    ),
                    downstream_type=d.downstream_type.value,
                    downstream_id=d.downstream_id,
                    downstream_name=self._resolve_node_name(
                        d.downstream_type.value, d.downstream_id, pipe_names, trans_names
                    ),
                )
                for d in rows
            ]
        self.error_message = ""

    async def add_dependency(self):
        org_id = await self._get_org_id()
        svc = self._get_service()
        try:
            upstream_id = int(self.form_upstream_id)
            downstream_id = int(self.form_downstream_id)
        except ValueError:
            self.error_message = "IDs must be integers"
            return
        try:
            with get_sync_session() as session:
                svc.add_dependency(
                    session,
                    org_id,
                    NodeType(self.form_upstream_type),
                    upstream_id,
                    NodeType(self.form_downstream_type),
                    downstream_id,
                )
                session.commit()
        except Exception as e:
            self.error_message = str(e)
            return
        self.form_upstream_id = ""
        self.form_downstream_id = ""
        self.error_message = ""
        await self.load_dependencies()

    async def remove_dependency(self, dep_id: int):
        org_id = await self._get_org_id()
        svc = self._get_service()
        with get_sync_session() as session:
            svc.remove_dependency(session, org_id, dep_id)
            session.commit()
        await self.load_dependencies()
