"""DAG (dependency) state for Reflex UI."""

import reflex as rx

from etlfabric.config import settings
from etlfabric.models.dependency import NodeType
from etlfabric.services.connection_service import ConnectionService
from etlfabric.services.dependency_service import DependencyService
from etlfabric.services.encryption import EncryptionService
from etlfabric.services.pipeline_service import PipelineService
from etlfabric.services.transformation_service import TransformationService
from etlfabric.ui.state.base_state import BaseState, get_sync_session


class DependencyItem(rx.Base):
    id: int = 0
    upstream_type: str = ""
    upstream_id: int = 0
    downstream_type: str = ""
    downstream_id: int = 0


class DagState(BaseState):
    dependencies: list[DependencyItem] = []
    form_upstream_type: str = "pipeline"
    form_upstream_id: str = ""
    form_downstream_type: str = "transformation"
    form_downstream_id: str = ""

    def _get_service(self) -> DependencyService:
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        pipe_svc = PipelineService(conn_svc)
        transform_svc = TransformationService()
        return DependencyService(pipe_svc, transform_svc)

    def load_dependencies(self):
        svc = self._get_service()
        with get_sync_session() as session:
            rows = svc.list_dependencies(session, self.org_id)
            self.dependencies = [
                DependencyItem(
                    id=d.id,
                    upstream_type=d.upstream_type.value,
                    upstream_id=d.upstream_id,
                    downstream_type=d.downstream_type.value,
                    downstream_id=d.downstream_id,
                )
                for d in rows
            ]
        self.error_message = ""

    def add_dependency(self):
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
                    self.org_id,
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
        self.load_dependencies()

    def remove_dependency(self, dep_id: int):
        svc = self._get_service()
        with get_sync_session() as session:
            svc.remove_dependency(session, self.org_id, dep_id)
            session.commit()
        self.load_dependencies()
