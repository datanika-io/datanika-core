"""Pipeline state for Reflex UI."""

import reflex as rx

from etlfabric.config import settings
from etlfabric.models.dependency import NodeType
from etlfabric.services.connection_service import ConnectionService
from etlfabric.services.encryption import EncryptionService
from etlfabric.services.execution_service import ExecutionService
from etlfabric.services.pipeline_service import PipelineService
from etlfabric.ui.state.base_state import BaseState, get_sync_session


class PipelineItem(rx.Base):
    id: int = 0
    name: str = ""
    description: str = ""
    status: str = ""
    source_connection_id: int = 0
    destination_connection_id: int = 0


class PipelineState(BaseState):
    pipelines: list[PipelineItem] = []
    form_name: str = ""
    form_description: str = ""
    form_source_id: str = ""
    form_dest_id: str = ""
    form_config: str = "{}"

    def _get_services(self):
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        pipe_svc = PipelineService(conn_svc)
        return pipe_svc, conn_svc

    def load_pipelines(self):
        pipe_svc, _ = self._get_services()
        with get_sync_session() as session:
            rows = pipe_svc.list_pipelines(session, self.org_id)
            self.pipelines = [
                PipelineItem(
                    id=p.id,
                    name=p.name,
                    description=p.description or "",
                    status=p.status.value,
                    source_connection_id=p.source_connection_id,
                    destination_connection_id=p.destination_connection_id,
                )
                for p in rows
            ]
        self.error_message = ""

    def create_pipeline(self):
        import json

        pipe_svc, _ = self._get_services()
        try:
            config = json.loads(self.form_config)
        except json.JSONDecodeError:
            self.error_message = "Invalid JSON in config"
            return
        try:
            src_id = int(self.form_source_id)
            dst_id = int(self.form_dest_id)
        except ValueError:
            self.error_message = "Source and destination IDs must be integers"
            return
        try:
            with get_sync_session() as session:
                pipe_svc.create_pipeline(
                    session,
                    self.org_id,
                    self.form_name,
                    self.form_description or None,
                    src_id,
                    dst_id,
                    config,
                )
                session.commit()
        except Exception as e:
            self.error_message = str(e)
            return
        self.form_name = ""
        self.form_description = ""
        self.form_source_id = ""
        self.form_dest_id = ""
        self.form_config = "{}"
        self.error_message = ""
        self.load_pipelines()

    def delete_pipeline(self, pipeline_id: int):
        pipe_svc, _ = self._get_services()
        with get_sync_session() as session:
            pipe_svc.delete_pipeline(session, self.org_id, pipeline_id)
            session.commit()
        self.load_pipelines()

    def run_pipeline(self, pipeline_id: int):
        exec_svc = ExecutionService()
        with get_sync_session() as session:
            exec_svc.create_run(session, self.org_id, NodeType.PIPELINE, pipeline_id)
            session.commit()
        self.error_message = ""
