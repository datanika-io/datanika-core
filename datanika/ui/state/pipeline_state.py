"""Pipeline state for Reflex UI."""

import json

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.dependency import NodeType
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.pipeline_service import PipelineService
from datanika.ui.state.base_state import BaseState, get_sync_session
from datanika.ui.state.connection_state import DESTINATION_TYPES, SOURCE_TYPES


class PipelineItem(BaseModel):
    id: int = 0
    name: str = ""
    description: str = ""
    status: str = ""
    source_connection_id: int = 0
    destination_connection_id: int = 0


class PipelineState(BaseState):
    pipelines: list[PipelineItem] = []
    source_conn_options: list[str] = []
    dest_conn_options: list[str] = []
    form_name: str = ""
    form_description: str = ""
    form_source_id: str = ""
    form_dest_id: str = ""
    # Structured mode fields
    form_mode: str = "full_database"
    form_write_disposition: str = "append"
    form_primary_key: str = ""
    form_table: str = ""
    form_source_schema: str = ""
    form_table_names: str = ""
    form_batch_size: str = ""
    form_enable_incremental: bool = False
    form_cursor_path: str = ""
    form_initial_value: str = ""
    form_row_order: str = ""
    # Schema contract
    form_sc_tables: str = ""
    form_sc_columns: str = ""
    form_sc_data_type: str = ""
    # Raw JSON fallback
    form_config: str = "{}"
    form_use_raw_json: bool = False

    def set_form_name(self, value: str):
        self.form_name = value

    def set_form_description(self, value: str):
        self.form_description = value

    def set_form_source_id(self, value: str):
        self.form_source_id = value

    def set_form_dest_id(self, value: str):
        self.form_dest_id = value

    def set_form_mode(self, value: str):
        self.form_mode = value

    def set_form_write_disposition(self, value: str):
        self.form_write_disposition = value

    def set_form_primary_key(self, value: str):
        self.form_primary_key = value

    def set_form_table(self, value: str):
        self.form_table = value

    def set_form_source_schema(self, value: str):
        self.form_source_schema = value

    def set_form_table_names(self, value: str):
        self.form_table_names = value

    def set_form_batch_size(self, value: str):
        self.form_batch_size = value

    def set_form_enable_incremental(self, value: bool):
        self.form_enable_incremental = value

    def set_form_cursor_path(self, value: str):
        self.form_cursor_path = value

    def set_form_initial_value(self, value: str):
        self.form_initial_value = value

    def set_form_row_order(self, value: str):
        self.form_row_order = value

    def set_form_sc_tables(self, value: str):
        self.form_sc_tables = value

    def set_form_sc_columns(self, value: str):
        self.form_sc_columns = value

    def set_form_sc_data_type(self, value: str):
        self.form_sc_data_type = value

    def set_form_config(self, value: str):
        self.form_config = value

    def set_form_use_raw_json(self, value: bool):
        self.form_use_raw_json = value

    def _get_services(self):
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        pipe_svc = PipelineService(conn_svc)
        return pipe_svc, conn_svc

    def _build_config(self) -> dict:
        """Build dlt_config from structured form fields."""
        if self.form_use_raw_json:
            return json.loads(self.form_config)

        config: dict = {}
        config["mode"] = self.form_mode

        if self.form_write_disposition:
            config["write_disposition"] = self.form_write_disposition
        if self.form_write_disposition == "merge" and self.form_primary_key:
            config["primary_key"] = self.form_primary_key

        if self.form_source_schema:
            config["source_schema"] = self.form_source_schema

        if self.form_batch_size:
            config["batch_size"] = int(self.form_batch_size)

        if self.form_mode == "single_table":
            if self.form_table:
                config["table"] = self.form_table
            if self.form_enable_incremental and self.form_cursor_path:
                inc: dict = {"cursor_path": self.form_cursor_path}
                if self.form_initial_value:
                    inc["initial_value"] = self.form_initial_value
                if self.form_row_order:
                    inc["row_order"] = self.form_row_order
                config["incremental"] = inc
        else:  # full_database
            if self.form_table_names:
                names = [t.strip() for t in self.form_table_names.split(",") if t.strip()]
                if names:
                    config["table_names"] = names

        # Schema contract
        sc: dict = {}
        if self.form_sc_tables:
            sc["tables"] = self.form_sc_tables
        if self.form_sc_columns:
            sc["columns"] = self.form_sc_columns
        if self.form_sc_data_type:
            sc["data_type"] = self.form_sc_data_type
        if sc:
            config["schema_contract"] = sc

        return config

    async def load_pipelines(self):
        org_id = await self._get_org_id()
        pipe_svc, conn_svc = self._get_services()
        with get_sync_session() as session:
            rows = pipe_svc.list_pipelines(session, org_id)
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
            # Load connections filtered by capability
            conns = conn_svc.list_connections(session, org_id)
            self.source_conn_options = [
                f"{c.id} — {c.name} ({c.connection_type.value})"
                for c in conns
                if c.connection_type.value in SOURCE_TYPES
            ]
            self.dest_conn_options = [
                f"{c.id} — {c.name} ({c.connection_type.value})"
                for c in conns
                if c.connection_type.value in DESTINATION_TYPES
            ]
        self.error_message = ""

    async def create_pipeline(self):
        org_id = await self._get_org_id()
        pipe_svc, _ = self._get_services()
        try:
            config = self._build_config()
        except (json.JSONDecodeError, ValueError) as e:
            self.error_message = f"Invalid config: {e}"
            return
        try:
            src_id = int(self.form_source_id.split(" — ")[0])
            dst_id = int(self.form_dest_id.split(" — ")[0])
        except (ValueError, IndexError):
            self.error_message = "Please select source and destination connections"
            return
        try:
            with get_sync_session() as session:
                pipe_svc.create_pipeline(
                    session,
                    org_id,
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
        self._reset_form()
        await self.load_pipelines()

    def _reset_form(self):
        self.form_name = ""
        self.form_description = ""
        self.form_source_id = ""
        self.form_dest_id = ""
        self.form_mode = "full_database"
        self.form_write_disposition = "append"
        self.form_primary_key = ""
        self.form_table = ""
        self.form_source_schema = ""
        self.form_table_names = ""
        self.form_batch_size = ""
        self.form_enable_incremental = False
        self.form_cursor_path = ""
        self.form_initial_value = ""
        self.form_row_order = ""
        self.form_sc_tables = ""
        self.form_sc_columns = ""
        self.form_sc_data_type = ""
        self.form_config = "{}"
        self.form_use_raw_json = False
        self.error_message = ""

    async def delete_pipeline(self, pipeline_id: int):
        org_id = await self._get_org_id()
        pipe_svc, _ = self._get_services()
        with get_sync_session() as session:
            pipe_svc.delete_pipeline(session, org_id, pipeline_id)
            session.commit()
        await self.load_pipelines()

    async def run_pipeline(self, pipeline_id: int):
        org_id = await self._get_org_id()
        exec_svc = ExecutionService()
        with get_sync_session() as session:
            exec_svc.create_run(session, org_id, NodeType.PIPELINE, pipeline_id)
            session.commit()
        self.error_message = ""
