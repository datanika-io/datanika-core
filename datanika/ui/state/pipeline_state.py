"""Pipeline state for Reflex UI — dbt pipeline orchestration."""

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.dependency import NodeType
from datanika.models.pipeline import DbtCommand
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.pipeline_service import PipelineService
from datanika.services.transformation_service import TransformationService
from datanika.tasks.pipeline_tasks import run_pipeline_task
from datanika.ui.state.base_state import BaseState, get_sync_session
from datanika.ui.state.connection_state import DESTINATION_TYPES


class PipelineItem(BaseModel):
    id: int = 0
    name: str = ""
    description: str = ""
    command: str = ""
    connection_name: str = ""
    model_count: int = 0
    status: str = ""
    full_refresh: bool = False
    last_run_status: str = ""


class ModelEntry(BaseModel):
    name: str = ""
    upstream: bool = False
    downstream: bool = False


class PipelineState(BaseState):
    pipelines: list[PipelineItem] = []
    dest_conn_options: list[str] = []
    # Form fields
    form_name: str = ""
    form_description: str = ""
    form_dest_id: str = ""
    form_command: str = "run"
    form_full_refresh: bool = False
    form_custom_selector: str = ""
    form_models: list[ModelEntry] = []
    # Model add field
    form_new_model_name: str = ""
    # Model autocomplete
    all_model_names: list[str] = []
    model_suggestions: list[str] = []
    show_model_suggestions: bool = False
    model_suggestion_index: int = -1
    model_warning: str = ""
    # 0 = creating new, >0 = editing existing
    editing_pipeline_id: int = 0

    def set_form_name(self, value: str):
        self.form_name = value

    def set_form_description(self, value: str):
        self.form_description = value

    async def set_form_dest_id(self, value: str):
        self.form_dest_id = value
        await self._load_model_names()

    def set_form_command(self, value: str):
        self.form_command = value

    def set_form_full_refresh(self, value: bool):
        self.form_full_refresh = value

    def set_form_custom_selector(self, value: str):
        self.form_custom_selector = value

    def set_form_new_model_name(self, value: str):
        self.form_new_model_name = value
        self.model_warning = ""
        if value.strip():
            query = value.strip().lower()
            self.model_suggestions = [n for n in self.all_model_names if query in n.lower()]
            self.show_model_suggestions = len(self.model_suggestions) > 0
            self.model_suggestion_index = 0 if self.model_suggestions else -1
        else:
            self.model_suggestions = []
            self.show_model_suggestions = False
            self.model_suggestion_index = -1

    def select_model_suggestion(self, name: str):
        self.form_new_model_name = name
        self.model_suggestions = []
        self.show_model_suggestions = False
        self.model_suggestion_index = -1
        self.model_warning = ""

    def model_nav_up(self):
        if not self.show_model_suggestions or not self.model_suggestions:
            return
        self.model_suggestion_index = max(self.model_suggestion_index - 1, 0)

    def model_nav_down(self):
        if not self.show_model_suggestions or not self.model_suggestions:
            return
        self.model_suggestion_index = min(
            self.model_suggestion_index + 1, len(self.model_suggestions) - 1
        )

    def model_select_current(self):
        if self.show_model_suggestions and 0 <= self.model_suggestion_index < len(
            self.model_suggestions
        ):
            self.select_model_suggestion(self.model_suggestions[self.model_suggestion_index])
        else:
            self.add_model()

    def model_dismiss(self):
        self.show_model_suggestions = False
        self.model_suggestions = []
        self.model_suggestion_index = -1

    def add_model(self):
        if not self.form_new_model_name.strip():
            return
        name = self.form_new_model_name.strip()
        if self.all_model_names and name not in self.all_model_names:
            self.model_warning = name
        else:
            self.model_warning = ""
        self.form_models.append(ModelEntry(name=name))
        self.form_new_model_name = ""
        self.model_suggestions = []
        self.show_model_suggestions = False
        self.model_suggestion_index = -1

    def remove_model(self, index: int):
        if 0 <= index < len(self.form_models):
            self.form_models.pop(index)

    def toggle_model_upstream(self, index: int):
        if 0 <= index < len(self.form_models):
            m = self.form_models[index]
            self.form_models[index] = ModelEntry(
                name=m.name, upstream=not m.upstream, downstream=m.downstream
            )

    def toggle_model_downstream(self, index: int):
        if 0 <= index < len(self.form_models):
            m = self.form_models[index]
            self.form_models[index] = ModelEntry(
                name=m.name, upstream=m.upstream, downstream=not m.downstream
            )

    def _get_services(self):
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        pipeline_svc = PipelineService()
        return pipeline_svc, conn_svc

    async def _load_model_names(self):
        try:
            dst_id = int(self.form_dest_id.split(" — ")[0])
        except (ValueError, IndexError, AttributeError):
            self.all_model_names = []
            self.model_suggestions = []
            self.show_model_suggestions = False
            return
        org_id = await self._get_org_id()
        if not org_id:
            return
        svc = TransformationService()
        with get_sync_session() as session:
            transformations = svc.list_transformations(session, org_id)
        self.all_model_names = [
            t.name for t in transformations if t.destination_connection_id == dst_id
        ]
        self.model_suggestions = []
        self.show_model_suggestions = False

    async def load_pipelines(self):
        org_id = await self._get_org_id()
        pipeline_svc, conn_svc = self._get_services()
        exec_svc = ExecutionService()
        with get_sync_session() as session:
            conns = conn_svc.list_connections(session, org_id)
            conn_names = {c.id: f"{c.name} ({c.connection_type.value})" for c in conns}
            rows = pipeline_svc.list_pipelines(session, org_id)
            items = []
            for p in rows:
                runs = exec_svc.list_runs(
                    session, org_id, target_type=NodeType.PIPELINE, target_id=p.id, limit=1
                )
                last_status = runs[0].status.value if runs else ""
                items.append(
                    PipelineItem(
                        id=p.id,
                        name=p.name,
                        description=p.description or "",
                        command=p.command.value,
                        connection_name=conn_names.get(
                            p.destination_connection_id, f"#{p.destination_connection_id}"
                        ),
                        model_count=len(p.models) if p.models else 0,
                        status=p.status.value,
                        full_refresh=p.full_refresh,
                        last_run_status=last_status,
                    )
                )
            self.pipelines = items
            self.dest_conn_options = [
                f"{c.id} — {c.name} ({c.connection_type.value})"
                for c in conns
                if c.connection_type.value in DESTINATION_TYPES
            ]
        self.error_message = ""
        await self._load_model_names()

    async def save_pipeline(self):
        if not self.form_name.strip():
            self.error_message = "Pipeline name cannot be empty"
            return
        org_id = await self._get_org_id()
        pipeline_svc, _ = self._get_services()
        try:
            dst_id = int(self.form_dest_id.split(" — ")[0])
        except (ValueError, IndexError):
            self.error_message = "Please select a destination connection"
            return

        models = [
            {"name": m.name, "upstream": m.upstream, "downstream": m.downstream}
            for m in self.form_models
        ]

        try:
            command = DbtCommand(self.form_command)
        except ValueError:
            self.error_message = f"Invalid command: {self.form_command}"
            return

        try:
            with get_sync_session() as session:
                if self.editing_pipeline_id:
                    pipeline_svc.update_pipeline(
                        session,
                        org_id,
                        self.editing_pipeline_id,
                        name=self.form_name,
                        description=self.form_description or None,
                        destination_connection_id=dst_id,
                        command=command,
                        full_refresh=self.form_full_refresh,
                        models=models,
                        custom_selector=self.form_custom_selector or None,
                    )
                else:
                    pipeline_svc.create_pipeline(
                        session,
                        org_id,
                        self.form_name,
                        self.form_description or None,
                        dst_id,
                        command,
                        full_refresh=self.form_full_refresh,
                        models=models,
                        custom_selector=self.form_custom_selector or None,
                    )
                session.commit()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to save pipeline")
            return
        self._reset_form()
        await self.load_pipelines()

    def _reset_form(self):
        self.editing_pipeline_id = 0
        self.form_name = ""
        self.form_description = ""
        self.form_dest_id = ""
        self.form_command = "run"
        self.form_full_refresh = False
        self.form_custom_selector = ""
        self.form_models = []
        self.form_new_model_name = ""
        self.all_model_names = []
        self.model_suggestions = []
        self.show_model_suggestions = False
        self.model_suggestion_index = -1
        self.model_warning = ""
        self.error_message = ""

    def _populate_form_from_pipeline(self, pipeline, conn_options_dst):
        self.form_name = pipeline.name
        self.form_description = pipeline.description or ""
        self.form_command = pipeline.command.value
        self.form_full_refresh = pipeline.full_refresh
        self.form_custom_selector = pipeline.custom_selector or ""
        self.error_message = ""

        dst_prefix = f"{pipeline.destination_connection_id} — "
        self.form_dest_id = next((o for o in conn_options_dst if o.startswith(dst_prefix)), "")

        models = pipeline.models or []
        self.form_models = [
            ModelEntry(
                name=m.get("name", ""),
                upstream=m.get("upstream", False),
                downstream=m.get("downstream", False),
            )
            for m in models
        ]

    async def edit_pipeline(self, pipeline_id: int):
        org_id = await self._get_org_id()
        pipeline_svc, conn_svc = self._get_services()
        with get_sync_session() as session:
            pipeline = pipeline_svc.get_pipeline(session, org_id, pipeline_id)
            if pipeline is None:
                self.error_message = "Pipeline not found"
                return
            conns = conn_svc.list_connections(session, org_id)
            dst_opts = [
                f"{c.id} — {c.name} ({c.connection_type.value})"
                for c in conns
                if c.connection_type.value in DESTINATION_TYPES
            ]
            self.dest_conn_options = dst_opts
            self._populate_form_from_pipeline(pipeline, dst_opts)
        self.editing_pipeline_id = pipeline_id
        await self._load_model_names()

    async def copy_pipeline(self, pipeline_id: int):
        org_id = await self._get_org_id()
        pipeline_svc, conn_svc = self._get_services()
        with get_sync_session() as session:
            pipeline = pipeline_svc.get_pipeline(session, org_id, pipeline_id)
            if pipeline is None:
                self.error_message = "Pipeline not found"
                return
            conns = conn_svc.list_connections(session, org_id)
            dst_opts = [
                f"{c.id} — {c.name} ({c.connection_type.value})"
                for c in conns
                if c.connection_type.value in DESTINATION_TYPES
            ]
            self.dest_conn_options = dst_opts
            self._populate_form_from_pipeline(pipeline, dst_opts)
        self.form_name = f"{self.form_name} copy"
        self.editing_pipeline_id = 0
        await self._load_model_names()

    def cancel_edit(self):
        self._reset_form()

    async def delete_pipeline(self, pipeline_id: int):
        org_id = await self._get_org_id()
        pipeline_svc, _ = self._get_services()
        with get_sync_session() as session:
            pipeline_svc.delete_pipeline(session, org_id, pipeline_id)
            session.commit()
        await self.load_pipelines()

    async def run_pipeline(self, pipeline_id: int):
        org_id = await self._get_org_id()
        exec_svc = ExecutionService()
        with get_sync_session() as session:
            run = exec_svc.create_run(session, org_id, NodeType.PIPELINE, pipeline_id)
            session.commit()
            run_id = run.id
        run_pipeline_task.delay(run_id=run_id, org_id=org_id)
        self.error_message = ""
