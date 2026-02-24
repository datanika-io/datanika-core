"""Schedule state for Reflex UI."""

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.dependency import NodeType
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.pipeline_service import PipelineService
from datanika.services.schedule_service import ScheduleService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService
from datanika.ui.state.base_state import BaseState, get_sync_session


class ScheduleItem(BaseModel):
    id: int = 0
    target_type: str = ""
    target_id: int = 0
    target_name: str = ""
    cron_expression: str = ""
    timezone: str = ""
    is_active: bool = True


class ScheduleState(BaseState):
    schedules: list[ScheduleItem] = []
    form_target_type: str = "upload"
    form_target_name: str = ""
    form_cron: str = ""
    form_timezone: str = "UTC"
    # 0 = creating new, >0 = editing existing schedule
    editing_schedule_id: int = 0
    # Target combobox
    target_options: list[str] = []
    target_suggestions: list[str] = []
    show_target_suggestions: bool = False
    target_suggestion_index: int = -1
    _target_name_to_id: dict[str, int] = {}

    async def set_form_target_type(self, value: str):
        self.form_target_type = value
        self.form_target_name = ""
        await self._load_target_options()

    def set_form_target_name(self, value: str):
        self.form_target_name = value
        if value.strip():
            query = value.strip().lower()
            self.target_suggestions = [n for n in self.target_options if query in n.lower()]
            self.show_target_suggestions = len(self.target_suggestions) > 0
            self.target_suggestion_index = 0 if self.target_suggestions else -1
        else:
            self.target_suggestions = list(self.target_options)
            self.show_target_suggestions = len(self.target_suggestions) > 0
            self.target_suggestion_index = 0 if self.target_suggestions else -1

    def show_target_all(self):
        self.target_suggestions = list(self.target_options)
        self.show_target_suggestions = len(self.target_suggestions) > 0
        self.target_suggestion_index = 0 if self.target_suggestions else -1

    def select_target_suggestion(self, name: str):
        self.form_target_name = name
        self.target_suggestions = []
        self.show_target_suggestions = False
        self.target_suggestion_index = -1

    def target_nav_up(self):
        if not self.show_target_suggestions or not self.target_suggestions:
            return
        self.target_suggestion_index = max(self.target_suggestion_index - 1, 0)

    def target_nav_down(self):
        if not self.show_target_suggestions or not self.target_suggestions:
            return
        self.target_suggestion_index = min(
            self.target_suggestion_index + 1, len(self.target_suggestions) - 1
        )

    def target_select_current(self):
        if self.show_target_suggestions and 0 <= self.target_suggestion_index < len(
            self.target_suggestions
        ):
            self.select_target_suggestion(self.target_suggestions[self.target_suggestion_index])

    def target_dismiss(self):
        self.show_target_suggestions = False
        self.target_suggestions = []
        self.target_suggestion_index = -1

    def set_form_cron(self, value: str):
        self.form_cron = value

    def set_form_timezone(self, value: str):
        self.form_timezone = value

    def _get_services(self):
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        transform_svc = TransformationService()
        pipeline_svc = PipelineService()
        return upload_svc, transform_svc, pipeline_svc

    def _get_schedule_service(self) -> ScheduleService:
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        transform_svc = TransformationService()
        from datanika.scheduler import scheduler_integration

        return ScheduleService(upload_svc, transform_svc, scheduler_integration)

    def _reset_form(self):
        self.editing_schedule_id = 0
        self.form_target_type = "upload"
        self.form_target_name = ""
        self.form_cron = ""
        self.form_timezone = "UTC"
        self.target_suggestions = []
        self.show_target_suggestions = False
        self.target_suggestion_index = -1
        self.error_message = ""

    async def _load_target_options(self):
        org_id = await self._get_org_id()
        upload_svc, transform_svc, pipeline_svc = self._get_services()
        name_to_id: dict[str, int] = {}
        with get_sync_session() as session:
            if self.form_target_type == "upload":
                items = upload_svc.list_uploads(session, org_id)
            elif self.form_target_type == "pipeline":
                items = pipeline_svc.list_pipelines(session, org_id)
            else:
                items = transform_svc.list_transformations(session, org_id)
            for item in items:
                name_to_id[item.name] = item.id
        self._target_name_to_id = name_to_id
        self.target_options = sorted(name_to_id.keys())
        self.target_suggestions = []
        self.show_target_suggestions = False
        self.target_suggestion_index = -1

    async def load_schedules(self):
        org_id = await self._get_org_id()
        svc = self._get_schedule_service()
        upload_svc, transform_svc, pipeline_svc = self._get_services()
        with get_sync_session() as session:
            # Build name lookups
            uploads = upload_svc.list_uploads(session, org_id)
            upload_names = {u.id: u.name for u in uploads}
            transformations = transform_svc.list_transformations(session, org_id)
            trans_names = {t.id: t.name for t in transformations}
            pipelines = pipeline_svc.list_pipelines(session, org_id)
            pipeline_names = {p.id: p.name for p in pipelines}

            rows = svc.list_schedules(session, org_id)
            self.schedules = [
                ScheduleItem(
                    id=s.id,
                    target_type=s.target_type.value,
                    target_id=s.target_id,
                    target_name=self._resolve_target_name(
                        s.target_type.value,
                        s.target_id,
                        upload_names,
                        trans_names,
                        pipeline_names,
                    ),
                    cron_expression=s.cron_expression,
                    timezone=s.timezone,
                    is_active=s.is_active,
                )
                for s in rows
            ]
        self.error_message = ""
        await self._load_target_options()

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

    async def save_schedule(self):
        org_id = await self._get_org_id()
        svc = self._get_schedule_service()
        target_id = self._target_name_to_id.get(self.form_target_name)
        if target_id is None:
            self.error_message = "Target not found — select a name from the list"
            return
        try:
            with get_sync_session() as session:
                if self.editing_schedule_id:
                    svc.update_schedule(
                        session,
                        org_id,
                        self.editing_schedule_id,
                        target_type=NodeType(self.form_target_type),
                        target_id=target_id,
                        cron_expression=self.form_cron,
                        timezone=self.form_timezone,
                    )
                else:
                    svc.create_schedule(
                        session,
                        org_id,
                        NodeType(self.form_target_type),
                        target_id,
                        self.form_cron,
                        timezone=self.form_timezone,
                    )
                session.commit()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to save schedule")
            return
        self._reset_form()
        await self.load_schedules()

    async def edit_schedule(self, schedule_id: int):
        """Load a schedule into the form for editing."""
        org_id = await self._get_org_id()
        svc = self._get_schedule_service()
        with get_sync_session() as session:
            s = svc.get_schedule(session, org_id, schedule_id)
            if s is None:
                self.error_message = "Schedule not found"
                return
            self.form_target_type = s.target_type.value
            self.form_cron = s.cron_expression
            self.form_timezone = s.timezone
        self.editing_schedule_id = schedule_id
        self.error_message = ""
        await self._load_target_options()
        # Resolve target_id back to name
        id_to_name = {v: k for k, v in self._target_name_to_id.items()}
        self.form_target_name = id_to_name.get(s.target_id, "")

    async def copy_schedule(self, schedule_id: int):
        """Load a schedule into the form as a new copy."""
        org_id = await self._get_org_id()
        svc = self._get_schedule_service()
        with get_sync_session() as session:
            s = svc.get_schedule(session, org_id, schedule_id)
            if s is None:
                self.error_message = "Schedule not found"
                return
            self.form_target_type = s.target_type.value
            self.form_cron = s.cron_expression
            self.form_timezone = s.timezone
        self.editing_schedule_id = 0
        self.error_message = ""
        await self._load_target_options()
        # Resolve target_id back to name
        id_to_name = {v: k for k, v in self._target_name_to_id.items()}
        self.form_target_name = id_to_name.get(s.target_id, "")

    def cancel_edit(self):
        """Cancel editing and reset the form."""
        self._reset_form()

    async def toggle_schedule(self, schedule_id: int):
        org_id = await self._get_org_id()
        svc = self._get_schedule_service()
        with get_sync_session() as session:
            svc.toggle_active(session, org_id, schedule_id)
            session.commit()
        await self.load_schedules()

    async def delete_schedule(self, schedule_id: int):
        org_id = await self._get_org_id()
        svc = self._get_schedule_service()
        with get_sync_session() as session:
            svc.delete_schedule(session, org_id, schedule_id)
            session.commit()
        await self.load_schedules()
