"""Schedule state for Reflex UI."""

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.dependency import NodeType
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.pipeline_service import PipelineService
from datanika.services.schedule_service import ScheduleService
from datanika.services.transformation_service import TransformationService
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
    form_target_type: str = "pipeline"
    form_target_id: str = ""
    form_cron: str = ""
    form_timezone: str = "UTC"
    # 0 = creating new, >0 = editing existing schedule
    editing_schedule_id: int = 0

    def set_form_target_type(self, value: str):
        self.form_target_type = value

    def set_form_target_id(self, value: str):
        self.form_target_id = value

    def set_form_cron(self, value: str):
        self.form_cron = value

    def set_form_timezone(self, value: str):
        self.form_timezone = value

    def _get_services(self):
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        pipe_svc = PipelineService(conn_svc)
        transform_svc = TransformationService()
        return pipe_svc, transform_svc

    def _get_schedule_service(self) -> ScheduleService:
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        pipe_svc = PipelineService(conn_svc)
        transform_svc = TransformationService()
        from datanika.scheduler import scheduler_integration

        return ScheduleService(pipe_svc, transform_svc, scheduler_integration)

    def _reset_form(self):
        self.editing_schedule_id = 0
        self.form_target_type = "pipeline"
        self.form_target_id = ""
        self.form_cron = ""
        self.form_timezone = "UTC"
        self.error_message = ""

    async def load_schedules(self):
        org_id = await self._get_org_id()
        svc = self._get_schedule_service()
        pipe_svc, transform_svc = self._get_services()
        with get_sync_session() as session:
            # Build name lookups
            pipelines = pipe_svc.list_pipelines(session, org_id)
            pipe_names = {p.id: p.name for p in pipelines}
            transformations = transform_svc.list_transformations(session, org_id)
            trans_names = {t.id: t.name for t in transformations}

            rows = svc.list_schedules(session, org_id)
            self.schedules = [
                ScheduleItem(
                    id=s.id,
                    target_type=s.target_type.value,
                    target_id=s.target_id,
                    target_name=self._resolve_target_name(
                        s.target_type.value, s.target_id, pipe_names, trans_names
                    ),
                    cron_expression=s.cron_expression,
                    timezone=s.timezone,
                    is_active=s.is_active,
                )
                for s in rows
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

    async def save_schedule(self):
        org_id = await self._get_org_id()
        svc = self._get_schedule_service()
        try:
            target_id = int(self.form_target_id)
        except ValueError:
            self.error_message = "Target ID must be an integer"
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
            self.form_target_id = str(s.target_id)
            self.form_cron = s.cron_expression
            self.form_timezone = s.timezone
        self.editing_schedule_id = schedule_id
        self.error_message = ""

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
            self.form_target_id = str(s.target_id)
            self.form_cron = s.cron_expression
            self.form_timezone = s.timezone
        self.editing_schedule_id = 0
        self.error_message = ""

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
