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
    cron_expression: str = ""
    timezone: str = ""
    is_active: bool = True


class ScheduleState(BaseState):
    schedules: list[ScheduleItem] = []
    form_target_type: str = "pipeline"
    form_target_id: str = ""
    form_cron: str = ""
    form_timezone: str = "UTC"

    def set_form_target_type(self, value: str):
        self.form_target_type = value

    def set_form_target_id(self, value: str):
        self.form_target_id = value

    def set_form_cron(self, value: str):
        self.form_cron = value

    def set_form_timezone(self, value: str):
        self.form_timezone = value

    def _get_service(self) -> ScheduleService:
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        pipe_svc = PipelineService(conn_svc)
        transform_svc = TransformationService()
        from datanika.scheduler import scheduler_integration

        return ScheduleService(pipe_svc, transform_svc, scheduler_integration)

    async def load_schedules(self):
        org_id = await self._get_org_id()
        svc = self._get_service()
        with get_sync_session() as session:
            rows = svc.list_schedules(session, org_id)
            self.schedules = [
                ScheduleItem(
                    id=s.id,
                    target_type=s.target_type.value,
                    target_id=s.target_id,
                    cron_expression=s.cron_expression,
                    timezone=s.timezone,
                    is_active=s.is_active,
                )
                for s in rows
            ]
        self.error_message = ""

    async def create_schedule(self):
        org_id = await self._get_org_id()
        svc = self._get_service()
        try:
            target_id = int(self.form_target_id)
        except ValueError:
            self.error_message = "Target ID must be an integer"
            return
        try:
            with get_sync_session() as session:
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
            self.error_message = str(e)
            return
        self.form_target_id = ""
        self.form_cron = ""
        self.form_timezone = "UTC"
        self.error_message = ""
        await self.load_schedules()

    async def toggle_schedule(self, schedule_id: int):
        org_id = await self._get_org_id()
        svc = self._get_service()
        with get_sync_session() as session:
            svc.toggle_active(session, org_id, schedule_id)
            session.commit()
        await self.load_schedules()

    async def delete_schedule(self, schedule_id: int):
        org_id = await self._get_org_id()
        svc = self._get_service()
        with get_sync_session() as session:
            svc.delete_schedule(session, org_id, schedule_id)
            session.commit()
        await self.load_schedules()
