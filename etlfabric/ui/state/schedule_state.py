"""Schedule state for Reflex UI."""

import reflex as rx

from etlfabric.config import settings
from etlfabric.models.dependency import NodeType
from etlfabric.services.connection_service import ConnectionService
from etlfabric.services.encryption import EncryptionService
from etlfabric.services.pipeline_service import PipelineService
from etlfabric.services.schedule_service import ScheduleService
from etlfabric.services.transformation_service import TransformationService
from etlfabric.ui.state.base_state import BaseState, get_sync_session


class ScheduleItem(rx.Base):
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

    def _get_service(self) -> ScheduleService:
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        pipe_svc = PipelineService(conn_svc)
        transform_svc = TransformationService()
        return ScheduleService(pipe_svc, transform_svc)

    def load_schedules(self):
        svc = self._get_service()
        with get_sync_session() as session:
            rows = svc.list_schedules(session, self.org_id)
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

    def create_schedule(self):
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
                    self.org_id,
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
        self.load_schedules()

    def toggle_schedule(self, schedule_id: int):
        svc = self._get_service()
        with get_sync_session() as session:
            svc.toggle_active(session, self.org_id, schedule_id)
            session.commit()
        self.load_schedules()

    def delete_schedule(self, schedule_id: int):
        svc = self._get_service()
        with get_sync_session() as session:
            svc.delete_schedule(session, self.org_id, schedule_id)
            session.commit()
        self.load_schedules()
