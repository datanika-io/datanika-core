"""Audit log state — list and filter."""

from pydantic import BaseModel

from datanika.models.audit_log import AuditAction
from datanika.services.audit_service import AuditService
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.base_state import BaseState, get_sync_session


class AuditLogItem(BaseModel):
    id: int = 0
    action: str = ""
    resource_type: str = ""
    resource_id: str = ""
    ip_address: str = ""
    created_at: str = ""


class AuditState(BaseState):
    logs: list[AuditLogItem] = []
    filter_action: str = ""
    filter_resource_type: str = ""

    def set_filter_action(self, value: str):
        self.filter_action = value

    def set_filter_resource_type(self, value: str):
        self.filter_resource_type = value

    async def load_audit_logs(self):
        auth_state = await self.get_state(AuthState)
        if not auth_state.current_org.id:
            return
        svc = AuditService()
        action = None
        if self.filter_action and self.filter_action != "all":
            try:
                action = AuditAction(self.filter_action)
            except ValueError:
                pass
        resource_type = None
        if self.filter_resource_type and self.filter_resource_type != "all":
            resource_type = self.filter_resource_type

        with get_sync_session() as session:
            rows = svc.list_logs(
                session,
                org_id=auth_state.current_org.id,
                action=action,
                resource_type=resource_type,
                limit=200,
            )
            self.logs = [
                AuditLogItem(
                    id=log.id,
                    action=log.action.value if hasattr(log.action, "value") else str(log.action),
                    resource_type=log.resource_type,
                    resource_id=str(log.resource_id) if log.resource_id else "",
                    ip_address=log.ip_address or "",
                    created_at=(
                        log.created_at.strftime("%Y-%m-%d %H:%M:%S") if log.created_at else ""
                    ),
                )
                for log in rows
            ]

    async def apply_filters(self):
        await self.load_audit_logs()
