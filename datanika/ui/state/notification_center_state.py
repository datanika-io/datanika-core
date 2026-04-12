"""Notification Center state — bell badge + dropdown list."""

from pydantic import BaseModel

from datanika.services.in_app_notification_service import InAppNotificationService
from datanika.ui.state.base_state import BaseState, get_sync_session

NOTIFICATION_TYPE_ICONS: dict[str, str] = {
    "run_failed": "circle-x",
    "run_succeeded": "circle-check",
    "quota_warning": "triangle-alert",
    "quota_exceeded": "octagon-alert",
}


class NotificationItem(BaseModel):
    id: int = 0
    type: str = ""
    title: str = ""
    message: str = ""
    resource_type: str = ""
    resource_id: int = 0
    read_at: str = ""
    created_at: str = ""


class NotificationCenterState(BaseState):
    notifications: list[NotificationItem] = []
    unread_count: int = 0
    dropdown_open: bool = False

    async def load_notifications(self):
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        org_id = auth.current_org.id or 0
        user_id = auth.current_user.id or 0
        if org_id == 0 or user_id == 0:
            return

        svc = InAppNotificationService()
        with get_sync_session() as session:
            items = svc.list_for_user(session, org_id, user_id, limit=10)
            self.notifications = [
                NotificationItem(
                    id=n.id,
                    type=n.type.value,
                    title=n.title,
                    message=n.message or "",
                    resource_type=n.resource_type,
                    resource_id=n.resource_id,
                    read_at=n.read_at.isoformat() if n.read_at else "",
                    created_at=n.created_at.isoformat() if n.created_at else "",
                )
                for n in items
            ]
            self.unread_count = svc.unread_count(session, org_id, user_id)

    async def load_unread_count(self):
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        org_id = auth.current_org.id or 0
        user_id = auth.current_user.id or 0
        if org_id == 0 or user_id == 0:
            return

        svc = InAppNotificationService()
        with get_sync_session() as session:
            self.unread_count = svc.unread_count(session, org_id, user_id)

    async def mark_read(self, notification_id: int):
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        org_id = auth.current_org.id or 0
        if org_id == 0:
            return

        svc = InAppNotificationService()
        with get_sync_session() as session:
            svc.mark_read(session, notification_id, org_id)
            session.commit()
        await self.load_notifications()

    async def mark_all_read(self):
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        org_id = auth.current_org.id or 0
        user_id = auth.current_user.id or 0
        if org_id == 0 or user_id == 0:
            return

        svc = InAppNotificationService()
        with get_sync_session() as session:
            svc.mark_all_read(session, org_id, user_id)
            session.commit()
        await self.load_notifications()

    async def dismiss(self, notification_id: int):
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        org_id = auth.current_org.id or 0
        if org_id == 0:
            return

        svc = InAppNotificationService()
        with get_sync_session() as session:
            svc.dismiss(session, notification_id, org_id)
            session.commit()
        await self.load_notifications()

    def toggle_dropdown(self):
        self.dropdown_open = not self.dropdown_open
