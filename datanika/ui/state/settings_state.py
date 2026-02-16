"""Settings state — org profile and member management."""

from pydantic import BaseModel

from etlfabric.config import settings as app_settings
from etlfabric.services.auth import AuthService
from etlfabric.services.user_service import UserService
from etlfabric.ui.state.auth_state import AuthState
from etlfabric.ui.state.base_state import BaseState, get_sync_session


class MemberItem(BaseModel):
    id: int = 0
    user_id: int = 0
    email: str = ""
    full_name: str = ""
    role: str = ""


class SettingsState(BaseState):
    org_name: str = ""
    org_slug: str = ""
    members: list[MemberItem] = []
    invite_email: str = ""
    invite_role: str = "viewer"
    edit_org_name: str = ""
    edit_org_slug: str = ""

    def set_edit_org_name(self, value: str):
        self.edit_org_name = value

    def set_edit_org_slug(self, value: str):
        self.edit_org_slug = value

    def set_invite_email(self, value: str):
        self.invite_email = value

    def set_invite_role(self, value: str):
        self.invite_role = value

    def _get_user_service(self) -> UserService:
        auth = AuthService(app_settings.secret_key)
        return UserService(auth)

    async def load_settings(self):
        auth_state = await self.get_state(AuthState)
        if not auth_state.current_org.id:
            return
        svc = self._get_user_service()
        with get_sync_session() as session:
            org = svc.update_org(session, auth_state.current_org.id)
            if org:
                self.org_name = org.name
                self.org_slug = org.slug
                self.edit_org_name = org.name
                self.edit_org_slug = org.slug
            members = svc.list_members(session, auth_state.current_org.id)
            self.members = []
            for m in members:
                user = svc.get_user(session, m.user_id)
                self.members.append(
                    MemberItem(
                        id=m.id,
                        user_id=m.user_id,
                        email=user.email if user else "",
                        full_name=user.full_name if user else "",
                        role=m.role.value,
                    )
                )
        self.error_message = ""

    async def update_org(self):
        auth_state = await self.get_state(AuthState)
        svc = self._get_user_service()
        try:
            with get_sync_session() as session:
                svc.update_org(
                    session,
                    auth_state.current_org.id,
                    name=self.edit_org_name,
                    slug=self.edit_org_slug,
                )
                session.commit()
        except Exception as e:
            self.error_message = str(e)
            return
        self.org_name = self.edit_org_name
        self.org_slug = self.edit_org_slug
        # Update AuthState's current_org
        from etlfabric.ui.state.auth_state import OrgInfo

        auth_state.current_org = OrgInfo(
            id=auth_state.current_org.id,
            name=self.edit_org_name,
            slug=self.edit_org_slug,
        )
        self.error_message = ""

    async def add_member_by_email(self):
        auth_state = await self.get_state(AuthState)
        svc = self._get_user_service()
        try:
            with get_sync_session() as session:
                user = svc.get_user_by_email(session, self.invite_email)
                if user is None:
                    self.error_message = "User not found"
                    return
                from etlfabric.models.user import MemberRole

                svc.add_member(
                    session,
                    auth_state.current_org.id,
                    user.id,
                    MemberRole(self.invite_role),
                )
                session.commit()
        except Exception as e:
            self.error_message = str(e)
            return
        self.invite_email = ""
        self.error_message = ""
        await self.load_settings()

    async def change_member_role(self, membership_id: int, new_role: str):
        auth_state = await self.get_state(AuthState)
        svc = self._get_user_service()
        try:
            with get_sync_session() as session:
                from etlfabric.models.user import MemberRole

                svc.change_role(
                    session,
                    auth_state.current_org.id,
                    membership_id,
                    MemberRole(new_role),
                )
                session.commit()
        except Exception as e:
            self.error_message = str(e)
            return
        self.error_message = ""
        await self.load_settings()

    async def remove_member(self, membership_id: int):
        auth_state = await self.get_state(AuthState)
        svc = self._get_user_service()
        try:
            with get_sync_session() as session:
                svc.remove_member(session, auth_state.current_org.id, membership_id)
                session.commit()
        except Exception as e:
            self.error_message = str(e)
            return
        self.error_message = ""
        await self.load_settings()
