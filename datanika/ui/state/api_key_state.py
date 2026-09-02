"""API Keys state — list, create, revoke."""

from pydantic import BaseModel

from datanika.services.api_key_service import ApiKeyService
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.base_state import BaseState, get_sync_session


class ApiKeyItem(BaseModel):
    id: int = 0
    name: str = ""
    scopes: str = ""
    created_at: str = ""
    last_used_at: str = ""
    expires_at: str = ""


class ApiKeyState(BaseState):
    keys: list[ApiKeyItem] = []
    new_key_name: str = ""
    new_key_raw: str = ""
    show_create: bool = False

    def set_new_key_name(self, value: str):
        self.new_key_name = value

    def toggle_create(self):
        self.show_create = not self.show_create
        self.new_key_raw = ""

    async def load_api_keys(self):
        auth_state = await self.get_state(AuthState)
        if not auth_state.current_org.id:
            return
        svc = ApiKeyService()
        with get_sync_session() as session:
            rows = svc.list_api_keys(session, auth_state.current_org.id)
            self.keys = [
                ApiKeyItem(
                    id=k.id,
                    name=k.name,
                    scopes=", ".join(k.scopes) if k.scopes else "all",
                    created_at=k.created_at.strftime("%Y-%m-%d %H:%M") if k.created_at else "",
                    last_used_at=(
                        k.last_used_at.strftime("%Y-%m-%d %H:%M") if k.last_used_at else "Never"
                    ),
                    expires_at=(
                        k.expires_at.strftime("%Y-%m-%d %H:%M") if k.expires_at else "Never"
                    ),
                )
                for k in rows
            ]

    async def create_api_key(self):
        if not await self._check_role("admin"):
            return
        auth_state = await self.get_state(AuthState)
        if not self.new_key_name.strip():
            self.error_message = "Name is required"
            return
        svc = ApiKeyService()
        try:
            with get_sync_session() as session:
                api_key, raw_key = svc.create_api_key(
                    session,
                    org_id=auth_state.current_org.id,
                    user_id=auth_state.current_user.id,
                    name=self.new_key_name.strip(),
                )
                self._audit(
                    session,
                    auth_state.current_org.id,
                    auth_state.current_user.id,
                    "create",
                    "api_key",
                    resource_id=api_key.id,
                    new_values={"name": self.new_key_name.strip()},
                )
                session.commit()
            self.new_key_raw = raw_key
            self.new_key_name = ""
            self.error_message = ""
            await self.load_api_keys()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to create API key")
            return
        yield await self._saved_toast("api_keys.created_toast", "API key created")

    async def revoke_api_key(self, key_id: int):
        if not await self._check_role("admin"):
            return
        auth_state = await self.get_state(AuthState)
        svc = ApiKeyService()
        try:
            with get_sync_session() as session:
                key_info = next((k for k in self.keys if k.id == key_id), None)
                old_values = {"name": key_info.name} if key_info else {}
                svc.revoke_api_key(session, auth_state.current_org.id, key_id)
                self._audit(
                    session,
                    auth_state.current_org.id,
                    auth_state.current_user.id,
                    "delete",
                    "api_key",
                    resource_id=key_id,
                    old_values=old_values,
                )
                session.commit()
            self.error_message = ""
            await self.load_api_keys()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to revoke API key")
            # Without this the success toast below fires on the failure path
            # too — a signal that looks identical whether or not the key was
            # revoked, which is the defect core#851 exists to remove.
            return
        yield await self._deleted_toast("api_keys.revoked_toast", "API key revoked")
