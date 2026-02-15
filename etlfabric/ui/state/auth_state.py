"""Authentication state — login, signup, logout, org switching."""

import re

import reflex as rx
from pydantic import BaseModel

from etlfabric.config import settings
from etlfabric.services.auth import AuthService
from etlfabric.services.user_service import UserService
from etlfabric.ui.state.base_state import get_sync_session


class UserInfo(BaseModel):
    id: int = 0
    email: str = ""
    full_name: str = ""


class OrgInfo(BaseModel):
    id: int = 0
    name: str = ""
    slug: str = ""


def _slugify(text: str) -> str:
    """Simple slug from text: lowercase, replace non-alnum with hyphens."""
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "org"


class AuthState(rx.State):

    access_token: str = ""
    refresh_token: str = ""
    current_user: UserInfo = UserInfo()
    current_org: OrgInfo = OrgInfo()
    user_orgs: list[OrgInfo] = []

    auth_error: str = ""

    def clear_auth_error(self):
        self.auth_error = ""

    @rx.var
    def is_authenticated(self) -> bool:
        return self.access_token != ""

    @rx.var
    def org_id(self) -> int:
        return self.current_org.id if self.current_org.id else 0

    def _get_user_service(self) -> UserService:
        auth = AuthService(settings.secret_key)
        return UserService(auth)

    def login(self, form_data: dict):
        self.auth_error = ""
        email = form_data.get("email", "")
        password = form_data.get("password", "")
        if not email or not password:
            self.auth_error = "Email and password are required"
            return
        svc = self._get_user_service()
        try:
            with get_sync_session() as session:
                result = svc.authenticate(session, email, password)
                if result is None:
                    self.auth_error = "Invalid email or password"
                    return
                user = result["user"]
                self.access_token = result["access_token"]
                self.refresh_token = result["refresh_token"]
                self.current_user = UserInfo(
                    id=user.id,
                    email=user.email,
                    full_name=user.full_name,
                )
                # Decode token to get org_id
                auth = AuthService(settings.secret_key)
                payload = auth.decode_token(self.access_token)
                org_id = payload["org_id"]
                # Load org info
                orgs = svc.get_user_orgs(session, user.id)
                self.user_orgs = [
                    OrgInfo(id=o.id, name=o.name, slug=o.slug) for o in orgs
                ]
                for o in self.user_orgs:
                    if o.id == org_id:
                        self.current_org = o
                        break
        except Exception as e:
            self.auth_error = f"Login failed: {e}"
            return
        return rx.redirect("/")

    def signup(self, form_data: dict):
        self.auth_error = ""
        email = form_data.get("email", "")
        password = form_data.get("password", "")
        full_name = form_data.get("full_name", "")
        svc = self._get_user_service()
        try:
            with get_sync_session() as session:
                user = svc.register_user(session, email, password, full_name)
                org_name = f"{full_name}'s Org"
                org_slug = _slugify(full_name)
                org = svc.create_org(session, org_name, org_slug, user.id)
                # Capture id before commit expires ORM attributes
                org_id = org.id
                session.commit()

            # Now authenticate to get tokens
            with get_sync_session() as session:
                result = svc.authenticate(session, email, password)
                if result is None:
                    self.auth_error = "Signup succeeded but login failed"
                    return
                self.access_token = result["access_token"]
                self.refresh_token = result["refresh_token"]
                self.current_user = UserInfo(
                    id=result["user"].id,
                    email=result["user"].email,
                    full_name=result["user"].full_name,
                )
                self.current_org = OrgInfo(id=org_id, name=org_name, slug=org_slug)
                self.user_orgs = [self.current_org]
        except Exception as e:
            self.auth_error = str(e)
            return
        return rx.redirect("/")

    def logout(self):
        self.access_token = ""
        self.refresh_token = ""
        self.current_user = UserInfo()
        self.current_org = OrgInfo()
        self.user_orgs = []
        self.auth_error = ""
        return rx.redirect("/login")

    def switch_org(self, org_id: int):
        self.auth_error = ""
        # Create tokens directly for the new org (password not available in state)
        auth = AuthService(settings.secret_key)
        self.access_token = auth.create_access_token(self.current_user.id, org_id)
        self.refresh_token = auth.create_refresh_token(self.current_user.id)
        # Update current org
        for o in self.user_orgs:
            if o.id == org_id:
                self.current_org = o
                break
        return rx.redirect("/")

    def handle_oauth_complete(self):
        """Extract tokens from URL query params after OAuth callback redirect."""
        params = self.router.page.params
        token = params.get("token", "")
        refresh = params.get("refresh", "")

        if not token:
            self.auth_error = "OAuth authentication failed"
            return rx.redirect("/login")

        self.access_token = token
        self.refresh_token = refresh

        # Decode token to get user_id and org_id
        auth = AuthService(settings.secret_key)
        try:
            payload = auth.decode_token(token)
        except Exception:
            self.auth_error = "Invalid authentication token"
            return rx.redirect("/login")

        user_id = payload["user_id"]
        org_id = payload["org_id"]

        svc = self._get_user_service()
        with get_sync_session() as session:
            user = svc.get_user(session, user_id)
            if user is None:
                self.auth_error = "User not found"
                return rx.redirect("/login")

            self.current_user = UserInfo(
                id=user.id, email=user.email, full_name=user.full_name
            )
            orgs = svc.get_user_orgs(session, user_id)
            self.user_orgs = [OrgInfo(id=o.id, name=o.name, slug=o.slug) for o in orgs]
            for o in self.user_orgs:
                if o.id == org_id:
                    self.current_org = o
                    break
        return rx.redirect("/")

    def check_auth(self):
        if not self.access_token:
            return rx.redirect("/login")
