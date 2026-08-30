"""State behind the Account card on /settings (core#623, Part A).

The first *user*-scoped control on a page where everything else is org-scoped.

Like ``password_reset_state``, no password is ever a state var: the card uses
``rx.form`` + ``on_submit`` so the plaintext arrives once in a form payload
rather than per keystroke, and never lands in server-side Reflex state.
"""

import logging

from datanika.config import settings
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService, UserServiceError
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.base_state import BaseState, get_sync_session

logger = logging.getLogger(__name__)


class AccountState(BaseState):
    # Whether this account has a password a human chose, which decides between
    # "Change password" and "Set a password". Read from
    # ``UserService.has_usable_password`` — never inferred from
    # ``oauth_provider``, which is backfilled onto password accounts on first
    # social login and would strip their current-password check.
    has_password: bool = True

    success: bool = False
    error: str = ""

    def _service(self) -> UserService:
        return UserService(AuthService(settings.secret_key))

    def clear_feedback(self):
        self.success = False
        self.error = ""

    async def load_account(self):
        auth = await self.get_state(AuthState)
        if not auth.current_user.id:
            return
        with get_sync_session() as session:
            user = self._service().get_user(session, auth.current_user.id)
            if user is not None:
                self.has_password = UserService.has_usable_password(user)

    async def change_password(self, form_data: dict):
        self.success = False
        self.error = ""

        auth = await self.get_state(AuthState)
        user_id = auth.current_user.id
        if not user_id:
            self.error = "You are not signed in."
            return

        password = form_data.get("password") or ""
        confirm = form_data.get("confirm") or ""
        current = form_data.get("current_password") or ""

        # Typo catch, client-side in spirit — checked here only so a mismatch
        # never reaches the service and never writes anything.
        if password != confirm:
            self.error = "The two passwords do not match."
            return

        try:
            with get_sync_session() as session:
                self._service().change_password(
                    session, user_id, password, current_password=current
                )
                # Event only. Never the password, never the hash.
                self._audit(
                    session,
                    auth.current_org.id,
                    user_id,
                    "update",
                    "password",
                    resource_id=user_id,
                )
                session.commit()
        except UserServiceError as exc:
            # Curated, user-facing messages ("Current password is incorrect",
            # "Password must be at least 8 characters"). Safe verbatim.
            self.error = str(exc)
            return
        except Exception:
            logger.exception("Password change failed")
            self.error = "Could not update your password. Please try again."
            return

        self.has_password = True
        self.success = True
