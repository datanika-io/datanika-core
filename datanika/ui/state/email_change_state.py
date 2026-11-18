"""State behind /confirm-email-change (SPEC_EMAIL_CHANGE).

The page load only validates. Consumption is the button. A mail scanner that
fetches the link must not be the thing that changes the account's address.

Nothing here signs the visitor in. The link proves the new address; the next
sign-in is a separate step, with the password they already have.
"""

import logging

import reflex as rx

from datanika.config import settings
from datanika.services.auth import AuthService
from datanika.services.email_change_service import EmailChangeService
from datanika.services.user_service import UserService
from datanika.ui.state.base_state import get_sync_session

logger = logging.getLogger(__name__)


class EmailChangeState(rx.State):
    token: str = ""
    token_valid: bool = False
    confirmed: bool = False

    def _service(self) -> EmailChangeService:
        return EmailChangeService(UserService(AuthService(settings.secret_key)))

    async def load_token(self):
        """Validate for rendering. Does not write."""
        self.confirmed = False
        raw = self.router.page.params.get("token", "")
        if isinstance(raw, list):
            raw = raw[0] if raw else ""
        self.token = raw
        try:
            with get_sync_session() as session:
                self.token_valid = self._service().validate_token(session, raw) is not None
        except Exception:
            logger.exception("Email change token validation failed")
            self.token_valid = False

        # Drop the token from the address bar, history, and any Referer.
        return rx.call_script("window.history.replaceState({}, '', '/confirm-email-change')")

    async def confirm(self):
        """Spend the token and move the address. One success, then the link is dead."""
        self.confirmed = False
        try:
            with get_sync_session() as session:
                user = self._service().confirm_change(session, self.token)
                # Commit on both branches. A dead link writes nothing. A link
                # whose address was taken meanwhile claims the token and clears
                # the pending row, and that clear has to stick or the settings
                # page stays on a change that can never finish.
                session.commit()
        except Exception:
            logger.exception("Email change confirmation failed")
            self.token_valid = False
            return
        if user is None:
            self.token_valid = False
            return
        self.confirmed = True
        self.token_valid = False
