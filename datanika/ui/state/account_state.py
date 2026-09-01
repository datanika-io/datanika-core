"""State behind the Account card on /settings (core#623, Part A).

The first *user*-scoped control on a page where everything else is org-scoped.

Like ``password_reset_state``, no password is ever a state var: the card uses
``rx.form`` + ``on_submit`` so the plaintext arrives once in a form payload
rather than per keystroke, and never lands in server-side Reflex state.
"""

import logging

from datanika.config import settings
from datanika.services.auth import AuthService
from datanika.services.email_verification import request_email_verification
from datanika.services.rate_limit_service import RateLimitService
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

    # core#700 AC4. `users.email_verified` was read in `ui/` nowhere: the one
    # hit, `AuthState.show_email_verified`, reads the `?verified=1` query param,
    # which says "you just clicked a link" rather than "your address is
    # confirmed". A user who missed that single redirect had no way to find out.
    #
    # Defaults to True so first paint, before `load_account` runs, does not
    # flash "unverified" at a verified user on every page load.
    email_verified: bool = True
    # The address the resend is aimed at, shown so the user can see *which*
    # address is unconfirmed. Display only — `resend_verification` re-reads the
    # `User` row and never trusts this.
    account_email: str = ""
    # One of queued / no_relay / failed / rate_limited, or "" when nothing has
    # been attempted. Four values rather than a bool, for the reason
    # `VerificationMailResult` has three: collapsing "this deployment has no
    # relay" into "the send failed" is what made the outcome unactionable.
    resend_state: str = ""

    success: bool = False
    error: str = ""

    def _service(self) -> UserService:
        return UserService(AuthService(settings.secret_key))

    def _rate_limiter(self) -> RateLimitService:
        return RateLimitService()

    def clear_feedback(self):
        self.success = False
        self.error = ""
        self.resend_state = ""

    async def load_account(self):
        auth = await self.get_state(AuthState)
        if not auth.current_user.id:
            return
        with get_sync_session() as session:
            user = self._service().get_user(session, auth.current_user.id)
            if user is not None:
                self.has_password = UserService.has_usable_password(user)
                self.email_verified = bool(user.email_verified)
                self.account_email = user.email

    # How many resends one account may trigger, and over what span. A window
    # measured in minutes bounds clicks, not mail; an hour bounds mail.
    RESEND_LIMIT = 3
    RESEND_WINDOW_SECONDS = 3600

    async def resend_verification(self):
        """Send the confirmation mail again, at most a few times an hour.

        Every outcome is recorded in ``resend_state`` and rendered. That is the
        point of core#700: ``request_email_verification`` never raises, so a
        caller that drops its return value produces a screen on which a failed
        send and a successful one are the same event.

        **What this is aimed at.** The destination is re-read from the ``User``
        row, never taken from ``account_email`` or any other state var, so the
        worst a signed-in caller can do is mail themselves. The issue's note
        about "any address a signed-in user can name" describes a design where
        the address is an input; this is deliberately not that design, and
        ``test_the_address_comes_from_the_user_row`` keeps it that way.

        **The limiter fails closed**, unlike ``concurrency_service``, which
        degrades to a default when its hook raises. That asymmetry is
        intentional: a permissive concurrency limit slows one org's runs, while
        a permissive mail limit spends our relay reputation, and a Redis outage
        is exactly when nobody is watching.
        """
        self.resend_state = ""
        auth = await self.get_state(AuthState)
        user_id = auth.current_user.id
        if not user_id:
            return

        with get_sync_session() as session:
            user = self._service().get_user(session, user_id)
            if user is None or user.email_verified:
                # Nothing to confirm. Silent by design: there is no state to
                # report and no action to offer, and the control is not
                # rendered for a verified account in the first place.
                return
            email = user.email

        try:
            allowed = (
                self._rate_limiter()
                .check_window(
                    f"verify-resend:{user_id}",
                    limit=self.RESEND_LIMIT,
                    window_seconds=self.RESEND_WINDOW_SECONDS,
                )
                .allowed
            )
        except Exception:
            logger.exception("Rate limiter unavailable; refusing verification resend")
            allowed = False

        if not allowed:
            self.resend_state = "rate_limited"
            return

        result = request_email_verification(user_id, email, AuthService(settings.secret_key))
        self.resend_state = result.value

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
