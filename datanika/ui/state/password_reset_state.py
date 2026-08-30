"""State behind /forgot-password and /reset-password (core#623).

Two things about this file are deliberate and easy to undo by accident.

**No password is ever a state var.** Every field below is a flag, an echoed
address, or the opaque token. The forms use ``rx.form`` + ``on_submit``, so the
plaintext arrives once in a form payload and is gone when the handler returns.
The controlled ``value=``/``on_change=`` pattern used elsewhere in the app would
ship each keystroke to the server and then hold the finished password in
server-side Reflex state for the life of the session.

**The responses are opaque.** A registered address, an unregistered address and
an address whose hourly bucket is spent all reach exactly the same screen. The
dead end that opacity usually creates is removed with copy instead — the address
is echoed back, the TTL is stated, and there is a link to /signup — because the
common complaint about opaque reset forms is really a complaint about having
nowhere to go next.
"""

import logging

import reflex as rx

from datanika.config import settings
from datanika.services.auth import AuthService
from datanika.services.client_ip import resolve_client_ip
from datanika.services.password_reset_service import PasswordResetService
from datanika.services.rate_limit_service import RateLimitService
from datanika.services.user_service import UserService, UserServiceError
from datanika.tasks.email_tasks import send_password_reset_email_task as send_reset_email
from datanika.ui.state.base_state import get_sync_session

logger = logging.getLogger(__name__)

_limiter = RateLimitService()

# D5. Three buckets, one hour each.
_EMAIL_LIMIT = 3
_IP_LIMIT = 10
_CONSUME_LIMIT = 20
_WINDOW = 3600


def _allow(bucket: str, limit: int) -> bool:
    """Whether ``bucket`` is under its hourly limit.

    Redis failures propagate, matching the API limiter: a limiter that fails
    open is not a limiter. The caller turns the exception into the same generic
    message everything else on this page produces.
    """
    return _limiter.check_window(bucket, limit, window_seconds=_WINDOW).allowed


class PasswordResetState(rx.State):
    # /forgot-password
    submitted: bool = False
    submitted_email: str = ""
    unavailable: bool = False

    # /reset-password
    token: str = ""
    token_valid: bool = False
    token_checked: bool = False

    error: str = ""

    def _service(self) -> PasswordResetService:
        return PasswordResetService(UserService(AuthService(settings.secret_key)))

    def _client_ip(self) -> str:
        """The caller's address, or "" when it cannot be established.

        Empty means the IP buckets are skipped. See ``services/client_ip.py``:
        in production every socket peer is ``127.0.0.1``, so a limiter that
        trusts it locks out the whole internet on the tenth request.
        """
        try:
            return resolve_client_ip(dict(self.router.headers.raw_headers))
        except Exception:
            return ""

    def check_availability(self):
        """D9: an instance with no SMTP can only ever say "check your inbox"."""
        self.unavailable = not settings.smtp_host

    def reset_form(self):
        self.submitted = False
        self.submitted_email = ""
        self.error = ""

    def clear_error(self):
        self.error = ""

    # -- Screen 1: request a link --------------------------------------

    def request_reset(self, form_data: dict):
        self.error = ""
        email = (form_data.get("email") or "").strip().lower()
        if not email:
            self.error = "Enter your email address."
            return

        if not settings.smtp_host:
            self.unavailable = True
            return

        client_ip = self._client_ip()

        try:
            # The IP bucket says nothing about any account, so refusing it out
            # loud is safe. It is checked first so an over-limit source never
            # reaches the account lookup at all.
            if client_ip and not _allow(PasswordResetService.ip_bucket(client_ip), _IP_LIMIT):
                self.error = "Too many requests from this address. Try again later."
                return

            # The email bucket is the opposite: a visible refusal scoped to an
            # address is an oracle — send four, watch the fourth differ, and you
            # have learned the address is real. So over-limit here is silent and
            # renders the ordinary confirmation.
            email_ok = _allow(PasswordResetService.email_bucket(email), _EMAIL_LIMIT)

            if email_ok:
                with get_sync_session() as session:
                    token = self._service().request_reset(session, email)
                    session.commit()
                if token:
                    send_reset_email.delay(email, token)
        except Exception:
            # Never name the failure: a distinguishable error is the same oracle
            # by another route. Log it; show the user the ordinary screen.
            logger.exception("Password reset request failed")

        self.submitted = True
        self.submitted_email = email

    # -- Screen 3: set the new password --------------------------------

    def load_token(self):
        """Validate the token for rendering only. **Never consumes it** (D3).

        Corporate mail security prefetches every URL in an inbound message, so a
        consuming GET means the scanner burns the token and the recipient's own
        click always lands on "already used" — a bug that reproduces only for
        users at companies with mail scanning, and never for us.
        """
        self.error = ""
        raw = self.router.page.params.get("token", "")
        if isinstance(raw, list):
            raw = raw[0] if raw else ""
        self.token = raw
        self.token_checked = True
        try:
            with get_sync_session() as session:
                self.token_valid = self._service().validate_token(session, raw) is not None
        except Exception:
            logger.exception("Password reset token validation failed")
            self.token_valid = False

        # Drop the token from the address bar, browser history and any Referer.
        return rx.call_script("window.history.replaceState({}, '', '/reset-password')")

    def submit_new_password(self, form_data: dict):
        self.error = ""
        password = form_data.get("password") or ""
        confirm = form_data.get("confirm") or ""

        # Client-side typo catch only. Never re-checked server-side as though it
        # were a security control, because it is not one.
        if password != confirm:
            self.error = "The two passwords do not match."
            return

        client_ip = self._client_ip()
        try:
            if client_ip and not _allow(
                PasswordResetService.consume_bucket(client_ip), _CONSUME_LIMIT
            ):
                self.error = "Too many attempts from this address. Try again later."
                return

            with get_sync_session() as session:
                user = self._service().consume_token(session, self.token, password)
                if user is None:
                    session.rollback()
                    # One state for expired, used, superseded and never-existed:
                    # distinguishing them tells an attacker which tokens were real.
                    self.token_valid = False
                    return
                session.commit()
        except UserServiceError as exc:
            # Password-rule messages are curated and safe to surface; the token
            # is untouched, so a typo does not cost a trip through the mailbox.
            self.error = str(exc)
            return
        except Exception:
            logger.exception("Password reset consumption failed")
            self.error = "Could not set your password. Please try again."
            return

        self.token = ""
        # D4: never auto-sign-in. An emailed link that produces a live session
        # makes the email itself a bearer credential, and it skips the one
        # moment where the user demonstrates they know what they just set.
        return rx.redirect("/login?reset=1")
