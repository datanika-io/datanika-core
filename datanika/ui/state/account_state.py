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

        # #673: the check below reads `current_user.id` off the state object,
        # which a tab that never navigates keeps holding long after its session
        # ended. Revalidate first, or a password is set for a session a
        # previous password change was meant to end.
        if not await self._require_live_session():
            return

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

    # ------------------------------------------------------------------
    # Account deletion — SPEC_PII_SEPARATION D9/D10, core#655
    # ------------------------------------------------------------------

    #: Set when the signed-in person is the **only** member of an org, so the dialog can
    #: say that org goes with them (``account.delete_org_too``) *before* they confirm
    #: rather than after.
    sole_member_org: str = ""

    #: Set when they are the sole **owner** of a **shared** org, which is a refusal
    #: (§9a(1)). Rendered up front with both exits named — transfer ownership, or delete
    #: the organization — because a refusal that arrives after the confirmation is the
    #: same failure as one that arrives a week later, only faster.
    blocking_org: str = ""

    delete_error: str = ""

    async def load_delete_preconditions(self):
        """Ask the service what erasing this account would do, before anything is typed.

        Deliberately not a second copy of the rule here: ``erasure_preconditions`` runs
        the same classifier ``erase_user`` runs, so the dialog cannot promise a deletion
        the service then refuses.
        """
        self.sole_member_org = ""
        self.blocking_org = ""
        auth = await self.get_state(AuthState)
        if not auth.current_user.id:
            return
        with get_sync_session() as session:
            facts = self._service().erasure_preconditions(session, auth.current_user.id)
        self.sole_member_org = facts["sole_member_org"]
        self.blocking_org = facts["blocking_org"]

    async def delete_account(self, form_data: dict):
        """Erase this account. There is no undo (D2).

        ⚠️ ``rx.form`` + ``on_submit``, for the same reason the password card uses it: the
        password variant of the confirmation *is* a password, and binding it to a state
        var would ship the plaintext on every keystroke and leave it in server-side Reflex
        state for the life of the session.

        The confirmation is **typed**, never a second button (D9), and which text is
        required depends on whether the account has ever had a password — the same
        discriminator core#623 established (``password_changed_at IS NULL``), never
        ``oauth_provider``, which is backfilled onto password accounts on first social
        login and would demand a password from someone who has none.
        """
        self.delete_error = ""
        # core#673. A Reflex handler runs against server-side state a tab keeps holding
        # long after its session ended, so revalidate before writing. It matters more here
        # than anywhere else in the codebase: this is the one handler whose write cannot
        # be undone, and `test_mutating_handlers_guard_the_session.py` caught its absence.
        if not await self._require_live_session():
            return None

        auth = await self.get_state(AuthState)
        user_id = auth.current_user.id
        if not user_id:
            return None

        typed = (form_data.get("confirmation") or "").strip()
        svc = self._service()
        with get_sync_session() as session:
            user = svc.get_user(session, user_id)
            if user is None:
                self.delete_error = "Account not found."
                return None

            if UserService.has_usable_password(user):
                auth_svc = AuthService(settings.secret_key)
                if not typed or not auth_svc.verify_password(typed, user.password_hash):
                    # The same message whether the field was empty or wrong: one that
                    # distinguished them would be a password oracle sitting on a
                    # destructive control.
                    self.delete_error = "That password is not correct."
                    return None
            else:
                org = svc.get_org_by_id(session, auth.current_org.id)
                if org is None or typed != org.name:
                    self.delete_error = "That is not the organization name."
                    return None

            try:
                svc.erase_user(session, user_id)
            except UserServiceError as exc:
                # §9a(1)'s refusal, synchronous, naming both exits.
                self.delete_error = str(exc)
                session.rollback()
                return None
            session.commit()

        # A count would be a value; the id is not personal data. Criterion 20.
        logger.info("Account erased from the settings control: user id=%s", user_id)
        # No success toast: there is no session left to render one in, and the account
        # that would have read it no longer exists.
        return AuthState.logout
