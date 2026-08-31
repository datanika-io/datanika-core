"""Base state with auth-based org_id and sync session helper."""

import logging

import reflex as rx
from sqlalchemy.orm import Session

from datanika.db import get_sync_session  # noqa: F401 — re-exported

_log = logging.getLogger(__name__)

ROLE_HIERARCHY = {"owner": 4, "admin": 3, "editor": 2, "viewer": 1}


def check_role_hierarchy(current_role: str, required_role: str) -> bool:
    """Check if current_role meets or exceeds required_role."""
    return ROLE_HIERARCHY.get(current_role, 0) >= ROLE_HIERARCHY.get(required_role, 0)


class BaseState(rx.State):
    """Base state with org_id from AuthState available to all substates."""

    error_message: str = ""
    is_quota_error: bool = False
    # V2 pricing pivot — QuotaExceededError metric discriminator. Populated
    # from ``getattr(exc, 'metric', '')`` so the attribute is optional on
    # cloud's QuotaExceededError; blank until Engineering adds the attr.
    # Possible values: "bytes_processed", "runs", "connections", "schedules",
    # "seats", "sso".
    quota_metric: str = ""

    async def _get_org_id(self) -> int:
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        return auth.current_org.id if auth.current_org.id else 0

    async def _check_role(self, min_role: str) -> bool:
        """Check that the session is live **and** carries at least ``min_role``.

        Two questions, in this order, because they have different answers (#673).

        #671 put session revalidation in ``AuthState.check_auth``, which runs
        from ``on_load`` — so it fires on *navigation*. A tab that is already
        open and never navigates again keeps acting on the state object it
        holds, so a mutating handler still executed for a session whose access
        token had aged out, and transitively for one a password change was meant
        to end. Every such handler already routes through here, so this is the
        one place that covers all of them.

        The common case stays free: ``_revalidate_session`` returns on a
        signature check with **no database read**, and only an aged-out token
        pays for a query. That is what makes a per-handler guard affordable, and
        it is asserted by a test.

        ⚠️ **``_get_org_id`` is deliberately NOT guarded** (#673 AC5). It is on
        the read path and is called while rendering, so revalidating there would
        put a session decision — and, on renewal, a database write — inside
        template evaluation, where the failure mode is a half-rendered page
        rather than a refused action. Page loads are already covered by
        ``check_auth``, so the exposure left is *reading* stale data in a tab
        that never navigates, which is bounded by that tab staying open. Writes
        are the thing worth stopping, and writes come through here.
        """
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)

        if not auth._revalidate_session():
            auth._clear_session()
            auth.session_expired = True
            # Not an error_message: "Permission denied. Requires admin role or
            # higher." is the wrong thing to tell somebody who needs to sign in
            # — it sends them to ask an admin for access they already have. The
            # layout renders the translated signed-out panel off the flag.
            self.error_message = ""
            return False

        role = auth.current_role
        if not check_role_hierarchy(role, min_role):
            self.error_message = f"Permission denied. Requires {min_role} role or higher."
            return False
        return True

    @staticmethod
    def _audit(
        session: Session,
        org_id: int,
        user_id: int,
        action: str,
        resource_type: str,
        resource_id: int | None = None,
        old_values: dict | None = None,
        new_values: dict | None = None,
    ):
        """Log an audit entry. Never raises — but never fails silently either."""
        try:
            from datanika.models.audit_log import AuditAction
            from datanika.services.audit_service import AuditService

            AuditService().log_action(
                session,
                org_id,
                user_id,
                AuditAction(action),
                resource_type,
                resource_id=resource_id,
                old_values=old_values,
                new_values=new_values,
            )
        except Exception:
            # The swallow is deliberate: audit logging must never break the
            # operation it describes. The LOG is what makes it safe -- without
            # it the trail can stop recording and "no audit rows" is
            # indistinguishable from "nothing happened" (core#723).
            _log.exception(
                "Audit write failed and was dropped: action=%s resource=%s org=%s user=%s",
                action,
                resource_type,
                org_id,
                user_id,
            )

    def _set_error(self, exc: Exception, fallback: str = "An error occurred") -> None:
        """Set error_message, is_quota_error, and quota_metric from an exception."""
        _log.exception("Caught exception in state handler")
        self.is_quota_error = type(exc).__name__ == "QuotaExceededError"
        self.quota_metric = (getattr(exc, "metric", None) or "") if self.is_quota_error else ""
        if isinstance(exc, ValueError):
            self.error_message = str(exc)
        else:
            self.error_message = fallback

    @staticmethod
    def _safe_error(exc: Exception, fallback: str = "An error occurred") -> str:
        """Return a user-safe error message. Logs the full exception."""
        _log.exception("Caught exception in state handler")
        if isinstance(exc, ValueError):
            return str(exc)
        return fallback
