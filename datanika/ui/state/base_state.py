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

    async def _get_org_id(self) -> int:
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        return auth.current_org.id if auth.current_org.id else 0

    async def _check_role(self, min_role: str) -> bool:
        """Check if current user has at least min_role. Sets error_message if not."""
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
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
        """Log an audit entry. Silently ignores failures."""
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
            pass  # Audit logging should never break the main operation

    def _set_error(self, exc: Exception, fallback: str = "An error occurred") -> None:
        """Set error_message and is_quota_error from an exception."""
        _log.exception("Caught exception in state handler")
        self.is_quota_error = type(exc).__name__ == "QuotaExceededError"
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
