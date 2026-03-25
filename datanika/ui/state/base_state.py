"""Base state with auth-based org_id and sync session helper."""

import logging

import reflex as rx
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from datanika.config import settings

_engine = create_engine(settings.database_url_sync)
_log = logging.getLogger(__name__)

ROLE_HIERARCHY = {"owner": 4, "admin": 3, "editor": 2, "viewer": 1}


def check_role_hierarchy(current_role: str, required_role: str) -> bool:
    """Check if current_role meets or exceeds required_role."""
    return ROLE_HIERARCHY.get(current_role, 0) >= ROLE_HIERARCHY.get(required_role, 0)


def get_sync_session() -> Session:
    """Create a sync session for use in Reflex event handlers."""
    return Session(_engine)


class BaseState(rx.State):
    """Base state with org_id from AuthState available to all substates."""

    error_message: str = ""

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
    def _safe_error(exc: Exception, fallback: str = "An error occurred") -> str:
        """Return a user-safe error message. Logs the full exception."""
        _log.exception("Caught exception in state handler")
        if isinstance(exc, ValueError):
            return str(exc)
        return fallback
