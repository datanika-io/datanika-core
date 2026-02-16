"""Base state with auth-based org_id and sync session helper."""

import reflex as rx
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from datanika.config import settings

_engine = create_engine(settings.database_url_sync)


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
