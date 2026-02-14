"""Base state with hardcoded org_id and sync session helper."""

import reflex as rx
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from etlfabric.config import settings

# Hardcoded org_id — real auth deferred to later phase
ORG_ID = 1

_engine = create_engine(settings.database_url_sync)


def get_sync_session() -> Session:
    """Create a sync session for use in Reflex event handlers."""
    return Session(_engine)


class BaseState(rx.State):
    """Base state with org_id available to all substates."""

    error_message: str = ""

    @property
    def org_id(self) -> int:
        return ORG_ID
