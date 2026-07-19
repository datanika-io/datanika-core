"""Per-request session resolution for the Datanika MCP tool surface.

The stdio server runs one process per user, so a single process-global
``(client, allow_write)`` pair was enough. A *remote* Streamable-HTTP server
(``SPEC_REMOTE_MCP.md`` P1) serves many orgs from one process and must
resolve those two values **per request** instead — otherwise one caller's
credential and write-grant would leak into another's tool call.

``DatanikaSession`` bundles the two values, and ``use_session`` binds one to
a :class:`contextvars.ContextVar` for the duration of a request. Tool bodies
read the active session via ``server._session()``, which prefers the
contextvar and falls back to the module globals — so the stdio path (and the
existing test-suite that sets those globals directly) keeps working unchanged.

This module is deliberately transport-agnostic and imports nothing from the
``mcp`` / FastMCP SDK: it is the version-insensitive foundation the remote
transport is built on. Reading the FastMCP ``Context`` / bearer token and
calling ``use_session`` is the transport layer's job, added in a later phase.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from datanika_mcp.client import DatanikaClient

__all__ = ["DatanikaSession", "current_session", "use_session"]


@dataclass(frozen=True)
class DatanikaSession:
    """The two values every tool needs, resolved per request.

    ``client`` — an authenticated :class:`DatanikaClient` scoped to the
    caller's org (a fixed CLI key on stdio; a per-request key on remote).
    May be ``None`` only in the narrow "check write permission before a
    client exists" path; any session handed to a tool has a live client.

    ``allow_write`` — whether mutation tools are permitted for this session
    (the stdio ``--allow-write`` flag; the OAuth write-consent grant on
    remote).
    """

    client: DatanikaClient | None = None
    allow_write: bool = False

    def require_write(self, action: str) -> None:
        """Raise ``RuntimeError`` if this session may not perform ``action``."""
        if not self.allow_write:
            raise RuntimeError(
                f"Write access required for '{action}'. "
                "Restart the server with --allow-write to enable mutations."
            )


# Per-request binding. Unset (``None``) on stdio and in unit tests, where the
# module globals in ``server.py`` are the source of truth instead.
_current_session: ContextVar[DatanikaSession | None] = ContextVar(
    "datanika_mcp_session", default=None
)


def current_session() -> DatanikaSession | None:
    """Return the :class:`DatanikaSession` bound to the current context, if any."""
    return _current_session.get()


@contextmanager
def use_session(session: DatanikaSession) -> Iterator[DatanikaSession]:
    """Bind ``session`` to the current context for the duration of the block.

    The remote transport wraps each request in this; unit tests use it to
    exercise the per-request path without mutating the module globals.
    """
    token = _current_session.set(session)
    try:
        yield session
    finally:
        _current_session.reset(token)
