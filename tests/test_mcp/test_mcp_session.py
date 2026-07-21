"""Tests for the de-globalized MCP tool surface (core#363, Remote-MCP P1).

Proves the tool surface resolves a per-request ``DatanikaSession``: a
contextvar binding wins over the module globals, and the globals remain the
stdio / legacy fallback. Companion to ``test_mcp_server.py``, which pins the
tool registry and the module-global path.
"""

import sys
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture(autouse=True)
def _add_mcp_to_path():
    """Make ``datanika_mcp`` importable from the MCP sub-package."""
    import pathlib

    mcp_src = str(pathlib.Path(__file__).resolve().parents[2] / "datanika-mcp" / "src")
    if mcp_src not in sys.path:
        sys.path.insert(0, mcp_src)


@pytest.fixture
def srv():
    """The server module with its process globals snapshotted and restored.

    De-globalization keeps ``_client`` / ``_allow_write`` as the stdio
    fallback; these tests mutate them to prove per-request precedence, so we
    restore the originals afterwards to stay hermetic across the file.
    """
    import datanika_mcp.server as server

    saved_client, saved_allow = server._client, server._allow_write
    try:
        yield server
    finally:
        server._client, server._allow_write = saved_client, saved_allow


class TestSessionResolution:
    async def test_bound_session_wins_over_globals(self, srv):
        from datanika_mcp.session import DatanikaSession, use_session

        global_client = AsyncMock(name="global")
        bound_client = AsyncMock(name="bound")
        bound_client.list_connections.return_value = {"items": ["bound"]}
        srv._client = global_client
        srv._allow_write = False

        with use_session(DatanikaSession(client=bound_client, allow_write=True)):
            result = await srv.list_connections()

        bound_client.list_connections.assert_awaited_once()
        global_client.list_connections.assert_not_awaited()
        assert "bound" in result

    async def test_globals_used_when_no_session_bound(self, srv):
        client = AsyncMock()
        client.list_uploads.return_value = {"items": []}
        srv._client = client

        result = await srv.list_uploads()

        client.list_uploads.assert_awaited_once()
        assert '"items"' in result

    def test_session_reset_after_context_exit(self, srv):
        from datanika_mcp.session import DatanikaSession, current_session, use_session

        assert current_session() is None
        with use_session(DatanikaSession(client=MagicMock(), allow_write=True)):
            assert current_session() is not None
        assert current_session() is None

    async def test_uninitialized_fallback_raises(self, srv):
        srv._client = None
        srv._allow_write = False
        with pytest.raises(RuntimeError, match="not initialized"):
            await srv.list_connections()


class TestSessionWriteGuard:
    async def test_read_only_bound_session_blocks_writes(self, srv):
        from datanika_mcp.session import DatanikaSession, use_session

        client = AsyncMock()
        # Globals grant write; the bound read-only session must still win.
        srv._client = AsyncMock()
        srv._allow_write = True

        with (
            use_session(DatanikaSession(client=client, allow_write=False)),
            pytest.raises(RuntimeError, match="Write access required"),
        ):
            await srv.create_connection("t", "postgres", {"host": "h"})

        client.create_connection.assert_not_awaited()

    async def test_read_write_bound_session_allows_writes(self, srv):
        from datanika_mcp.session import DatanikaSession, use_session

        client = AsyncMock()
        client.create_connection.return_value = {"id": 7}
        # Globals have no client and forbid writes; the bound session must win.
        srv._client = None
        srv._allow_write = False

        with use_session(DatanikaSession(client=client, allow_write=True)):
            result = await srv.create_connection("t", "postgres", {"host": "h"})

        client.create_connection.assert_awaited_once_with("t", "postgres", {"host": "h"})
        assert '"id": 7' in result

    def test_require_write_shim_reads_bound_session(self, srv):
        """The back-compat ``_require_write`` shim honors the bound session."""
        from datanika_mcp.session import DatanikaSession, use_session

        srv._allow_write = False  # global default forbids writes
        with use_session(DatanikaSession(client=MagicMock(), allow_write=True)):
            srv._require_write("create_connection")  # bound grant → no raise

        with pytest.raises(RuntimeError, match="Write access required"):
            srv._require_write("create_connection")  # unbound, global False → blocked


class TestWriteRefusalWordingByTransport:
    """The refusal message must be actionable for whoever is reading it (#409).

    The gate itself is correct on both transports — this is only about the
    advice. Over the hosted endpoint there is no server the caller runs, no
    ``--allow-write`` they can pass and no restart they can perform, so the
    stdio wording is not merely unhelpful: an agent reads it as a recoverable
    condition and either retries or instructs a human to do something
    impossible. Agents are the primary consumer of this string.
    """

    def test_stdio_keeps_the_flag_advice(self, srv):
        """On stdio the advice is correct and must not regress."""
        from datanika_mcp.session import DatanikaSession

        session = DatanikaSession(allow_write=False)
        with pytest.raises(RuntimeError) as exc:
            session.require_write("create_connection")

        assert "--allow-write" in str(exc.value)

    def test_remote_does_not_tell_the_caller_to_restart_anything(self, srv):
        from datanika_mcp.session import DatanikaSession

        session = DatanikaSession(allow_write=False, transport="remote")
        with pytest.raises(RuntimeError) as exc:
            session.require_write("create_connection")

        message = str(exc.value)
        assert "Restart" not in message
        # It must not imply a client-side switch exists over the hosted path.
        assert "read-only" in message.lower()

    def test_both_keep_the_stable_prefix(self, srv):
        """Callers (and our own tests) match on this prefix — keep it stable."""
        from datanika_mcp.session import DatanikaSession

        for transport in ("stdio", "remote"):
            session = DatanikaSession(allow_write=False, transport=transport)
            with pytest.raises(RuntimeError) as exc:
                session.require_write("trigger_pipeline")
            assert str(exc.value).startswith("Write access required for 'trigger_pipeline'.")

    def test_default_transport_is_stdio(self, srv):
        """Back-compat: the stdio server constructs sessions positionally."""
        from datanika_mcp.session import DatanikaSession

        assert DatanikaSession().transport == "stdio"
