"""Tests for the Datanika MCP server (#140).

These tests verify tool registration, the write-guard, and the CLI
argument parser without requiring a live Datanika instance.

Tools are ``async def`` and await the client (core#388), so the tool-body tests
are coroutines and the mock clients are ``AsyncMock``s.
"""

import sys
from unittest.mock import AsyncMock, patch

import pytest


@pytest.fixture(autouse=True)
def _add_mcp_to_path():
    """Make ``datanika_mcp`` importable from the MCP sub-package."""
    import pathlib

    mcp_src = str(pathlib.Path(__file__).resolve().parents[2] / "datanika-mcp" / "src")
    if mcp_src not in sys.path:
        sys.path.insert(0, mcp_src)


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------


class TestToolRegistration:
    """All expected tools must be registered on the FastMCP instance."""

    def test_read_only_tools_registered(self):
        from datanika_mcp.server import mcp as server

        tool_names = {t.name for t in server._tool_manager.list_tools()}
        read_tools = {
            "get_agent_tiers",
            "get_connection_types",
            "list_connections",
            "get_connection",
            "introspect_connection",
            "preview_connection",
            "query_connection",
            "compile_transformation",
            "preview_transformation",
            "list_uploads",
            "list_pipelines",
            "list_transformations",
            "list_runs",
            "get_run",
            "get_run_logs",
            "list_catalog",
            "get_catalog_entry",
        }
        assert read_tools.issubset(tool_names), f"Missing read tools: {read_tools - tool_names}"

    def test_write_tools_registered(self):
        from datanika_mcp.server import mcp as server

        tool_names = {t.name for t in server._tool_manager.list_tools()}
        write_tools = {
            "create_connection",
            "create_upload",
            "create_pipeline",
            "create_transformation",
            "bulk_import",
            "trigger_upload",
            "trigger_pipeline",
            "trigger_transformation",
        }
        assert write_tools.issubset(tool_names), f"Missing write tools: {write_tools - tool_names}"

    def test_total_tool_count(self):
        from datanika_mcp.server import mcp as server

        tools = server._tool_manager.list_tools()
        assert len(tools) == 25  # 17 read + 8 write


# ---------------------------------------------------------------------------
# Write guard
# ---------------------------------------------------------------------------


class TestWriteGuard:
    def test_write_blocked_by_default(self):
        import datanika_mcp.server as srv

        srv._allow_write = False
        with pytest.raises(RuntimeError, match="Write access required"):
            srv._require_write("create_connection")

    def test_write_allowed_when_enabled(self):
        import datanika_mcp.server as srv

        srv._allow_write = True
        srv._require_write("create_connection")  # should not raise

    async def test_create_connection_calls_require_write(self):
        import datanika_mcp.server as srv

        srv._allow_write = False
        srv._client = AsyncMock()
        with pytest.raises(RuntimeError, match="Write access required"):
            await srv.create_connection("test", "postgres", {"host": "localhost"})

    async def test_trigger_pipeline_calls_require_write(self):
        import datanika_mcp.server as srv

        srv._allow_write = False
        srv._client = AsyncMock()
        with pytest.raises(RuntimeError, match="Write access required"):
            await srv.trigger_pipeline(1, wait=False)


# ---------------------------------------------------------------------------
# Read tools call client correctly
# ---------------------------------------------------------------------------


class TestReadToolsCallClient:
    async def test_list_connections_calls_client(self):
        import datanika_mcp.server as srv

        mock_client = AsyncMock()
        mock_client.list_connections.return_value = {"items": []}
        srv._client = mock_client

        result = await srv.list_connections()
        mock_client.list_connections.assert_awaited_once()
        assert '"items"' in result

    async def test_get_run_logs_calls_client(self):
        import datanika_mcp.server as srv

        mock_client = AsyncMock()
        mock_client.get_run_logs.return_value = {"run_id": 42, "logs": "ok"}
        srv._client = mock_client

        result = await srv.get_run_logs(42)
        mock_client.get_run_logs.assert_awaited_once_with(42)
        assert "42" in result

    async def test_compile_transformation_calls_client(self):
        import datanika_mcp.server as srv

        mock_client = AsyncMock()
        mock_client.compile_transformation.return_value = {
            "compiled_sql": "SELECT 1",
            "node": "model.t.stg_orders",
        }
        srv._client = mock_client

        result = await srv.compile_transformation(5)
        mock_client.compile_transformation.assert_awaited_once_with(5)
        assert "SELECT 1" in result


# ---------------------------------------------------------------------------
# Write tools call client when allowed
# ---------------------------------------------------------------------------


class TestWriteToolsCallClient:
    async def test_create_connection_calls_client(self):
        import datanika_mcp.server as srv

        srv._allow_write = True
        mock_client = AsyncMock()
        mock_client.create_connection.return_value = {"id": 1, "name": "test"}
        srv._client = mock_client

        result = await srv.create_connection("test", "postgres", {"host": "localhost"})
        mock_client.create_connection.assert_awaited_once_with(
            "test", "postgres", {"host": "localhost"}
        )
        assert '"id": 1' in result

    async def test_bulk_import_calls_client(self):
        import datanika_mcp.server as srv

        srv._allow_write = True
        mock_client = AsyncMock()
        mock_client.bulk_import.return_value = {"created": {"connections": [1]}}
        srv._client = mock_client

        payload = {"version": 2, "connections": [{"name": "a"}]}
        result = await srv.bulk_import(payload)
        mock_client.bulk_import.assert_awaited_once_with(payload)
        assert "connections" in result

    async def test_trigger_upload_with_wait(self):
        import datanika_mcp.server as srv

        srv._allow_write = True
        mock_client = AsyncMock()
        mock_client.trigger_upload.return_value = {"run_id": 10, "status": "success"}
        srv._client = mock_client

        result = await srv.trigger_upload(3, wait=True)
        mock_client.trigger_upload.assert_awaited_once_with(3, True)
        assert "success" in result


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


class TestCLI:
    def test_missing_api_key_exits(self):
        """Server should exit with error if no API key is provided."""
        import datanika_mcp.server as srv

        with (
            patch.dict("os.environ", {}, clear=True),
            patch("sys.argv", ["datanika-mcp"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            # Remove any env vars that might provide the key
            import os

            os.environ.pop("DATANIKA_API_KEY", None)
            os.environ.pop("DATANIKA_URL", None)
            os.environ.pop("DATANIKA_ALLOW_WRITE", None)
            srv.main()

        assert exc_info.value.code == 1

    def test_version_flag(self):
        import datanika_mcp.server as srv

        with (
            patch("sys.argv", ["datanika-mcp", "--version"]),
            pytest.raises(SystemExit) as exc_info,
        ):
            srv.main()

        assert exc_info.value.code == 0
