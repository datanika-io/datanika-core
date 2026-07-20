"""Datanika MCP server — exposes the Datanika REST API as MCP tools.

Read-only by default. Pass ``--allow-write`` to enable mutation tools
(create resources, trigger pipeline runs).

Usage::

    # Read-only (safe default)
    datanika-mcp --url https://app.datanika.io --api-key etf_...

    # With write access
    datanika-mcp --url https://app.datanika.io --api-key etf_... --allow-write
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
from collections.abc import AsyncIterator, Callable

from mcp.server.fastmcp import FastMCP
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager

from datanika_mcp.client import DatanikaClient
from datanika_mcp.session import DatanikaSession, current_session

# ---------------------------------------------------------------------------
# Session resolution — contextvar-first, module-global fallback
# ---------------------------------------------------------------------------
#
# ``main()`` populates the module globals for the single-tenant stdio server.
# A remote transport (SPEC_REMOTE_MCP.md P1) instead binds a per-request
# ``DatanikaSession`` via ``session.use_session``; ``_session()`` prefers that
# binding and falls back to the globals, so both transports share one tool
# surface and the existing tests (which set the globals directly) stay green.

_client: DatanikaClient | None = None
_allow_write: bool = False

mcp = FastMCP(
    "Datanika",
    instructions=(
        "Browse data connections, preview tables, compile and validate "
        "dbt transformations, monitor pipeline runs, and (with --allow-write) "
        "create resources and trigger runs in Datanika."
    ),
)


def _session() -> DatanikaSession:
    """Resolve the active session for the current tool call.

    Prefers a per-request session bound by the transport (remote); falls back
    to the process globals set by ``main()`` (stdio / tests). Raises if no
    client is available on the fallback path — i.e. ``main()`` never ran.
    """
    session = current_session()
    if session is not None:
        return session
    if _client is None:
        raise RuntimeError("DatanikaClient not initialized — call main() first")
    return DatanikaSession(client=_client, allow_write=_allow_write)


def _require_write(action: str) -> None:
    """Module-level write guard, kept for backward compatibility.

    Tool bodies call ``_session().require_write`` directly; this shim exists
    so code (and tests) referencing ``server._require_write`` keep working. It
    resolves the write-grant from the bound session if present, else the
    module globals — deliberately without a live client, so a write can be
    rejected before the client is constructed.
    """
    session = current_session()
    if session is None:
        session = DatanikaSession(client=_client, allow_write=_allow_write)
    session.require_write(action)


# ===================================================================
# Read-only tools — always available
# ===================================================================


# --- Tier 1: Discover & Introspect ---


@mcp.tool()
def get_agent_tiers() -> str:
    """Get the 5-tier agent capability stack — describes what the API can do."""
    return json.dumps(_session().client.get_agent_tiers(), indent=2)


@mcp.tool()
def get_connection_types() -> str:
    """List all supported connection types with their config schemas."""
    return json.dumps(_session().client.get_connection_types(), indent=2)


@mcp.tool()
def list_connections() -> str:
    """List all connections in the organization."""
    return json.dumps(_session().client.list_connections(), indent=2)


@mcp.tool()
def get_connection(connection_id: int) -> str:
    """Get details of a specific connection by ID."""
    return json.dumps(_session().client.get_connection(connection_id), indent=2)


@mcp.tool()
def introspect_connection(connection_id: int, schema: str | None = None) -> str:
    """List schemas and tables of a source connection.

    Args:
        connection_id: The connection to introspect.
        schema: Optional schema name to filter tables.
    """
    return json.dumps(_session().client.introspect_connection(connection_id, schema), indent=2)


@mcp.tool()
def preview_connection(
    connection_id: int, table: str, schema: str | None = None, limit: int = 100
) -> str:
    """Preview the first N rows of a table from a source connection.

    Args:
        connection_id: The connection to query.
        table: Table name to preview.
        schema: Optional schema name.
        limit: Max rows to return (default 100).
    """
    return json.dumps(
        _session().client.preview_connection(connection_id, table, schema, limit),
        indent=2,
    )


@mcp.tool()
def query_connection(connection_id: int, query: str) -> str:
    """Execute a read-only SQL query against a source connection.

    Args:
        connection_id: The connection to query.
        query: A single SELECT statement (no mutations allowed).
    """
    return json.dumps(_session().client.query_connection(connection_id, query), indent=2)


# --- Tier 3: Validate ---


@mcp.tool()
def compile_transformation(transformation_id: int) -> str:
    """Compile a dbt transformation — resolves Jinja, ref(), source(). No execution.

    Args:
        transformation_id: The transformation to compile.
    """
    return json.dumps(_session().client.compile_transformation(transformation_id), indent=2)


@mcp.tool()
def preview_transformation(transformation_id: int, limit: int = 100) -> str:
    """Compile and execute a transformation, returning preview rows.

    Args:
        transformation_id: The transformation to preview.
        limit: Max rows to return (default 100, max 1000).
    """
    return json.dumps(_session().client.preview_transformation(transformation_id, limit), indent=2)


# --- Tier 4: Control (read half) ---


@mcp.tool()
def list_uploads() -> str:
    """List all uploads (extract + load jobs) in the organization."""
    return json.dumps(_session().client.list_uploads(), indent=2)


@mcp.tool()
def list_pipelines() -> str:
    """List all pipelines (dbt transform orchestration) in the organization."""
    return json.dumps(_session().client.list_pipelines(), indent=2)


@mcp.tool()
def list_transformations() -> str:
    """List all dbt transformations in the organization."""
    return json.dumps(_session().client.list_transformations(), indent=2)


@mcp.tool()
def list_runs(target_type: str | None = None, status: str | None = None, limit: int = 50) -> str:
    """List pipeline/upload/transformation runs with optional filters.

    Args:
        target_type: Filter by type — 'upload', 'pipeline', or 'transformation'.
        status: Filter by status — 'pending', 'running', 'success', 'failed', 'cancelled'.
        limit: Max results (default 50, max 200).
    """
    return json.dumps(_session().client.list_runs(target_type, status, limit), indent=2)


@mcp.tool()
def get_run(run_id: int) -> str:
    """Get details of a specific run by ID.

    Args:
        run_id: The run ID to look up.
    """
    return json.dumps(_session().client.get_run(run_id), indent=2)


@mcp.tool()
def get_run_logs(run_id: int) -> str:
    """Get the logs of a specific run.

    Args:
        run_id: The run ID whose logs to fetch.
    """
    return json.dumps(_session().client.get_run_logs(run_id), indent=2)


@mcp.tool()
def list_catalog() -> str:
    """List all catalog entries (source tables and dbt models)."""
    return json.dumps(_session().client.list_catalog(), indent=2)


@mcp.tool()
def get_catalog_entry(entry_id: int) -> str:
    """Get details of a specific catalog entry.

    Args:
        entry_id: The catalog entry ID.
    """
    return json.dumps(_session().client.get_catalog_entry(entry_id), indent=2)


# ===================================================================
# Write tools — only available with --allow-write
# ===================================================================


# --- Tier 2: Build ---


@mcp.tool()
def create_connection(name: str, connection_type: str, config: dict) -> str:
    """Create a new data connection.

    Requires --allow-write.

    Args:
        name: Human-readable name for the connection.
        connection_type: One of the supported types (e.g. 'postgres', 'mysql', 'stripe').
        config: Connection-specific configuration (host, port, credentials, etc.).
    """
    _session().require_write("create_connection")
    return json.dumps(_session().client.create_connection(name, connection_type, config), indent=2)


@mcp.tool()
def create_upload(
    name: str,
    source_connection_id: int,
    destination_connection_id: int,
    dlt_config: dict | None = None,
    description: str | None = None,
) -> str:
    """Create a new upload (extract + load job).

    Requires --allow-write.

    Args:
        name: Upload name.
        source_connection_id: ID of the source connection.
        destination_connection_id: ID of the destination connection.
        dlt_config: Optional dlt extraction config (load_mode, table_name, etc.).
        description: Optional description.
    """
    _session().require_write("create_upload")
    return json.dumps(
        _session().client.create_upload(
            name, source_connection_id, destination_connection_id, dlt_config, description
        ),
        indent=2,
    )


@mcp.tool()
def create_pipeline(
    name: str,
    destination_connection_id: int,
    command: str = "run",
    description: str | None = None,
) -> str:
    """Create a new pipeline (dbt transform orchestration).

    Requires --allow-write.

    Args:
        name: Pipeline name.
        destination_connection_id: ID of the destination connection.
        command: dbt command — 'run', 'build', 'test', 'seed', 'snapshot', 'compile'.
        description: Optional description.
    """
    _session().require_write("create_pipeline")
    return json.dumps(
        _session().client.create_pipeline(name, destination_connection_id, command, description),
        indent=2,
    )


@mcp.tool()
def create_transformation(
    name: str,
    sql_body: str,
    materialization: str = "view",
    description: str | None = None,
    schema_name: str = "staging",
) -> str:
    """Create a new dbt SQL transformation.

    Requires --allow-write.

    Args:
        name: Model name (letters, digits, underscores, hyphens; must start with letter or _).
        sql_body: dbt-compatible SQL (supports ref(), source(), Jinja).
        materialization: 'view', 'table', 'incremental', 'ephemeral', or 'snapshot'.
        description: Optional description.
        schema_name: Target schema (default 'staging').
    """
    _session().require_write("create_transformation")
    return json.dumps(
        _session().client.create_transformation(
            name, sql_body, materialization, description, schema_name
        ),
        indent=2,
    )


@mcp.tool()
def bulk_import(payload: dict) -> str:
    """Bulk-import connections, uploads, pipelines, and transformations in one call.

    Requires --allow-write. Uses the JSON v2 import format.
    Validates everything first — if any errors, nothing is created.

    Args:
        payload: JSON v2 import payload with version, connections, uploads,
                 pipelines, transformations sections. See AI_IMPORT_GUIDE.md.
    """
    _session().require_write("bulk_import")
    return json.dumps(_session().client.bulk_import(payload), indent=2)


# --- Tier 4: Execute ---


@mcp.tool()
def trigger_upload(upload_id: int, wait: bool = False) -> str:
    """Trigger an upload run.

    Requires --allow-write.

    Args:
        upload_id: The upload to run.
        wait: If true, block until the run completes (up to 120s).
    """
    _session().require_write("trigger_upload")
    return json.dumps(_session().client.trigger_upload(upload_id, wait), indent=2)


@mcp.tool()
def trigger_pipeline(pipeline_id: int, wait: bool = False) -> str:
    """Trigger a pipeline run (dbt build/run/test).

    Requires --allow-write.

    Args:
        pipeline_id: The pipeline to run.
        wait: If true, block until the run completes (up to 120s).
    """
    _session().require_write("trigger_pipeline")
    return json.dumps(_session().client.trigger_pipeline(pipeline_id, wait), indent=2)


@mcp.tool()
def trigger_transformation(transformation_id: int, wait: bool = False) -> str:
    """Trigger a transformation run.

    Requires --allow-write.

    Args:
        transformation_id: The transformation to run.
        wait: If true, block until the run completes (up to 120s).
    """
    _session().require_write("trigger_transformation")
    return json.dumps(_session().client.trigger_transformation(transformation_id, wait), indent=2)


# ===================================================================
# Remote transport — Streamable HTTP (SPEC_REMOTE_MCP.md P1 step 2)
# ===================================================================
#
# One tool surface, two transports. stdio runs ``mcp.run(transport="stdio")``
# in ``main()`` below; the remote transport serves the same 25 ``@mcp.tool()``s
# over Streamable HTTP. Per-request auth (bearer -> DatanikaSession) is layered
# on by the hosting app (``datanika/services/mcp_routes.py``), which binds the
# session into the contextvar the tools resolve via ``_session()`` — the tool
# bodies are unchanged and unaware of the transport.
#
# Stateless-JSON (SPEC §11.1): no per-session affinity to coordinate across
# workers for a read-only P1; the caller is authenticated per request.


class _StreamableHTTPApp:
    """ASGI adapter over a ``StreamableHTTPSessionManager``.

    Starlette treats a *callable object* (not a plain function) as a raw ASGI
    app, so a request reaches the MCP transport without the request/response
    wrapper.
    """

    def __init__(self, manager: StreamableHTTPSessionManager) -> None:
        self._manager = manager

    async def __call__(self, scope, receive, send) -> None:
        await self._manager.handle_request(scope, receive, send)


def make_remote_transport() -> tuple[
    _StreamableHTTPApp, Callable[[], contextlib.AbstractAsyncContextManager[None]]
]:
    """Build a Streamable-HTTP transport bound to the shared tool surface.

    Returns ``(asgi_app, lifespan)``:

    - ``asgi_app`` — the raw ASGI endpoint to mount at ``/mcp`` (wrap with auth
      first). Stateless-JSON: a POST carries the JSON-RPC request + response.
    - ``lifespan`` — an ``@asynccontextmanager`` factory that must be active for
      the endpoint to serve (it owns the session manager's task group). Register
      via ``rx.App.register_lifespan_task``.

    A **fresh** manager per call (not a process global): ``run()`` may only be
    entered once per ``StreamableHTTPSessionManager`` instance, so each hosting
    app — and each test — gets its own.
    """
    manager = StreamableHTTPSessionManager(
        app=mcp._mcp_server,  # the low-level Server the FastMCP instance wraps
        stateless=True,
        json_response=True,
    )

    @contextlib.asynccontextmanager
    async def mcp_session_manager_lifespan() -> AsyncIterator[None]:
        async with manager.run():
            yield

    return _StreamableHTTPApp(manager), mcp_session_manager_lifespan


def create_streamable_http_app():
    """A standalone Starlette app serving the tool surface at ``/mcp``.

    No auth wrapper — for transport smoke tests and self-host experiments. The
    authenticated production mount lives in ``datanika/services/mcp_routes.py``.
    """
    from starlette.applications import Starlette
    from starlette.routing import Route

    asgi_app, lifespan = make_remote_transport()
    return Starlette(
        routes=[Route("/mcp", endpoint=asgi_app)],
        lifespan=lambda _app: lifespan(),
    )


# ===================================================================
# Entry point
# ===================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="datanika-mcp",
        description="Datanika MCP server — data pipeline tools for Claude Desktop",
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("DATANIKA_URL", "https://app.datanika.io"),
        help="Datanika instance URL (default: $DATANIKA_URL or https://app.datanika.io)",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("DATANIKA_API_KEY"),
        help="API key (default: $DATANIKA_API_KEY)",
    )
    parser.add_argument(
        "--allow-write",
        action="store_true",
        default=os.environ.get("DATANIKA_ALLOW_WRITE", "").lower() in ("1", "true"),
        help="Enable write tools (create resources, trigger runs)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0",
    )

    args = parser.parse_args()

    if not args.api_key:
        print("Error: --api-key or $DATANIKA_API_KEY is required", file=sys.stderr)
        sys.exit(1)

    global _client, _allow_write
    _client = DatanikaClient(args.url, args.api_key)
    _allow_write = args.allow_write

    mode = "read-write" if _allow_write else "read-only"
    print(f"Datanika MCP server starting ({mode})...", file=sys.stderr)
    print(f"  URL: {args.url}", file=sys.stderr)

    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
