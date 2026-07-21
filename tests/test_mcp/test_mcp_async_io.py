"""Regression probe for core#388 — a hosted MCP tool must never block the loop.

The hosted ``/mcp`` endpoint is mounted into this app's *own* backend, and every
tool is a forwarder to this app's *own* REST API on ``127.0.0.1:8000``. The
FastMCP SDK runs a **sync** tool function inline on the event loop
(``func_metadata.call_fn_with_arg_validation``: ``await fn(...)`` when the tool
is a coroutine function, plain ``fn(...)`` otherwise). So a blocking
``httpx.Client`` call inside a tool holds the only loop that could serve the
loopback request — the self-call can never be answered, and every hosted tool
call died at the client's 30s timeout (``Error executing tool list_connections:
timed out``). That is what shipped to prod on 2026-07-21: the protocol layer
(``initialize`` / ``tools/list``) was fine, every ``tools/call`` timed out.

The pre-existing remote tests could not catch it: they drive the transport
through ``httpx.ASGITransport`` with a mocked client, so nothing ever performed
real I/O from inside the loop. Two layers guard the class here:

- **structural** — every registered tool and every ``DatanikaClient`` request
  method is a coroutine function (a future ``def`` tool re-introduces the bug);
- **behavioural** — a real ``tools/call`` whose REST API is served *by the same
  event loop* over a real socket. Under the sync client this hangs the loop and
  fails on the timeout; with the async client it answers in milliseconds.
"""

import asyncio
import inspect
import json
import pathlib
import sys

import anyio
import httpx
import pytest

# datanika-mcp is not installed in the test venv (the Docker image installs it);
# make it importable before importing the route module that depends on it.
_MCP_SRC = str(pathlib.Path(__file__).resolve().parents[2] / "datanika-mcp" / "src")
if _MCP_SRC not in sys.path:
    sys.path.insert(0, _MCP_SRC)

from datanika_mcp.client import DatanikaClient  # noqa: E402
from datanika_mcp.server import make_remote_transport  # noqa: E402
from datanika_mcp.server import mcp as server  # noqa: E402

import datanika.services.mcp_routes as routes  # noqa: E402

_HEADERS = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}
_INIT = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "0"},
    },
}


def _rpc(method: str, rpc_id: int, **params) -> dict:
    return {"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params}


# ---------------------------------------------------------------------------
# Structural — nothing on the tool path may be synchronous
# ---------------------------------------------------------------------------


class TestNoSyncOnTheToolPath:
    def test_every_registered_tool_is_a_coroutine_function(self):
        """A sync tool is executed inline on the loop -> loopback deadlock."""
        sync_tools = [t.name for t in server._tool_manager.list_tools() if not t.is_async]
        assert sync_tools == [], (
            f"Tools must be `async def` (core#388): {sync_tools}. The FastMCP SDK "
            "calls a sync tool inline on the event loop, which deadlocks the "
            "self-call to this app's own REST API."
        )

    def test_every_client_method_is_a_coroutine_function(self):
        """``DatanikaClient`` is awaited from tool bodies — no blocking I/O."""
        blocking = [
            name
            for name, attr in vars(DatanikaClient).items()
            if not name.startswith("__") and inspect.isfunction(attr)
            if not inspect.iscoroutinefunction(attr)
        ]
        assert blocking == [], f"DatanikaClient methods must be `async def` (core#388): {blocking}"

    def test_client_built_outside_a_loop_still_works_inside_one(self):
        """The stdio path builds the client *before* ``mcp.run()`` starts a loop.

        A deliberately sync test (no ambient loop) so the ``AsyncClient`` is
        constructed exactly as ``main()`` constructs it; a transport bound to
        the wrong loop would surface here, not on the remote path.
        """
        client = DatanikaClient("http://127.0.0.1:9", "etf_stdio")

        async def _use() -> None:
            with pytest.raises(httpx.ConnectError):  # nothing listens on :9
                await client.list_connections()
            await client.aclose()

        asyncio.run(_use())


# ---------------------------------------------------------------------------
# Behavioural — the loopback self-call the sync client deadlocked on
# ---------------------------------------------------------------------------


async def _serve_fake_api(payload: dict, seen: dict) -> tuple[asyncio.AbstractServer, int]:
    """A one-shot HTTP/1.1 server on the *test's own* event loop.

    Stands in for the backend the MCP tools call back into. Because it shares
    the loop with the tool being exercised, it can only answer if that tool
    yields — which is exactly the invariant under test.
    """
    body = json.dumps(payload).encode()
    head = (
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n"
    )

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            seen["request"] = request.decode("latin-1")
            writer.write(head + body)
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass  # client vanished during teardown
        finally:
            writer.close()

    srv = await asyncio.start_server(handle, "127.0.0.1", 0)
    return srv, srv.sockets[0].getsockname()[1]


class TestLoopbackToolCall:
    async def test_tool_call_against_a_same_loop_api_does_not_deadlock(self):
        seen: dict = {}
        api, port = await _serve_fake_api({"connections": [{"id": 1, "name": "loopback-pg"}]}, seen)
        asgi, lifespan = make_remote_transport()
        app = routes.BearerSessionApp(asgi, base_url=f"http://127.0.0.1:{port}")
        auth = {**_HEADERS, "Authorization": "Bearer etf_loopback"}

        try:
            async with (
                lifespan(),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://t"
                ) as client,
            ):
                await client.post("/mcp", json=_INIT, headers=auth)
                # Generous vs. a healthy call (ms), far under the client's 30s
                # timeout: a blocked loop cannot answer, so this is the failure
                # signal rather than a 30s hang.
                with anyio.fail_after(10):
                    call = await client.post(
                        "/mcp",
                        json=_rpc("tools/call", 3, name="list_connections", arguments={}),
                        headers=auth,
                    )
        finally:
            api.close()
            await api.wait_closed()

        assert call.status_code == 200
        result = call.json()["result"]
        assert result.get("isError") is not True, result
        assert "loopback-pg" in json.dumps(result)
        # The caller's bearer key reached the REST API over the real hop.
        assert "Bearer etf_loopback" in seen["request"]
        assert "/api/v1/connections" in seen["request"]
