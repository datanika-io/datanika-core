"""Tests for the remote MCP transport + bearer auth (core#370, Remote-MCP P1 step 2).

Covers three layers:
- the bearer-token ASGI auth wrapper (401s; binds a per-request read-only session);
- the Streamable-HTTP transport (stateless-JSON: initialize + tools/list);
- end-to-end tool routing (a bearer'd tools/call reaches the bound session's client;
  a write tool is blocked read-only).

The transport (`mcp` SDK) is exercised in-process via httpx ASGITransport with the
session manager's lifespan entered manually (ASGITransport does not run lifespans).
"""

import json
import pathlib
import sys
from unittest.mock import AsyncMock

import httpx

# datanika-mcp is not installed in the test venv (the Docker image installs it);
# make it importable before importing the route module that depends on it.
_MCP_SRC = str(pathlib.Path(__file__).resolve().parents[2] / "datanika-mcp" / "src")
if _MCP_SRC not in sys.path:
    sys.path.insert(0, _MCP_SRC)

from datanika_mcp.server import create_streamable_http_app, make_remote_transport  # noqa: E402
from datanika_mcp.session import current_session  # noqa: E402

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
# Bearer auth wrapper
# ---------------------------------------------------------------------------


async def _ok_asgi(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})


class TestBearerAuth:
    async def test_missing_bearer_returns_401(self):
        called = {"inner": False}

        async def inner(scope, receive, send):
            called["inner"] = True
            await _ok_asgi(scope, receive, send)

        app = routes.BearerSessionApp(inner)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as c:
            r = await c.post("/mcp")

        assert r.status_code == 401
        assert r.headers.get("www-authenticate", "").lower().startswith("bearer")
        assert called["inner"] is False  # short-circuits before the transport

    async def test_non_bearer_scheme_returns_401(self):
        app = routes.BearerSessionApp(_ok_asgi)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as c:
            r = await c.post("/mcp", headers={"Authorization": "Basic Zm9vOmJhcg=="})
        assert r.status_code == 401

    async def test_valid_bearer_binds_readonly_session(self, monkeypatch):
        captured = {}

        class _FakeClient:
            def __init__(self, base_url, token):
                captured["base_url"] = base_url
                captured["token"] = token

            async def aclose(self):
                captured["closed"] = True

        monkeypatch.setattr(routes, "DatanikaClient", _FakeClient)

        seen = {}

        async def inner(scope, receive, send):
            session = current_session()
            seen["allow_write"] = session.allow_write
            seen["client_is_fake"] = isinstance(session.client, _FakeClient)
            await _ok_asgi(scope, receive, send)

        app = routes.BearerSessionApp(inner)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as c:
            r = await c.post("/mcp", headers={"Authorization": "Bearer etf_abc123"})

        assert r.status_code == 200
        assert captured["token"] == "etf_abc123"  # key forwarded to the REST client
        assert captured.get("closed") is True  # client closed after the request
        assert seen["allow_write"] is False  # P1 = read-only
        assert seen["client_is_fake"] is True  # tool would route through this client


# ---------------------------------------------------------------------------
# Streamable-HTTP transport (stateless-JSON)
# ---------------------------------------------------------------------------


class TestTransport:
    def test_standalone_app_has_mcp_route(self):
        app = create_streamable_http_app()
        paths = {getattr(r, "path", None) for r in app.routes}
        assert "/mcp" in paths

    async def test_initialize_and_tools_list(self):
        asgi, lifespan = make_remote_transport()
        async with (
            lifespan(),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=asgi), base_url="http://t") as c,
        ):
            init = await c.post("/mcp", json=_INIT, headers=_HEADERS)
            assert init.status_code == 200
            assert init.json()["result"]["protocolVersion"]

            listed = await c.post("/mcp", json=_rpc("tools/list", 2), headers=_HEADERS)
            assert listed.status_code == 200
            tools = listed.json()["result"]["tools"]
            assert len(tools) == 25  # 17 read + 8 write, shared with stdio


# ---------------------------------------------------------------------------
# End-to-end tool routing through the bound session
# ---------------------------------------------------------------------------


class TestToolRouting:
    async def test_read_tool_call_routes_through_session(self, monkeypatch):
        # AsyncMock: the client is awaited from the tool body (core#388).
        fake_client = AsyncMock()
        fake_client.list_connections.return_value = {"connections": [{"id": 1, "name": "pg"}]}
        monkeypatch.setattr(routes, "DatanikaClient", lambda base_url, token: fake_client)

        asgi, lifespan = make_remote_transport()
        app = routes.BearerSessionApp(asgi)
        auth = {**_HEADERS, "Authorization": "Bearer etf_read"}
        async with (
            lifespan(),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c,
        ):
            await c.post("/mcp", json=_INIT, headers=auth)
            call = await c.post(
                "/mcp",
                json=_rpc("tools/call", 3, name="list_connections", arguments={}),
                headers=auth,
            )

        assert call.status_code == 200
        fake_client.list_connections.assert_called_once()
        # The tool's JSON payload surfaces in the tool-call result content.
        assert "pg" in json.dumps(call.json())

    async def test_write_tool_blocked_read_only(self, monkeypatch):
        fake_client = AsyncMock()
        monkeypatch.setattr(routes, "DatanikaClient", lambda base_url, token: fake_client)

        asgi, lifespan = make_remote_transport()
        app = routes.BearerSessionApp(asgi)
        auth = {**_HEADERS, "Authorization": "Bearer etf_write"}
        async with (
            lifespan(),
            httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c,
        ):
            await c.post("/mcp", json=_INIT, headers=auth)
            call = await c.post(
                "/mcp",
                json=_rpc(
                    "tools/call",
                    4,
                    name="create_connection",
                    arguments={"name": "x", "connection_type": "postgres", "config": {}},
                ),
                headers=auth,
            )

        assert call.status_code == 200
        # Read-only session -> the write guard fires; the tool never calls the client.
        body = json.dumps(call.json())
        assert "Write access required" in body
        fake_client.create_connection.assert_not_called()

        # ...and the advice must fit the hosted transport (#409). The stdio
        # wording ("restart the server with --allow-write") is unactionable
        # here — there is no server the caller runs — and an agent reads it as
        # a recoverable condition and retries. This is the string an agent
        # actually receives, which is why it is asserted end-to-end and not
        # just on the session object.
        assert "Restart the server" not in body
        assert "read-only" in body.lower()

    async def test_bearer_session_is_marked_remote(self, monkeypatch):
        """The wording above can only differ if the transport reaches the session."""
        from datanika_mcp.session import current_session

        monkeypatch.setattr(routes, "DatanikaClient", lambda base_url, token: AsyncMock())
        seen = {}

        async def inner(scope, receive, send):
            seen["transport"] = current_session().transport
            await _ok_asgi(scope, receive, send)

        app = routes.BearerSessionApp(inner)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as c:
            await c.post("/mcp", headers={"Authorization": "Bearer etf_x"})

        assert seen["transport"] == "remote"
