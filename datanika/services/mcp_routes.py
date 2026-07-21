"""Remote MCP endpoint (``/mcp``) — Streamable HTTP, bearer = API key (P1).

Mounts the shared ``datanika-mcp`` tool surface over Streamable HTTP on the
existing Starlette backend and wraps it with a thin bearer-token auth layer.
The MCP server is a **thin authenticated client of this app's own REST API**
at ``127.0.0.1:8000``: it forwards the caller's key on every tool call, so org
isolation, scope enforcement, per-key rate limits, and V2 byte-metering are
all inherited from ``api_middleware`` — none of it is re-implemented here
(SPEC_REMOTE_MCP §5.4).

P1 scope: **read-only** (``allow_write=False``); auth = an existing Datanika
API key presented as ``Authorization: Bearer <key>`` (no OAuth AS — that is
P2). The write tools are P3, gated on the #338 worker-egress guard.

The per-request ``DatanikaSession`` is bound into the contextvar the tool
surface resolves via ``server._session()`` — see ``datanika_mcp/session.py``.
anyio copies the request task's context into the task the session manager
spawns to execute the tool, so the binding reaches the tool body.

Because that REST call is a **loopback into this same process**, the whole tool
path must be non-blocking: the loop serving the tool call is also the loop that
has to answer it. ``DatanikaClient`` is therefore an ``httpx.AsyncClient`` and
every tool is ``async def`` — a single sync hop deadlocks the endpoint until the
client's timeout fires (core#388).
"""

from __future__ import annotations

import json
import os

from datanika_mcp.client import DatanikaClient
from datanika_mcp.server import make_remote_transport
from datanika_mcp.session import DatanikaSession, use_session
from starlette.datastructures import Headers
from starlette.routing import Route

# The MCP server calls this app's own REST API with the caller's key. The mount
# runs in the backend process, so this is loopback in prod; overridable for
# unusual topologies.
_INTERNAL_API_URL = os.environ.get("DATANIKA_MCP_INTERNAL_URL", "http://127.0.0.1:8000")


def _bearer_token(scope) -> str:
    """Extract the bearer token from the ASGI scope, or "" if absent/malformed."""
    auth = Headers(scope=scope).get("authorization", "")
    if auth[:7].lower() == "bearer ":
        return auth[7:].strip()
    return ""


async def _send_401(send) -> None:
    body = json.dumps(
        {
            "error": "unauthorized",
            "detail": "Provide a Datanika API key as an 'Authorization: Bearer <key>' header.",
        }
    ).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 401,
            "headers": [
                (b"content-type", b"application/json"),
                (b"www-authenticate", b'Bearer realm="datanika-mcp"'),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


class BearerSessionApp:
    """ASGI middleware: authenticate the bearer API key and bind a per-request,
    read-only :class:`DatanikaSession` for the wrapped MCP transport.

    A missing or blank bearer token short-circuits to ``401`` with a
    ``WWW-Authenticate: Bearer`` header. The key itself is validated lazily by
    the REST API on the first tool call (the MCP server is a pure forwarder),
    so ``initialize`` / ``tools/list`` succeed for any presented token — a
    P2 pre-flight validation is a follow-up.
    """

    def __init__(self, app, *, base_url: str = _INTERNAL_API_URL) -> None:
        self._app = app
        self._base_url = base_url

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        token = _bearer_token(scope)
        if not token:
            await _send_401(send)
            return

        client = DatanikaClient(self._base_url, token)
        session = DatanikaSession(client=client, allow_write=False)
        try:
            with use_session(session):
                await self._app(scope, receive, send)
        finally:
            await client.aclose()


# Build the transport once (import time) and export the pieces datanika.py wires:
#   - ``mcp_routes``   -> appended to ``app._api.routes``
#   - ``mcp_lifespan`` -> registered via ``rx.App.register_lifespan_task`` so the
#                         session manager's task group is active for app lifetime.
_asgi_app, mcp_lifespan = make_remote_transport()

mcp_routes = [Route("/mcp", endpoint=BearerSessionApp(_asgi_app))]
