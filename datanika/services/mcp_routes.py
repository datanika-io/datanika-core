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

import anyio
from datanika_mcp.client import DatanikaClient
from datanika_mcp.server import make_remote_transport
from datanika_mcp.session import DatanikaSession, use_session
from starlette.datastructures import Headers
from starlette.routing import Route

from datanika.services.mcp_oauth import McpOAuthService

# The MCP server calls this app's own REST API with the caller's key. The mount
# runs in the backend process, so this is loopback in prod; overridable for
# unusual topologies.
_INTERNAL_API_URL = os.environ.get("DATANIKA_MCP_INTERNAL_URL", "http://127.0.0.1:8000")

# Access tokens the AS issues are prefixed so the resource server can tell them
# apart from a pasted API key (``etf_``) without a database round-trip.
_OAUTH_TOKEN_PREFIX = "dtk_at_"  # noqa: S105 - a public prefix, not a secret


def _bearer_token(scope) -> str:
    """Extract the bearer token from the ASGI scope, or "" if absent/malformed."""
    auth = Headers(scope=scope).get("authorization", "")
    if auth[:7].lower() == "bearer ":
        return auth[7:].strip()
    return ""


async def _send_401(send) -> None:
    """401 + the pointer an MCP client follows to start the OAuth flow.

    ``WWW-Authenticate: ... resource_metadata=...`` is the entry point of the
    MCP remote-auth spec: an unauthenticated client reads it, fetches the
    protected-resource metadata, discovers the AS, registers, and runs the
    one-click flow (core#393). Without the parameter a client has no way to
    find the AS and can only fall back to a hand-pasted API key.
    """
    svc = McpOAuthService()
    body = json.dumps(
        {
            "error": "unauthorized",
            "detail": (
                "Authenticate with OAuth (see the resource metadata in the "
                "WWW-Authenticate header) or present a Datanika API key as "
                "'Authorization: Bearer <key>'."
            ),
        }
    ).encode()
    challenge = (
        f'Bearer realm="datanika-mcp", '
        f'resource_metadata="{svc.issuer}/.well-known/oauth-protected-resource"'
    ).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 401,
            "headers": [
                (b"content-type", b"application/json"),
                (b"www-authenticate", challenge),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})


def _resolve_oauth_token_sync(token: str):
    """Exchange an issued access token for its credential + granted authority."""
    from datanika.db import get_sync_session

    with get_sync_session() as session:
        return McpOAuthService().resolve_access_token(session, token)


async def _resolve_credential(token: str) -> tuple[str | None, bool]:
    """Map a presented bearer to ``(api_key, allow_write)``.

    Two credentials are accepted: an OAuth access token issued by our own AS
    (P2), and a raw Datanika API key pasted by the user (P1, unchanged — it
    still works, and self-hosters rely on it).

    **Write authority comes from the grant, never from the transport**
    (core#442): an OAuth session is writable only if a human consented to it,
    and a pasted key stays read-only here because nothing recorded a human
    decision — the key's own scopes still bound what the REST API will do, but
    this layer will not open the write tools on an inference.

    The token lookup is a **blocking** DB call, so it runs in a worker thread:
    this handler is on the same event loop that has to serve the tool's own
    loopback request, and holding it is what made every hosted tool call time
    out in core#388.
    """
    if token.startswith(_OAUTH_TOKEN_PREFIX):
        resolved = await anyio.to_thread.run_sync(_resolve_oauth_token_sync, token)
        if resolved is None:
            return None, False
        return resolved.api_key, resolved.allow_write
    return token, False


class BearerSessionApp:
    """ASGI middleware: authenticate the bearer API key and bind a per-request,
    read-only :class:`DatanikaSession` for the wrapped MCP transport.

    A missing or blank bearer token short-circuits to ``401`` with a
    ``WWW-Authenticate: Bearer`` header carrying the resource-metadata pointer
    that starts the OAuth flow.

    Validation differs by credential type. An **OAuth access token** (P2) is
    resolved up front — unknown, expired, revoked, or bound to a revoked API
    key all 401 before the transport is touched. A **raw API key** (P1) is
    still validated lazily by the REST API on the first tool call, since the
    MCP server is a pure forwarder; ``initialize`` / ``tools/list`` therefore
    succeed for any pasted string, which is unchanged behaviour.
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

        credential, allow_write = await _resolve_credential(token)
        if not credential:
            # An OAuth token that is expired, revoked, or simply not ours. The
            # same 401 sends the client back through discovery, which is how it
            # learns to refresh rather than guess.
            await _send_401(send)
            return

        client = DatanikaClient(self._base_url, credential)
        # allow_write reflects what a human consented to at the OAuth consent
        # screen (core#442) — never the transport, and never an inference from
        # the key's own scopes. transport="remote" shapes the refusal wording
        # when write was not granted (core#409).
        session = DatanikaSession(client=client, allow_write=allow_write, transport="remote")
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
