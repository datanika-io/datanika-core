"""The hosted MCP transport must not route its own loopback self-call through a proxy (core#1279).

`DatanikaClient` builds `httpx.AsyncClient` without naming `trust_env`, so it defaults to `True`
and picks up ambient proxy configuration. The hosted `/mcp` transport uses that client for a
request it makes **to its own backend** (`mcp_routes.py:44` — `http://127.0.0.1:8000`). In a
deployment where a proxy is configured in the app container's environment, that self-call is sent
to the proxy instead of to the local backend: every `tools/call` fails while `initialize` and
`tools/list` keep working and the container reads healthy — which is [core#388]'s signature, and
took a week to characterise the first time.

⚠️ **Both directions are asserted, and that is the point of the file.** The stdio entry point
(`server.py:576`) builds the same client against a **real remote URL**, where a user behind a
corporate proxy genuinely needs it honoured. A blanket `trust_env=False` would satisfy the first
test and break exactly the users the fix protects, so the second test exists to fail on it.

These tests reach into `client._http` and use httpx's `_transport_for_url`. That is deliberate:
the subject *is* the transport wiring, and asserting `trust_env is False` instead would pass just
as well against the blanket change this file forbids.
"""

from __future__ import annotations

import asyncio
import pathlib
import sys

import httpx
import pytest

_MCP_SRC = str(pathlib.Path(__file__).resolve().parents[2] / "datanika-mcp" / "src")
if _MCP_SRC not in sys.path:
    sys.path.insert(0, _MCP_SRC)

from datanika_mcp.client import DatanikaClient  # noqa: E402

import datanika.services.mcp_routes as routes  # noqa: E402

PROXY = "http://127.0.0.1:9999"


def _goes_via_proxy(client: DatanikaClient, url: str) -> bool:
    """Does this client route `url` through a proxy rather than direct?

    The honest signal: httpx installs a *separate* transport per proxy mount, so a proxied URL
    resolves to something other than the client's own default transport.
    """
    http = client._http
    return http._transport_for_url(httpx.URL(url)) is not http._transport


@pytest.fixture
def ambient_proxy(monkeypatch):
    """An operator's proxy, set the way a corporate deployment sets one."""
    for name in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"):
        monkeypatch.setenv(name, PROXY)
    for name in ("NO_PROXY", "no_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.delenv(name, raising=False)


def test_the_probe_discriminates(ambient_proxy):
    """Anti-vacuity. If `_goes_via_proxy` returned False for everything, every assertion below
    would pass while measuring nothing — which is this project's signature defect."""
    honouring = DatanikaClient("http://app.datanika.io", "etf_x")
    ignoring = DatanikaClient("http://app.datanika.io", "etf_x", trust_env=False)
    assert _goes_via_proxy(honouring, "http://app.datanika.io/api/v1/connections") is True
    assert _goes_via_proxy(ignoring, "http://app.datanika.io/api/v1/connections") is False


def test_the_hosted_transport_does_not_proxy_its_own_loopback_call(ambient_proxy, monkeypatch):
    """core#1279. Drives the REAL call site rather than asserting on a constructor argument."""
    captured: dict = {}
    real_cls = routes.DatanikaClient

    def _capture(base_url, api_key, **kwargs):
        client = real_cls(base_url, api_key, **kwargs)
        # Decided at construction, before the `finally: await client.aclose()` below.
        captured["proxied"] = _goes_via_proxy(client, f"{base_url}/api/v1/connections")
        captured["base_url"] = base_url
        return client

    monkeypatch.setattr(routes, "DatanikaClient", _capture)

    async def _resolve(token):
        return ("etf_test_key", True)

    monkeypatch.setattr(routes, "_resolve_credential", _resolve)

    async def _inner(scope, receive, send):
        return None

    async def _receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def _send(message):
        return None

    app = routes.BearerSessionApp(_inner)
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/mcp",
        "headers": [(b"authorization", b"Bearer etf_test_key")],
    }
    asyncio.run(app(scope, _receive, _send))

    assert "proxied" in captured, (
        "the hosted transport never constructed a client, so this test asserted nothing. "
        "Check that _resolve_credential was patched and the bearer header parsed."
    )
    assert captured["base_url"].startswith("http://127.0.0.1"), (
        f"expected the loopback self-call, got {captured['base_url']!r} — if this base URL "
        "stopped being loopback, the reasoning in core#1279 needs revisiting, not this assert."
    )
    assert captured["proxied"] is False, (
        f"the hosted MCP transport routed its own loopback self-call to {PROXY}. Every "
        "`tools/call` then fails while the container reads healthy (core#388's signature). "
        "Pass `trust_env=False` at the mcp_routes call site."
    )


def test_the_stdio_client_still_honours_the_operators_proxy(ambient_proxy):
    """The other direction, and the reason the fix must be parameterised rather than blanket.

    `server.py:576` builds this client against a **real remote host**. A user running the stdio
    server from inside a corporate network needs that proxy honoured, and a blanket
    `trust_env=False` would silently cut them off while making the test above pass.
    """
    client = DatanikaClient("http://app.datanika.io", "etf_stdio")
    assert _goes_via_proxy(client, "http://app.datanika.io/api/v1/connections") is True, (
        "the stdio client stopped honouring the environment's proxy. If this went red because "
        "`trust_env=False` became the default, that is the blanket change core#1279 rejects: "
        "scope it to the hosted loopback call site."
    )
