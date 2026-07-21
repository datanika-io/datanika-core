"""The OAuth consent screen (core#394, Remote-MCP P2).

Two halves, deliberately:

The **unit** classes drive the handlers with a stand-in ``self`` (the
``test_run_dispatch.py`` convention) to pin the decisions taken before any
network call — the login bounce that keeps the authorization request alive, and
the refusal to redirect anywhere that hasn't been verified.

``TestAgainstTheRealAuthorizationServer`` points the state's HTTP seam at an ASGI
transport over the **real** ``mcp_oauth_routes``, backed by the real service and
real rows. Mocking the two endpoints would have proven only that this file
agrees with itself; here a consent actually mints an ``OAuthGrant`` and an API
key, and a tampered link actually fails to.

The security cases are the point of the file. This page is a plain URL: anyone
can open it with any ``client_id`` and any ``redirect_uri``. It shows a name it
resolved server-side, and it will not bounce a browser to an address the client
never registered — Deny redirects, so unverified that is an open redirect
wearing Datanika's hostname.
"""

import asyncio
import base64
import hashlib
import inspect
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs, urlparse

import anyio
import httpx
import pytest
from starlette.applications import Starlette

import datanika.ui.state.mcp_consent_state as consent_module
from datanika.models.mcp_oauth import OAuthGrant
from datanika.models.user import Organization, User
from datanika.services.auth import AuthService
from datanika.services.mcp_oauth import McpOAuthService
from datanika.services.mcp_oauth_routes import mcp_oauth_routes
from datanika.ui.state.mcp_consent_state import McpConsentState, consent_url

_REDIRECT = "https://claude.ai/api/mcp/auth_callback"
_SECRET = "test-secret-key"
_CHALLENGE = base64.urlsafe_b64encode(hashlib.sha256(b"v" * 64).digest()).decode().rstrip("=")


def _params(**overrides) -> dict:
    """A well-formed authorization request as /oauth/authorize forwards it."""
    params = {
        "client_id": "mcp_test",
        "redirect_uri": _REDIRECT,
        "code_challenge": _CHALLENGE,
        "code_challenge_method": "S256",
        "scope": "connections:read runs:read",
        "state": "opaque-client-state",
    }
    params.update(overrides)
    return {k: v for k, v in params.items() if v is not None}


def _fake_state(params: dict | None = None, *, access_token: str = "jwt-token", **overrides):
    """A stand-in ``self`` carrying the state class's declared defaults.

    Reflex's ``__setattr__`` does parent-state delegation that needs a live app;
    these handlers only touch plain attributes, ``router`` and ``get_state``.
    """
    state = SimpleNamespace(
        client_name="",
        verified_redirect_uri="",
        request_params={},
        is_loading=True,
        is_submitting=False,
        error_code="",
        error_detail="",
        router=SimpleNamespace(page=SimpleNamespace(params=params or {})),
        _describe=McpConsentState._describe,
    )
    state.get_state = AsyncMock(return_value=SimpleNamespace(access_token=access_token))
    for key, value in overrides.items():
        setattr(state, key, value)
    return state


def _redirect_target(spec) -> str:
    """The URL an ``rx.redirect`` EventSpec sends the browser to."""
    assert spec is not None, "expected a redirect, got None"
    for name, value in spec.args:
        if name._js_expr == "path":
            return value._var_value
    raise AssertionError("EventSpec is not a redirect")


async def _events(result) -> list:
    """Every event a handler emits, whether it returns one or yields several.

    ``allow`` is an async generator (it flushes "Approving…" mid-flight), so
    its outcome is the *last* thing it yields rather than a return value.
    """
    if inspect.isasyncgen(result):
        return [event async for event in result if event is not None]
    return [] if result is None else [result]


async def _only_redirect(result) -> str:
    emitted = await _events(result)
    assert len(emitted) == 1, f"expected exactly one redirect, got {emitted}"
    return _redirect_target(emitted[0])


class TestConsentUrl:
    """The ``?next=`` payload that has to survive the login wall."""

    def test_carries_every_forwarded_param(self):
        url = consent_url(_params(resource="https://app.datanika.io/mcp"))
        query = parse_qs(urlparse(url).query)

        assert urlparse(url).path == "/oauth/consent"
        assert query["client_id"] == ["mcp_test"]
        assert query["code_challenge"] == [_CHALLENGE]
        assert query["state"] == ["opaque-client-state"]
        assert query["resource"] == ["https://app.datanika.io/mcp"]

    def test_drops_params_that_are_not_part_of_the_request(self):
        """Only what /oauth/authorize forwards — nothing a caller bolted on."""
        url = consent_url({**_params(), "next": "/evil", "token": "leak"})
        query = parse_qs(urlparse(url).query)

        assert "next" not in query
        assert "token" not in query


class TestLoginGate:
    async def test_logged_out_user_is_sent_to_login_and_back(self):
        """The MCP client cannot re-send this request, so it must survive login."""
        state = _fake_state(_params(), access_token="")

        target = _redirect_target(await McpConsentState.load_consent.fn(state))

        assert target.startswith("/login?next=")
        resumed = parse_qs(urlparse(target).query)["next"][0]
        assert parse_qs(urlparse(resumed).query)["client_id"] == ["mcp_test"]
        assert parse_qs(urlparse(resumed).query)["state"] == ["opaque-client-state"]

    async def test_spinner_holds_through_the_bounce(self):
        """Not a flash of a consent card with no app name in it."""
        state = _fake_state(_params(), access_token="")

        await McpConsentState.load_consent.fn(state)

        assert state.is_loading is True
        assert state.error_code == ""

    @pytest.mark.parametrize(
        "missing", ["client_id", "redirect_uri", "code_challenge", "code_challenge_method"]
    )
    async def test_incomplete_request_never_reaches_the_backend(self, missing):
        state = _fake_state(_params(**{missing: None}))

        with patch.object(consent_module, "_http_client") as client:
            await McpConsentState.load_consent.fn(state)

        assert state.error_code == "incomplete_request"
        assert state.is_loading is False
        client.assert_not_called()


class TestDeny:
    def test_returns_access_denied_with_state(self):
        state = _fake_state(verified_redirect_uri=_REDIRECT, request_params=_params())

        target = _redirect_target(McpConsentState.deny.fn(state))
        query = parse_qs(urlparse(target).query)

        assert query["error"] == ["access_denied"]
        assert query["state"] == ["opaque-client-state"]

    def test_appends_to_a_redirect_uri_that_already_has_a_query(self):
        uri = "https://claude.ai/cb?session=42"
        state = _fake_state(verified_redirect_uri=uri, request_params=_params())

        target = _redirect_target(McpConsentState.deny.fn(state))
        query = parse_qs(urlparse(target).query)

        assert query["session"] == ["42"]
        assert query["error"] == ["access_denied"]

    def test_omits_state_when_the_client_sent_none(self):
        state = _fake_state(verified_redirect_uri=_REDIRECT, request_params=_params(state=None))

        query = parse_qs(urlparse(_redirect_target(McpConsentState.deny.fn(state))).query)

        assert "state" not in query

    def test_never_redirects_to_an_unverified_address(self):
        """The open-redirect guard: no verification, no bounce."""
        state = _fake_state(
            verified_redirect_uri="", request_params=_params(redirect_uri="https://evil.test/cb")
        )

        assert McpConsentState.deny.fn(state) is None


class TestAllowPreconditions:
    async def test_refuses_without_a_verified_redirect(self):
        state = _fake_state(_params(), verified_redirect_uri="")

        with patch.object(consent_module, "_http_client") as client:
            assert await _events(McpConsentState.allow.fn(state)) == []

        client.assert_not_called()

    async def test_refuses_while_a_grant_is_already_in_flight(self):
        """Double-click must not mint two grants and two API keys."""
        state = _fake_state(_params(), verified_redirect_uri=_REDIRECT, is_submitting=True)

        with patch.object(consent_module, "_http_client") as client:
            assert await _events(McpConsentState.allow.fn(state)) == []

        client.assert_not_called()

    async def test_expired_session_goes_back_to_login_with_the_request(self):
        state = _fake_state(
            _params(),
            access_token="",
            verified_redirect_uri=_REDIRECT,
            request_params=_params(),
        )

        target = await _only_redirect(McpConsentState.allow.fn(state))

        assert target.startswith("/login?next=")
        resumed = parse_qs(urlparse(target).query)["next"][0]
        assert parse_qs(urlparse(resumed).query)["client_id"] == ["mcp_test"]


class TestComputedVars:
    def test_api_key_name_matches_what_consent_mints(self):
        """The user searches Settings -> API Keys for this exact string."""
        state = _fake_state(client_name="Claude.ai")

        assert McpConsentState.api_key_name.fget(state) == "MCP: Claude.ai"

    def test_is_ready_requires_a_verified_redirect(self):
        loaded = _fake_state(is_loading=False, verified_redirect_uri=_REDIRECT)
        unverified = _fake_state(is_loading=False, verified_redirect_uri="")
        errored = _fake_state(
            is_loading=False, verified_redirect_uri=_REDIRECT, error_code="rejected"
        )

        assert McpConsentState.is_ready.fget(loaded) is True
        assert McpConsentState.is_ready.fget(unverified) is False
        assert McpConsentState.is_ready.fget(errored) is False


# --------------------------------------------------------------------------- #
# Against the real authorization server
# --------------------------------------------------------------------------- #


@pytest.fixture
def backend(db_session, monkeypatch):
    """Wire the consent state to the real routes, service and rows.

    Mirrors the ``wired`` fixture in tests/test_services/test_mcp_oauth_routes.py:
    the routes commit, the fixture's outer transaction still rolls back, and the
    deployment's Fernet key is the insecure placeholder in tests so the service
    gets a throwaway one.
    """
    from contextlib import contextmanager

    import datanika.services.mcp_oauth_routes as routes

    @contextmanager
    def _session():
        yield db_session

    monkeypatch.setattr(routes, "_get_session", _session)
    monkeypatch.setattr(
        routes,
        "_service",
        lambda: McpOAuthService(
            public_base_url="https://app.datanika.io",
            encryption_key_provider=lambda: base64.urlsafe_b64encode(b"0" * 32).decode(),
        ),
    )
    monkeypatch.setattr(db_session, "commit", db_session.flush)

    app = Starlette(routes=mcp_oauth_routes)
    monkeypatch.setattr(
        consent_module,
        "_http_client",
        lambda: httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://consent.test"
        ),
    )
    return db_session


@pytest.fixture
def registered(backend) -> dict:
    """A client registered exactly as Claude.ai would register itself."""
    service = McpOAuthService(
        public_base_url="https://app.datanika.io",
        encryption_key_provider=lambda: base64.urlsafe_b64encode(b"0" * 32).decode(),
    )
    client = service.register_client(backend, client_name="Claude.ai", redirect_uris=[_REDIRECT])
    backend.flush()
    return client


@pytest.fixture
def session_jwt(backend) -> str:
    org = Organization(name="Acme", slug="acme-consent-screen")
    user = User(email="consent@example.com", password_hash="h", full_name="Consent User")
    backend.add_all([org, user])
    backend.flush()
    return AuthService(_SECRET).create_access_token(user_id=user.id, org_id=org.id)


class TestAgainstTheRealAuthorizationServer:
    async def test_app_name_comes_from_the_registration(self, registered):
        """Not from the URL — a crafted link must not get to pick the label."""
        state = _fake_state(
            _params(client_id=registered["client_id"], client_name="Datanika Official")
        )

        await McpConsentState.load_consent.fn(state)

        assert state.client_name == "Claude.ai"
        assert state.error_code == ""
        assert state.verified_redirect_uri == _REDIRECT
        assert state.is_loading is False

    async def test_unknown_client_is_refused(self, backend):
        state = _fake_state(_params(client_id="mcp_never_registered"))

        await McpConsentState.load_consent.fn(state)

        assert state.error_code == "unknown_client"
        assert state.client_name == ""

    async def test_unregistered_redirect_uri_is_refused(self, registered):
        """A hand-built link pointing the return trip somewhere else."""
        state = _fake_state(
            _params(client_id=registered["client_id"], redirect_uri="https://evil.test/collect")
        )

        await McpConsentState.load_consent.fn(state)

        assert state.error_code == "unregistered_redirect"
        assert state.verified_redirect_uri == ""
        # And Deny must not bounce there either.
        assert McpConsentState.deny.fn(state) is None

    async def test_allow_mints_a_grant_and_returns_to_the_client(
        self, backend, registered, session_jwt
    ):
        state = _fake_state(_params(client_id=registered["client_id"]), access_token=session_jwt)

        with patch("datanika.services.mcp_oauth_routes.settings.secret_key", _SECRET):
            await McpConsentState.load_consent.fn(state)
            target = await _only_redirect(McpConsentState.allow.fn(state))

        query = parse_qs(urlparse(target).query)
        assert target.startswith(_REDIRECT)
        assert query["state"] == ["opaque-client-state"]

        grant = backend.query(OAuthGrant).one()
        assert grant.client_id == registered["client_id"]
        assert grant.scope  # read-only set, narrowed server-side
        # The code travels once, in the URL — only its hash is stored.
        assert query["code"][0] not in (grant.code_hash or "")

    async def test_the_approving_state_reaches_the_browser_mid_flight(
        self, backend, registered, session_jwt
    ):
        """Why ``allow`` is a generator: one delta at the end is too late.

        A plain handler would send ``is_submitting`` bundled with the redirect,
        so the button would sit dead through the whole round-trip and then
        navigate — the click would look like it did nothing.
        """
        state = _fake_state(_params(client_id=registered["client_id"]), access_token=session_jwt)

        with patch("datanika.services.mcp_oauth_routes.settings.secret_key", _SECRET):
            await McpConsentState.load_consent.fn(state)
            grant = McpConsentState.allow.fn(state)

            assert await grant.__anext__() is None  # the bare flush
            assert state.is_submitting is True  # ...and it carries the affordance

            remaining = [event async for event in grant if event is not None]

        assert _redirect_target(remaining[0]).startswith(_REDIRECT)

    async def test_the_minted_key_is_the_one_the_screen_told_the_user_to_revoke(
        self, backend, registered, session_jwt
    ):
        from datanika.models.api_key import ApiKey

        state = _fake_state(_params(client_id=registered["client_id"]), access_token=session_jwt)

        with patch("datanika.services.mcp_oauth_routes.settings.secret_key", _SECRET):
            await McpConsentState.load_consent.fn(state)
            await _events(McpConsentState.allow.fn(state))

        assert backend.query(ApiKey).one().name == McpConsentState.api_key_name.fget(state)

    async def test_a_backend_refusal_is_shown_not_redirected(
        self, backend, registered, session_jwt
    ):
        """OAuth 2.1 requires S256; the AS refuses `plain` at consent time.

        The client is entitled to an error at its redirect_uri only for errors
        the *user* caused, so a malformed request stops here.
        """
        state = _fake_state(
            _params(client_id=registered["client_id"], code_challenge_method="plain"),
            access_token=session_jwt,
        )

        with patch("datanika.services.mcp_oauth_routes.settings.secret_key", _SECRET):
            await McpConsentState.load_consent.fn(state)
            assert await _events(McpConsentState.allow.fn(state)) == []

        assert state.error_code == "rejected"
        assert "S256" in state.error_detail
        assert state.is_submitting is False
        assert backend.query(OAuthGrant).count() == 0

    async def test_an_unauthenticated_consent_call_mints_nothing(self, backend, registered):
        """The grant binds to the signed session; a junk JWT gets a 401."""
        state = _fake_state(
            _params(client_id=registered["client_id"]), access_token="not-a-real-jwt"
        )

        with patch("datanika.services.mcp_oauth_routes.settings.secret_key", _SECRET):
            await McpConsentState.load_consent.fn(state)
            assert await _events(McpConsentState.allow.fn(state)) == []

        assert state.error_code == "rejected"
        assert backend.query(OAuthGrant).count() == 0


# --------------------------------------------------------------------------- #
# core#388 guard — this page calls back into the app it is served by
# --------------------------------------------------------------------------- #


async def _serve_oauth_api(*, redirect_to: str, seen: list) -> tuple[asyncio.AbstractServer, int]:
    """Stand-in for our own backend, on the *test's own* event loop.

    Sharing the loop is the whole point: it can only answer if the handler
    under test yields, which is exactly the invariant #388 violated.
    """

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = (await reader.readuntil(b"\r\n\r\n")).decode("latin-1")
            length = next(
                (
                    int(line.split(":", 1)[1])
                    for line in head.split("\r\n")
                    if line.lower().startswith("content-length:")
                ),
                0,
            )
            body = (await reader.readexactly(length)).decode() if length else ""
            seen.append((head, body))

            payload = (
                {
                    "client_id": "mcp_test",
                    "client_name": "Claude.ai",
                    "redirect_uris": [_REDIRECT],
                }
                if head.startswith("GET /api/oauth/client-info")
                else {"redirect_to": redirect_to}
            )
            data = json.dumps(payload).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                + str(len(data)).encode()
                + b"\r\nConnection: close\r\n\r\n"
                + data
            )
            await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass  # client vanished during teardown
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


class TestRealSocketLoopback:
    """The consent call is a loopback, and loopbacks are how #388 happened.

    ``/oauth/consent`` is served by the same backend process it POSTs to, so a
    blocking ``httpx.Client`` here would hold the only loop that could answer
    the request it is making — the page would hang on Allow, in production
    only, with every mocked-transport test still green. The ASGI-transport
    tests above cannot see it: they never perform real I/O.
    """

    async def test_the_client_and_handlers_are_async(self):
        """Structural: a future `httpx.Client` or `def` handler re-opens it."""
        client = consent_module._http_client()
        try:
            assert isinstance(client, httpx.AsyncClient)
        finally:
            await client.aclose()

        # Either async form is fine (``allow`` is a generator so it can flush
        # mid-flight); a plain ``def`` would run its I/O inline on the loop.
        for handler in (McpConsentState.load_consent.fn, McpConsentState.allow.fn):
            assert inspect.iscoroutinefunction(handler) or inspect.isasyncgenfunction(handler), (
                f"{handler.__name__} must be async (core#388): this page POSTs back "
                "to the backend that serves it, so blocking I/O here holds the only "
                "loop that could answer."
            )

    async def test_allow_over_a_same_loop_socket_completes(self, monkeypatch):
        """Behavioural: real sockets, no ASGI shortcut. Blocked loop -> timeout."""
        seen: list = []
        api, port = await _serve_oauth_api(
            redirect_to=f"{_REDIRECT}?code=dtk_ac_real&state=opaque-client-state", seen=seen
        )
        monkeypatch.setattr(
            consent_module,
            "_http_client",
            lambda: httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=10.0),
        )
        state = _fake_state(_params(), access_token="jwt-abc")

        try:
            # Generous vs. a healthy round-trip (ms). A held loop cannot answer,
            # so this fails fast instead of hanging on the client timeout.
            with anyio.fail_after(10):
                await McpConsentState.load_consent.fn(state)
                target = await _only_redirect(McpConsentState.allow.fn(state))
        finally:
            api.close()
            await api.wait_closed()

        assert target == f"{_REDIRECT}?code=dtk_ac_real&state=opaque-client-state"
        assert state.client_name == "Claude.ai"

        lookup_head, _ = seen[0]
        consent_head, consent_body = seen[1]
        assert "client_id=mcp_test" in lookup_head
        # The JWT rides in the header, never the query string.
        assert "Bearer jwt-abc" in consent_head
        assert "jwt-abc" not in consent_head.split("\r\n")[0]
        assert json.loads(consent_body)["code_challenge"] == _CHALLENGE
