"""HTTP-level tests for the MCP OAuth AS (core#393, Remote-MCP P2).

Drives the endpoints an MCP client actually calls, in the order it calls them:
discovery → registration → authorize → consent → token. The service-level
guarantees (PKCE, single-use codes, resource binding) are proven in
``test_mcp_oauth.py``; this file is about the wire — status codes, CORS,
form-vs-JSON bodies, the consent hand-off, and the ``WWW-Authenticate`` pointer
that makes the flow discoverable in the first place.
"""

import base64
import hashlib
import pathlib
import sys
from unittest.mock import patch

import httpx
import pytest
from starlette.applications import Starlette

from datanika.models.user import Organization, User
from datanika.services.auth import AuthService
from datanika.services.mcp_oauth import MCP_RESOURCE_PATH, McpOAuthService
from datanika.services.mcp_oauth_routes import mcp_oauth_routes
from tests.factories import make_user

# datanika-mcp is not installed in the test venv; the resource-server test
# below needs its transport, so make the package importable first.
_MCP_SRC = str(pathlib.Path(__file__).resolve().parents[2] / "datanika-mcp" / "src")
if _MCP_SRC not in sys.path:
    sys.path.insert(0, _MCP_SRC)

_PUBLIC_BASE = "https://app.datanika.io"
_REDIRECT = "https://claude.ai/api/mcp/auth_callback"
_SECRET = "test-secret-key"


def _pkce(verifier: str = "v" * 64) -> tuple[str, str]:
    digest = hashlib.sha256(verifier.encode()).digest()
    return verifier, base64.urlsafe_b64encode(digest).decode().rstrip("=")


@pytest.fixture
def app() -> Starlette:
    return Starlette(routes=mcp_oauth_routes)


@pytest.fixture
def wired(db_session, monkeypatch):
    """Point the routes at the test session + a fixed public base URL."""
    from contextlib import contextmanager

    import datanika.services.mcp_oauth_routes as routes

    @contextmanager
    def _session():
        # The routes commit; the fixture's outer transaction still rolls back.
        yield db_session

    monkeypatch.setattr(routes, "_get_session", _session)
    monkeypatch.setattr(
        routes,
        "_service",
        # A throwaway Fernet key: the deployment's real CREDENTIAL_ENCRYPTION_KEY
        # is the insecure placeholder in tests, which Fernet rejects outright.
        lambda: McpOAuthService(
            public_base_url=_PUBLIC_BASE,
            encryption_key_provider=lambda: base64.urlsafe_b64encode(b"0" * 32).decode(),
        ),
    )
    monkeypatch.setattr(db_session, "commit", db_session.flush)
    return db_session


@pytest.fixture
def org(db_session) -> Organization:
    row = Organization(name="Acme", slug="acme-oauth-routes")
    db_session.add(row)
    db_session.flush()
    return row


@pytest.fixture
def user(db_session) -> User:
    return make_user(
        db_session, email="routes@example.com", full_name="Routes User", password_hash="h"
    )


@pytest.fixture
def jwt(org, user) -> str:
    return AuthService(_SECRET).create_access_token(user_id=user.id, org_id=org.id)


async def _client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


class TestDiscovery:
    async def test_protected_resource_metadata_points_at_the_as(self, app, wired):
        async with await _client(app) as c:
            r = await c.get("/.well-known/oauth-protected-resource")

        assert r.status_code == 200
        body = r.json()
        assert body["resource"] == f"{_PUBLIC_BASE}{MCP_RESOURCE_PATH}"
        assert body["authorization_servers"] == [_PUBLIC_BASE]

    async def test_as_metadata_requires_s256_and_public_clients(self, app, wired):
        async with await _client(app) as c:
            r = await c.get("/.well-known/oauth-authorization-server")

        body = r.json()
        # OAuth 2.1: `plain` must not be advertised, or clients will offer it.
        assert body["code_challenge_methods_supported"] == ["S256"]
        assert body["token_endpoint_auth_methods_supported"] == ["none"]
        assert body["registration_endpoint"] == f"{_PUBLIC_BASE}/oauth/register"

    async def test_discovery_is_cors_readable(self, app, wired):
        """Browser-based clients fetch discovery cross-origin; no CORS, no flow."""
        async with await _client(app) as c:
            r = await c.get("/.well-known/oauth-protected-resource")
            preflight = await c.options("/.well-known/oauth-authorization-server")

        assert r.headers["access-control-allow-origin"] == "*"
        assert preflight.status_code == 204


class TestRegistration:
    async def test_registers_and_returns_201(self, app, wired):
        async with await _client(app) as c:
            r = await c.post(
                "/oauth/register",
                json={"client_name": "Claude.ai", "redirect_uris": [_REDIRECT]},
            )

        assert r.status_code == 201
        assert r.json()["client_id"].startswith("mcp_")

    async def test_rejects_plaintext_redirect(self, app, wired):
        async with await _client(app) as c:
            r = await c.post(
                "/oauth/register",
                json={"client_name": "Sketchy", "redirect_uris": ["http://evil.test/cb"]},
            )

        assert r.status_code == 400
        assert r.json()["error"] == "invalid_redirect_uri"


class TestAuthorize:
    async def _register(self, c) -> str:
        r = await c.post(
            "/oauth/register", json={"client_name": "Claude.ai", "redirect_uris": [_REDIRECT]}
        )
        return r.json()["client_id"]

    async def test_valid_request_redirects_to_the_consent_screen(self, app, wired):
        _verifier, challenge = _pkce()
        async with await _client(app) as c:
            client_id = await self._register(c)
            r = await c.get(
                "/oauth/authorize",
                params={
                    "client_id": client_id,
                    "redirect_uri": _REDIRECT,
                    "response_type": "code",
                    "code_challenge": challenge,
                    "code_challenge_method": "S256",
                    "state": "xyz",
                },
            )

        assert r.status_code == 302
        assert "/oauth/consent?" in r.headers["location"]
        assert "state=xyz" in r.headers["location"]

    async def test_unregistered_redirect_does_not_redirect(self, app, wired):
        """An open redirect here would hand codes to whoever asked for one."""
        _verifier, challenge = _pkce()
        async with await _client(app) as c:
            client_id = await self._register(c)
            r = await c.get(
                "/oauth/authorize",
                params={
                    "client_id": client_id,
                    "redirect_uri": "https://evil.test/cb",
                    "response_type": "code",
                    "code_challenge": challenge,
                    "code_challenge_method": "S256",
                },
            )

        assert r.status_code == 400
        assert r.json()["error"] == "invalid_request"

    async def test_plain_pkce_is_refused(self, app, wired):
        async with await _client(app) as c:
            client_id = await self._register(c)
            r = await c.get(
                "/oauth/authorize",
                params={
                    "client_id": client_id,
                    "redirect_uri": _REDIRECT,
                    "response_type": "code",
                    "code_challenge": "anything",
                    "code_challenge_method": "plain",
                },
            )

        assert r.status_code == 400


class TestClientInfo:
    """The consent screen resolves the app's name server-side, not from the URL."""

    async def test_returns_the_registered_name(self, app, wired):
        async with await _client(app) as c:
            reg = await c.post(
                "/oauth/register", json={"client_name": "Claude.ai", "redirect_uris": [_REDIRECT]}
            )
            r = await c.get("/api/oauth/client-info", params={"client_id": reg.json()["client_id"]})

        assert r.status_code == 200
        assert r.json()["client_name"] == "Claude.ai"

    async def test_unknown_client_is_404(self, app, wired):
        async with await _client(app) as c:
            r = await c.get("/api/oauth/client-info", params={"client_id": "mcp_nope"})
        assert r.status_code == 404


class TestConsentAndToken:
    async def _flow_to_code(self, c, jwt: str) -> tuple[str, str]:
        """Register → consent → return (code, verifier)."""
        verifier, challenge = _pkce()
        reg = await c.post(
            "/oauth/register", json={"client_name": "Claude.ai", "redirect_uris": [_REDIRECT]}
        )
        client_id = reg.json()["client_id"]

        consent = await c.post(
            "/api/oauth/consent",
            headers={"Authorization": f"Bearer {jwt}"},
            json={
                "client_id": client_id,
                "redirect_uri": _REDIRECT,
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "state": "xyz",
            },
        )
        assert consent.status_code == 200, consent.text
        redirect_to = consent.json()["redirect_to"]
        code = redirect_to.split("code=")[1].split("&")[0]
        return code, verifier

    async def test_consent_requires_the_app_session(self, app, wired):
        async with await _client(app) as c:
            r = await c.post("/api/oauth/consent", json={"client_id": "mcp_x"})
        assert r.status_code == 401

    async def test_consent_rejects_a_forged_jwt(self, app, wired):
        async with await _client(app) as c:
            r = await c.post(
                "/api/oauth/consent",
                headers={"Authorization": "Bearer not-a-real-jwt"},
                json={"client_id": "mcp_x"},
            )
        assert r.status_code == 401

    async def test_full_flow_issues_a_token(self, app, wired, jwt):
        with patch("datanika.services.mcp_oauth_routes.settings.secret_key", _SECRET):
            async with await _client(app) as c:
                code, verifier = await self._flow_to_code(c, jwt)
                # Form-encoded, as an OAuth client posts it.
                r = await c.post(
                    "/oauth/token",
                    data={
                        "grant_type": "authorization_code",
                        "code": code,
                        "code_verifier": verifier,
                        "redirect_uri": _REDIRECT,
                    },
                )

        assert r.status_code == 200
        body = r.json()
        assert body["token_type"] == "Bearer"
        assert body["access_token"].startswith("dtk_at_")
        # Tokens must never be cached by an intermediary.
        assert r.headers["cache-control"] == "no-store"

    async def test_wrong_verifier_is_refused_at_the_token_endpoint(self, app, wired, jwt):
        with patch("datanika.services.mcp_oauth_routes.settings.secret_key", _SECRET):
            async with await _client(app) as c:
                code, _verifier = await self._flow_to_code(c, jwt)
                r = await c.post(
                    "/oauth/token",
                    data={
                        "grant_type": "authorization_code",
                        "code": code,
                        "code_verifier": "wrong" * 13,
                        "redirect_uri": _REDIRECT,
                    },
                )

        assert r.status_code == 400
        assert r.json()["error"] == "invalid_grant"

    async def test_unsupported_grant_type(self, app, wired):
        async with await _client(app) as c:
            r = await c.post("/oauth/token", data={"grant_type": "password"})
        assert r.status_code == 400
        assert r.json()["error"] == "unsupported_grant_type"

    async def test_consent_takes_the_org_from_the_token_not_the_body(
        self, app, wired, db_session, jwt, org, user
    ):
        """The body carries `client_id`; it must not be able to carry the org.

        `/api/oauth/consent` mints a **real API key** for whatever org the grant
        names, so a body-supplied `org_id` would be a one-request path to a
        working credential for someone else's tenant. The handler takes the org
        from the signed access token and says so in a comment — this pins the
        behaviour, because a comment is not a control.

        Pinned by `tests/test_security/test_body_carried_reference_census.py`
        (core#719 item 2), which requires every body-carried reference to name a
        test. `client_id` is the reference here; the assertion is that the org
        beside it is *not* one.
        """
        from datanika.models.api_key import ApiKey
        from datanika.models.mcp_oauth import OAuthGrant

        victim = Organization(name="Victim", slug="victim-oauth-routes")
        db_session.add(victim)
        db_session.flush()
        assert victim.id != org.id

        _verifier, challenge = _pkce()
        with patch("datanika.services.mcp_oauth_routes.settings.secret_key", _SECRET):
            async with await _client(app) as c:
                reg = await c.post(
                    "/oauth/register",
                    json={"client_name": "Claude.ai", "redirect_uris": [_REDIRECT]},
                )
                client_id = reg.json()["client_id"]
                r = await c.post(
                    "/api/oauth/consent",
                    headers={"Authorization": f"Bearer {jwt}"},
                    json={
                        "client_id": client_id,
                        "redirect_uri": _REDIRECT,
                        "code_challenge": challenge,
                        "code_challenge_method": "S256",
                        # The poison: a body claiming a different tenant.
                        "org_id": victim.id,
                        "user_id": 999999,
                    },
                )

        assert r.status_code == 200, r.text
        grants = db_session.query(OAuthGrant).all()
        assert len(grants) == 1, f"expected exactly one grant, got {len(grants)}"
        grant = grants[0]
        assert grant.org_id == org.id, (
            f"the grant was recorded against org {grant.org_id} (the request BODY's "
            f"{victim.id}) instead of the signed token's {org.id}"
        )
        key = db_session.get(ApiKey, grant.api_key_id)
        assert key is not None and key.org_id == org.id, (
            f"consent minted an API key for org {key and key.org_id} from a body-supplied "
            f"org_id; the token said {org.id}"
        )


class TestResourceServerAcceptsIssuedTokens:
    """The `/mcp` side of P2: discovery pointer + OAuth tokens as credentials."""

    async def test_401_carries_the_resource_metadata_pointer(self):
        """This header is how an MCP client finds the AS at all."""
        import datanika.services.mcp_routes as mcp_routes

        app = mcp_routes.BearerSessionApp(_unused_asgi)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as c:
            r = await c.post("/mcp")

        assert r.status_code == 401
        challenge = r.headers["www-authenticate"]
        assert "resource_metadata=" in challenge
        assert "/.well-known/oauth-protected-resource" in challenge

    async def test_oauth_token_resolves_to_the_granted_api_key(self, monkeypatch):
        """A `dtk_at_` bearer is exchanged for the minted key before forwarding."""
        import datanika.services.mcp_routes as mcp_routes
        from datanika.services.mcp_oauth import ResolvedGrant

        # Since core#442 the resolver returns the credential *and* the
        # authority the human granted, not a bare key.
        monkeypatch.setattr(
            mcp_routes,
            "_resolve_oauth_token_sync",
            lambda token: ResolvedGrant(
                api_key="etf_resolved_key", allow_write=False, scope="connections:read"
            ),
        )
        seen = {}

        class _FakeClient:
            def __init__(self, base_url, key):
                seen["key"] = key

            async def aclose(self):
                pass

        monkeypatch.setattr(mcp_routes, "DatanikaClient", _FakeClient)

        app = mcp_routes.BearerSessionApp(_ok_asgi)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as c:
            r = await c.post("/mcp", headers={"Authorization": "Bearer dtk_at_something"})

        assert r.status_code == 200
        # The forwarded credential is the API key, never the OAuth token.
        assert seen["key"] == "etf_resolved_key"

    async def test_unresolvable_oauth_token_401s(self, monkeypatch):
        import datanika.services.mcp_routes as mcp_routes

        monkeypatch.setattr(mcp_routes, "_resolve_oauth_token_sync", lambda token: None)

        app = mcp_routes.BearerSessionApp(_unused_asgi)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as c:
            r = await c.post("/mcp", headers={"Authorization": "Bearer dtk_at_revoked"})

        assert r.status_code == 401

    async def test_raw_api_key_still_works(self, monkeypatch):
        """P1 back-compat: a pasted key is forwarded untouched, no DB lookup."""
        import datanika.services.mcp_routes as mcp_routes

        def _explode(token):  # pragma: no cover - must not be called
            raise AssertionError("a raw API key must not hit the OAuth token table")

        monkeypatch.setattr(mcp_routes, "_resolve_oauth_token_sync", _explode)
        seen = {}

        class _FakeClient:
            def __init__(self, base_url, key):
                seen["key"] = key

            async def aclose(self):
                pass

        monkeypatch.setattr(mcp_routes, "DatanikaClient", _FakeClient)

        app = mcp_routes.BearerSessionApp(_ok_asgi)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://t"
        ) as c:
            r = await c.post("/mcp", headers={"Authorization": "Bearer etf_pasted"})

        assert r.status_code == 200
        assert seen["key"] == "etf_pasted"


async def _ok_asgi(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": b"ok"})


async def _unused_asgi(scope, receive, send):  # pragma: no cover - must not run
    raise AssertionError("the transport must not be reached on a rejected credential")
