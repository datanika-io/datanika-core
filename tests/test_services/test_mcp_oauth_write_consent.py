"""Write access is granted at OAuth consent, or not at all (core#442, MCP P3).

SPEC_REMOTE_MCP §10 asked for a per-call confirmation before a remote agent
spends compute or money. That was written assuming an interactive session; the
hosted transport is stateless, and a two-phase token the agent mints and echoes
to itself is friction, not a gate — the agent is the thing being gated.

So the human decision moves to **consent time**: the user decides once,
knowingly, whether this client may write, and the grant's API key carries
exactly that. Enforcement stays where it already lives — ``api_middleware`` on
the minted key's scopes — so the MCP layer adds no authz of its own (§5.4).

**Why there is no separate "may build but may not trigger" tier:** the REST API
gives ``create_upload`` and ``trigger_upload`` the *same* scope
(``uploads:write``). A consent checkbox splitting them could only be enforced
in the MCP layer while the credential still permitted both — a control the
token doesn't back, which is the same theatre as the echoed confirmation token.
Spend is bounded by the per-key rate limit and byte quota instead, which is
what §10 named alongside the phasing.

These tests are about the *grant*, since that is where the decision now lives.
"""

import base64

import pytest

from datanika.models.api_key import ApiKey
from datanika.models.mcp_oauth import OAuthClient
from datanika.models.user import Organization, User
from datanika.services.mcp_oauth import (
    MCP_RESOURCE_PATH,
    McpOAuthService,
    read_only_scopes,
    write_scopes,
)
from tests.factories import make_user

_REDIRECT = "https://claude.ai/api/mcp/auth_callback"
_TEST_FERNET_KEY = base64.urlsafe_b64encode(b"0" * 32).decode()
_PUBLIC_BASE = "https://app.datanika.io"


def _pkce_challenge() -> str:
    import hashlib

    digest = hashlib.sha256(b"v" * 64).digest()
    return base64.urlsafe_b64encode(digest).decode().rstrip("=")


@pytest.fixture
def svc() -> McpOAuthService:
    return McpOAuthService(
        public_base_url=_PUBLIC_BASE, encryption_key_provider=lambda: _TEST_FERNET_KEY
    )


@pytest.fixture
def org(db_session) -> Organization:
    row = Organization(name="Acme", slug="acme-p3")
    db_session.add(row)
    db_session.flush()
    return row


@pytest.fixture
def user(db_session) -> User:
    return make_user(db_session, email="p3@example.com", full_name="P3 User", password_hash="h")


@pytest.fixture
def client_row(db_session) -> OAuthClient:
    row = OAuthClient(client_id="mcp_p3", client_name="Claude.ai", redirect_uris=[_REDIRECT])
    db_session.add(row)
    db_session.flush()
    return row


def _consent(svc, db_session, client_row, org, user, scope):
    return svc.grant_consent(
        db_session,
        client_id=client_row.client_id,
        org_id=org.id,
        user_id=user.id,
        redirect_uri=_REDIRECT,
        code_challenge=_pkce_challenge(),
        code_challenge_method="S256",
        scope=scope,
        resource=f"{_PUBLIC_BASE}{MCP_RESOURCE_PATH}",
    )


class TestWriteRequiresAnExplicitAsk:
    def test_default_consent_is_still_read_only(self, db_session, svc, client_row, org, user):
        """Clients routinely omit `scope`; that must never mean write."""
        grant, _code = _consent(svc, db_session, client_row, org, user, "")
        key = db_session.get(ApiKey, grant.api_key_id)

        assert not any(s.endswith(":write") for s in key.scopes)

    def test_read_scopes_alone_do_not_escalate(self, db_session, svc, client_row, org, user):
        grant, _code = _consent(
            svc, db_session, client_row, org, user, " ".join(read_only_scopes())
        )
        key = db_session.get(ApiKey, grant.api_key_id)
        assert not any(s.endswith(":write") for s in key.scopes)

    def test_asking_for_write_grants_write(self, db_session, svc, client_row, org, user):
        grant, _code = _consent(svc, db_session, client_row, org, user, " ".join(write_scopes()))
        key = db_session.get(ApiKey, grant.api_key_id)

        assert any(s.endswith(":write") for s in key.scopes)
        # Read access comes along with it — a write-only grant is useless.
        assert any(s.endswith(":read") for s in key.scopes)

    def test_unknown_scopes_are_dropped_not_honoured(self, db_session, svc, client_row, org, user):
        grant, _code = _consent(
            svc, db_session, client_row, org, user, "connections:read admin:everything"
        )
        key = db_session.get(ApiKey, grant.api_key_id)
        assert "admin:everything" not in key.scopes


class TestSessionReflectsTheGrant:
    """The tool surface must open only as far as the grant actually went."""

    def test_read_only_grant_resolves_without_write(self, db_session, svc, client_row, org, user):
        grant, code = _consent(svc, db_session, client_row, org, user, "")
        issued = svc.exchange_code(
            db_session, code=code, code_verifier="v" * 64, redirect_uri=_REDIRECT
        )

        resolved = svc.resolve_access_token(db_session, issued["access_token"])
        assert resolved is not None
        assert resolved.allow_write is False

    def test_write_grant_resolves_with_write(self, db_session, svc, client_row, org, user):
        grant, code = _consent(svc, db_session, client_row, org, user, " ".join(write_scopes()))
        issued = svc.exchange_code(
            db_session, code=code, code_verifier="v" * 64, redirect_uri=_REDIRECT
        )

        resolved = svc.resolve_access_token(db_session, issued["access_token"])
        assert resolved.allow_write is True
        assert resolved.api_key.startswith("etf_")

    def test_revoked_key_still_kills_a_write_grant(self, db_session, svc, client_row, org, user):
        """Revocation must not become weaker just because write was granted."""
        from datetime import UTC, datetime

        grant, code = _consent(svc, db_session, client_row, org, user, " ".join(write_scopes()))
        issued = svc.exchange_code(
            db_session, code=code, code_verifier="v" * 64, redirect_uri=_REDIRECT
        )
        key = db_session.get(ApiKey, grant.api_key_id)
        key.deleted_at = datetime.now(UTC)
        db_session.flush()

        assert svc.resolve_access_token(db_session, issued["access_token"]) is None


class TestAdvertisedScopes:
    def test_metadata_advertises_write_so_clients_can_ask(self, svc):
        """A client cannot request what the AS never advertises."""
        supported = svc.authorization_server_metadata()["scopes_supported"]
        assert any(s.endswith(":write") for s in supported)
        assert any(s.endswith(":read") for s in supported)
