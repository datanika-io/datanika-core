"""Tests for the MCP OAuth 2.1 authorization server (core#393, Remote-MCP P2).

The security-critical half of P2 lives in ``McpOAuthService``: PKCE
verification, single-use authorization codes, redirect-URI matching, resource
binding, and resolving an access token back to the org-scoped API key the tool
path replays. Everything here is exercised against a real (SQLite) session with
real rows — the failure modes this guards against are all about *state*
(a code redeemed twice, a revoked key still working), which a mocked session
cannot express.
"""

import base64
import hashlib
from datetime import UTC, datetime, timedelta

import pytest

from datanika.models.api_key import ApiKey
from datanika.models.mcp_oauth import OAuthClient, OAuthToken
from datanika.models.user import Organization, User
from datanika.services.mcp_oauth import (
    MCP_RESOURCE_PATH,
    McpOAuthError,
    McpOAuthService,
    read_only_scopes,
)

_REDIRECT = "https://claude.ai/api/mcp/auth_callback"


def _pkce(verifier: str = "a" * 64) -> tuple[str, str]:
    """Return (verifier, S256 challenge) exactly as an MCP client computes it."""
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).decode().rstrip("=")
    return verifier, challenge


# A throwaway Fernet key so the tests never depend on the deployment's real one.
_TEST_FERNET_KEY = base64.urlsafe_b64encode(b"0" * 32).decode()
_PUBLIC_BASE = "https://app.datanika.io"


@pytest.fixture
def svc() -> McpOAuthService:
    return McpOAuthService(
        public_base_url=_PUBLIC_BASE, encryption_key_provider=lambda: _TEST_FERNET_KEY
    )


@pytest.fixture
def org(db_session) -> Organization:
    row = Organization(name="Acme", slug="acme-mcp-oauth")
    db_session.add(row)
    db_session.flush()
    return row


@pytest.fixture
def user(db_session) -> User:
    row = User(email="oauth@example.com", password_hash="hashed", full_name="OAuth User")
    db_session.add(row)
    db_session.flush()
    return row


@pytest.fixture
def client_row(db_session) -> OAuthClient:
    client = OAuthClient(
        client_id="mcp_testclient", client_name="Claude.ai", redirect_uris=[_REDIRECT]
    )
    db_session.add(client)
    db_session.flush()
    return client


@pytest.fixture
def consented(db_session, svc, client_row, org, user):
    """A completed consent → (grant, code, verifier)."""
    verifier, challenge = _pkce()
    grant, code = svc.grant_consent(
        db_session,
        client_id=client_row.client_id,
        org_id=org.id,
        user_id=user.id,
        redirect_uri=_REDIRECT,
        code_challenge=challenge,
        code_challenge_method="S256",
        scope=" ".join(read_only_scopes()),
        resource=f"https://app.datanika.io{MCP_RESOURCE_PATH}",
    )
    db_session.flush()
    return grant, code, verifier


class TestConsentMintsAReadOnlyKey:
    def test_mints_one_org_scoped_api_key(self, db_session, consented, org):
        grant, _code, _verifier = consented
        key = db_session.get(ApiKey, grant.api_key_id)

        assert key is not None
        assert key.org_id == org.id
        # Read-only: every granted scope ends in :read, so the write endpoints
        # reject this key at api_middleware without MCP re-implementing authz.
        assert key.scopes and all(s.endswith(":read") for s in key.scopes)
        assert not any(s.endswith(":write") for s in key.scopes)

    def test_key_is_recoverable_only_via_encryption(self, db_session, svc, consented):
        """The tool path replays the key, so it must decrypt — and only decrypt."""
        grant, _code, _verifier = consented
        assert "etf_" not in grant.encrypted_api_key  # not stored in the clear
        assert svc.decrypt_api_key(grant).startswith("etf_")


class TestAuthorizationCodeExchange:
    def test_happy_path_returns_access_and_refresh_tokens(self, db_session, svc, consented):
        grant, code, verifier = consented

        issued = svc.exchange_code(
            db_session, code=code, code_verifier=verifier, redirect_uri=_REDIRECT
        )

        assert issued["token_type"] == "Bearer"
        assert issued["access_token"] and issued["refresh_token"]
        assert issued["expires_in"] > 0
        assert issued["scope"] == grant.scope

    def test_code_is_single_use(self, db_session, svc, consented):
        _grant, code, verifier = consented
        svc.exchange_code(db_session, code=code, code_verifier=verifier, redirect_uri=_REDIRECT)

        with pytest.raises(McpOAuthError, match="invalid_grant"):
            svc.exchange_code(db_session, code=code, code_verifier=verifier, redirect_uri=_REDIRECT)

    def test_wrong_verifier_is_rejected(self, db_session, svc, consented):
        _grant, code, _verifier = consented
        with pytest.raises(McpOAuthError, match="invalid_grant"):
            svc.exchange_code(db_session, code=code, code_verifier="b" * 64, redirect_uri=_REDIRECT)

    def test_mismatched_redirect_uri_is_rejected(self, db_session, svc, consented):
        _grant, code, verifier = consented
        with pytest.raises(McpOAuthError, match="invalid_grant"):
            svc.exchange_code(
                db_session, code=code, code_verifier=verifier, redirect_uri="https://evil.test/cb"
            )

    def test_expired_code_is_rejected(self, db_session, svc, consented):
        grant, code, verifier = consented
        grant.code_expires_at = datetime.now(UTC) - timedelta(seconds=1)
        db_session.flush()

        with pytest.raises(McpOAuthError, match="invalid_grant"):
            svc.exchange_code(db_session, code=code, code_verifier=verifier, redirect_uri=_REDIRECT)


class TestPkceIsMandatory:
    def test_plain_challenge_method_is_refused(self, db_session, svc, client_row, org, user):
        """OAuth 2.1 drops `plain`; accepting it would make PKCE decorative."""
        with pytest.raises(McpOAuthError, match="invalid_request"):
            svc.grant_consent(
                db_session,
                client_id=client_row.client_id,
                org_id=org.id,
                user_id=user.id,
                redirect_uri=_REDIRECT,
                code_challenge="whatever",
                code_challenge_method="plain",
                scope=" ".join(read_only_scopes()),
                resource=f"https://app.datanika.io{MCP_RESOURCE_PATH}",
            )

    def test_unregistered_redirect_uri_is_refused_at_consent(
        self, db_session, svc, client_row, org, user
    ):
        _verifier, challenge = _pkce()
        with pytest.raises(McpOAuthError, match="invalid_request"):
            svc.grant_consent(
                db_session,
                client_id=client_row.client_id,
                org_id=org.id,
                user_id=user.id,
                redirect_uri="https://evil.test/cb",
                code_challenge=challenge,
                code_challenge_method="S256",
                scope=" ".join(read_only_scopes()),
                resource=f"https://app.datanika.io{MCP_RESOURCE_PATH}",
            )


class TestAccessTokenResolution:
    """Resolving a bearer token to the credential the MCP tool path replays."""

    def test_resolves_to_the_granted_api_key(self, db_session, svc, consented):
        grant, code, verifier = consented
        issued = svc.exchange_code(
            db_session, code=code, code_verifier=verifier, redirect_uri=_REDIRECT
        )

        resolved = svc.resolve_access_token(db_session, issued["access_token"])
        assert resolved == svc.decrypt_api_key(grant)

    def test_unknown_token_resolves_to_none(self, db_session, svc):
        assert svc.resolve_access_token(db_session, "dtk_at_nonsense") is None

    def test_expired_token_resolves_to_none(self, db_session, svc, consented):
        _grant, code, verifier = consented
        issued = svc.exchange_code(
            db_session, code=code, code_verifier=verifier, redirect_uri=_REDIRECT
        )
        token = db_session.query(OAuthToken).one()
        token.expires_at = datetime.now(UTC) - timedelta(seconds=1)
        db_session.flush()

        assert svc.resolve_access_token(db_session, issued["access_token"]) is None

    def test_revoking_the_api_key_kills_the_token(self, db_session, svc, consented):
        """Revocation is the existing API-keys UI (SPEC §5.3) — it must bite here."""
        grant, code, verifier = consented
        issued = svc.exchange_code(
            db_session, code=code, code_verifier=verifier, redirect_uri=_REDIRECT
        )
        key = db_session.get(ApiKey, grant.api_key_id)
        key.deleted_at = datetime.now(UTC)
        db_session.flush()

        assert svc.resolve_access_token(db_session, issued["access_token"]) is None


class TestRefresh:
    def test_refresh_rotates_both_tokens(self, db_session, svc, consented):
        _grant, code, verifier = consented
        first = svc.exchange_code(
            db_session, code=code, code_verifier=verifier, redirect_uri=_REDIRECT
        )

        second = svc.refresh(db_session, refresh_token=first["refresh_token"])

        assert second["access_token"] != first["access_token"]
        assert second["refresh_token"] != first["refresh_token"]
        # The old access token dies with its row — no lingering parallel session.
        assert svc.resolve_access_token(db_session, first["access_token"]) is None
        assert svc.resolve_access_token(db_session, second["access_token"]) is not None

    def test_reusing_a_rotated_refresh_token_is_rejected(self, db_session, svc, consented):
        _grant, code, verifier = consented
        first = svc.exchange_code(
            db_session, code=code, code_verifier=verifier, redirect_uri=_REDIRECT
        )
        svc.refresh(db_session, refresh_token=first["refresh_token"])

        with pytest.raises(McpOAuthError, match="invalid_grant"):
            svc.refresh(db_session, refresh_token=first["refresh_token"])


class TestDynamicClientRegistration:
    def test_registers_a_public_client(self, db_session, svc):
        registered = svc.register_client(
            db_session, client_name="Cursor", redirect_uris=["https://cursor.sh/cb"]
        )

        assert registered["client_id"].startswith("mcp_")
        # Public client: no secret is issued, so none can leak.
        assert "client_secret" not in registered
        assert registered["token_endpoint_auth_method"] == "none"

    def test_rejects_non_https_redirect(self, db_session, svc):
        with pytest.raises(McpOAuthError, match="invalid_redirect_uri"):
            svc.register_client(
                db_session, client_name="Sketchy", redirect_uris=["http://evil.test/cb"]
            )

    def test_allows_http_loopback_redirect(self, db_session, svc):
        """Native clients legitimately use 127.0.0.1 loopback (RFC 8252)."""
        registered = svc.register_client(
            db_session, client_name="Local", redirect_uris=["http://127.0.0.1:33418/cb"]
        )
        assert registered["client_id"]

    def test_requires_at_least_one_redirect(self, db_session, svc):
        with pytest.raises(McpOAuthError, match="invalid_redirect_uri"):
            svc.register_client(db_session, client_name="Nowhere", redirect_uris=[])


class TestResourceBinding:
    def test_grant_for_another_resource_is_refused(self, db_session, svc, client_row, org, user):
        """Confused-deputy (SPEC §10): we only mint tokens for our own /mcp."""
        _verifier, challenge = _pkce()
        with pytest.raises(McpOAuthError, match="invalid_target"):
            svc.grant_consent(
                db_session,
                client_id=client_row.client_id,
                org_id=org.id,
                user_id=user.id,
                redirect_uri=_REDIRECT,
                code_challenge=challenge,
                code_challenge_method="S256",
                scope=" ".join(read_only_scopes()),
                resource="https://someone-else.example/mcp",
            )
