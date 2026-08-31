"""Security tests — OAuth CSRF state validation and open redirect prevention."""

from unittest.mock import patch

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.services.oauth_routes import _sign_state, oauth_routes
from datanika.services.oauth_service import google_provider


@pytest.fixture
def client():
    app = Starlette(routes=oauth_routes)
    return TestClient(app, follow_redirects=False)


def _make_state_cookie(state: str) -> str:
    return f"{state}:{_sign_state(state)}"


class TestOAuthCSRF:
    """CSRF attacks via state parameter manipulation."""

    def test_forged_state_rejected(self, client):
        """Attacker replaces state in callback URL with their own value."""
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {"google": google_provider("gid", "gsecret")}
            client.cookies.set("oauth_state", _make_state_cookie("legitimate_state"))
            resp = client.get("/api/auth/callback/google?code=abc&state=attacker_state")
            assert resp.status_code == 302
            assert "auth_error=invalid_state" in resp.headers["location"]

    def test_empty_state_parameter_rejected(self, client):
        """Empty state in callback URL."""
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {"google": google_provider("gid", "gsecret")}
            client.cookies.set("oauth_state", _make_state_cookie("real"))
            resp = client.get("/api/auth/callback/google?code=abc&state=")
            assert resp.status_code == 302
            assert "auth_error=invalid_state" in resp.headers["location"]

    def test_missing_state_parameter_rejected(self, client):
        """No state parameter at all in callback URL."""
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {"google": google_provider("gid", "gsecret")}
            client.cookies.set("oauth_state", _make_state_cookie("real"))
            resp = client.get("/api/auth/callback/google?code=abc")
            assert resp.status_code == 302
            assert "auth_error=invalid_state" in resp.headers["location"]

    def test_replayed_state_with_different_cookie(self, client):
        """Attacker uses a valid state from a different session."""
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {"google": google_provider("gid", "gsecret")}
            # Cookie has state A, but URL has state B (attacker's captured state)
            client.cookies.set("oauth_state", _make_state_cookie("session_A_state"))
            resp = client.get("/api/auth/callback/google?code=abc&state=session_B_state")
            assert resp.status_code == 302
            assert "auth_error=invalid_state" in resp.headers["location"]

    def test_tampered_signature_in_cookie(self, client):
        """Attacker modifies the HMAC signature in the cookie."""
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {"google": google_provider("gid", "gsecret")}
            state = "my_state"
            bad_cookie = f"{state}:{'a' * 64}"  # forged signature
            client.cookies.set("oauth_state", bad_cookie)
            resp = client.get(f"/api/auth/callback/google?code=abc&state={state}")
            assert resp.status_code == 302
            assert "auth_error=invalid_state" in resp.headers["location"]

    def test_no_cookie_at_all(self, client):
        """No oauth_state cookie present."""
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {"google": google_provider("gid", "gsecret")}
            resp = client.get("/api/auth/callback/google?code=abc&state=any")
            assert resp.status_code == 302
            assert "auth_error=invalid_state" in resp.headers["location"]

    def test_cookie_without_colon_separator(self, client):
        """Malformed cookie value without colon separator."""
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {"google": google_provider("gid", "gsecret")}
            client.cookies.set("oauth_state", "no_colon_here")
            resp = client.get("/api/auth/callback/google?code=abc&state=no_colon_here")
            assert resp.status_code == 302
            assert "auth_error=invalid_state" in resp.headers["location"]


class TestOAuthProviderInjection:
    """Prevent provider name injection."""

    def test_path_traversal_in_provider(self, client):
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {}
            resp = client.get("/api/auth/login/../admin")
            # Should either 404 or redirect to login with error
            assert resp.status_code in (302, 404)
            if resp.status_code == 302:
                assert "/login" in resp.headers["location"]

    def test_empty_provider_name(self, client):
        """Empty provider should not match any route or crash."""
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {}
            resp = client.get("/api/auth/login/")
            # Starlette may return 307 for trailing slash or 404
            assert resp.status_code in (302, 307, 404)
