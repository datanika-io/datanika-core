"""Tests for OAuth routes — login redirect and callback handling."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.services.oauth_routes import oauth_routes
from datanika.services.oauth_service import github_provider, google_provider


@pytest.fixture
def client():
    app = Starlette(routes=oauth_routes)
    return TestClient(app, follow_redirects=False)


# ---------------------------------------------------------------------------
# Login redirect
# ---------------------------------------------------------------------------
class TestOAuthLogin:
    def test_google_redirects_to_provider(self, client):
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {"google": google_provider("gid", "gsecret")}
            resp = client.get("/api/auth/login/google")
            assert resp.status_code == 302
            location = resp.headers["location"]
            assert "accounts.google.com" in location
            assert "client_id=gid" in location
            assert "response_type=code" in location

    def test_github_redirects_to_provider(self, client):
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {"github": github_provider("ghid", "ghsecret")}
            resp = client.get("/api/auth/login/github")
            assert resp.status_code == 302
            assert "github.com" in resp.headers["location"]

    def test_unknown_provider_redirects_to_login(self, client):
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {}
            resp = client.get("/api/auth/login/unknown")
            assert resp.status_code == 302
            assert "/login" in resp.headers["location"]


# ---------------------------------------------------------------------------
# Callback
# ---------------------------------------------------------------------------
class TestOAuthCallback:
    def test_exchanges_code_and_redirects(self, client):
        with (
            patch("datanika.services.oauth_routes._get_providers") as prov_mock,
            patch("datanika.services.oauth_routes._get_service") as svc_mock,
            patch("datanika.services.oauth_routes._get_session") as sess_mock,
        ):
            prov_mock.return_value = {"google": google_provider("gid", "gsecret")}

            mock_svc = MagicMock()
            mock_svc.handle_callback = AsyncMock(return_value={
                "access_token": "jwt_tok",
                "refresh_token": "jwt_ref",
                "user": MagicMock(id=1),
                "is_new": False,
            })
            svc_mock.return_value = mock_svc

            mock_session = MagicMock()
            sess_mock.return_value.__enter__ = MagicMock(return_value=mock_session)
            sess_mock.return_value.__exit__ = MagicMock(return_value=False)

            resp = client.get("/api/auth/callback/google?code=authcode123&state=xyz")
            assert resp.status_code == 302
            location = resp.headers["location"]
            assert "/auth/complete" in location
            assert "token=jwt_tok" in location
            assert "refresh=jwt_ref" in location

    def test_new_user_redirects_to_complete(self, client):
        with (
            patch("datanika.services.oauth_routes._get_providers") as prov_mock,
            patch("datanika.services.oauth_routes._get_service") as svc_mock,
            patch("datanika.services.oauth_routes._get_session") as sess_mock,
        ):
            prov_mock.return_value = {"google": google_provider("gid", "gsecret")}

            mock_svc = MagicMock()
            mock_svc.handle_callback = AsyncMock(return_value={
                "access_token": "jwt_tok",
                "refresh_token": "jwt_ref",
                "user": MagicMock(id=2),
                "is_new": True,
            })
            svc_mock.return_value = mock_svc

            mock_session = MagicMock()
            sess_mock.return_value.__enter__ = MagicMock(return_value=mock_session)
            sess_mock.return_value.__exit__ = MagicMock(return_value=False)

            resp = client.get("/api/auth/callback/google?code=newcode")
            assert resp.status_code == 302
            assert "/auth/complete" in resp.headers["location"]
            assert "is_new=1" in resp.headers["location"]

    def test_missing_code_redirects_to_login(self, client):
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {"google": google_provider("gid", "gsecret")}
            resp = client.get("/api/auth/callback/google")
            assert resp.status_code == 302
            assert "/login" in resp.headers["location"]

    def test_unknown_provider_redirects_to_login(self, client):
        with patch("datanika.services.oauth_routes._get_providers") as mock:
            mock.return_value = {}
            resp = client.get("/api/auth/callback/unknown?code=abc")
            assert resp.status_code == 302
            assert "/login" in resp.headers["location"]

    def test_service_error_redirects_to_login(self, client):
        with (
            patch("datanika.services.oauth_routes._get_providers") as prov_mock,
            patch("datanika.services.oauth_routes._get_service") as svc_mock,
            patch("datanika.services.oauth_routes._get_session") as sess_mock,
        ):
            prov_mock.return_value = {"google": google_provider("gid", "gsecret")}

            mock_svc = MagicMock()
            mock_svc.handle_callback = AsyncMock(
                side_effect=Exception("Provider error")
            )
            svc_mock.return_value = mock_svc

            mock_session = MagicMock()
            sess_mock.return_value.__enter__ = MagicMock(return_value=mock_session)
            sess_mock.return_value.__exit__ = MagicMock(return_value=False)

            resp = client.get("/api/auth/callback/google?code=badcode")
            assert resp.status_code == 302
            assert "/login" in resp.headers["location"]
