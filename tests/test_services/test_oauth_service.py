"""Tests for OAuthService — OAuth2 social login (Google + GitHub)."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from datanika.services.oauth_service import (
    OAuthError,
    OAuthService,
    github_provider,
    google_provider,
)


@pytest.fixture
def mock_auth():
    auth = MagicMock()
    auth.create_access_token.return_value = "jwt_access"
    auth.create_refresh_token.return_value = "jwt_refresh"
    auth.hash_password.return_value = "hashed"
    return auth


@pytest.fixture
def mock_user_svc():
    return MagicMock()


@pytest.fixture
def svc(mock_auth, mock_user_svc):
    return OAuthService(mock_auth, mock_user_svc)


@pytest.fixture
def google():
    return google_provider("google-id", "google-secret")


@pytest.fixture
def github():
    return github_provider("github-id", "github-secret")


# ---------------------------------------------------------------------------
# Provider config
# ---------------------------------------------------------------------------
class TestProviderConfig:
    def test_google_provider_config(self, google):
        assert google.name == "google"
        assert google.client_id == "google-id"
        assert "accounts.google.com" in google.authorize_url
        assert "googleapis.com" in google.token_url
        assert "openid" in google.scopes

    def test_github_provider_config(self, github):
        assert github.name == "github"
        assert github.client_id == "github-id"
        assert "github.com" in github.authorize_url
        assert "read:user" in github.scopes


# ---------------------------------------------------------------------------
# get_authorize_url
# ---------------------------------------------------------------------------
class TestGetAuthorizeUrl:
    def test_google_url(self, svc, google):
        url = svc.get_authorize_url(google, "http://localhost/callback", "state123")
        assert "accounts.google.com" in url
        assert "client_id=google-id" in url
        assert "state=state123" in url
        assert "response_type=code" in url

    def test_github_url(self, svc, github):
        url = svc.get_authorize_url(github, "http://localhost/callback", "xyz")
        assert "github.com" in url
        assert "client_id=github-id" in url


# ---------------------------------------------------------------------------
# handle_callback
# ---------------------------------------------------------------------------
class TestHandleCallback:
    @pytest.mark.asyncio
    async def test_existing_user(self, svc, google, mock_user_svc):
        user = MagicMock()
        user.id = 1
        org = MagicMock()
        org.id = 10
        mock_user_svc.find_or_create_oauth_user.return_value = (user, False)
        mock_user_svc.get_user_orgs.return_value = [org]

        svc._exchange_code = AsyncMock(return_value={"access_token": "provider_token"})
        svc._fetch_userinfo = AsyncMock(
            return_value={"email": "user@test.com", "name": "Test User", "sub": "123"}
        )

        session = MagicMock()
        result = await svc.handle_callback(google, "auth_code", "http://cb", session)

        assert result["access_token"] == "jwt_access"
        assert result["refresh_token"] == "jwt_refresh"
        assert result["is_new"] is False
        mock_user_svc.find_or_create_oauth_user.assert_called_once_with(
            session, "user@test.com", "Test User", "google", "123"
        )

    @pytest.mark.asyncio
    async def test_new_user(self, svc, google, mock_user_svc):
        user = MagicMock()
        user.id = 2
        org = MagicMock()
        org.id = 20
        mock_user_svc.find_or_create_oauth_user.return_value = (user, True)
        mock_user_svc.get_user_orgs.return_value = [org]

        svc._exchange_code = AsyncMock(return_value={"access_token": "tok"})
        svc._fetch_userinfo = AsyncMock(
            return_value={"email": "new@test.com", "name": "New", "sub": "456"}
        )

        session = MagicMock()
        result = await svc.handle_callback(google, "code", "http://cb", session)
        assert result["is_new"] is True

    @pytest.mark.asyncio
    async def test_invalid_code(self, svc, google):
        svc._exchange_code = AsyncMock(return_value={})

        session = MagicMock()
        with pytest.raises(OAuthError, match="access token"):
            await svc.handle_callback(google, "bad_code", "http://cb", session)

    @pytest.mark.asyncio
    async def test_missing_email(self, svc, google):
        svc._exchange_code = AsyncMock(return_value={"access_token": "tok"})
        svc._fetch_userinfo = AsyncMock(return_value={"name": "No Email"})

        session = MagicMock()
        with pytest.raises(OAuthError, match="email"):
            await svc.handle_callback(google, "code", "http://cb", session)

    @pytest.mark.asyncio
    async def test_github_email_fallback(self, svc, github, mock_user_svc):
        """GitHub may not return email in userinfo; falls back to /user/emails."""
        user = MagicMock()
        user.id = 3
        org = MagicMock()
        org.id = 30
        mock_user_svc.find_or_create_oauth_user.return_value = (user, True)
        mock_user_svc.get_user_orgs.return_value = [org]

        svc._exchange_code = AsyncMock(return_value={"access_token": "tok"})
        svc._fetch_userinfo = AsyncMock(
            return_value={"login": "ghuser", "id": 789}
        )
        svc._fetch_github_email = AsyncMock(return_value="ghuser@github.com")

        session = MagicMock()
        result = await svc.handle_callback(github, "code", "http://cb", session)
        assert result["is_new"] is True
        svc._fetch_github_email.assert_called_once_with("tok")


# ---------------------------------------------------------------------------
# OAuthError
# ---------------------------------------------------------------------------
class TestOAuthError:
    def test_is_value_error_subclass(self):
        assert issubclass(OAuthError, ValueError)
