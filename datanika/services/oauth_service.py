"""OAuth / Social Login service — Google + GitHub."""

from dataclasses import dataclass
from urllib.parse import urlencode

import httpx
from sqlalchemy.orm import Session

from datanika.services.auth import AuthService
from datanika.services.user_service import UserService


class OAuthError(ValueError):
    """Raised when OAuth operations fail."""


@dataclass
class OAuthProvider:
    """Configuration for an OAuth2 provider."""

    name: str
    client_id: str
    client_secret: str
    authorize_url: str
    token_url: str
    userinfo_url: str
    scopes: list[str]


def google_provider(client_id: str, client_secret: str) -> OAuthProvider:
    return OAuthProvider(
        name="google",
        client_id=client_id,
        client_secret=client_secret,
        authorize_url="https://accounts.google.com/o/oauth2/v2/auth",
        token_url="https://oauth2.googleapis.com/token",
        userinfo_url="https://openidconnect.googleapis.com/v1/userinfo",
        scopes=["openid", "email", "profile"],
    )


def github_provider(client_id: str, client_secret: str) -> OAuthProvider:
    return OAuthProvider(
        name="github",
        client_id=client_id,
        client_secret=client_secret,
        authorize_url="https://github.com/login/oauth/authorize",
        token_url="https://github.com/login/oauth/access_token",
        userinfo_url="https://api.github.com/user",
        scopes=["read:user", "user:email"],
    )


class OAuthService:
    """Handles OAuth2 authorization URL generation and callback processing."""

    def __init__(self, auth_service: AuthService, user_service: UserService):
        self._auth = auth_service
        self._user = user_service

    def get_authorize_url(self, provider: OAuthProvider, redirect_uri: str, state: str) -> str:
        """Build the OAuth2 authorization URL."""
        params = {
            "client_id": provider.client_id,
            "redirect_uri": redirect_uri,
            "scope": " ".join(provider.scopes),
            "state": state,
            "response_type": "code",
        }
        return f"{provider.authorize_url}?{urlencode(params)}"

    async def handle_callback(
        self,
        provider: OAuthProvider,
        code: str,
        redirect_uri: str,
        session: Session,
    ) -> dict:
        """Exchange auth code for tokens, find/create user, return JWT.

        Returns: {"access_token": str, "refresh_token": str, "user": User, "is_new": bool}
        """
        # Exchange code for access token
        token_data = await self._exchange_code(provider, code, redirect_uri)
        access_token = token_data.get("access_token")
        if not access_token:
            raise OAuthError("Failed to obtain access token from provider")

        # Fetch user info
        user_info = await self._fetch_userinfo(provider, access_token)
        email = user_info.get("email")
        if not email:
            # GitHub may not return email in userinfo, need separate call
            if provider.name == "github":
                email = await self._fetch_github_email(access_token)
            if not email:
                raise OAuthError("OAuth provider did not return an email address")

        full_name = user_info.get("name") or user_info.get("login") or ""
        provider_id = str(user_info.get("sub") or user_info.get("id") or "")

        # Find or create user
        user, is_new = self._user.find_or_create_oauth_user(
            session, email, full_name, provider.name, provider_id
        )

        # Get user's first org
        orgs = self._user.get_user_orgs(session, user.id)
        if not orgs:
            raise OAuthError("User has no organization")
        org_id = orgs[0].id

        return {
            "access_token": self._auth.create_access_token(user.id, org_id),
            "refresh_token": self._auth.create_refresh_token(user.id),
            "user": user,
            "is_new": is_new,
        }

    async def _exchange_code(self, provider: OAuthProvider, code: str, redirect_uri: str) -> dict:
        """Exchange authorization code for tokens."""
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                provider.token_url,
                data={
                    "client_id": provider.client_id,
                    "client_secret": provider.client_secret,
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "grant_type": "authorization_code",
                },
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            return resp.json()

    async def _fetch_userinfo(self, provider: OAuthProvider, access_token: str) -> dict:
        """Fetch user info from provider's userinfo endpoint."""
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                provider.userinfo_url,
                headers={"Authorization": f"Bearer {access_token}"},
            )
            resp.raise_for_status()
            return resp.json()

    async def _fetch_github_email(self, access_token: str) -> str | None:
        """Fetch primary email from GitHub /user/emails endpoint."""
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.github.com/user/emails",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            if resp.status_code != 200:
                return None
            emails = resp.json()
            for e in emails:
                if e.get("primary") and e.get("verified"):
                    return e["email"]
            return emails[0]["email"] if emails else None
