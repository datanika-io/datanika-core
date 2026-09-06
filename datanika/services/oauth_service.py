"""OAuth / Social Login service — Google + GitHub."""

from dataclasses import dataclass
from urllib.parse import urlencode

import httpx
from sqlalchemy.orm import Session

from datanika.errors import UserFacingError
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService


class OAuthError(UserFacingError):
    """Raised when OAuth operations fail."""


def _claim_is_true(value: object) -> bool:
    """Read a boolean OIDC claim, failing closed.

    OIDC specifies ``email_verified`` as a boolean, but implementations vary and
    some serialise it as a string. Anything else — including a missing claim —
    is *not* an assertion of verification, so it reads as false.
    """
    if value is True:
        return True
    return isinstance(value, str) and value.strip().lower() == "true"


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
        token_url="https://oauth2.googleapis.com/token",  # noqa: S106 - a URL
        userinfo_url="https://openidconnect.googleapis.com/v1/userinfo",
        scopes=["openid", "email", "profile"],
    )


def github_provider(client_id: str, client_secret: str) -> OAuthProvider:
    return OAuthProvider(
        name="github",
        client_id=client_id,
        client_secret=client_secret,
        authorize_url="https://github.com/login/oauth/authorize",
        token_url="https://github.com/login/oauth/access_token",  # noqa: S106 - a URL
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

        # SECURITY (auth boundary): an email is a *claim* until the provider
        # says it verified it. Only a verified address may be matched against
        # local accounts — see find_or_create_oauth_user, which refuses the rest.
        email = await self._resolve_verified_email(provider, access_token, user_info)
        if not email:
            raise OAuthError(
                "This provider did not give us a verified email address. "
                "Verify your email with the provider, then try again."
            )

        full_name = user_info.get("name") or user_info.get("login") or ""
        provider_id = str(user_info.get("sub") or user_info.get("id") or "")

        # Find or create user
        user, is_new = self._user.find_or_create_oauth_user(
            session, email, full_name, provider.name, provider_id, email_verified=True
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

    async def _resolve_verified_email(
        self, provider: OAuthProvider, access_token: str, user_info: dict
    ) -> str | None:
        """Return an address the provider states it has verified, or ``None``.

        This never returns an unverified address. That is deliberate: a caller
        cannot authenticate on one by mistake because it is never handed one.
        """
        if provider.name == "github":
            # ``/user`` also carries an ``email``, but it is the *public profile*
            # field and comes with no verification flag — short-circuiting on it
            # would skip the only endpoint that reports ``verified``.
            return await self._fetch_github_email(access_token)

        # OIDC-shaped providers (Google): the userinfo document carries the claim.
        if not _claim_is_true(user_info.get("email_verified")):
            return None
        return user_info.get("email") or None

    async def _fetch_github_email(self, access_token: str) -> str | None:
        """Return the account's primary *and* verified address, or ``None``.

        GitHub is the only party that knows whether an address on the account
        was confirmed; ``/user/emails`` is the one endpoint that says so. An
        account with no primary-and-verified address gets no answer rather than
        an arbitrary one — the ordering of this list guarantees nothing, and the
        one situation a fallback would serve is precisely the situation in which
        every candidate is untrustworthy.
        """
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                "https://api.github.com/user/emails",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            if resp.status_code != 200:
                return None
            for e in resp.json():
                if e.get("primary") and e.get("verified"):
                    return e["email"]
            return None
