"""OAuth / Social Login service — Google + GitHub."""

import logging
from dataclasses import dataclass
from urllib.parse import urlencode

import httpx
from sqlalchemy.orm import Session

from datanika.errors import UserFacingError
from datanika.services.auth import AuthService
from datanika.services.invitation_service import InvitationService
from datanika.services.user_service import UserService

logger = logging.getLogger(__name__)


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
        *,
        invite_token: str = "",
    ) -> dict:
        """Exchange auth code for tokens, find/create user, return JWT.

        Returns: ``{"access_token": str, "refresh_token": str, "user": User, "is_new": bool,
        "invite": str}``.

        ``invite`` reports what became of ``invite_token`` (core#624): ``""`` when none was
        supplied, ``"joined"`` when it was applied and the session lands in that org, and
        ``"not_applied"`` when one was supplied and could not be.
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

        if not invite_token:
            # Find or create user
            user, is_new = self._user.find_or_create_oauth_user(
                session, email, full_name, provider.name, provider_id, email_verified=True
            )
            invite, joined_org_id = "", None
        else:
            # 🚨 SPEC_SIGNUP_SOCIAL_AUTH §8e: the invitation is tried FIRST and the personal org
            # is the fallback. ``find_or_create_oauth_user`` creates that org as part of creating
            # the user, so it is asked not to. The email path had exactly this defect until
            # core#981 — an unconditional create_org, then the invitation *appended* — and every
            # invited signup finished in two orgs.
            user, is_new = self._user.find_or_create_oauth_user(
                session,
                email,
                full_name,
                provider.name,
                provider_id,
                email_verified=True,
                create_personal_org=False,
            )
            joined_org_id = self._apply_invitation(session, invite_token, user.id)
            invite = "not_applied" if joined_org_id is None else "joined"
            if is_new and joined_org_id is None:
                # §8e rule 4: never zero orgs. Owned here because this caller opted out above.
                self._user.create_personal_org(session, user, full_name)

        orgs = self._user.get_user_orgs(session, user.id)
        if not orgs:
            raise OAuthError("User has no organization")
        # Land in the org the invitation just joined; otherwise the first org, as always.
        org_id = joined_org_id if joined_org_id is not None else orgs[0].id

        return {
            "access_token": self._auth.create_access_token(user.id, org_id),
            "refresh_token": self._auth.create_refresh_token(user.id),
            "user": user,
            "is_new": is_new,
            "invite": invite,
        }

    def _apply_invitation(self, session: Session, invite_token: str, user_id: int) -> int | None:
        """Accept ``invite_token`` for ``user_id``; the org it joined, or ``None``.

        ``user_id`` is passed so the invitation can only ever join the account signing in, never
        whichever account holds the address it was issued to (see ``accept_invitation``).

        Logged in both failure shapes, because ``accept_invitation`` *returns* ``None`` for the
        common ones — expired, already used, cancelled, issued to another account — and a log line
        that fires only on an exception misses every one of them (core#981's second silence). The
        user is told separately, through the ``invite`` flag the callback forwards.
        """
        try:
            membership = InvitationService(self._auth).accept_invitation(
                session, invite_token, user_id=user_id
            )
        except Exception:
            logger.exception(
                "Invitation acceptance raised during social sign-in and was dropped: user_id=%s",
                user_id,
            )
            return None
        if membership is None:
            logger.warning(
                "Invitation token was not applicable during social sign-in and was dropped: "
                "user_id=%s (expired, already used, cancelled, or issued to another account)",
                user_id,
            )
            return None
        return membership.org_id

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
