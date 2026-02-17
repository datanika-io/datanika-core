"""OAuth routes — Starlette routes for social login (Google + GitHub)."""

import hashlib
import hmac
import secrets
from urllib.parse import urlencode

from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.routing import Route

from datanika.config import settings
from datanika.services.auth import AuthService
from datanika.services.oauth_service import (
    OAuthProvider,
    OAuthService,
    github_provider,
    google_provider,
)
from datanika.services.user_service import UserService

_OAUTH_STATE_COOKIE = "oauth_state"


def _get_providers() -> dict[str, OAuthProvider]:
    providers: dict[str, OAuthProvider] = {}
    if settings.google_client_id:
        providers["google"] = google_provider(
            settings.google_client_id, settings.google_client_secret
        )
    if settings.github_client_id:
        providers["github"] = github_provider(
            settings.github_client_id, settings.github_client_secret
        )
    return providers


def _get_service() -> OAuthService:
    auth = AuthService(settings.secret_key)
    user_svc = UserService(auth)
    return OAuthService(auth, user_svc)


def _get_session():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    engine = create_engine(settings.database_url_sync)
    return SASession(engine)


def _frontend(path: str) -> str:
    """Build a full frontend URL for redirects from the backend."""
    return f"{settings.frontend_url}{path}"


def _sign_state(state: str) -> str:
    """Create an HMAC signature for the OAuth state parameter."""
    return hmac.new(
        settings.secret_key.encode(), state.encode(), hashlib.sha256
    ).hexdigest()


def _verify_state(state: str, signature: str) -> bool:
    """Verify an OAuth state parameter signature."""
    expected = _sign_state(state)
    return hmac.compare_digest(expected, signature)


async def oauth_login(request: Request) -> RedirectResponse:
    provider = request.path_params["provider"]
    providers = _get_providers()
    if provider not in providers:
        return RedirectResponse(url=_frontend("/login?error=Unknown+provider"), status_code=302)

    svc = _get_service()
    state = secrets.token_urlsafe(32)
    redirect_uri = f"{settings.oauth_redirect_base_url}/api/auth/callback/{provider}"
    url = svc.get_authorize_url(providers[provider], redirect_uri, state)

    response = RedirectResponse(url=url, status_code=302)
    signed = f"{state}:{_sign_state(state)}"
    response.set_cookie(
        _OAUTH_STATE_COOKIE, signed, max_age=600, httponly=True, samesite="lax",
    )
    return response


async def oauth_callback(request: Request) -> RedirectResponse:
    provider = request.path_params["provider"]
    providers = _get_providers()
    if provider not in providers:
        return RedirectResponse(url=_frontend("/login?error=Unknown+provider"), status_code=302)

    code = request.query_params.get("code")
    if not code:
        return RedirectResponse(
            url=_frontend("/login?error=Missing+authorization+code"), status_code=302
        )

    # Validate CSRF state
    returned_state = request.query_params.get("state", "")
    cookie_value = request.cookies.get(_OAUTH_STATE_COOKIE, "")
    if ":" not in cookie_value:
        return RedirectResponse(
            url=_frontend("/login?error=Invalid+OAuth+state"), status_code=302
        )
    stored_state, signature = cookie_value.rsplit(":", 1)
    if (
        not returned_state
        or returned_state != stored_state
        or not _verify_state(stored_state, signature)
    ):
        return RedirectResponse(
            url=_frontend("/login?error=Invalid+OAuth+state"), status_code=302
        )

    svc = _get_service()
    redirect_uri = f"{settings.oauth_redirect_base_url}/api/auth/callback/{provider}"

    with _get_session() as session:
        try:
            result = await svc.handle_callback(
                providers[provider], code, redirect_uri, session
            )
            session.commit()
        except Exception:
            return RedirectResponse(
                url=_frontend("/login?error=OAuth+authentication+failed"), status_code=302
            )

    params = urlencode({
        "token": result["access_token"],
        "refresh": result["refresh_token"],
        "is_new": "1" if result["is_new"] else "0",
    })
    response = RedirectResponse(url=_frontend(f"/auth/complete?{params}"), status_code=302)
    response.delete_cookie(_OAUTH_STATE_COOKIE)
    return response


oauth_routes = [
    Route("/api/auth/login/{provider}", oauth_login),
    Route("/api/auth/callback/{provider}", oauth_callback),
]
