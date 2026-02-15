"""OAuth routes — Starlette routes for social login (Google + GitHub)."""

import secrets
from urllib.parse import urlencode

from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.routing import Route

from etlfabric.config import settings
from etlfabric.services.auth import AuthService
from etlfabric.services.oauth_service import (
    OAuthProvider,
    OAuthService,
    github_provider,
    google_provider,
)
from etlfabric.services.tenant import TenantService
from etlfabric.services.user_service import UserService


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


async def oauth_login(request: Request) -> RedirectResponse:
    provider = request.path_params["provider"]
    providers = _get_providers()
    if provider not in providers:
        return RedirectResponse(url="/login?error=Unknown+provider", status_code=302)

    svc = _get_service()
    state = secrets.token_urlsafe(32)
    redirect_uri = f"{settings.oauth_redirect_base_url}/api/auth/callback/{provider}"
    url = svc.get_authorize_url(providers[provider], redirect_uri, state)
    return RedirectResponse(url=url, status_code=302)


async def oauth_callback(request: Request) -> RedirectResponse:
    provider = request.path_params["provider"]
    providers = _get_providers()
    if provider not in providers:
        return RedirectResponse(url="/login?error=Unknown+provider", status_code=302)

    code = request.query_params.get("code")
    if not code:
        return RedirectResponse(
            url="/login?error=Missing+authorization+code", status_code=302
        )

    svc = _get_service()
    redirect_uri = f"{settings.oauth_redirect_base_url}/api/auth/callback/{provider}"

    with _get_session() as session:
        try:
            result = await svc.handle_callback(
                providers[provider], code, redirect_uri, session
            )
            session.commit()

            # Provision tenant schema for new OAuth users
            if result["is_new"]:
                auth = AuthService(settings.secret_key)
                payload = auth.decode_token(result["access_token"])
                TenantService().provision_tenant_sync(session, payload["org_id"])
        except Exception:
            return RedirectResponse(
                url="/login?error=OAuth+authentication+failed", status_code=302
            )

    params = urlencode({
        "token": result["access_token"],
        "refresh": result["refresh_token"],
        "is_new": "1" if result["is_new"] else "0",
    })
    return RedirectResponse(url=f"/auth/complete?{params}", status_code=302)


oauth_routes = [
    Route("/api/auth/login/{provider}", oauth_login),
    Route("/api/auth/callback/{provider}", oauth_callback),
]
