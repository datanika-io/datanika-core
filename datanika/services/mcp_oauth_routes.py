"""Starlette routes for the MCP OAuth 2.1 authorization server (core#393, P2).

The HTTP half of :mod:`datanika.services.mcp_oauth` — see that module for the
flow and the design rationale. Routes are appended to ``app._api.routes`` the
same way the social-login, SSO and webhook routes are (SPEC §4.3).

Discovery, registration and token endpoints answer **cross-origin**: browser
based MCP clients (Claude.ai) fetch them from their own origin, so without CORS
the one-click flow dies at the first preflight. They are unauthenticated by
design — metadata is public, and registration is open, which is what "dynamic"
means in RFC 7591. The credential-bearing endpoint (``/api/oauth/consent``) is
same-origin and JWT-authenticated, and is deliberately *not* CORS-open.
"""

from __future__ import annotations

import json

from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response
from starlette.routing import Route

from datanika.config import settings
from datanika.services.auth import AuthService
from datanika.services.mcp_oauth import McpOAuthError, McpOAuthService

_CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, MCP-Protocol-Version",
    "Access-Control-Max-Age": "3600",
}


def _service() -> McpOAuthService:
    return McpOAuthService()


def _get_session():
    from datanika.db import get_sync_session

    return get_sync_session()


def _public(payload: dict) -> JSONResponse:
    return JSONResponse(payload, headers=_CORS_HEADERS)


def _oauth_error(exc: McpOAuthError, status: int = 400) -> JSONResponse:
    return JSONResponse(
        {"error": exc.code, "error_description": exc.description},
        status_code=status,
        headers=_CORS_HEADERS,
    )


async def _body_params(request: Request) -> dict:
    """OAuth posts form-encoded; some clients send JSON. Accept both."""
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/json"):
        try:
            return json.loads(await request.body() or b"{}")
        except json.JSONDecodeError:
            return {}
    form = await request.form()
    return dict(form)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


async def protected_resource_metadata(request: Request) -> Response:
    if request.method == "OPTIONS":
        return Response(status_code=204, headers=_CORS_HEADERS)
    return _public(_service().protected_resource_metadata())


async def authorization_server_metadata(request: Request) -> Response:
    if request.method == "OPTIONS":
        return Response(status_code=204, headers=_CORS_HEADERS)
    return _public(_service().authorization_server_metadata())


# ---------------------------------------------------------------------------
# RFC 7591 — dynamic client registration
# ---------------------------------------------------------------------------


async def register_client(request: Request) -> Response:
    if request.method == "OPTIONS":
        return Response(status_code=204, headers=_CORS_HEADERS)

    payload = await _body_params(request)
    redirect_uris = payload.get("redirect_uris") or []
    if isinstance(redirect_uris, str):
        redirect_uris = [redirect_uris]

    svc = _service()
    with _get_session() as session:
        try:
            registered = svc.register_client(
                session,
                client_name=str(payload.get("client_name") or "MCP client"),
                redirect_uris=[str(u) for u in redirect_uris],
            )
        except McpOAuthError as exc:
            return _oauth_error(exc)
        session.commit()

    return JSONResponse(registered, status_code=201, headers=_CORS_HEADERS)


# ---------------------------------------------------------------------------
# Authorization — validate, then hand off to the consent screen
# ---------------------------------------------------------------------------


async def authorize(request: Request) -> Response:
    q = request.query_params
    svc = _service()

    with _get_session() as session:
        try:
            svc.validate_authorization_request(
                session,
                client_id=q.get("client_id", ""),
                redirect_uri=q.get("redirect_uri", ""),
                response_type=q.get("response_type", ""),
                code_challenge=q.get("code_challenge", ""),
                code_challenge_method=q.get("code_challenge_method", ""),
                resource=q.get("resource"),
            )
        except McpOAuthError as exc:
            # Never redirect on a validation failure: an unvalidated
            # redirect_uri is precisely what an attacker would supply, so
            # bouncing the error there would be an open redirect.
            return _oauth_error(exc)

    # Consent needs the user's app session, which lives in the frontend as a
    # JWT — so the browser goes there, and the frontend calls back into
    # /api/oauth/consent with an Authorization header (never a query string).
    consent_params = {
        k: v
        for k, v in (
            ("client_id", q.get("client_id")),
            ("redirect_uri", q.get("redirect_uri")),
            ("code_challenge", q.get("code_challenge")),
            ("code_challenge_method", q.get("code_challenge_method")),
            ("scope", q.get("scope")),
            ("state", q.get("state")),
            ("resource", q.get("resource")),
        )
        if v
    }
    return RedirectResponse(url=svc.consent_redirect_url(consent_params), status_code=302)


async def client_info(request: Request) -> Response:
    """What the consent screen must show about the app asking for access.

    The screen looks the name up here rather than trusting one passed through
    the URL: a crafted consent link could otherwise label a real ``client_id``
    with any name it liked, and the user would be consenting to something other
    than what they read. Registrations are not secret — the name and redirect
    URIs are exactly what a user needs to see to decide.
    """
    client_id = request.query_params.get("client_id", "")
    with _get_session() as session:
        client = _service().get_client(session, client_id)

    if client is None:
        return JSONResponse({"error": "invalid_client"}, status_code=404)
    return JSONResponse(
        {
            "client_id": client.client_id,
            "client_name": client.client_name,
            "redirect_uris": client.redirect_uris,
        }
    )


# ---------------------------------------------------------------------------
# Consent grant — called by the frontend consent screen with the user's JWT
# ---------------------------------------------------------------------------


async def consent(request: Request) -> Response:
    auth = request.headers.get("authorization", "")
    if auth[:7].lower() != "bearer ":
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    claims = AuthService(settings.secret_key).decode_token(auth[7:].strip(), expected_type="access")
    if not claims:
        return JSONResponse({"error": "unauthorized"}, status_code=401)

    payload = await _body_params(request)
    svc = _service()

    with _get_session() as session:
        try:
            _grant, code = svc.grant_consent(
                session,
                client_id=str(payload.get("client_id", "")),
                # The org comes from the signed session, never from the request
                # body — otherwise a consent screen could be talked into
                # granting access to an org the user isn't a member of.
                org_id=int(claims["org_id"]),
                user_id=int(claims["user_id"]),
                redirect_uri=str(payload.get("redirect_uri", "")),
                code_challenge=str(payload.get("code_challenge", "")),
                code_challenge_method=str(payload.get("code_challenge_method", "")),
                scope=str(payload.get("scope") or ""),
                resource=payload.get("resource"),
            )
        except McpOAuthError as exc:
            return _oauth_error(exc)
        session.commit()

    return JSONResponse(
        {
            "redirect_to": svc.client_redirect_url(
                str(payload.get("redirect_uri", "")), code, payload.get("state")
            )
        }
    )


# ---------------------------------------------------------------------------
# Token endpoint
# ---------------------------------------------------------------------------


async def token(request: Request) -> Response:
    if request.method == "OPTIONS":
        return Response(status_code=204, headers=_CORS_HEADERS)

    payload = await _body_params(request)
    grant_type = str(payload.get("grant_type", ""))
    svc = _service()

    with _get_session() as session:
        try:
            if grant_type == "authorization_code":
                issued = svc.exchange_code(
                    session,
                    code=str(payload.get("code", "")),
                    code_verifier=str(payload.get("code_verifier", "")),
                    redirect_uri=str(payload.get("redirect_uri", "")),
                )
            elif grant_type == "refresh_token":
                issued = svc.refresh(session, refresh_token=str(payload.get("refresh_token", "")))
            else:
                return _oauth_error(
                    McpOAuthError(
                        "unsupported_grant_type",
                        "Supported: authorization_code, refresh_token.",
                    )
                )
        except McpOAuthError as exc:
            return _oauth_error(exc)
        session.commit()

    return JSONResponse(
        issued,
        headers={**_CORS_HEADERS, "Cache-Control": "no-store", "Pragma": "no-cache"},
    )


mcp_oauth_routes = [
    Route(
        "/.well-known/oauth-protected-resource",
        protected_resource_metadata,
        methods=["GET", "OPTIONS"],
    ),
    Route(
        "/.well-known/oauth-authorization-server",
        authorization_server_metadata,
        methods=["GET", "OPTIONS"],
    ),
    Route("/oauth/register", register_client, methods=["POST", "OPTIONS"]),
    Route("/oauth/authorize", authorize, methods=["GET"]),
    Route("/api/oauth/client-info", client_info, methods=["GET"]),
    Route("/api/oauth/consent", consent, methods=["POST"]),
    Route("/oauth/token", token, methods=["POST", "OPTIONS"]),
]
