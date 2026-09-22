"""OAuth routes — Starlette routes for social login (Google + GitHub)."""

import base64
import hashlib
import hmac
import json
import re
import secrets
from urllib.parse import urlencode

from starlette.requests import Request
from starlette.responses import RedirectResponse
from starlette.routing import Route

from datanika.config import settings
from datanika.services.auth import AuthService
from datanika.services.auth_redirects import TEMPLATE_SLUG_RE, login_error_path, safe_next_path
from datanika.services.oauth_service import (
    OAuthProvider,
    OAuthService,
    github_provider,
    google_provider,
)
from datanika.services.signup_conversion import mark_signup_completed
from datanika.services.user_service import UserService, UserServiceError

_OAUTH_STATE_COOKIE = "oauth_state"

#: The sign-in context a flow carries across the provider round trip (core#624).
#:
#: ⚠️ A SECOND cookie rather than a new ``oauth_state`` format, and the reason is the deploy:
#: under blue/green a flow can start on one colour and complete on the other. The previous release
#: verifies ``oauth_state`` exactly as before and never reads this cookie, so it completes a flow
#: this release started (without context — which is all it ever did); and this release completes a
#: flow the previous one started, finding no context cookie. A changed ``oauth_state`` format would
#: instead reject every flow that crossed the swap.
_OAUTH_CONTEXT_COOKIE = "oauth_ctx"

_FLOW_COOKIE_MAX_AGE = 600

#: Longest ``invite_token`` a social sign-in will carry (core#624). An invitation link holds a
#: JWT from ``AuthService.create_email_verification_token`` — a few hundred characters — so 512
#: is comfortable, and it keeps the worst-case context inside one cookie. A browser silently
#: drops a cookie over 4096 bytes, and the flow would then complete without its context.
MAX_INVITE_TOKEN_LEN = 512

#: A JWT's alphabet: base64url segments joined by dots. Anything else is not a token we issued,
#: and refusing it keeps the value inert wherever it is stored or logged.
_INVITE_TOKEN_RE = re.compile(r"[A-Za-z0-9._-]+")


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
    from datanika.db import get_sync_session

    return get_sync_session()


def _frontend(path: str) -> str:
    """Build a full frontend URL for redirects from the backend."""
    return f"{settings.frontend_url}{path}"


def _sign_state(state: str) -> str:
    """Create an HMAC signature for the OAuth state parameter."""
    return hmac.new(settings.secret_key.encode(), state.encode(), hashlib.sha256).hexdigest()


def _verify_state(state: str, signature: str) -> bool:
    """Verify an OAuth state parameter signature.

    ``False`` for a signature that is not ASCII, instead of letting ``hmac.compare_digest``
    raise ``TypeError`` on it: the value comes from a cookie, so a crafted one must be a
    refusal, not a 500. The raise was reachable before on the equal-state path; since
    core#624 AC13 this also runs when the two states differ, to tell a superseded flow from a
    forged cookie.
    """
    expected = _sign_state(state)
    try:
        return hmac.compare_digest(expected, signature)
    except TypeError:
        return False


def _as_text(value) -> str:
    return value if isinstance(value, str) else ""


def _clean_context(raw) -> dict[str, str]:
    """The part of ``raw`` a flow may carry, each value validated on its own terms.

    Applied twice — to the ``/api/auth/login`` query string on the way in, and to the decoded
    cookie on the way out — because a valid signature proves WE stored a value, not that the value
    is safe: anyone can put anything in their own ``?next=`` and have it faithfully signed
    (``SPEC_SIGNUP_SOCIAL_AUTH`` §2 constraint 1). Keys outside the context set are dropped, never
    passed through. ``/auth/complete`` checks ``next`` and ``template`` a third time, on the way
    into the redirect, with the same helpers.
    """
    context: dict[str, str] = {}
    nxt = safe_next_path(_as_text(raw.get("next")))
    if nxt:
        context["next"] = nxt
    template = _as_text(raw.get("template"))
    if TEMPLATE_SLUG_RE.fullmatch(template):
        context["template"] = template
    token = _as_text(raw.get("invite_token"))
    if len(token) <= MAX_INVITE_TOKEN_LEN and _INVITE_TOKEN_RE.fullmatch(token):
        context["invite_token"] = token
    return context


def _sign_context(state: str, payload: str) -> str:
    """HMAC over the flow's ``state`` AND its context.

    Binding the state is what keeps a context inside its own flow (§8d): a context cookie left
    behind by an abandoned flow, or paired with another tab's state, does not verify against the
    state of the flow now completing. The label keeps this signature distinct from a state
    signature, which is an HMAC over the state alone.
    """
    message = f"oauth-context\x00{state}\x00{payload}"
    return hmac.new(settings.secret_key.encode(), message.encode(), hashlib.sha256).hexdigest()


def _encode_context(state: str, context: dict[str, str]) -> str:
    raw = json.dumps(context, separators=(",", ":"), sort_keys=True).encode()
    payload = base64.urlsafe_b64encode(raw).decode().rstrip("=")
    return f"{payload}:{_sign_context(state, payload)}"


def _decode_context(value: str, state: str) -> dict[str, str]:
    """The context stored for THIS flow, re-validated — or ``{}``. Never raises.

    Failing closed means the sign-in completes WITHOUT context, which is what every social sign-in
    did before core#624. It never means completing with a context that belongs to another flow.
    """
    if ":" not in value:
        return {}
    payload, signature = value.rsplit(":", 1)
    try:
        if not hmac.compare_digest(_sign_context(state, payload), signature):
            return {}
        decoded = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    except (TypeError, ValueError):  # non-ASCII signature, bad base64, bad JSON
        return {}
    return _clean_context(decoded) if isinstance(decoded, dict) else {}


async def oauth_login(request: Request) -> RedirectResponse:
    provider = request.path_params["provider"]
    providers = _get_providers()
    if provider not in providers:
        return RedirectResponse(
            url=_frontend(login_error_path("unknown_provider")), status_code=302
        )

    svc = _get_service()
    state = secrets.token_urlsafe(32)
    redirect_uri = f"{settings.oauth_redirect_base_url}/api/auth/callback/{provider}"
    url = svc.get_authorize_url(providers[provider], redirect_uri, state)

    response = RedirectResponse(url=url, status_code=302)
    signed = f"{state}:{_sign_state(state)}"
    response.set_cookie(
        _OAUTH_STATE_COOKIE,
        signed,
        max_age=_FLOW_COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
    )
    context = _clean_context(request.query_params)
    if context:
        response.set_cookie(
            _OAUTH_CONTEXT_COOKIE,
            _encode_context(state, context),
            max_age=_FLOW_COOKIE_MAX_AGE,
            httponly=True,
            samesite="lax",
        )
    else:
        # A context left by an abandoned flow could not verify against this state anyway; clearing
        # it stops it lingering for the rest of its lifetime.
        response.delete_cookie(_OAUTH_CONTEXT_COOKIE)
    return response


async def oauth_callback(request: Request) -> RedirectResponse:
    provider = request.path_params["provider"]
    providers = _get_providers()
    if provider not in providers:
        return RedirectResponse(
            url=_frontend(login_error_path("unknown_provider")), status_code=302
        )

    code = request.query_params.get("code")
    if not code:
        return RedirectResponse(url=_frontend(login_error_path("missing_code")), status_code=302)

    # Validate CSRF state
    returned_state = request.query_params.get("state", "")
    cookie_value = request.cookies.get(_OAUTH_STATE_COOKIE, "")
    if ":" not in cookie_value:
        return RedirectResponse(url=_frontend(login_error_path("invalid_state")), status_code=302)
    stored_state, signature = cookie_value.rsplit(":", 1)
    if not returned_state or not _verify_state(stored_state, signature):
        return RedirectResponse(url=_frontend(login_error_path("invalid_state")), status_code=302)
    if returned_state != stored_state:
        # 🚨 Two tabs share one cookie jar, so a second flow started before this one completed
        # lands here (§8d, AC12). The comparison stays exact: loosening it to make that case
        # succeed would complete this flow against the OTHER flow's context — its invitation.
        # core#624 AC13: the cookie VERIFIED, so this browser really started another flow, and the
        # refusal says so. "Please try again" from /login would sign the user in without this
        # flow's invitation or template. A missing or forged cookie keeps the generic reason above.
        return RedirectResponse(url=_frontend(login_error_path("superseded_flow")), status_code=302)

    # core#624. Read only once the state has verified, and only as bound to that state.
    context = _decode_context(request.cookies.get(_OAUTH_CONTEXT_COOKIE, ""), stored_state)

    svc = _get_service()
    redirect_uri = f"{settings.oauth_redirect_base_url}/api/auth/callback/{provider}"

    with _get_session() as session:
        try:
            result = await svc.handle_callback(
                providers[provider],
                code,
                redirect_uri,
                session,
                invite_token=context.get("invite_token", ""),
            )
            new_user_id = result["user"].id if result["is_new"] else None
            session.commit()
        except UserServiceError:
            # A refusal, not a failure. The user can act on this one — their
            # password still works and the reset flow proves the address — so it
            # must not be flattened into "authentication failed", which reads as
            # "try again" and never succeeds. The flag is bounded on purpose:
            # the login page renders a fixed translated callout, never text from
            # the query string.
            return RedirectResponse(url=_frontend("/login?link_blocked=1"), status_code=302)
        except Exception:
            return RedirectResponse(
                url=_frontend(login_error_path("oauth_failed")), status_code=302
            )

    # core#1369. Only once the account is committed, and only the backend's own fact: the
    # `is_new` written into the URL below is the user's to edit, so /auth/complete never reads it
    # for this. The marker is one-shot and fails quiet — see `signup_conversion`.
    if new_user_id is not None:
        mark_signup_completed(new_user_id)

    params = {
        "token": result["access_token"],
        "refresh": result["refresh_token"],
        "is_new": "1" if result["is_new"] else "0",
    }
    # A bounded flag; the page picks the sentence. The token itself never goes back into a URL: the
    # service has already applied it (or could not), and nothing downstream needs it.
    if result.get("invite") == "not_applied":
        params["invite"] = "not_applied"
    # Checked again by /auth/complete on the way into the redirect (§2 constraint 1): once a value
    # is back in a URL the signature that protected it between the two backend hops says nothing.
    for key in ("next", "template"):
        if key in context:
            params[key] = context[key]

    response = RedirectResponse(
        url=_frontend(f"/auth/complete?{urlencode(params)}"), status_code=302
    )
    # Both cookies go on the very redirect that sends the browser to /auth/complete (AC11): that
    # page is a Reflex frontend route, and §8c rules out it ever reading either.
    response.delete_cookie(_OAUTH_STATE_COOKIE)
    response.delete_cookie(_OAUTH_CONTEXT_COOKIE)
    return response


oauth_routes = [
    Route("/api/auth/login/{provider}", oauth_login),
    Route("/api/auth/callback/{provider}", oauth_callback),
]
