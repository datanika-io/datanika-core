"""The closed set of sign-in failure reasons ``/login`` will render (#686).

Every OAuth and SSO failure path redirects the browser to ``/login``. A full-page 302 from
a Starlette route cannot write ``AuthState.auth_error`` — that is server-side state, and the
page the browser then loads holds a *different* state object. So the reason has to travel in
the query string.

It travels as a **slug from this table**, never as free text. The previous free-text
``error`` parameter meant anyone could send a login link carrying
*"Your account was flagged, call this number"* and have it render inside our own sign-in
card, under our logo, in our styling — a phishing surface that costs nothing to aim. A slug
outside this table renders nothing at all.

**Why the slugs are finer-grained than the messages.** Nine of the redirect sites in
``sso_routes.py`` log nothing at all before redirecting, so the slug in the URL is their only
diagnostic; collapsing them at the source would delete that. Several therefore share one
i18n key: an end user cannot act differently on "SAML IdP not configured" than on "Invalid
OIDC configuration" — both mean *your administrator has to finish the setup* — but an
operator reading a URL out of a support ticket can tell them apart.

Adding a redirect site means adding its slug here. ``tests/test_services/test_login_signals.py``
derives its expectations from the route sources, so a slug that is not in this table fails the
build rather than silently rendering nothing.
"""

from datanika.errors import InternalInvariantError

# slug -> i18n key. Many-to-one on purpose; see the module docstring.
AUTH_ERROR_KEYS: dict[str, str] = {
    # --- OAuth (services/oauth_routes.py) ---
    "unknown_provider": "auth.error.unknown_provider",
    "missing_code": "auth.error.retry",
    "invalid_state": "auth.error.retry",
    "oauth_failed": "auth.error.provider_failed",
    # --- SSO (services/sso_routes.py) ---
    "sso_invalid_state": "auth.error.retry",
    "sso_not_configured": "auth.error.sso_not_configured",
    "sso_unsupported_protocol": "auth.error.sso_not_configured",
    "sso_misconfigured": "auth.error.sso_not_configured",
    "saml_idp_not_configured": "auth.error.sso_not_configured",
    "saml_request_failed": "auth.error.provider_failed",
    "sso_unreachable": "auth.error.sso_unreachable",
    "sso_no_email": "auth.error.sso_no_email",
    "sso_failed": "auth.error.provider_failed",
}


def login_error_path(reason: str) -> str:
    """Return ``/login?auth_error=<reason>``.

    Raises ``InternalInvariantError`` for a slug outside :data:`AUTH_ERROR_KEYS` — a redirect that
    would render nothing is a bug at the call site, and failing here makes it a test failure
    instead of a blank sign-in page in production.
    """
    if reason not in AUTH_ERROR_KEYS:
        # An instruction to edit source. Unreachable from any request today — all
        # 18 call sites pass literals, 13 distinct, every one already in
        # AUTH_ERROR_KEYS, and `auth_state.py` filters `?auth_error=` before it can
        # arrive. It stays a hard failure because a future caller passing a
        # variable is exactly what the marker should not silently render (core#1113).
        raise InternalInvariantError(
            f"unknown auth error reason {reason!r}; add it to AUTH_ERROR_KEYS with an i18n key"
        )
    return f"/login?auth_error={reason}"
