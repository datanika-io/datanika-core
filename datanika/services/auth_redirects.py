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

import re

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


# ---------------------------------------------------------------------------
# Where a completed sign-in may send the browser next.
#
# Moved here from ``ui/state/auth_state.py`` (core#624). The OAuth callback is a backend route and
# now has to apply these same checks before a value is stored or put back into a URL, and a
# service must not import from the UI layer. ``AuthState`` imports them from here, so the email
# path and the social path share one definition rather than two that can drift.
# ---------------------------------------------------------------------------

#: Option C auth bridge: the shape a ``?template=<slug>`` must have to survive the signup wall.
#: Cold-traffic visitors who click "Try this template" on a public ``/templates/<slug>`` landing
#: page keep the slug, so the post-auth redirect can land on ``/connections?template=<slug>``.
#: Compared against this pattern (not the in-app template registry) to keep the auth layer
#: decoupled from ``ConnectionState``; unknown-but-well-formed slugs are ignored downstream.
#: Rejecting malformed or over-long slugs avoids an open-redirect vector.
TEMPLATE_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")

#: Longest ``?next=`` we will honour. An OAuth consent URL carries a PKCE challenge and an opaque
#: client state, so it is comfortably the biggest legitimate one.
MAX_NEXT_LEN = 2048

#: The query parameters a social sign-in carries across the provider round trip (core#624).
#:
#: ⚠️ ``email`` is deliberately absent, although the email path reads ``?email=``. There it
#: pre-fills an input; on the social path the provider supplies the address, so nothing would
#: consume it, and carrying it would only put an address into a cookie and a redirect URL.
OAUTH_CONTEXT_KEYS: tuple[str, ...] = ("next", "template", "invite_token")


def safe_next_path(raw: str) -> str:
    """Return ``raw`` if it is a same-site absolute path, else ``""``.

    The ``?next=`` bridge exists so an interrupted flow can resume after the
    login wall — ``/oauth/consent`` (#394) is the first caller, where dropping
    the user on the dashboard would strand an MCP client mid-handshake.

    It is also the classic open-redirect vector, so the allowed shape is
    deliberately narrow: an absolute path on this site and nothing else.
    ``//evil.com`` is protocol-relative, and a browser normalises the
    backslashes in ``/\\evil.com`` to the same thing — both would send a
    freshly-authenticated user straight off-site, which is precisely when they
    are most likely to type a password into whatever they land on.
    """
    if not raw or len(raw) > MAX_NEXT_LEN:
        return ""
    if not raw.startswith("/"):
        return ""
    if raw.startswith("//") or "\\" in raw:
        return ""
    if any(ch.isspace() or ord(ch) < 0x20 for ch in raw):
        return ""
    return raw
