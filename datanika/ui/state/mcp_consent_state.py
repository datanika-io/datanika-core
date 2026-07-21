"""OAuth consent screen state (core#394, Remote-MCP P2).

The human half of the authorization-code flow built in #393. ``/oauth/authorize``
validates the OAuth request on the backend and 302s the browser here with the
request carried in the query string; this state resolves who is asking, decides
whether the request is safe to show at all, and turns Allow/Deny into the two
outcomes the MCP client is waiting for.

Two things are load-bearing and neither is obvious from the URL:

**The app's name is resolved server-side.** ``client_name`` comes from
``GET /api/oauth/client-info``, never from a query parameter — a crafted consent
link could otherwise label a real ``client_id`` anything it liked and the user
would be consenting to something other than what they read.

**The return address is verified before it is ever used.** ``/oauth/authorize``
exact-matches ``redirect_uri`` against the registration, but this page is a plain
URL and can be opened directly with any ``redirect_uri`` an attacker likes. Deny
redirects there, so unverified it would be an open redirect wearing Datanika's
hostname. ``client-info`` returns the registered ``redirect_uris`` for exactly
this reason: nothing renders, and no redirect happens, until the one in the URL
is found in that list.

The consent call is a loopback to our own backend — the endpoint is JWT
authenticated and same-origin, so it is reached process-locally the same way
``services/mcp_routes.py`` reaches the REST API. It is deliberately **async**: a
sync client here would hold the event loop that has to serve the very request it
is making, which is exactly what made every hosted tool call time out in #388.
"""

from __future__ import annotations

import logging
import os
from urllib.parse import quote, urlencode

import httpx
import reflex as rx

from datanika.ui.state.auth_state import AuthState

logger = logging.getLogger(__name__)

#: Loopback base for our own backend. Mirrors ``mcp_routes._INTERNAL_API_URL``:
#: Apache proxies ``/api/`` to this address, so the call never leaves the box.
_INTERNAL_API_URL = os.environ.get("DATANIKA_INTERNAL_API_URL", "http://127.0.0.1:8000")

_CONSENT_PATH = "/oauth/consent"

#: Everything ``/oauth/authorize`` forwards, in the order it builds them.
_FORWARDED = (
    "client_id",
    "redirect_uri",
    "code_challenge",
    "code_challenge_method",
    "scope",
    "state",
    "resource",
)

#: Without all four there is no authorization request to consent to. The
#: backend re-validates every one of them; this is only about not rendering a
#: consent prompt for something that cannot possibly succeed.
_REQUIRED = ("client_id", "redirect_uri", "code_challenge", "code_challenge_method")

_HTTP_TIMEOUT = 10.0


def _http_client() -> httpx.AsyncClient:
    """The client used for both backend calls.

    A seam, not indirection for its own sake: tests point it at an ASGI
    transport over the *real* ``mcp_oauth_routes``, so they exercise
    Engineering's handlers and real grant rows rather than a mock of them.
    """
    return httpx.AsyncClient(base_url=_INTERNAL_API_URL, timeout=_HTTP_TIMEOUT)


def _first(value) -> str:
    """Query params arrive as ``str`` or, when repeated, ``list[str]``."""
    if isinstance(value, list):
        return str(value[0]) if value else ""
    return "" if value is None else str(value)


def consent_url(params: dict[str, str]) -> str:
    """This page's URL for a given authorization request.

    Used to build the ``?next=`` that survives the login wall, so a logged-out
    user resumes the flow instead of landing on the dashboard with the MCP
    client still waiting.
    """
    carried = [(k, params[k]) for k in _FORWARDED if params.get(k)]
    return f"{_CONSENT_PATH}?{urlencode(carried)}" if carried else _CONSENT_PATH


class McpConsentState(rx.State):
    """Backs ``/oauth/consent``."""

    #: Resolved from client-info. Empty until then — never echoed from the URL.
    client_name: str = ""
    #: Set only once the URL's redirect_uri is found in the registration.
    verified_redirect_uri: str = ""
    #: Snapshot of the authorization request, taken at load.
    request_params: dict[str, str] = {}

    is_loading: bool = True
    is_submitting: bool = False

    #: One of: "", "incomplete_request", "unknown_client",
    #: "unregistered_redirect", "rejected", "unavailable". The view translates
    #: it; storing a code rather than prose keeps the message reactive to a
    #: language switch and keeps assertions off English strings.
    error_code: str = ""
    #: The backend's ``error_description``, when it sent one. Developer-facing
    #: detail shown under the translated headline.
    error_detail: str = ""

    @rx.var
    def has_error(self) -> bool:
        return self.error_code != ""

    @rx.var
    def is_ready(self) -> bool:
        """Whether it is safe to render the consent prompt itself."""
        return not self.is_loading and not self.error_code and self.verified_redirect_uri != ""

    @rx.var
    def api_key_name(self) -> str:
        """The API key consent mints — what the user looks for to revoke it."""
        return f"MCP: {self.client_name}"

    @rx.var
    def granted_scopes(self) -> list[str]:
        """Exactly what Allow will hand over, resolved the way the grant is.

        Deliberately routed through ``mcp_oauth.narrow_scope`` rather than read
        off ``scope`` or re-derived here. The requested scope is not the granted
        scope — it is narrowed, unknown values are dropped, and an absent one
        collapses to read-only — so a screen that displayed the request would
        describe something other than what it mints. That gap is core#450.
        """
        from datanika.services.mcp_oauth import narrow_scope

        return narrow_scope(self.request_params.get("scope", ""))

    @rx.var
    def grants_write(self) -> bool:
        """Whether Allow hands over the ability to change things.

        Drives the access block. Read-only is the default in every ambiguous
        case, because the failure modes are not symmetric: under-promising
        access costs a confused user, over-promising safety costs them consent
        they did not knowingly give.
        """
        from datanika.services.mcp_oauth import grants_write

        return grants_write(self.request_params.get("scope", ""))

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    async def load_consent(self):
        """Resolve the request, or refuse to show a prompt for it."""
        self.is_loading = True
        self.is_submitting = False
        self.error_code = ""
        self.error_detail = ""
        self.client_name = ""
        self.verified_redirect_uri = ""

        params = {k: _first(self.router.page.params.get(k)) for k in _FORWARDED}
        params = {k: v for k, v in params.items() if v}
        self.request_params = params

        auth = await self.get_state(AuthState)
        if not auth.access_token:
            # Come back here afterwards — the MCP client is still waiting on
            # this exact request, and it is not re-sendable from our side.
            # ``is_loading`` stays set so the spinner holds until the browser
            # navigates, rather than flashing a consent card with no app in it.
            return rx.redirect(f"/login?next={quote(consent_url(params), safe='')}")

        if any(not params.get(k) for k in _REQUIRED):
            self.error_code = "incomplete_request"
            self.is_loading = False
            return

        try:
            async with _http_client() as client:
                response = await client.get(
                    "/api/oauth/client-info", params={"client_id": params["client_id"]}
                )
        except Exception:
            logger.exception("client-info lookup failed for the consent screen")
            self.error_code = "unavailable"
            self.is_loading = False
            return

        if response.status_code == 404:
            self.error_code = "unknown_client"
            self.is_loading = False
            return
        if response.status_code != 200:
            logger.error("client-info returned %s", response.status_code)
            self.error_code = "unavailable"
            self.is_loading = False
            return

        info = response.json()
        registered = info.get("redirect_uris") or []
        if params["redirect_uri"] not in registered:
            # Someone hand-built this link. Neither consent nor the deny
            # redirect may touch an address the client never registered.
            self.error_code = "unregistered_redirect"
            self.is_loading = False
            return

        self.client_name = info.get("client_name") or params["client_id"]
        self.verified_redirect_uri = params["redirect_uri"]
        self.is_loading = False

    # ------------------------------------------------------------------
    # Allow / Deny
    # ------------------------------------------------------------------

    async def allow(self):
        """Mint the grant and hand the browser back to the client.

        A generator, not a plain handler: Reflex sends one state delta when a
        plain handler returns, so ``is_submitting`` would only ever reach the
        browser bundled with the redirect and the "Approving…" affordance would
        never render. On a slow link that is the difference between a button
        that acknowledges the click and one that looks broken.
        """
        if self.is_submitting or not self.verified_redirect_uri or self.error_code:
            return

        auth = await self.get_state(AuthState)
        if not auth.access_token:
            yield rx.redirect(f"/login?next={quote(consent_url(self.request_params), safe='')}")
            return

        self.is_submitting = True
        yield  # flush the pending state before the round-trip

        payload = dict(self.request_params)
        # The verified value, not whatever the URL says now.
        payload["redirect_uri"] = self.verified_redirect_uri

        try:
            async with _http_client() as client:
                response = await client.post(
                    "/api/oauth/consent",
                    json=payload,
                    headers={"Authorization": f"Bearer {auth.access_token}"},
                )
        except Exception:
            logger.exception("consent grant failed")
            self.is_submitting = False
            self.error_code = "unavailable"
            return

        if response.status_code != 200:
            # A refusal is shown, never redirected — the client is entitled to
            # an error at its redirect_uri only for errors the *user* caused.
            self.is_submitting = False
            self.error_code = "rejected"
            self.error_detail = self._describe(response)
            return

        redirect_to = (response.json() or {}).get("redirect_to") or ""
        if not redirect_to:
            self.is_submitting = False
            self.error_code = "unavailable"
            return
        # ``is_submitting`` deliberately stays set: the button holds its
        # "Approving…" state until the browser leaves for the client.
        #
        # Not ``is_external=True`` (#417). That flag reads like "this URL is
        # off-site" but Reflex means it literally — ``window.open(path,
        # "_blank")`` — so the callback opened in a second tab and left this
        # one on a disabled button forever. The default path already handles
        # cross-origin: it compares hosts and calls ``window.location.assign``,
        # which is what an authorization-code redirect is supposed to do.
        yield rx.redirect(redirect_to)

    def deny(self):
        """Send the user back empty-handed, per RFC 6749 §4.1.2.1."""
        if not self.verified_redirect_uri:
            return
        query = {"error": "access_denied"}
        state = self.request_params.get("state", "")
        if state:
            query["state"] = state
        separator = "&" if "?" in self.verified_redirect_uri else "?"
        # Same tab, for the same reason as ``allow`` — see #417.
        return rx.redirect(f"{self.verified_redirect_uri}{separator}{urlencode(query)}")

    @staticmethod
    def _describe(response: httpx.Response) -> str:
        try:
            body = response.json() or {}
        except ValueError:
            return ""
        return str(body.get("error_description") or body.get("error") or "")
