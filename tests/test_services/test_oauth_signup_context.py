"""core#624 — the social path carries signup context across the provider round trip.

``/signup?template=<slug>``, ``?invite_token=`` and ``?next=`` are honoured by the email path
and were dropped by the social one: ``oauth_login`` carried nothing but ``state``, and the
callback redirected to ``/auth/complete?token&refresh&is_new``. Adding social buttons to
``/signup`` without closing that makes two funnels worse than they are today
(``SPEC_SIGNUP_SOCIAL_AUTH`` §2), so it is a ship gate rather than a follow-up.

The transport, and why each hop is shaped the way it is:

1. The shared button forwards only :data:`OAUTH_CONTEXT_KEYS` from the page's own query string
   to ``/api/auth/login/<provider>``.
2. ``oauth_login`` re-validates every value and stores the survivors in a **second** signed
   cookie, ``oauth_ctx``. A second cookie rather than a new ``oauth_state`` format, so either
   colour of a blue/green swap can complete a flow the other one started: the previous release
   verifies ``oauth_state`` exactly as before and never reads ``oauth_ctx``. The signature
   covers the flow's ``state`` as well as the context, so a context cannot outlive its flow or
   be paired with a different one (§8d).
3. The callback — a backend route, and the only hop that can read an ``httponly`` cookie —
   verifies, hands ``invite_token`` to the service (which must apply it *before* any personal
   org exists, §8e), and puts ``next``/``template`` plus a bounded ``invite`` flag into the
   ``/auth/complete`` query string (§8c). Both cookies are deleted on that same redirect (AC11).
"""

import base64
import json
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.services import oauth_routes
from datanika.services.auth_redirects import MAX_NEXT_LEN
from datanika.services.oauth_service import google_provider

_PROVIDERS = {"google": google_provider("gid", "gsecret")}
_STATE = "oauth_state"
_CONTEXT = "oauth_ctx"


def _client() -> TestClient:
    """One browser. Tests that need two tabs share one of these, as two tabs share a jar."""
    return TestClient(Starlette(routes=oauth_routes.oauth_routes), follow_redirects=False)


def _set_cookies(response) -> dict[str, str]:
    """``name -> full Set-Cookie header`` for every cookie this response writes."""
    return {h.split("=", 1)[0].strip(): h for h in response.headers.get_list("set-cookie")}


def _cookie_value(header: str) -> str:
    return header.split("=", 1)[1].split(";", 1)[0]


def _login(client: TestClient, **query: str):
    suffix = f"?{urlencode(query)}" if query else ""
    with patch.object(oauth_routes, "_get_providers", return_value=_PROVIDERS):
        response = client.get(f"/api/auth/login/google{suffix}")
    assert response.status_code == 302, response.text
    return response


def _state_of(login_response) -> str:
    """The value the provider echoes back — read from OUR authorize URL, as it would be."""
    return parse_qs(urlsplit(login_response.headers["location"]).query)["state"][0]


class _Service:
    """Records what reached the service.

    ⚠️ Deliberately not a ``MagicMock``: a mock accepts any call shape, so "the token was
    handed over" would hold for a callback that handed over nothing at all.
    """

    def __init__(self, result: dict | None = None):
        self.calls: list[dict] = []
        self._result = {
            "access_token": "jwt-access",
            "refresh_token": "jwt-refresh",
            "user": MagicMock(id=1),
            "is_new": True,
            **(result or {}),
        }

    async def handle_callback(self, provider, code, redirect_uri, session, **kwargs):
        self.calls.append(kwargs)
        return self._result


def _callback(client: TestClient, state: str, service: _Service):
    session_cm = MagicMock()
    session_cm.__enter__.return_value = MagicMock()
    session_cm.__exit__.return_value = False
    with (
        patch.object(oauth_routes, "_get_providers", return_value=_PROVIDERS),
        patch.object(oauth_routes, "_get_service", return_value=service),
        patch.object(oauth_routes, "_get_session", return_value=session_cm),
    ):
        return client.get(f"/api/auth/callback/google?code=provider-code&state={state}")


def _complete_query(response) -> dict[str, str]:
    location = response.headers["location"]
    assert "/auth/complete?" in location, f"the callback did not complete: {location}"
    return {k: v[0] for k, v in parse_qs(urlsplit(location).query).items()}


# ---------------------------------------------------------------------------
# AC3, AC4, AC6 — the context arrives where the email path would have used it
# ---------------------------------------------------------------------------
class TestTheContextSurvivesTheRoundTrip:
    def test_template_reaches_auth_complete(self):
        """AC3. Red before core#624: the callback carried token, refresh and is_new only."""
        client = _client()
        state = _state_of(_login(client, template="stripe-to-postgres"))

        query = _complete_query(_callback(client, state, _Service()))

        assert query.get("template") == "stripe-to-postgres", (
            "a social signup from a template page lands on an empty dashboard instead of "
            f"/connections?template=… — /auth/complete received {sorted(query)}"
        )

    def test_a_same_site_next_reaches_auth_complete(self):
        """AC6, the accepting half — the MCP consent bounce is the first real caller."""
        client = _client()
        nxt = "/oauth/consent?client_id=mcp_abc&state=xyz"
        state = _state_of(_login(client, next=nxt))

        assert _complete_query(_callback(client, state, _Service())).get("next") == nxt

    @pytest.mark.parametrize(
        "hostile",
        [
            "//evil.example/phish",
            "https://evil.example/phish",
            "/\\evil.example/phish",
            "/oauth/consent\n/evil",
            "javascript:alert(1)",
        ],
    )
    def test_a_crafted_next_never_reaches_auth_complete(self, hostile):
        """AC6, the refusing half.

        ⚠️ On the unfixed code this passes by carrying nothing at all. The accepting half above
        is what makes it evidence — the two are one criterion.
        """
        client = _client()
        state = _state_of(_login(client, next=hostile))

        assert "next" not in _complete_query(_callback(client, state, _Service()))

    def test_the_invite_token_reaches_the_service_and_not_the_url(self):
        """AC4's transport. The service applies it before a personal org exists (§8e)."""
        client = _client()
        state = _state_of(_login(client, invite_token="inv.token-1"))
        service = _Service()

        response = _callback(client, state, service)

        assert len(service.calls) == 1
        assert service.calls[0].get("invite_token") == "inv.token-1", (
            f"the invitation did not reach the service: {service.calls[0]}"
        )
        assert "inv.token-1" not in response.headers["location"], (
            "the token was applied in the callback, so nothing downstream needs it — "
            "putting it back into a URL only adds a place for it to leak"
        )

    def test_an_invitation_that_did_not_apply_is_flagged_to_the_page(self):
        """AC5's transport: a bounded flag, never a sentence (the page picks the words)."""
        client = _client()
        state = _state_of(_login(client, invite_token="inv.token-1"))

        query = _complete_query(_callback(client, state, _Service({"invite": "not_applied"})))

        assert query.get("invite") == "not_applied"

    def test_an_applied_invitation_raises_no_flag(self):
        client = _client()
        state = _state_of(_login(client, invite_token="inv.token-1"))

        query = _complete_query(_callback(client, state, _Service({"invite": "joined"})))

        assert "invite" not in query

    def test_a_flow_without_context_is_unchanged(self):
        """Control. An ordinary sign-in carries exactly what it carried before."""
        client = _client()
        state = _state_of(_login(client))
        service = _Service()

        query = _complete_query(_callback(client, state, service))

        assert set(query) == {"token", "refresh", "is_new"}
        assert service.calls[0].get("invite_token", "") == ""

    def test_parameters_outside_the_context_set_are_not_carried(self):
        """``email`` in particular: the provider supplies the address, so nothing consumes it."""
        client = _client()
        state = _state_of(_login(client, email="someone@example.com", org_id="7"))

        query = _complete_query(_callback(client, state, _Service()))

        assert set(query) == {"token", "refresh", "is_new"}


# ---------------------------------------------------------------------------
# AC11 — nothing outlives the flow
# ---------------------------------------------------------------------------
class TestTheCookiesDoNotOutliveTheFlow:
    def test_both_cookies_are_deleted_on_the_redirect_to_auth_complete(self):
        """AC11. Assert the absence: it is what stops someone reintroducing a cookie read on
        ``/auth/complete``, which §8c rules out (httponly, and already gone)."""
        client = _client()
        state = _state_of(_login(client, template="csv-to-duckdb"))

        written = _set_cookies(_callback(client, state, _Service()))

        for name in (_STATE, _CONTEXT):
            assert name in written, f"the callback does not clear {name}"
            assert "max-age=0" in written[name].lower(), written[name]

    def test_a_flow_with_context_writes_a_short_lived_httponly_cookie(self):
        header = _set_cookies(_login(_client(), template="csv-to-duckdb"))[_CONTEXT].lower()

        assert "httponly" in header
        assert "max-age=600" in header
        assert "samesite=lax" in header

    def test_a_flow_without_context_clears_a_stale_context_cookie(self):
        """An abandoned invite flow must not leave its context for the next sign-in to find."""
        written = _set_cookies(_login(_client()))

        assert _CONTEXT in written
        assert "max-age=0" in written[_CONTEXT].lower()

    def test_the_largest_accepted_context_still_fits_in_one_cookie(self):
        """A browser silently drops a cookie over 4096 bytes, and the flow then completes
        WITHOUT its context — no error anywhere. Pin the worst case rather than meet it."""
        nxt = "/" + "a" * (MAX_NEXT_LEN - 1)
        token = "t" * oauth_routes.MAX_INVITE_TOKEN_LEN

        written = _set_cookies(_login(_client(), next=nxt, template="a" * 64, invite_token=token))

        value = _cookie_value(written[_CONTEXT])
        assert len(value) > MAX_NEXT_LEN, "premise: the worst-case context was not stored at all"
        assert len(_CONTEXT) + 1 + len(value) <= 4096, (
            f"the worst-case context cookie is {len(_CONTEXT) + 1 + len(value)} bytes"
        )


# ---------------------------------------------------------------------------
# §8d / AC12 — a context cannot be forged, and cannot cross into another flow
# ---------------------------------------------------------------------------
class TestTheContextCannotBeForgedOrCrossed:
    def test_a_tampered_context_is_ignored(self):
        login = _login(_client(), invite_token="inv.token-1")
        written = _set_cookies(login)
        signature = _cookie_value(written[_CONTEXT]).rsplit(":", 1)[1]
        forged = base64.urlsafe_b64encode(
            json.dumps({"invite_token": "someone-elses-token"}).encode()
        ).decode()

        browser = _client()
        browser.cookies.set(_STATE, _cookie_value(written[_STATE]))
        browser.cookies.set(_CONTEXT, f"{forged.rstrip('=')}:{signature}")
        service = _Service()
        _callback(browser, _state_of(login), service)

        assert service.calls, "premise: the state cookie was genuine, so the flow must complete"
        assert service.calls[0].get("invite_token", "") == ""

    def test_a_context_issued_to_another_flow_is_ignored(self):
        """The signature binds a context to its own state, so a pairing cannot be assembled."""
        plain = _login(_client())
        invited = _login(_client(), invite_token="org-b-token")

        browser = _client()
        browser.cookies.set(_STATE, _cookie_value(_set_cookies(plain)[_STATE]))
        browser.cookies.set(_CONTEXT, _cookie_value(_set_cookies(invited)[_CONTEXT]))
        service = _Service()
        _callback(browser, _state_of(plain), service)

        assert service.calls[0].get("invite_token", "") == "", (
            "a flow completed against an invitation that was issued to a different flow"
        )

    def test_a_second_flow_in_the_same_browser_fails_the_first_closed(self):
        """AC12. Two tabs share one jar: an invitation in one, a template page in the other.

        🚨 The tempting repair for the resulting mismatch — loosening the state comparison —
        would complete the first tab against the SECOND tab's invitation. So the assertion is
        not merely that an error appeared: it is that the first flow never reached the service
        that applies invitations.
        """
        browser = _client()
        first = _state_of(_login(browser, invite_token="org-a-token"))
        second = _state_of(_login(browser, invite_token="org-b-token"))

        superseded = _Service()
        response = _callback(browser, first, superseded)

        assert "auth_error=invalid_state" in response.headers["location"]
        assert superseded.calls == [], (
            "the superseded flow reached the service, which is where the second flow's "
            "invitation would have been applied to it"
        )

        # Control: the live flow still completes, with its own context.
        live = _Service()
        _callback(browser, second, live)
        assert live.calls[0].get("invite_token") == "org-b-token"
