"""core#624 AC13 and AC14 (``SPEC_SIGNUP_SOCIAL_AUTH`` §8g.3, §8g.4): the two refusals that social
signup makes common now say what happened.

**AC13 — a superseded flow says so.** Two tabs share one cookie jar, so a second OAuth flow started
before the first finished overwrites the first flow's ``oauth_state`` cookie, and the first flow's
callback then arrives carrying a state the cookie no longer holds. That refusal is an auth boundary
and the comparison stays EXACT (§8d): loosening it would complete this flow against the other flow's
context, i.e. its invitation. What was wrong is the *message*. It rendered the generic "please try
again", and trying again from ``/login`` signs the user in without the invitation or template the
flow was carrying, and without the notice that would tell them so.

So when the cookie **verifies** but holds a **different** state, the refusal carries its own reason.
A missing, malformed, unsigned or tampered cookie keeps the generic one: those are not a flow the
browser really started, and the new sentence would describe something that did not happen.

⚠️ The sentence stays true when the other link came from someone else. A validly signed state cookie
exists only if THIS browser started a flow within the cookie's lifetime, so "you started another
sign-in in this browser" is a fact about the browser either way; and the advice (finish that one,
or start again from where you came) is safe in both cases.

**AC14 — the invitation notice names the address case.** On the social path the likeliest reason an
invitation does not apply is that the provider's verified address differs from the invited one, and
the old copy's remedy ("ask for a new one") loops, because a new invitation to the same address
fails the same way. The notice still names no single cause (``AuthState.invite_notice`` records
why: saying which cause applied tells an unauthenticated caller something about a token they may
not own), so the copy lists every cause, the address case included.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.services.auth_redirects import AUTH_ERROR_KEYS
from datanika.services.oauth_routes import _sign_state, _verify_state, oauth_routes
from datanika.services.oauth_service import google_provider

I18N = Path("datanika/i18n")
LOGIN_PAGE = Path("datanika/ui/pages/login.py")

AC13_EN = (
    "You started another sign-in in this browser before this one finished, so this one was "
    "stopped. Finish that one, or go back to the page you came from and start again."
)
AC14_EN = (
    "Your account is ready and you're signed in to your own workspace. The invitation may have "
    "expired or been used already, or it was sent to a different email address from the one you "
    "signed in with. Ask whoever invited you to send a new invitation to the address you use to "
    "sign in."
)


def _en() -> dict:
    return json.loads((I18N / "en.json").read_text(encoding="utf-8"))


@pytest.fixture
def client():
    return TestClient(Starlette(routes=oauth_routes), follow_redirects=False)


def _callback(client, cookie: str | None, state: str):
    """Drive the real ``oauth_callback``; return (location, whether the provider was contacted).

    ``_get_service`` is what the callback calls to exchange the code with the provider, so a
    refusal at the state check must leave it uncalled. That is the property the refusal exists
    for, and it is asserted beside the reason every time: a reason alone would also be satisfied
    by a flow that went on to complete and failed somewhere later.
    """
    service = MagicMock(name="_get_service")
    with (
        patch("datanika.services.oauth_routes._get_providers") as providers,
        patch("datanika.services.oauth_routes._get_service", service),
    ):
        providers.return_value = {"google": google_provider("gid", "gsecret")}
        client.cookies.clear()
        if cookie is not None:
            client.cookies.set("oauth_state", cookie)
        resp = client.get(f"/api/auth/callback/google?code=abc&state={state}")
    assert resp.status_code == 302, resp.status_code
    return resp.headers["location"], service.called


def _signed(state: str) -> str:
    return f"{state}:{_sign_state(state)}"


class TestAC13ASupersededFlowSaysSo:
    """The witness and the control named in §8g.3, plus the arm that makes the witness
    discriminate."""

    def test_a_verified_cookie_holding_another_state_is_a_superseded_flow(self, client):
        """Witness: a cookie signed for state B, a callback carrying state A."""
        location, contacted = _callback(client, _signed("state_B_second_tab"), "state_A_first_tab")
        assert "auth_error=superseded_flow" in location, location
        assert not contacted, "a superseded flow must be refused before the provider is contacted"

    def test_control_no_cookie_at_all_keeps_the_generic_reason(self, client):
        """§8g.3's named control."""
        location, contacted = _callback(client, None, "state_A_first_tab")
        assert "auth_error=invalid_state" in location, location
        assert not contacted

    def test_a_mismatch_under_a_forged_signature_keeps_the_generic_reason(self, client):
        """What makes the witness discriminate. The states differ exactly as in the witness, but
        the cookie is not one we signed, so no flow of this browser's was superseded -- and the new
        reason must not be reachable by a mismatch alone."""
        forged = f"state_B_second_tab:{'a' * 64}"
        location, contacted = _callback(client, forged, "state_A_first_tab")
        assert "auth_error=invalid_state" in location, location
        assert not contacted

    def test_a_verified_cookie_with_no_state_returned_keeps_the_generic_reason(self, client):
        location, contacted = _callback(client, _signed("state_B_second_tab"), "")
        assert "auth_error=invalid_state" in location, location
        assert not contacted

    def test_the_new_reason_has_its_own_sentence_on_the_login_page(self):
        assert AUTH_ERROR_KEYS.get("superseded_flow") == "auth.error.superseded"
        assert _en()["auth.error.superseded"] == AC13_EN
        assert '("superseded_flow", _t["auth.error.superseded"])' in LOGIN_PAGE.read_text(
            encoding="utf-8"
        ), "login.py renders no arm for the new reason, so /login would show nothing"

    def test_the_generic_reason_still_renders_the_generic_sentence(self):
        """So the split does not quietly change what the other invalid-state cases say."""
        assert AUTH_ERROR_KEYS["invalid_state"] == "auth.error.retry"
        assert _en()["auth.error.retry"] == "We couldn't complete that sign-in. Please try again."


class TestASignatureThatIsNotOursIsRefusedNotRaised:
    """AC13 makes ``_verify_state`` run on cookies it used to skip.

    Before, ``or`` short-circuited on a state mismatch and the signature was checked only when the
    two states were equal. AC13 has to verify the signature to tell a superseded flow from a forged
    one, so it now runs on the mismatch path too. ``hmac.compare_digest`` RAISES ``TypeError`` on a
    ``str`` holding non-ASCII characters, and a cookie is attacker-controlled text: unguarded, a
    crafted cookie turns a redirect into a 500. The equal-state path already had that exposure
    before this change.
    """

    def test_verify_state_returns_false_for_a_non_ascii_signature(self):
        assert _verify_state("some_state", "é" * 64) is False

    def test_verify_state_still_accepts_its_own_signature(self):
        """The positive arm, so the guard is not passing by refusing everything."""
        assert _verify_state("some_state", _sign_state("some_state")) is True


class TestAC14TheInvitationNoticeNamesTheAddressCase:
    def test_the_english_notice_is_the_spec_sentence(self):
        assert _en()["auth.invite_not_applied_help"] == AC14_EN

    def test_the_notice_rendered_for_not_applied_shows_that_sentence(self):
        """§8g.4's witness: render the notice. A rendered Reflex tree carries the translation key,
        not the resolved text, so the rendering is asserted to reference the key under the
        ``not_applied`` condition, and the key's English value is asserted above."""
        from datanika.ui.components.layout import invite_notice

        rendered = str(invite_notice().render())
        assert '["auth.invite_not_applied_help"]' in rendered
        assert "not_applied" in rendered

    @pytest.mark.parametrize("locale", ["ru", "el", "de", "fr", "es", "zh", "ar", "sr"])
    def test_every_locale_carries_both_sentences_translated(self, locale):
        """Parity only proves a key exists. These must be translations, not English pasted in."""
        data = json.loads((I18N / f"{locale}.json").read_text(encoding="utf-8"))
        en = _en()
        for key in ("auth.error.superseded", "auth.invite_not_applied_help"):
            assert data.get(key, "").strip(), f"{locale}.json has no {key}"
            assert data[key] != en[key], f"{locale}.json's {key} is the English string verbatim"
