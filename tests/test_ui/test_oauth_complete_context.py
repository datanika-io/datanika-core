"""core#624 — ``/auth/complete`` applies what the callback carried, with the email path's helpers.

``SPEC_SIGNUP_SOCIAL_AUTH`` §8c: the page reads its OWN query string. It can never read the
``oauth_ctx`` cookie — that cookie is ``httponly``, and the callback deletes it on the very
redirect that brings the browser here. So the only things that can reach this page are the ones
the callback put in the URL, and each is applied through the helper the email path uses.

⚠️ ``_safe_next_path`` on the way OUT is the constraint that matters most here (§2 constraint 1).
The signed cookie protected the value between the two backend hops; once it is back in a URL, a
user can put anything in it, so this page is the last gate before a redirect.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from datanika.config import settings
from datanika.services.auth import AuthService
from datanika.ui.state.auth_state import AuthState


def _redirect_target(spec) -> str:
    """The URL an ``rx.redirect`` EventSpec sends the browser to (as test_mcp_consent_state)."""
    assert spec is not None, "expected a redirect, got None"
    for name, value in spec.args:
        if name._js_expr == "path":
            return value._var_value
    raise AssertionError("EventSpec has no 'path' argument")


class _CompleteState:
    """Only what ``handle_oauth_complete`` may touch.

    ⚠️ Not a ``MagicMock``: a mock answers every attribute, so an assertion that the notice was
    raised would pass against a handler that never wrote it.
    """

    def __init__(self, params: dict, *, invite_notice: str = ""):
        self.router = SimpleNamespace(page=SimpleNamespace(params=params))
        self.auth_error = ""
        self.access_token = ""
        self.refresh_token = ""
        self.current_user = None
        self.current_org = SimpleNamespace(id=0, name="", slug="")
        self.user_orgs = []
        self.current_role = ""
        self.invite_notice = invite_notice

    #: The real helper, bound rather than reimplemented — it is the thing under test for AC3/AC6.
    _post_auth_redirect_target = getattr(
        AuthState._post_auth_redirect_target, "fn", AuthState._post_auth_redirect_target
    )

    def _get_user_service(self):
        svc = MagicMock()
        svc.get_user.return_value = SimpleNamespace(
            id=7, email="nina@example.com", full_name="Nina"
        )
        svc.get_user_orgs.return_value = [SimpleNamespace(id=70, name="Acme", slug="acme")]
        return svc

    def _load_current_role(self, user_id, org_id):
        self.current_role = "editor"


def _complete(params: dict, **kwargs):
    state = _CompleteState(
        {"token": AuthService(settings.secret_key).create_access_token(7, 70), "refresh": "r"}
        | params,
        **kwargs,
    )
    session_cm = MagicMock()
    session_cm.__enter__.return_value = MagicMock()
    session_cm.__exit__.return_value = False
    with patch("datanika.ui.state.auth_state.get_sync_session", return_value=session_cm):
        spec = AuthState.handle_oauth_complete.fn(state)
    return state, spec


class TestTheInvitationOutcomeReachesTheUser:
    def test_an_invitation_that_did_not_apply_raises_the_notice(self):
        """AC5 on the social path. Red before core#624: the page never read the flag.

        The notice is rendered by the shell (``layout.invite_notice``), which is where the
        email path's core#981 notice already lives — so no new sentence is needed.
        """
        state, _spec = _complete({"invite": "not_applied"})

        assert state.invite_notice == "not_applied"

    def test_a_sign_in_without_the_flag_clears_an_earlier_notice(self):
        """A notice about an earlier attempt must not survive into this sign-in."""
        state, _spec = _complete({}, invite_notice="not_applied")

        assert state.invite_notice == ""

    @pytest.mark.parametrize("value", ["joined", "NOT_APPLIED", "not_applied ", "<b>x</b>"])
    def test_only_the_exact_flag_raises_it(self, value):
        """A bounded flag, not a passthrough — the query string is the user's to edit."""
        state, _spec = _complete({"invite": value})

        assert state.invite_notice == ""


class TestTheRedirectUsesTheEmailPathsHelper:
    def test_a_template_lands_on_the_prefilled_connection_form(self):
        """AC3, the page half: the same destination the email path reaches."""
        _state, spec = _complete({"template": "stripe-to-postgres"})

        assert _redirect_target(spec) == "/connections?template=stripe-to-postgres"

    @pytest.mark.parametrize(
        "hostile", ["//evil.example/phish", "https://evil.example/phish", "/ok\n/evil"]
    )
    def test_a_crafted_next_goes_to_the_dashboard(self, hostile):
        """AC6, the page half."""
        _state, spec = _complete({"next": hostile})

        assert _redirect_target(spec) == "/"

    def test_a_template_slug_with_a_trailing_newline_is_refused(self):
        """``re.match`` with ``$`` matches before a trailing newline, so the slug pattern
        accepted ``"stripe-to-postgres\\n"`` and put the newline into the redirect. The pattern
        is now applied as a full match on both paths."""
        _state, spec = _complete({"template": "stripe-to-postgres\n"})

        assert _redirect_target(spec) == "/"
