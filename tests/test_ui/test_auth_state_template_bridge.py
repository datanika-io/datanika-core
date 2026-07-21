"""Option C auth bridge: ?template=<slug> must survive the signup wall.

When a cold-traffic visitor clicks "Try this template" on a public landing
page at ``datanika.io/templates/<slug>`` and signs up, the post-auth
redirect must land on ``/connections?template=<slug>`` so
``ConnectionState.load_template_from_query`` (shipped in PR #81) can
prefill the connection form.

See plans/product/SPEC_PUBLIC_TEMPLATE_LANDING.md "The auth bridge" section
and datanika-io/datanika-landing#125.
"""

import inspect

import pytest

import datanika.ui.state.auth_state as auth_state_module
from datanika.ui.state.auth_state import AuthState


class _FakePage:
    def __init__(self, params: dict):
        self.params = params


class _FakeRouter:
    def __init__(self, params: dict):
        self.page = _FakePage(params)


def _make_state(query_params: dict) -> AuthState:
    """Build an AuthState instance with a mocked router for unit tests.

    Reflex state classes subclass ``rx.State`` and override ``__setattr__``
    with parent-state delegation we don't care about. Bypass it with
    ``object.__setattr__`` — the helper under test only reads
    ``self.router.page.params``, nothing else.
    """
    state = AuthState.__new__(AuthState)
    object.__setattr__(state, "router", _FakeRouter(query_params))
    return state


class TestPostAuthRedirectTarget:
    """The shared helper that each auth entry point consults."""

    def test_returns_connections_with_template_when_param_present(self):
        state = _make_state({"template": "stripe-to-postgres"})
        assert state._post_auth_redirect_target() == "/connections?template=stripe-to-postgres"

    def test_returns_root_when_param_absent(self):
        state = _make_state({})
        assert state._post_auth_redirect_target() == "/"

    def test_returns_root_when_param_is_empty_string(self):
        state = _make_state({"template": ""})
        assert state._post_auth_redirect_target() == "/"

    def test_template_slug_is_urlencoded(self):
        """A slug with only [a-z0-9-] characters needs no escaping, but the
        helper must not break if Python's urlencode introduces one.
        """
        state = _make_state({"template": "postgres-to-bigquery"})
        target = state._post_auth_redirect_target()
        assert target == "/connections?template=postgres-to-bigquery"

    def test_unknown_characters_in_slug_are_rejected(self):
        """Only slugs matching the public template pattern should survive.
        An attacker-controlled value like 'foo?injected=1' or 'foo bar' must
        not produce an open-redirect vector. Reject by returning the root.
        """
        state = _make_state({"template": "foo bar/baz?x=1"})
        assert state._post_auth_redirect_target() == "/"

    def test_slug_length_limit(self):
        """Overlong slugs are rejected to avoid amplification."""
        state = _make_state({"template": "a" * 200})
        assert state._post_auth_redirect_target() == "/"


class TestNextBridge:
    """``?next=`` — resume an interrupted flow after the login wall (#394).

    ``/oauth/consent`` is the first caller: the OAuth request lives entirely in
    that page's query string and an MCP client cannot re-send it, so dropping
    the user on the dashboard strands the handshake.

    It is also the classic open-redirect vector, and the moment right after
    authentication is exactly when a user is most likely to type a password
    into whatever they land on — so the rejected shapes below matter as much
    as the accepted one.
    """

    def test_returns_a_same_site_path(self):
        state = _make_state({"next": "/oauth/consent?client_id=mcp_abc&state=xyz"})
        assert state._post_auth_redirect_target() == "/oauth/consent?client_id=mcp_abc&state=xyz"

    def test_next_wins_over_template(self):
        """Both present means the user was pulled out of a specific flow."""
        state = _make_state({"next": "/oauth/consent?client_id=a", "template": "csv-to-duckdb"})
        assert state._post_auth_redirect_target() == "/oauth/consent?client_id=a"

    def test_absent_next_falls_through_to_template(self):
        state = _make_state({"template": "stripe-to-postgres"})
        assert state._post_auth_redirect_target() == "/connections?template=stripe-to-postgres"

    @pytest.mark.parametrize(
        "hostile",
        [
            "https://evil.test/phish",  # absolute URL
            "//evil.test/phish",  # protocol-relative
            "/\\evil.test/phish",  # backslash, normalised to protocol-relative
            "/\\/evil.test",
            "javascript:alert(1)",  # not a path at all
            "  /oauth/consent",  # leading whitespace
            "/oauth/consent\n/evil",  # header/URL splitting attempt
            "a" * 3000,
        ],
    )
    def test_off_site_targets_are_rejected(self, hostile):
        state = _make_state({"next": hostile})
        assert state._post_auth_redirect_target() == "/"


class TestAuthStateSourceWiring:
    """Source-inspection regression tests — matches the PR #96 pattern in
    tests/test_ui/test_auth_state.py. Catches a future refactor that
    drops the helper call from one of the three auth entry points.
    """

    def test_signup_source_calls_post_auth_redirect_target(self):
        source = inspect.getsource(auth_state_module.AuthState.signup.fn)
        assert "_post_auth_redirect_target" in source, (
            "signup() no longer consults _post_auth_redirect_target — "
            "cold-traffic ?template=<slug> will be dropped at the signup wall. "
            "See plans/product/SPEC_PUBLIC_TEMPLATE_LANDING.md auth bridge."
        )

    def test_login_source_calls_post_auth_redirect_target(self):
        source = inspect.getsource(auth_state_module.AuthState.login.fn)
        assert "_post_auth_redirect_target" in source, (
            "login() no longer consults _post_auth_redirect_target — "
            "a returning visitor with a bookmarked ?template=<slug> URL will "
            "be redirected to / and lose their template intent."
        )

    def test_handle_oauth_complete_source_calls_post_auth_redirect_target(self):
        source = inspect.getsource(auth_state_module.AuthState.handle_oauth_complete.fn)
        assert "_post_auth_redirect_target" in source, (
            "handle_oauth_complete() no longer consults "
            "_post_auth_redirect_target — OAuth/SSO signups from template "
            "landing pages will drop the template intent."
        )

    def test_signup_preserves_plugin_event_collection_when_templating(self):
        """Regression guard: the Option C change must not delete the plugin
        event collection path from PR #102 (open-core split). Both the
        collect_events call (which the cloud plugin subscribes to and turns
        into a Google Ads conversion event via rx.call_script) AND the
        template redirect target must coexist in signup(). Updated from the
        earlier google_ads_conversion_event_js assertion — that helper moved
        from datanika/ui/analytics.py to datanika_cloud/analytics.py as part
        of the open-core refactor (#99/#102).
        """
        source = inspect.getsource(auth_state_module.AuthState.signup.fn)
        assert "collect_events" in source
        assert "user.signup_completed" in source
        assert "_post_auth_redirect_target" in source
