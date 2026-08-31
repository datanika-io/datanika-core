"""A refusal the user never sees is indistinguishable from a broken button.

``find_or_create_oauth_user`` now refuses to bind a provider identity to an
account that never proved its own email address. That refusal has a remedy — sign
in with the password, or run the reset — but only if it reaches the person.

It did not. ``oauth_callback`` funnelled every exception into a free-text
``error`` parameter, and **nothing on the login page read it** — ``login_page``
renders ``AuthState.auth_error``, which is server-side state that a full-page
redirect from a Starlette route never sets. So the entire class of OAuth and SSO
failure messages was dropped on the floor.

**Fixed in #686**: those redirects now carry a slug from a closed set, and the page
renders one translated sentence per slug. This file still pins the one case the link
guard needs, which is a *different* signal (``link_blocked``) precisely because its
remedy is specific rather than generic.

The signal is a **bounded flag**, not the message text, for the same reason
``?reset=1`` and ``?expired=1`` are: a login page that renders arbitrary text from
its own query string is a phishing surface anyone can aim.
"""

import pytest

from datanika.services.user_service import UserServiceError


class TestTheCallbackDistinguishesARefusalFromAFailure:
    @pytest.mark.asyncio
    async def test_a_refused_link_redirects_to_the_bounded_flag(self, monkeypatch):
        from datanika.services import oauth_routes

        monkeypatch.setattr(
            oauth_routes,
            "_get_providers",
            lambda: {"google": object()},
        )
        monkeypatch.setattr(oauth_routes, "_verify_state", lambda s, sig: True)

        class _Svc:
            async def handle_callback(self, *a, **kw):
                raise UserServiceError("An account already exists ... use 'Forgot password'")

        monkeypatch.setattr(oauth_routes, "_get_service", lambda: _Svc())
        monkeypatch.setattr(oauth_routes, "_get_session", _SessionCtx)

        response = await oauth_routes.oauth_callback(_request("google"))
        location = response.headers["location"]
        assert "link_blocked=1" in location, (
            "a refusal with a remedy must be distinguishable from a generic "
            f"authentication failure; got {location!r}"
        )

    @pytest.mark.asyncio
    async def test_an_ordinary_failure_still_reads_as_a_failure(self, monkeypatch):
        """Negative control: the new branch must not swallow real breakage.

        A provider outage is not something the user can fix by resetting their
        password, and telling them so would send them down a dead end.
        """
        from datanika.services import oauth_routes

        monkeypatch.setattr(oauth_routes, "_get_providers", lambda: {"google": object()})
        monkeypatch.setattr(oauth_routes, "_verify_state", lambda s, sig: True)

        class _Svc:
            async def handle_callback(self, *a, **kw):
                raise RuntimeError("provider is down")

        monkeypatch.setattr(oauth_routes, "_get_service", lambda: _Svc())
        monkeypatch.setattr(oauth_routes, "_get_session", _SessionCtx)

        response = await oauth_routes.oauth_callback(_request("google"))
        location = response.headers["location"]
        assert "link_blocked" not in location
        assert "auth_error=" in location


class TestTheLoginPageShowsIt:
    def test_the_state_reads_the_flag(self):
        from datanika.ui.state.auth_state import AuthState

        st = _StubState({"link_blocked": "1"})
        assert AuthState.show_link_blocked.fget(st) is True

    def test_the_flag_is_off_by_default(self):
        from datanika.ui.state.auth_state import AuthState

        assert AuthState.show_link_blocked.fget(_StubState({})) is False

    def test_the_page_renders_the_callout(self):
        """Rendered from ``login_page()``, not from the callout helper.

        Asserting on a component built in the test proves the component works and
        says nothing about whether the page mounts it.
        """
        from datanika.ui.pages.login import login_page

        html = str(login_page())
        assert "show_link_blocked" in html, "login_page does not mount the callout"

    def test_the_copy_is_translated_everywhere(self):
        """All three keys, all nine locales — heading plus two remedy variants."""
        import json
        from pathlib import Path

        i18n_dir = Path("datanika/i18n")
        locales = sorted(p.stem for p in i18n_dir.glob("*.json"))
        assert len(locales) == 9, f"expected 9 locale files, found {locales}"
        for locale in locales:
            data = json.loads((i18n_dir / f"{locale}.json").read_text(encoding="utf-8"))
            for key in (
                "auth.social_link_blocked",
                "auth.social_link_blocked_help",
                "auth.social_link_blocked_help_no_email",
            ):
                assert key in data, f"{key} missing from {locale}.json"
                assert data[key].strip(), f"{key} is empty in {locale}.json"

    def test_the_remedy_matches_what_the_instance_can_do(self, monkeypatch):
        """With no relay there is no reset link and no verification mail, so the
        text must not tell the user to confirm their address."""
        from datanika.ui.pages import login as login_mod

        monkeypatch.setattr(login_mod.settings, "smtp_host", "smtp.example.com")
        with_mail = str(login_mod._help_key())
        monkeypatch.setattr(login_mod.settings, "smtp_host", "")
        without_mail = str(login_mod._help_key())

        assert "social_link_blocked_help" in with_mail
        assert "no_email" not in with_mail
        assert "social_link_blocked_help_no_email" in without_mail


# ---------------------------------------------------------------------------
# Stand-ins. Deliberately not MagicMock: it answers every attribute truthily,
# so a var that stopped reading the parameter would still "pass".
# ---------------------------------------------------------------------------
class _StubPage:
    def __init__(self, params):
        self.params = params


class _StubRouter:
    def __init__(self, params):
        self.page = _StubPage(params)


class _StubState:
    def __init__(self, params):
        self.router = _StubRouter(params)


class _SessionCtx:
    def __enter__(self):
        return None

    def __exit__(self, *a):
        return False


def _request(provider: str):
    class _R:
        path_params = {"provider": provider}
        query_params = {"code": "abc", "state": "st"}
        cookies = {"oauth_state": "st:sig"}

    return _R()
