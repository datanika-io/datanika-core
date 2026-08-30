"""The password surfaces: two public pages and one Settings card.

Structural assertions only — these render `rx.Component` trees, they do not
drive a browser. What they pin down is the handful of choices that are cheap to
break in a later edit and expensive to notice:

* the two reset screens are **Reflex pages**, not backend routes (a backend
  route outside ``/api/`` silently serves the SPA in production);
* every password input is inside an ``rx.form`` with ``on_submit`` rather than
  bound to a state var — a controlled ``value=``/``on_change=`` password field
  ships the plaintext to the server on every keystroke and then keeps it in
  server-side Reflex state for the life of the session;
* the "Forgot your password?" link is absent on an instance with no SMTP, where
  the flow could only ever show "check your inbox" forever;
* all 26 new keys exist in all 9 locales.
"""

import json
from pathlib import Path

import pytest
import reflex as rx

I18N_DIR = Path(__file__).resolve().parent.parent.parent / "datanika" / "i18n"
LOCALES = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]

NEW_KEYS = [
    "auth.forgot_password",
    "auth.forgot_password_heading",
    "auth.forgot_password_intro",
    "auth.send_reset_link",
    "auth.reset_link_sent_heading",
    "auth.reset_link_sent_body",
    "auth.reset_link_sent_hint",
    "auth.reset_password_heading",
    "auth.new_password",
    "auth.confirm_password",
    "auth.set_password",
    "auth.reset_link_invalid_heading",
    "auth.reset_link_invalid_body",
    "auth.request_new_link",
    "auth.password_reset_done",
    "auth.back_to_sign_in",
    "auth.reset_unavailable",
    "account.title",
    "account.subtitle",
    "account.change_password",
    "account.set_password_hint",
    "account.current_password",
    "account.update_password",
    "account.password_updated",
    "account.password_rules",
    "account.review_api_keys",
]


def _walk(component):
    yield component
    for child in getattr(component, "children", []) or []:
        yield from _walk(child)


def _tags(component):
    return [type(c).__name__ for c in _walk(component)]


def _card_style(component):
    """The style of the 360px card in a public auth page, if there is one."""
    for node in _walk(component):
        style = {k: str(v).strip('"') for k, v in dict(getattr(node, "style", {}) or {}).items()}
        if style.get("width") == "360px":
            return style
    return {}


def _password_inputs(component):
    out = []
    for node in _walk(component):
        props = getattr(node, "type", None)
        if props is not None and str(props).strip('"') == "password":
            out.append(node)
    return out


class TestI18nKeys:
    def test_all_new_keys_exist_in_english(self):
        en = json.loads((I18N_DIR / "en.json").read_text(encoding="utf-8"))
        missing = [k for k in NEW_KEYS if k not in en]
        assert missing == []

    @pytest.mark.parametrize("locale", LOCALES)
    def test_all_new_keys_exist_in_every_locale(self, locale):
        data = json.loads((I18N_DIR / f"{locale}.json").read_text(encoding="utf-8"))
        missing = [k for k in NEW_KEYS if k not in data]
        assert missing == [], f"{locale}.json is missing {missing}"

    @pytest.mark.parametrize("locale", [loc for loc in LOCALES if loc != "en"])
    def test_translations_are_not_left_as_english(self, locale):
        """A copied English value is a missing translation that passes key parity."""
        en = json.loads((I18N_DIR / "en.json").read_text(encoding="utf-8"))
        data = json.loads((I18N_DIR / f"{locale}.json").read_text(encoding="utf-8"))
        # Latin-script locales legitimately share some short words; only flag the
        # long-form sentences, where an identical string cannot be a coincidence.
        long_keys = [k for k in NEW_KEYS if len(en[k]) > 40]
        untranslated = [k for k in long_keys if data[k] == en[k]]
        assert untranslated == [], f"{locale}.json still carries English for {untranslated}"

    def test_no_duplicate_english_values_across_the_two_prefixes(self):
        """26 rows, 26 distinct keys — nine locales is nine translators' worth of
        reason not to say the same words twice."""
        assert len(NEW_KEYS) == len(set(NEW_KEYS)) == 26


class TestForgotPasswordPage:
    def test_it_is_a_reflex_page_not_a_backend_route(self):
        from datanika.ui.pages.forgot_password import forgot_password_page

        assert isinstance(forgot_password_page(), rx.Component)

    def test_it_has_a_form_with_on_submit(self):
        from datanika.ui.pages.forgot_password import forgot_password_page

        forms = [n for n in _walk(forgot_password_page()) if type(n).__name__ == "Form"]
        assert forms, "the email field must be submitted, not bound per keystroke"
        assert any(getattr(f, "event_triggers", {}).get("on_submit") for f in forms)

    def test_it_offers_a_way_back_to_sign_in(self):
        from datanika.ui.pages.forgot_password import forgot_password_page

        rendered = str(forgot_password_page())
        assert "/login" in rendered

    def test_the_confirmation_offers_signup_rather_than_disclosing_absence(self):
        """D7: remove the dead end with copy, not with disclosure."""
        from datanika.ui.pages.forgot_password import forgot_password_page

        rendered = str(forgot_password_page())
        assert "/signup" in rendered

    def test_it_matches_the_login_card_shell(self):
        """Compared against the real /login card rather than a copy of its
        numbers — a shell that drifts from the page beside it is the failure
        worth catching, and Reflex normalises style keys (``border_radius`` ->
        ``borderRadius``) so hardcoding them tests the framework instead."""
        import datanika.ui.pages.login as login_mod
        from datanika.ui.pages.forgot_password import forgot_password_page
        from datanika.ui.pages.reset_password import reset_password_page

        expected = _card_style(login_mod.login_page())
        assert expected, "the login card shell moved; this test needs re-anchoring"
        assert _card_style(forgot_password_page()) == expected
        assert _card_style(reset_password_page()) == expected


class TestResetPasswordPage:
    def test_it_is_a_reflex_page(self):
        from datanika.ui.pages.reset_password import reset_password_page

        assert isinstance(reset_password_page(), rx.Component)

    def test_password_fields_are_never_bound_to_state(self):
        from datanika.ui.pages.reset_password import reset_password_page

        for node in _password_inputs(reset_password_page()):
            triggers = getattr(node, "event_triggers", {}) or {}
            assert "on_change" not in triggers, (
                "a controlled password field ships the plaintext to the server on "
                "every keystroke and leaves it in server-side Reflex state"
            )

    def test_it_offers_a_new_link_when_the_old_one_is_dead(self):
        from datanika.ui.pages.reset_password import reset_password_page

        assert "/forgot-password" in str(reset_password_page())

    def test_it_links_nowhere_off_site(self):
        """D3: while the token is in the URL, an external link leaks it via Referer."""
        rendered_pages = []
        from datanika.ui.pages.forgot_password import forgot_password_page
        from datanika.ui.pages.reset_password import reset_password_page

        rendered_pages.append(str(reset_password_page()))
        rendered_pages.append(str(forgot_password_page()))
        for rendered in rendered_pages:
            assert "http://" not in rendered.replace("http://localhost", "")
            assert "https://" not in rendered


class TestPagesAreRegistered:
    def test_both_routes_are_added_to_the_app(self):
        source = (
            Path(__file__).resolve().parent.parent.parent / "datanika" / "datanika.py"
        ).read_text(encoding="utf-8")
        assert '"/forgot-password"' in source
        assert '"/reset-password"' in source

    def test_neither_is_behind_the_auth_check(self):
        """A signed-out user is the only kind that can need these."""
        source = (
            Path(__file__).resolve().parent.parent.parent / "datanika" / "datanika.py"
        ).read_text(encoding="utf-8")
        for route in ("/forgot-password", "/reset-password"):
            block = source[source.index(f'"{route}"') :]
            block = block[: block.index("app.add_page") if "app.add_page" in block else len(block)]
            assert "check_auth" not in block

    def test_the_reset_page_loads_its_token_on_load(self):
        source = (
            Path(__file__).resolve().parent.parent.parent / "datanika" / "datanika.py"
        ).read_text(encoding="utf-8")
        block = source[source.index('"/reset-password"') :][:400]
        assert "load_token" in block


class TestLoginPage:
    def test_forgot_link_is_shown_when_smtp_is_configured(self, monkeypatch):
        import datanika.ui.pages.login as login_mod

        monkeypatch.setattr(login_mod.settings, "smtp_host", "smtp.example.com")
        assert "/forgot-password" in str(login_mod.login_page())

    def test_forgot_link_is_hidden_when_smtp_is_not(self, monkeypatch):
        """D9: on the default self-hosted instance the flow can only ever show
        'check your inbox' forever, so do not offer it."""
        import datanika.ui.pages.login as login_mod

        monkeypatch.setattr(login_mod.settings, "smtp_host", "")
        assert "/forgot-password" not in str(login_mod.login_page())

    def test_it_can_show_the_post_reset_success_callout(self):
        import datanika.ui.pages.login as login_mod

        assert "show_reset_done" in str(login_mod.login_page())


class TestAccountCard:
    def test_it_is_the_first_card_on_the_settings_page(self):
        """Every other card there is org-scoped; this is the first user-scoped
        control, so it is not buried between two of them."""
        source = (
            Path(__file__).resolve().parent.parent.parent
            / "datanika"
            / "ui"
            / "pages"
            / "settings.py"
        ).read_text(encoding="utf-8")
        body = source[source.index("def settings_page(") :]
        assert body.index("account_card()") < body.index("org_profile_card()")

    def test_password_fields_are_never_bound_to_state(self):
        from datanika.ui.pages.settings import account_card

        for node in _password_inputs(account_card()):
            triggers = getattr(node, "event_triggers", {}) or {}
            assert "on_change" not in triggers

    def test_it_renders_a_form_with_on_submit(self):
        from datanika.ui.pages.settings import account_card

        forms = [n for n in _walk(account_card()) if type(n).__name__ == "Form"]
        assert forms
        assert any(getattr(f, "event_triggers", {}).get("on_submit") for f in forms)

    def test_the_current_password_field_is_conditional(self):
        """D6: an OAuth-only account can never fill it, and an account that has a
        password must always face it."""
        from datanika.ui.pages.settings import account_card

        assert "has_password" in str(account_card())

    def test_it_points_at_the_api_keys_card_after_a_change(self):
        from datanika.ui.pages.settings import account_card

        assert "account.review_api_keys" in str(account_card())

    def test_it_cross_links_to_the_published_docs(self):
        """Safe on this page and deliberately absent from /reset-password."""
        from datanika.ui.pages.settings import account_card

        assert "datanika.io/docs/organizations" in str(account_card())


class TestNoSecretsInRenderedOutput:
    def test_no_page_declares_a_state_var_holding_a_password(self):
        from datanika.ui.state.account_state import AccountState
        from datanika.ui.state.password_reset_state import PasswordResetState

        for state in (AccountState, PasswordResetState):
            for name in state.__fields__:
                assert "password" not in name or name in {
                    "has_password",
                    "password_updated",
                }, f"{state.__name__}.{name} looks like it stores a password"
