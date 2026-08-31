"""Every signal the backend sends to ``/login`` must reach the person reading the page.

Covers #686 (the login page never rendered ``?error=``), #659 (four `email_routes` signals
with no reader) and #700's acceptance criterion 3 (``?verified=1`` / ``?verify_error=1``).

The shape of the bug was always the same: a Starlette route 302s the browser to ``/login``
with a reason in the query string, and the page renders `AuthState.auth_error` — *server-side*
state that a full-page redirect never writes. So the reason was dropped on the floor. Six
distinct signals reached a page that read one.

**The important test in this file is `TestNoSignalIsSentWithoutAReader`.** It derives the
parameter list from the route sources, so adding a redirect without a reader fails the build.
Every previous instance of this bug was two hand-maintained lists with nothing linking them,
and a test that restates the list here would be a third.
"""

import json
import re
from pathlib import Path

import pytest

from datanika.services.auth_redirects import AUTH_ERROR_KEYS, login_error_path

SRC = Path("datanika")
I18N = SRC / "i18n"
AUTH_STATE = SRC / "ui" / "state" / "auth_state.py"
ROUTE_FILES = [
    SRC / "services" / "oauth_routes.py",
    SRC / "services" / "sso_routes.py",
    SRC / "services" / "email_routes.py",
]

# Every ``/login?<param>=`` literal anywhere under datanika/, including the ones the
# frontend sets on itself (``reset``, ``expired``, ``next``).
_LOGIN_PARAM = re.compile(r"/login\?([a-zA-Z_]+)=")
_SLUG_CALL = re.compile(r"login_error_path\(\s*[\"']([a-zA-Z_]+)[\"']\s*\)")


def _python_sources():
    return [p for p in SRC.rglob("*.py") if "__pycache__" not in p.parts]


def _params_sent_to_login() -> set[str]:
    found = set()
    for path in _python_sources():
        found.update(_LOGIN_PARAM.findall(path.read_text(encoding="utf-8")))
    return found


class TestNoSignalIsSentWithoutAReader:
    def test_every_login_query_param_the_code_sends_is_read_by_the_state(self):
        """Derived from the sources on both sides — neither list is written down here.

        This is the guard the three issues were all instances of. A new redirect that
        nothing reads fails here instead of silently showing a blank sign-in form.
        """
        sent = _params_sent_to_login()
        assert sent, "found no /login redirects at all — the regex has stopped matching"

        state_src = AUTH_STATE.read_text(encoding="utf-8")
        unread = sorted(p for p in sent if f'params.get("{p}"' not in state_src)

        assert not unread, (
            f"{len(unread)} signal(s) are sent to /login and read by nothing: {unread}. "
            "Add an rx.var to AuthState and a callout to login_page, or stop sending it."
        )

    def test_the_free_text_error_redirect_is_gone(self):
        """#686: delete the free-text mechanism rather than leaving both alive.

        ``/login?error=<sentence>`` let anyone put arbitrary text inside our own sign-in
        card. Leaving it in place beside the slug mechanism would keep that surface open
        for whichever call site was missed.
        """
        offenders = [
            f"{p}:{i}"
            for p in _python_sources()
            for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), 1)
            if "/login?error=" in line
        ]
        assert not offenders, f"free-text error redirects still present: {offenders}"

    def test_every_slug_used_by_a_route_is_in_the_whitelist(self):
        slugs = set()
        for path in ROUTE_FILES:
            slugs.update(_SLUG_CALL.findall(path.read_text(encoding="utf-8")))

        assert slugs, "no login_error_path() call sites found — the routes stopped using it"
        unknown = sorted(slugs - set(AUTH_ERROR_KEYS))
        assert not unknown, f"slugs with no i18n mapping: {unknown}"

    def test_the_whitelist_has_no_slug_no_route_uses(self):
        """The other direction: a slug nothing sends is dead copy to maintain."""
        slugs = set()
        for path in ROUTE_FILES:
            slugs.update(_SLUG_CALL.findall(path.read_text(encoding="utf-8")))

        orphans = sorted(set(AUTH_ERROR_KEYS) - slugs)
        assert not orphans, f"AUTH_ERROR_KEYS entries no route sends: {orphans}"


class TestTheWhitelistIsClosed:
    def test_a_known_slug_resolves_to_itself(self):
        from datanika.ui.state.auth_state import AuthState

        st = _StubState({"auth_error": "sso_no_email"})
        assert AuthState.auth_error_reason.fget(st) == "sso_no_email"

    def test_an_unknown_slug_renders_nothing(self):
        from datanika.ui.state.auth_state import AuthState

        st = _StubState({"auth_error": "not_a_real_reason"})
        assert AuthState.auth_error_reason.fget(st) == ""

    def test_an_injected_sentence_renders_nothing(self):
        """The reason the mechanism is a whitelist and not a passthrough.

        Under the old ``?error=`` shape this string rendered inside our sign-in card, under
        our logo, in our styling — a phishing page anyone could aim by sending a link.
        """
        from datanika.ui.state.auth_state import AuthState

        hostile = "Your account was flagged. Call +1-555-0100 to restore access."
        st = _StubState({"auth_error": hostile})
        assert AuthState.auth_error_reason.fget(st) == ""

    def test_no_parameter_at_all_renders_nothing(self):
        from datanika.ui.state.auth_state import AuthState

        assert AuthState.auth_error_reason.fget(_StubState({})) == ""

    def test_building_a_redirect_for_an_unknown_slug_is_an_error(self):
        """Fails at the call site rather than as a blank page in production."""
        with pytest.raises(ValueError):
            login_error_path("something_nobody_translated")

    def test_building_a_redirect_for_a_known_slug_works(self):
        assert login_error_path("sso_unreachable") == "/login?auth_error=sso_unreachable"


class TestTheFourEmailSignalsAreRead:
    @pytest.mark.parametrize(
        ("param", "var"),
        [
            ("verified", "show_email_verified"),
            ("verify_error", "show_verify_error"),
            ("invite_accepted", "show_invite_accepted"),
            ("invite_error", "show_invite_error"),
        ],
    )
    def test_the_state_reads_it(self, param, var):
        from datanika.ui.state.auth_state import AuthState

        assert getattr(AuthState, var).fget(_StubState({param: "1"})) is True
        assert getattr(AuthState, var).fget(_StubState({})) is False


class TestTheLoginPageMountsEverySignal:
    """Rendered from ``login_page()``, not from a component built in the test.

    Asserting on a component the test constructs proves the component works and says
    nothing about whether the page mounts it (the lesson from #679's callout).
    """

    @pytest.mark.parametrize(
        "var",
        [
            "auth_error_reason",
            "show_email_verified",
            "show_verify_error",
            "show_invite_accepted",
            "show_invite_error",
            "show_reset_done",
            "show_session_expired",
            "show_link_blocked",
        ],
    )
    def test_the_page_mounts_it(self, var):
        from datanika.ui.pages.login import login_page

        assert var in str(login_page()), f"login_page does not mount {var}"


class TestThePageAndTheWhitelistCannotDrift:
    """The arms in ``login_page`` carry literal i18n keys, not generated ones.

    That is deliberate: the project's own i18n scanner greps ``datanika/ui/`` for
    ``_t["..."]`` literals, and generating the arms from ``AUTH_ERROR_KEYS`` made every
    key in this feature look like an orphan in ``en.json`` — dead copy a later cleanup
    would delete. The cost of writing them out is a second list, so this class is the
    thing that links the two.
    """

    def test_every_whitelisted_key_appears_as_a_literal_on_the_page(self):
        page_src = (SRC / "ui" / "pages" / "login.py").read_text(encoding="utf-8")
        missing = sorted({k for k in AUTH_ERROR_KEYS.values() if f'_t["{k}"]' not in page_src})
        assert not missing, f"login.py has no arm rendering: {missing}"

    def test_every_slug_has_an_arm_on_the_page(self):
        page_src = (SRC / "ui" / "pages" / "login.py").read_text(encoding="utf-8")
        missing = sorted(s for s in AUTH_ERROR_KEYS if f'("{s}", _t[' not in page_src)
        assert not missing, f"login.py has no rx.match arm for: {missing}"

    def test_the_page_renders_no_auth_error_key_the_whitelist_does_not_know(self):
        """The other direction — an arm for a key nothing maps to is unreachable copy."""
        page_src = (SRC / "ui" / "pages" / "login.py").read_text(encoding="utf-8")
        rendered = set(re.findall(r"\(\"[a-z_]+\", _t\[\"(auth\.error\.[a-z_.]+)\"\]\)", page_src))
        assert rendered, "no rx.match arms found — the regex has stopped matching"
        orphans = sorted(rendered - set(AUTH_ERROR_KEYS.values()))
        assert not orphans, f"arms for keys no slug maps to: {orphans}"


class TestTheCalloutIconsAreReal:
    def test_building_the_page_substitutes_no_icon(self):
        """An invalid icon name does not raise — Reflex swaps in ``circle_help``.

        Measured, because it changes what the other tests in this file prove: constructing
        ``login_page()`` successfully says **nothing** about whether its icons exist. A
        typo'd ``mail_x`` would render a help bubble on the invite-failure callout and
        every test here would still pass, with only a line on stderr that nothing reads.

        This guard is on the rendering rather than on a list of names, so it covers icons
        added later too. The warning is emitted on every construction (checked — it is not
        cached), so a passing run means the page really was built clean.
        """
        import contextlib
        import io as _io

        from datanika.ui.pages.login import login_page

        buf = _io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            login_page()

        assert "Invalid icon tag" not in buf.getvalue(), (
            f"login_page silently substituted an icon: {buf.getvalue()}"
        )


class TestTheCopyExistsInEveryLocale:
    def test_every_whitelisted_slug_has_a_translated_message(self):
        locales = sorted(p.stem for p in I18N.glob("*.json"))
        assert len(locales) == 9, f"expected 9 locales, found {locales}"

        needed = sorted(set(AUTH_ERROR_KEYS.values()))
        for locale in locales:
            data = json.loads((I18N / f"{locale}.json").read_text(encoding="utf-8"))
            missing = [k for k in needed if not data.get(k, "").strip()]
            assert not missing, f"{locale}.json missing/blank: {missing}"

    def test_the_email_and_invite_copy_is_translated(self):
        locales = sorted(p.stem for p in I18N.glob("*.json"))
        needed = [
            "auth.email_verified",
            "auth.email_verify_error",
            "auth.invite_accepted",
            "auth.invite_error",
            "auth.invite_error_help",
        ]
        for locale in locales:
            data = json.loads((I18N / f"{locale}.json").read_text(encoding="utf-8"))
            missing = [k for k in needed if not data.get(k, "").strip()]
            assert not missing, f"{locale}.json missing/blank: {missing}"

    def test_the_invite_error_says_what_to_do_next(self):
        """#659 acceptance criterion 2 — stating that something failed is not enough.

        An invitee whose link died has no account and no reason not to click the same dead
        link again. The remedy is a separate key so it cannot be dropped by a copy edit to
        the headline.
        """
        en = json.loads((I18N / "en.json").read_text(encoding="utf-8"))
        assert "auth.invite_error_help" in en
        assert en["auth.invite_error_help"].strip()


class _StubRouter:
    def __init__(self, params):
        self.page = type("P", (), {"params": params})()


class _StubState:
    def __init__(self, params):
        self.router = _StubRouter(params)
