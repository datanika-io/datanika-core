"""Links must not strand the user in a second tab (#418, sibling of #417).

``is_external=True`` reads like "this URL is off-site". Reflex means it
literally: ``rx.link`` compiles it to ``target="_blank"`` and ``rx.redirect``
to ``window.open(path, "_blank")``. That is right for a link *out* of the
product and wrong for anything the user is meant to come back from.

It was wrong twice. On ``/oauth/consent`` the callback opened in a new tab and
left the original on a disabled "Approving…" button (#417). On ``/login`` the
provider opened in a new tab, so the user authenticated *there* and ended up
signed in in that tab while the original still showed the sign-in form.

The login case could not simply drop the flag. ``/api/auth/login/<provider>``
is a backend route, and both ``rx.link`` and ``rx.el.a`` render a react-router
``Link``, which treats a same-origin absolute URL as an in-app route. In
production the backend and frontend share ``app.datanika.io``, so dropping the
flag would have handed the click to the router and broken sign-in — *and it
would still have worked in dev*, where the origins differ. Hence the explicit
``window.location.assign``.

**core#624** moved the two buttons into ``datanika/ui/components/social_auth.py`` and put them on
``/signup`` as well. So every probe below runs against both pages, and the AST scan that used to
watch ``login.py`` alone also watches the component — otherwise it would keep passing against a
module the buttons no longer live in (ENGINEERING_RULES §54). ``signup.py`` itself is deliberately
NOT given a module-wide ban: its Terms and Privacy links must open in a new tab.
"""

import ast
import inspect

import pytest

import datanika.ui.components.cost_estimator_card as cost_estimator_card
import datanika.ui.components.elt_nudge_card as elt_nudge_card
import datanika.ui.components.social_auth as social_auth_module
import datanika.ui.pages.login as login_page_module
from datanika.services.auth_redirects import OAUTH_CONTEXT_KEYS
from datanika.ui.pages.login import login_page
from datanika.ui.pages.signup import signup_page

_PAGES = pytest.mark.parametrize("page", [login_page, signup_page], ids=["login", "signup"])


def _rendered(component) -> str:
    return str(component.render())


def _window_around(html: str, needle: str, radius: int = 400) -> str:
    start = html.find(needle)
    assert start != -1, f"{needle} not on the page"
    return html[max(0, start - radius) : start + radius]


def _is_external_lines(module) -> tuple[int, list[int]]:
    """(number of calls scanned, lines passing ``is_external``)."""
    calls = [n for n in ast.walk(ast.parse(inspect.getsource(module))) if isinstance(n, ast.Call)]
    return len(calls), [n.lineno for n in calls for k in n.keywords if k.arg == "is_external"]


class TestSocialLoginStaysInTheTab:
    """Narrowed 2026-08-30 (#656), deliberately, and the reason matters.

    This used to be ``"_blank" not in _rendered(login_page())`` — a page-wide
    ban. That was a fine proxy while the login page had no legitimate off-site
    link, and it stopped being one the moment Terms and Privacy were added: they
    *must* open in a new tab, or reading them loses the form. A page-wide ban
    would then have had to be deleted outright to unblock that, taking the real
    guard with it.

    So it is now scoped to what #418 was actually about — the two provider
    buttons — which makes it strictly more precise: a `_blank` on a social
    button still fails, and it would have failed before only by accident of
    being the only link on the page.
    """

    @_PAGES
    @pytest.mark.parametrize("provider", ["google", "github"])
    def test_a_social_button_never_opens_a_new_tab(self, page, provider):
        window = _window_around(_rendered(page()), f"/api/auth/login/{provider}")
        assert "_blank" not in window, (
            f"the {provider} button opens in a new tab, which leaves the user signed "
            "in there while the original tab still shows the form (#418)."
        )

    @_PAGES
    def test_the_window_probe_is_not_vacuous(self, page):
        """A radius that captured nothing would make the test above always pass."""
        window = _window_around(_rendered(page()), "/api/auth/login/google")
        assert "window.location.assign" in window

    @_PAGES
    def test_both_providers_do_a_real_browser_navigation(self, page):
        """Not a router push — the endpoint is served by the backend."""
        html = _rendered(page())

        for provider in ("google", "github"):
            assert f"/api/auth/login/{provider}" in html, provider
        assert html.count("window.location.assign") == 2

    @_PAGES
    def test_the_providers_are_not_react_router_links(self, page):
        """A router Link would swallow the click in production.

        Same-origin absolute URLs are treated as in-app routes, and prod serves
        the API and the frontend from one host. Dev would not have shown it.
        """
        html = _rendered(page())
        start = html.find("/api/auth/login/google")
        assert start != -1
        # The surrounding markup must not be a router link carrying `to=`.
        assert "ReactRouterLink" not in html[max(0, start - 600) : start + 600]


class TestTheButtonCarriesThePagesContext:
    """core#624 — the navigation forwards the page's own ``?template=``, ``?invite_token=`` and
    ``?next=`` to the backend, which re-validates each one (``test_oauth_signup_context.py``).
    Only a browser can run the script, so this pins that the forwarding is present at all."""

    @_PAGES
    def test_the_navigation_reads_the_pages_query_string(self, page):
        window = _window_around(_rendered(page()), "/api/auth/login/google", radius=700)
        assert "window.location.search" in window
        for key in OAUTH_CONTEXT_KEYS:
            assert key in window, f"the button does not forward ?{key}="


class TestOffSiteLinksAreAbsolute:
    """A root-relative href with ``is_external`` opens the *app* origin.

    ``/pricing/`` is a landing-site page; opening ``app.datanika.io/pricing/``
    lands on a route the Reflex app does not serve. Both call sites are behind
    ``datanika_dual_mode_ux_enabled`` (off), so this was dormant rather than
    broken in production — worth pinning before that flag flips.
    """

    def test_pricing_links_point_at_the_landing_site(self):
        for module in (cost_estimator_card, elt_nudge_card):
            source = inspect.getsource(module)
            assert 'href="/pricing/"' not in source, (
                f"{module.__name__} links /pricing/ root-relative; with "
                "is_external it opens app.datanika.io/pricing/ (#418)."
            )
            assert "https://datanika.io/pricing/" in source, module.__name__


class TestNoStrayExternalRedirects:
    """No call that builds a social button may ask for a new tab.

    AST rather than a substring scan (the repo's convention — see
    ``test_rbac_ui_visibility.py``) so the prose explaining *why* the flag is
    wrong here doesn't trip its own guard. State-level redirects are covered by
    ``test_mcp_consent_state.py::TestLeavesInTheSameTab``.
    """

    def test_login_module_never_asks_for_a_new_tab(self):
        scanned, offenders = _is_external_lines(login_page_module)

        assert scanned, "premise: the scan found no calls in login.py at all"
        assert not offenders, (
            f"login.py passes is_external at line(s) {offenders}. Social login "
            "must stay in the current tab — see social_login_button's docstring."
        )

    def test_the_social_button_component_never_asks_for_a_new_tab(self):
        scanned, offenders = _is_external_lines(social_auth_module)

        assert scanned, "premise: the scan found no calls in the component at all"
        assert not offenders, (
            f"social_auth.py passes is_external at line(s) {offenders}. The provider "
            "buttons must navigate in the current tab (#418)."
        )
