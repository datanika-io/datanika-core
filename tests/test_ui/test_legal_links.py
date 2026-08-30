"""Terms and Privacy have to be reachable from the product (#656).

The application referenced neither, anywhere — not at signup, where the
contract is formed, and not from inside the app afterwards. The pages have been
live on the landing site the whole time.

Two things this file learned the hard way, by being run against a deliberately
broken version before being trusted:

1. **Testing the component is not testing the page.** The first draft called
   ``legal_links()`` directly for the in-app case, so deleting it from the
   sidebar left every assertion green. It renders ``sidebar()`` now.
2. **A windowed ``_blank`` search around a URL is not a per-link assertion.**
   Terms and Privacy sit within a few hundred characters of each other, so
   removing ``is_external`` from *one* of them still found the other's
   ``_blank``. The new-tab property is checked at the source instead, per link.

The links are cross-origin, which is worth checking rather than assuming:
``rx.link`` and ``rx.el.a`` both compile to a react-router ``Link``, and a
*same-origin* absolute URL is swallowed by the router (#418/#430). These are
off-site, so a plain link is correct — but #418 shipped on "it should be fine".
"""

import ast
import inspect

import pytest

import datanika.ui.components.layout as layout_module
import datanika.ui.pages.signup as signup_module
from datanika.ui.components.layout import PRIVACY_URL, TERMS_URL, sidebar
from datanika.ui.pages.login import login_page
from datanika.ui.pages.signup import signup_page

SURFACES = {"signup": signup_page, "login": login_page, "in-app": sidebar}


def _rendered(component) -> str:
    return str(component.render())


def _links_to(module, url_const: str) -> list[ast.Call]:
    """Every ``rx.link(... href=<url_const> ...)`` call in a module's source."""
    tree = ast.parse(inspect.getsource(module))
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for kw in node.keywords
        if kw.arg == "href" and isinstance(kw.value, ast.Name) and kw.value.id == url_const
    ]


@pytest.mark.parametrize("surface", list(SURFACES), ids=list(SURFACES))
class TestBothDocumentsAreOneClickAway:
    def test_terms_is_linked(self, surface):
        assert TERMS_URL in _rendered(SURFACES[surface]())

    def test_privacy_is_linked(self, surface):
        assert PRIVACY_URL in _rendered(SURFACES[surface]())

    def test_the_links_are_absolute_and_off_site(self, surface):
        """A root-relative ``/terms/`` would open app.datanika.io/terms/, a 404."""
        html = _rendered(SURFACES[surface]())
        assert 'href={"/terms/"}' not in html
        assert 'href={"/privacy/"}' not in html


@pytest.mark.parametrize("module", [layout_module, signup_module], ids=["layout", "signup"])
@pytest.mark.parametrize("url_const", ["TERMS_URL", "PRIVACY_URL"])
class TestEachLinkOpensInANewTab:
    """Per link, at the source. A user reading the terms mid-signup must not
    come back to an emptied form."""

    def test_the_link_exists(self, module, url_const):
        assert _links_to(module, url_const), f"{module.__name__} does not link {url_const}"

    def test_it_asks_for_a_new_tab(self, module, url_const):
        for call in _links_to(module, url_const):
            flags = [
                kw.value.value
                for kw in call.keywords
                if kw.arg == "is_external" and isinstance(kw.value, ast.Constant)
            ]
            assert flags == [True], (
                f"{module.__name__} links {url_const} without is_external=True, so it "
                "replaces the current page instead of opening a tab"
            )


class TestTheProbesCanFail:
    """Guard the guards. Each of these would silently pass a broken suite."""

    def test_the_signup_page_renders_something(self):
        assert len(_rendered(signup_page())) > 500

    def test_the_sidebar_renders_something(self):
        assert len(_rendered(sidebar())) > 500

    def test_the_link_finder_is_not_matching_everything(self):
        """It must select *these* links, not every call in the module."""
        assert len(_links_to(layout_module, "TERMS_URL")) == 1
        assert _links_to(layout_module, "NOT_A_REAL_CONSTANT") == []

    def test_the_probe_does_not_match_an_unrelated_url(self):
        assert "https://datanika.io/refund/" not in _rendered(signup_page())
