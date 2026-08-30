"""Terms and Privacy have to be reachable from the product (#656).

The application referenced neither, anywhere — not at signup, where the
contract is formed, and not from inside the app afterwards. The pages have been
live on the landing site the whole time.

The links are **cross-origin**, which is the part worth checking rather than
assuming: `rx.link` and `rx.el.a` both compile to a react-router `Link`, and a
*same-origin* absolute URL is swallowed by the router (#418/#430). These are
off-site, so a plain link is correct — but the test asserts on the rendered
markup, because "it should be fine" is how #418 shipped.
"""

import pytest

from datanika.ui.components.layout import legal_links
from datanika.ui.pages.login import login_page
from datanika.ui.pages.signup import signup_page

TERMS = "https://datanika.io/terms/"
PRIVACY = "https://datanika.io/privacy/"


def _rendered(component) -> str:
    return str(component.render())


@pytest.mark.parametrize(
    "surface",
    [signup_page, login_page, legal_links],
    ids=["signup", "login", "in-app"],
)
class TestBothDocumentsAreOneClickAway:
    def test_terms_is_linked(self, surface):
        assert TERMS in _rendered(surface())

    def test_privacy_is_linked(self, surface):
        assert PRIVACY in _rendered(surface())

    def test_the_links_are_absolute_and_off_site(self, surface):
        """A root-relative `/terms/` would open app.datanika.io/terms/, a 404."""
        html = _rendered(surface())
        assert 'href={"/terms/"}' not in html
        assert 'href={"/privacy/"}' not in html

    def test_they_open_in_a_new_tab(self, surface):
        """Leaving the signup form to read the terms must not lose the form."""
        html = _rendered(surface())
        start = html.find("datanika.io/terms/")
        assert start != -1
        assert "_blank" in html[max(0, start - 400) : start + 400], (
            "the Terms link does not open in a new tab; a user reading it mid-signup "
            "loses what they had typed"
        )


class TestTheProbeCanFail:
    """Guard the guard: a renderer that returned an empty string would pass everything."""

    def test_the_signup_page_renders_something(self):
        assert len(_rendered(signup_page())) > 500

    def test_the_probe_does_not_match_an_unrelated_url(self):
        assert "https://datanika.io/refund/" not in _rendered(signup_page())
