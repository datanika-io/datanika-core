"""The two social buttons must fit inside the auth card (#605) — on /login AND /signup.

On production the **GitHub** button rendered outside the card's right border:
Google ended at x=666 inside a card ending at x=699, and GitHub ran from 678 to
**972**. Measured live, the row was ``display:flex`` at ``294.22px`` with
``scrollWidth: 600``.

The cause is not "two 100% widths" on its own — it is that Radix buttons compute
to ``flex: 0 0 auto``, so ``flex-basis`` resolves to the declared ``width: 100%``
(294.22px each) and ``flex-shrink: 0`` forbids the row from ever reducing them.
``294.22 + 12 gap + 294.22 = 600.44`` in a 294.22px row.

That distinction matters for the fix: adding ``min-width: 0`` alone would change
nothing, because nothing is permitted to shrink in the first place. The children
need a real flex sizing (``flex: 1 1 0``), and only then does ``min-width: 0``
do its usual job of letting a flex item go narrower than its content.

``width="100%"`` remains correct on every *other* control in this card — the
email input, the password input, the Sign In button — because each of those is
the sole child of a vstack. It is wrong only for siblings sharing a flex row.

**core#624** put the same two buttons on ``/signup``. This file used to end with a guard asking
whoever did that to "give them the same flex sizing as /login and extend the assertions above to
cover both pages" — which is what ``TestBothPagesRenderTheOneButton`` now does, by requiring both
pages to render the single shared helper rather than a copy of it.
"""

import ast
import importlib
import inspect

import pytest

import datanika.ui.components.social_auth as social_auth_module
from datanika.ui.components.social_auth import social_login_button
from datanika.ui.pages.login import login_page
from datanika.ui.pages.signup import signup_page

_PAGES = pytest.mark.parametrize("page", [login_page, signup_page], ids=["login", "signup"])


def _style(component) -> dict:
    """Style props as plain strings, so assertions read like CSS."""
    return {k: str(v).strip('"') for k, v in dict(getattr(component, "style", {}) or {}).items()}


def _social_buttons():
    return [social_login_button("Google", "google"), social_login_button("GitHub", "github")]


def _code_strings(module) -> list[str]:
    """String literals that are CODE — docstrings and bare string statements excluded.

    A docstring that mentions a URL is prose about it, not a place that builds it (§59), and the
    shared component's own docstring necessarily names the route it navigates to.
    """
    tree = ast.parse(inspect.getsource(module))
    prose = {
        id(node.value)
        for node in ast.walk(tree)
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
    }
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in prose
    ]


class TestSocialButtonsFitInsideTheCard:
    def test_neither_button_claims_the_whole_row(self):
        """Two siblings each asking for 100% of the row overflow it."""
        for button in _social_buttons():
            assert _style(button).get("width") != "100%", (
                "Both social buttons sit in one nowrap flex row inside a 360px "
                "card. A child at width:100% takes the full row on its own, so "
                "the second one is pushed outside the card border (#605)."
            )

    def test_each_button_may_shrink_to_share_the_row(self):
        """`flex-shrink: 0` is the operative half of the bug.

        Radix's default is ``flex: 0 0 auto``. Unless the component overrides
        shrink, no amount of ``min-width`` or ``max-width`` lets the row fit.
        """
        for button in _social_buttons():
            style = _style(button)
            shorthand = style.get("flex", "")
            shrink = style.get("flex_shrink", style.get("flexShrink", ""))

            assert shorthand or shrink, (
                "Neither `flex` nor `flex_shrink` is set, so the button keeps "
                "Radix's `flex: 0 0 auto` and cannot shrink to fit (#605)."
            )
            if shorthand:
                parts = shorthand.split()
                assert parts[0] not in ("0", "none"), f"flex-grow must not be 0: {shorthand!r}"
                assert len(parts) < 2 or parts[1] != "0", (
                    f"flex-shrink must not be 0 — that is the bug: {shorthand!r}"
                )
            else:
                assert shrink != "0", f"flex-shrink must not be 0: {shrink!r}"

    def test_each_button_can_go_narrower_than_its_label(self):
        """`min-width: auto` on a flex item floors it at its content width."""
        for button in _social_buttons():
            style = _style(button)
            assert style.get("min_width", style.get("minWidth")) == "0", (
                "A flex item defaults to `min-width: auto`, which refuses to "
                "shrink past its own content. Allowing shrink without this only "
                "moves the floor (#605)."
            )

    @_PAGES
    def test_both_providers_survive_the_layout_fix(self, page):
        """Guard against 'fixing' the overflow by dropping a button."""
        html = str(page().render())
        assert "/api/auth/login/google" in html and "/api/auth/login/github" in html, (
            "Both providers must still be on the page — this is a layout fix, not a removal."
        )


class TestBothPagesRenderTheOneButton:
    """A copy is a second place to regress #605 and #418 (SPEC_SIGNUP_SOCIAL_AUTH §4)."""

    @pytest.mark.parametrize("module_name", ["datanika.ui.pages.login", "datanika.ui.pages.signup"])
    def test_no_page_builds_its_own_provider_url(self, module_name):
        module = importlib.import_module(module_name)
        builders = [s for s in _code_strings(module) if "/api/auth/login" in s]
        assert not builders, (
            f"{module_name} builds a provider URL itself ({builders}). Render "
            "social_login_row() from datanika/ui/components/social_auth.py instead, which "
            "carries the #605 sizing and the #418 navigation."
        )

    def test_the_scan_can_see_the_url_where_it_does_live(self):
        """Floor for the test above: an instrument that finds nothing anywhere proves nothing."""
        assert any("/api/auth/login" in s for s in _code_strings(social_auth_module))

    @_PAGES
    def test_each_page_renders_exactly_one_pair(self, page):
        assert str(page().render()).count("window.location.assign") == 2
