"""The two social buttons must fit inside the login card (#605).

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
"""

from datanika.ui.pages.login import _social_login_button, login_page


def _style(component) -> dict:
    """Style props as plain strings, so assertions read like CSS."""
    return {k: str(v).strip('"') for k, v in dict(getattr(component, "style", {}) or {}).items()}


def _social_buttons():
    return [_social_login_button("Google", "google"), _social_login_button("GitHub", "github")]


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

    def test_both_providers_survive_the_layout_fix(self):
        """Guard against 'fixing' the overflow by dropping a button."""
        html = str(login_page().render())
        assert "/api/auth/login/google" in html and "/api/auth/login/github" in html, (
            "Both providers must still be on the page — this is a layout fix, not a removal."
        )


class TestOnlyLoginHasThisShape:
    def test_signup_does_not_reuse_the_social_button_helper(self):
        """`/signup` was checked live and has no social buttons at all.

        Pinned so that if someone later adds them there, they are made to look
        at this file rather than re-deriving #605 from a screenshot.
        """
        import datanika.ui.pages.signup as signup_module

        assert not hasattr(signup_module, "_social_login_button"), (
            "If /signup grows social buttons, give them the same flex sizing as "
            "/login and extend the assertions above to cover both pages."
        )
