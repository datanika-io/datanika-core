"""core#1081 AC2 / SPEC_PAGE_ENTRY.md §3 — /login is the fastest signup path and said it was not.

`/login`'s two largest controls after `Sign In` start `/api/auth/login/<provider>`, which lands in
`UserService.find_or_create_oauth_user` — whose own docstring is *"Find existing user by OAuth
identity or email, **else create**."* It returns `is_new` because **creating an account is a normal,
expected outcome of that path.** Nothing on the page said so.

Meanwhile the only control that *named* signing up was a **50 x 20 px** link inside a grey
paragraph, beneath two **141 x 40 px** buttons — and the page actually called **Sign Up** cannot
do the one-click thing at all.

> **The inversion is the defect.**

⚠️ **This is not a colour change, and a test that allowed one would be worse than no test.**
SPEC_PAGE_ENTRY §0c retracts my own filing: the link already rendered Radix accent blue
(`rgba(0, 109, 203, 0.95)`); the `color="gray"` belonged to the enclosing `rx.text`, and I
attributed the wrapper's colour to the link inside it. **A darker link measures as done and fixes
nothing.** So every assertion below is about *what kind of control it is*, never how it is painted.

## Two guards that point in opposite directions

`TestTheSignUpAffordanceIsARealControl` says the affordance must be a button.
`TestThisDidNotQuietlyDeliverCore624` says `/signup` must still have **no** social controls — that
is [core#624]'s job, with its own spec and the template/invite context propagation this change does
not touch. A PR that satisfies one by breaking the other has not done either.
"""

import json
from pathlib import Path

from datanika.ui.pages.login import _social_login_button, login_page
from datanika.ui.pages.signup import signup_page

_I18N = Path(__file__).resolve().parents[2] / "datanika" / "i18n"
_LOCALES = ("en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr")


def _walk(component, ancestors=()):
    """Every node, paired with the component types enclosing it."""
    yield component, ancestors
    for child in getattr(component, "children", []) or []:
        yield from _walk(child, (*ancestors, type(component).__name__))


def _nodes_rendering(component, key: str):
    """(node, ancestor type names) for every leaf whose text is this i18n key."""
    out = []
    for node, ancestors in _walk(component):
        if type(node).__name__ == "Bare" and key in str(getattr(node, "contents", "")):
            out.append((node, ancestors))
    return out


def _style(component) -> dict:
    """Style props keyed as Reflex emits them — camelCase, not snake_case."""
    return {k: str(v).strip('"') for k, v in dict(getattr(component, "style", {}) or {}).items()}


def _props(component) -> list[str]:
    return component.render().get("props", []) or []


class TestTheInstrumentSeesWhatItIsAsked:
    """Controls for every assertion below.

    A walk that stopped short, or a key that no longer appears in the tree,
    would make "the affordance is a button" pass by finding nothing at all.
    """

    def test_the_page_tree_is_reachable(self):
        assert sum(1 for _ in _walk(login_page())) > 50

    def test_the_social_buttons_are_findable_and_are_buttons(self):
        """Positive control for the comparison the whole section rests on."""
        for name, slug in (("Google", "google"), ("GitHub", "github")):
            button = _social_login_button(name, slug)
            assert type(button).__name__ == "Button"
            assert 'size:"3"' in _props(button)

    def test_the_sign_up_key_still_appears_somewhere_on_the_page(self):
        assert _nodes_rendering(login_page(), "auth.sign_up"), (
            "Nothing on /login renders auth.sign_up at all — every assertion in "
            "TestTheSignUpAffordanceIsARealControl would pass vacuously."
        )


class TestTheSignUpAffordanceIsARealControl:
    """AC2a — of the same ORDER as the social buttons, not body text."""

    def test_it_is_wrapped_in_a_button(self):
        found = _nodes_rendering(login_page(), "auth.sign_up")
        assert any("Button" in ancestors for _, ancestors in found), (
            "auth.sign_up renders outside any Button. It was a 50x20 px link "
            "under two 141x40 px buttons that create an account without saying "
            "so — the affordance has to be a control, not a sentence (AC2a)."
        )

    def test_it_is_not_a_link_inside_a_paragraph(self):
        """The retired shape, named so a revert cannot pass as a refactor."""
        for _, ancestors in _nodes_rendering(login_page(), "auth.sign_up"):
            if "Button" in ancestors:
                continue
            assert "Text" not in ancestors, (
                "auth.sign_up is back inside an rx.text paragraph. That is the "
                "exact shape §3 retired, and §0c warns it reads as styled rather "
                "than broken."
            )

    def test_it_is_the_same_size_step_as_the_social_buttons(self):
        buttons = [
            node
            for node, _ in _walk(login_page())
            if type(node).__name__ == "Button"
            and any("auth.sign_up" in str(getattr(c, "contents", "")) for c, _ in _walk(node))
        ]
        assert buttons, "no Button carries auth.sign_up"
        for button in buttons:
            assert 'size:"3"' in _props(button), (
                "The sign-up control is a smaller size step than the social "
                "buttons and Sign In, so it is styled as a control without being "
                "of the same order (AC2a's bar is comparative)."
            )
            assert _style(button).get("width") == "100%", (
                "A social button is half a row wide. AC2a's floor is half a "
                "social button's area; full width clears it with room, and a "
                "narrower control has to argue for itself."
            )


class TestTheSocialBlockSaysWhatItDoes:
    """AC2b — "or continue with" is true for a returning user and silent for a new one."""

    def test_the_disclosure_is_rendered(self):
        assert _nodes_rendering(login_page(), "auth.social_creates_account"), (
            "Nothing on /login says that the social buttons create an account. "
            "They land in find_or_create_oauth_user, whose docstring is 'else "
            "create', and it returns is_new because that is a normal outcome."
        )

    def test_the_disclosure_sits_with_the_social_block_not_the_form(self):
        """A disclosure that floats away from its controls stops being one."""
        order = [
            key
            for node, _ in _walk(login_page())
            if type(node).__name__ == "Bare"
            for key in ("auth.or_continue_with", "auth.social_creates_account", "auth.no_account")
            if key in str(getattr(node, "contents", ""))
        ]
        assert order == [
            "auth.or_continue_with",
            "auth.social_creates_account",
            "auth.no_account",
        ], f"the social disclosure is out of place: {order}"

    def test_the_key_is_translated_and_non_empty_in_all_nine_locales(self):
        """Key parity is enforced elsewhere; a BLANK value passes parity.

        A present-but-empty string counts as translated and renders as nothing,
        which is the one failure this page cannot afford: the disclosure would
        be absent while every check stayed green.
        """
        for locale in _LOCALES:
            table = json.loads((_I18N / f"{locale}.json").read_text(encoding="utf-8"))
            value = table.get("auth.social_creates_account")
            assert value is not None, f"{locale}.json is missing the key"
            assert value.strip(), f"{locale}.json has the key with an empty value"

    def test_the_english_copy_names_no_provider_and_counts_nothing(self):
        """Provider- and count-agnostic on purpose.

        "Google and GitHub", or "both options", becomes false in nine locales on
        the day core#624 adds a third provider — and it becomes false silently,
        because the sentence still renders.
        """
        en = json.loads((_I18N / "en.json").read_text(encoding="utf-8"))
        copy = en["auth.social_creates_account"].lower()
        for banned in ("google", "github", "both", "either", "two"):
            assert banned not in copy, f"the disclosure hardcodes {banned!r}: {copy!r}"


class TestThisDidNotQuietlyDeliverCore624:
    """AC2c — putting social buttons ON /signup is [core#624], not this.

    §3 names both failure directions: AC2b must not be written up as having
    delivered #624, and #624 must not be reported as blocked on this spec.
    """

    def test_signup_still_offers_no_social_controls(self):
        rendered = str(signup_page().render()).lower()
        for provider in ("google", "github"):
            assert provider not in rendered, (
                f"/signup now renders a {provider} control. That is core#624's "
                "job and it carries template/invite context propagation this "
                "change does not touch (AC2c)."
            )

    def test_login_still_offers_both(self):
        """Positive control: the finder above can see a provider when one exists."""
        rendered = str(login_page().render()).lower()
        assert "google" in rendered and "github" in rendered
