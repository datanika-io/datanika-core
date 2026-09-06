"""core#1090 / SPEC_PAGE_ENTRY.md §4 — the hydrating branch must not be a bare spinner.

Entering Reflex's backend event path costs **1933–9499 ms** on production, measured
across five signed-out samples. There is no "the 3.7 seconds": the range is the
finding, and the *slowest* sample was ``/pipelines/templates`` — the control page
with a single no-op ``on_load`` handler. Reflex's fast path is keyed on whether any
handler exists (``reflex/state.py``: ``if not load_events``), never on what one
costs, so a page with one handler and a page with four take the same branch.

``page_layout`` renders its ``is_authenticated`` false arm for that whole window, and
that arm was ``rx.center(rx.spinner(size="3"), height="100vh")``.

## Why this is a defect, in our own words

``signed_out_panel()``'s docstring, three lines away in the same file:

    "...previously a bare spinner, and a spinner forever is indistinguishable from a
    hang. This says what happened and offers the way back."

core#673 accepted that argument for the *session-ended* branch and left the
*hydrating* branch beside it unchanged. This file is the other half.

## What this test file does NOT assert

🚨 **It removes 0 ms.** Nothing here makes any page faster, and no PR body, changelog
or post may say it did. core#1090 stays open for R4 — *why entering the event path
varies 6.2x between two samples of the same route* — which is unclaimed.

## The instrument, and its control

Every assertion below walks the real component tree returned by ``page_layout()``.
``rx.cond`` keeps both branches as children, so the walk reaches the hydrating arm —
but that is a property of Reflex's object model, not an obvious one, and a walk that
silently failed to descend would make every "the spinner is gone" assertion pass for
the wrong reason. ``TestTheWalkerCanSeeWhatItIsAsked`` pins it against the real
artifact rather than a synthetic one.
"""

import ast
from pathlib import Path

import reflex as rx

from datanika.ui.components.layout import (
    app_shell_skeleton,
    page_layout,
    sidebar,
    signed_out_panel,
)

_PROTECTED_HREFS = {
    "/",
    "/connections",
    "/uploads",
    "/transformations",
    "/pipelines",
    "/models",
    "/dag",
    "/schedules",
    "/runs",
    "/audit-log",
    "/settings",
}


def _walk(component):
    """Every node in a Reflex component tree, both arms of every ``rx.cond``."""
    yield component
    for child in getattr(component, "children", []) or []:
        yield from _walk(child)


def _tags(component) -> list[str]:
    return [t for t in (getattr(n, "tag", None) for n in _walk(component)) if t]


def _texts(component) -> list[str]:
    """Every literal or Var-rendered string in the tree, as source text."""
    out = []
    for node in _walk(component):
        if type(node).__name__ == "Bare":
            out.append(str(getattr(node, "contents", "")))
    return out


def _style(component) -> dict:
    """Style props, keyed as Reflex emits them — ``marginLeft``, not ``margin_left``.

    Reflex camel-cases every style key on the way out. Reading the snake_case
    name returns ``None`` for *every* component, which makes an assertion about
    layout geometry pass or fail for a reason unrelated to layout.
    """
    return {k: str(v).strip('"') for k, v in dict(getattr(component, "style", {}) or {}).items()}


def _hrefs(component) -> list[str]:
    """Destinations, read off the RENDERED props.

    ``rx.link(href=...)`` renders ``asChild`` and delegates to a react-router
    ``Link`` whose prop is ``to``. The Python object's ``.href`` is ``None`` on
    every link in the tree, so an attribute read finds nothing and reports "this
    component offers no navigation" about the sidebar itself.
    """
    out = []
    for node in _walk(component):
        for prop in node.render().get("props", []) or []:
            if prop.startswith(("to:", "href:")):
                out.append(prop.split(":", 1)[1].strip().strip('"'))
    return out


class TestTheWalkerCanSeeWhatItIsAsked:
    """The control for every assertion in this file.

    Pinned against the **real** layout, not a synthetic fixture: a walker that
    stopped at the first ``rx.cond`` would report "no spinner" and "no nav links"
    and every test below would pass while measuring nothing.
    """

    def test_it_descends_into_both_arms_of_a_cond(self):
        """``page_layout``'s two arms are only reachable through ``rx.cond``."""
        tags = _tags(page_layout(title="Anything"))
        assert len(tags) > 100, f"walk found {len(tags)} tagged nodes — it is not descending"

    def test_it_finds_the_authenticated_arm(self):
        """The sidebar lives in the *true* arm and carries real navigation."""
        hrefs = _hrefs(page_layout(title="Anything"))
        assert "/connections" in hrefs, (
            "The walk cannot see the authenticated arm's navigation, so its "
            "verdict about the *other* arm's navigation is worthless."
        )

    def test_it_finds_links_when_links_are_present(self):
        """Positive control for `test_the_skeleton_offers_no_navigation`."""
        assert len(_hrefs(sidebar())) >= 10


class TestTheHydratingBranchIsNotABareSpinner:
    """AC4.1 — replace the bare spinner with an app-shell skeleton."""

    def test_no_spinner_survives_in_the_page_layout_tree(self):
        spinners = [t for t in _tags(page_layout(title="X")) if "Spinner" in t]
        assert not spinners, (
            "A bare centred spinner is held for up to 9.5 s on all 14 protected "
            "routes. A spinner forever is indistinguishable from a hang "
            "(signed_out_panel's own docstring, core#673)."
        )

    def test_the_skeleton_is_actually_wired_into_the_layout(self):
        """A component nobody renders is not a loading state.

        Asserts by *marker*, not by counting nodes: the skeleton's root carries
        ``aria-busy``, and finding it inside ``page_layout()`` is the only proof
        that the function is reached rather than merely defined.
        """
        assert _aria_busy_nodes(page_layout(title="X")), (
            "app_shell_skeleton() is defined but does not appear in page_layout's "
            "tree — the hydrating branch still renders something else."
        )


class TestTheSkeletonIsContentful:
    """AC4.2 — it must be contentful, and that is not a stylistic preference.

    ``first-contentful-paint`` is what every future measurement of this page will
    use, and FCP is structurally blind to a Radix spinner: CSS-animated ``<span>``
    elements, no text node, no image, no SVG. core#1090 was filed as *"a blank
    screen"* on exactly that reading — the same filing recorded ``first-paint`` at
    388 ms and annotated it *"background only"*, which is what a spinner painting
    at 388 ms looks like. A contentful loading state makes FCP mean *"the user saw
    something"* again.
    """

    def test_it_renders_at_least_one_real_text_node(self):
        texts = [t for t in _texts(app_shell_skeleton()) if t.strip()]
        assert texts, (
            "The skeleton renders no text at all, so first-contentful-paint stays "
            "blind to it and the next person to measure this page will make the "
            "same mistake core#1090 made (SPEC_PAGE_ENTRY §0a)."
        )

    def test_the_text_node_is_the_product_name_and_costs_no_new_locale_key(self):
        """AC4.3 — ``app.name`` already exists in all nine locales."""
        assert any("app.name" in t for t in _texts(app_shell_skeleton())), (
            "AC4.2 is satisfied by the product name, which is already a key. Any "
            "*other* visible string costs all nine locales (AC4.3)."
        )


class TestTheSkeletonIsApplicationChromeAndNothingElse:
    """AC4.1 — chrome only, content-neutral, and that is a product decision."""

    def test_it_reserves_the_sidebar_rail_at_the_real_width(self):
        """A shell that reflows when content arrives is a second layout shift."""
        widths = {_style(n).get("width") for n in _walk(app_shell_skeleton())}
        assert "240px" in widths, (
            "The authenticated layout puts a fixed 240px rail on the left and "
            "offsets the content by the same amount. A skeleton that does not "
            "reserve it makes hydration look like a page jump."
        )

    def test_it_offsets_the_content_area_by_the_rail(self):
        margins = {_style(n).get("marginLeft") for n in _walk(app_shell_skeleton())}
        assert "240px" in margins

    def test_the_skeleton_offers_no_navigation(self):
        """The destination is not yet known — that is the entire problem.

        An authenticated visitor is about to see this chrome filled in; a
        signed-out one is about to be moved to ``/login``. Real nav links are
        honest in one branch and a lie in the other, and they are clickable
        during the window.
        """
        offered = set(_hrefs(app_shell_skeleton())) & _PROTECTED_HREFS
        assert not offered, (
            f"The loading state links to {sorted(offered)}. It is rendered for "
            "visitors who are not signed in and may never be."
        )

    def test_the_skeleton_carries_no_content_beyond_the_product_name(self):
        """No stat cards, no counts, no chart shapes — AC4.1, deliberately.

        A dashboard-shaped skeleton with placeholder figures is honest for an
        authenticated user and a fabrication for everyone else.

        ⚠️ The obvious form of this test — *"no rendered string contains a
        digit"* — **cannot work here and was tried first.** Reflex serialises a
        translation lookup as its full state path, which contains the substring
        ``i18n``: the assertion goes red on the one text node the spec
        *requires*, for a reason that has nothing to do with fabricated content.
        Counting content-bearing node types is the sound version.
        """
        content_nodes = [
            type(n).__name__
            for n in _walk(app_shell_skeleton())
            if type(n).__name__ in ("Text", "Heading", "Badge", "Table", "Card")
        ]
        assert content_nodes == ["Heading"], (
            f"The loading state renders {content_nodes}. Exactly one content node "
            "is allowed — the product name, which AC4.2 requires and AC4.3 gets "
            "for free. Anything else states something about a destination that "
            "has not been resolved yet."
        )

    def test_it_announces_itself_as_busy(self):
        """Assistive technology gets no signal from a shimmer.

        ``aria-busy`` is the one accessibility affordance here that costs no
        locale key, so AC4.3 does not trade against it.
        """
        assert _aria_busy_nodes(app_shell_skeleton())


class TestTheSessionEndedBranchIsUntouched:
    """core#673's branch is correct and is not this issue's.

    Its own docstring is the argument §4 is built on; replacing it with a
    skeleton would delete the sentence that says what happened.
    """

    def test_signed_out_panel_still_reaches_the_layout(self):
        texts = _texts(page_layout(title="X"))
        assert any("auth.signed_out_title" in t for t in texts), (
            "The session-expired branch stopped rendering signed_out_panel(). "
            "core#673 is not this issue and must not be collateral."
        )

    def test_signed_out_panel_still_offers_the_way_back(self):
        assert "/login?expired=1" in _hrefs(signed_out_panel())


class TestCredentialPagesDoNotAcquireThisWindow:
    """AC4.4 — ``/login`` and ``/signup`` must not regress.

    🚨 **The spec's own shipping order was inverted in practice.** §8 said land §4
    (this file) *before* §2's credential-page guard, "or the one page with a 57 ms
    gap acquires the window with no loading state to cover it". §2 shipped first:
    ``/login`` and ``/signup`` now carry ``on_load=[AuthState.redirect_if_signed_in]``,
    so both are off Reflex's zero-handler fast path today.

    What keeps them painting in ~400 ms is that neither goes through
    ``page_layout`` — their card is unconditional markup in the client bundle, not
    the false arm of a server-resolved ``rx.cond``. That is a property nobody has
    written down, and wrapping either page in ``page_layout`` would hand a
    signed-out visitor a loading shell **on the sign-in form itself**.
    """

    _PAGES = Path(__file__).resolve().parents[2] / "datanika" / "ui" / "pages"

    def _names_used(self, module: str) -> set[str]:
        tree = ast.parse((self._PAGES / module).read_text(encoding="utf-8"))
        return {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)} | {
            alias.asname or alias.name
            for n in ast.walk(tree)
            if isinstance(n, ast.ImportFrom)
            for alias in n.names
        }

    def test_login_does_not_render_through_page_layout(self):
        assert "page_layout" not in self._names_used("login.py"), (
            "/login paints its own card from the bundle in ~404 ms. Routing it "
            "through page_layout gates that card on a server-resolved auth var "
            "and puts the 1.9-9.5 s window on the sign-in form (AC4.4)."
        )

    def test_signup_does_not_render_through_page_layout(self):
        assert "page_layout" not in self._names_used("signup.py")

    def test_a_protected_page_does_render_through_page_layout(self):
        """Positive control: `page_layout` is findable by this instrument."""
        assert "page_layout" in self._names_used("dashboard.py")


def _aria_busy_nodes(component) -> list:
    out = []
    for node in _walk(component):
        attrs = getattr(node, "custom_attrs", None) or {}
        if str(attrs.get("aria-busy", "")).strip('"').lower() == "true":
            out.append(node)
    return out


def test_reflex_cond_keeps_both_branches_as_children():
    """Pins the object-model assumption every walk in this file rests on.

    If a Reflex upgrade makes ``rx.cond`` render one arm lazily, the walker goes
    quiet rather than wrong — and a quiet walker turns every assertion above into
    a check that cannot fail.
    """
    both = rx.cond(True, rx.text("yes-arm"), rx.text("no-arm"))
    rendered = " ".join(_texts(both))
    assert "yes-arm" in rendered and "no-arm" in rendered
