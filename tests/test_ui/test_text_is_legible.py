"""Text is painted in a readable colour, and a link in a sentence looks like a link (core#1409).

Two of the classes the core#720 accessibility sweep reports as ``serious``:

* ``color-contrast``, on every page, sampled as ``.css-a2wsy3.rt-r-size-1``. ``rx.text(…,
  color="gray")`` is the **CSS** named colour ``gray`` — ``#808080``, about **3.95:1** on white,
  under WCAG AA's 4.5:1 for body text — not Radix's grey scale, and it ignores the dark theme.
  There were 59 such uses. ``var(--gray-11)`` is the Radix step designed for low-contrast text.
  33 more ``rx.text`` calls used grey steps **9 and 10**, which Radix designs for solid fills, not
  for text; they are step 11 now. Icons on those steps are not text and are left as they are.
* ``link-in-text-block`` on ``/signup``: the Terms, Privacy and Sign-in links sat inside grey
  sentences and differed from them by colour alone. ``underline="always"`` makes them links to
  anyone who cannot tell the two colours apart.

⚠️ **This does not clear ``color-contrast``.** The rest of it is every solid button: they are painted
with the default accent's step 9, which does not carry white text at AA. Changing that changes how
the product looks, so it is routed on core#1409 as a design decision rather than made here.

Both checks read the component tree of every page factory, like
``test_controls_have_accessible_names.py``, and each is shown to tell the two shapes apart through
the same function the census uses.
"""

from __future__ import annotations

import importlib
import io
import re
from contextlib import redirect_stdout

import reflex as rx

from tests.test_ui.test_every_page_constructs import FACTORIES

#: CSS named colours in the grey family. None is on a Radix scale, and none adapts to the theme.
_CSS_GREYS = frozenset({"gray", "grey", "darkgray", "darkgrey", "lightgray", "lightgrey"})

#: A Radix grey scale step used as a colour. Steps 9 and 10 are for solid backgrounds and 8 for
#: borders; **11 is the step designed for low-contrast text** and 12 for high-contrast text. Light
#: ``gray-9`` (``#8D8D8D``) is about 3.3:1 on white — computed, not read from axe.
_GREY_STEP = re.compile(r"var\(--(gray|slate|mauve|sage|olive|sand)-(\d+)\)")


def _walk(component, parent=None):
    yield component, parent
    for child in getattr(component, "children", []) or []:
        yield from _walk(child, component)


def _literal(value) -> str | None:
    """The literal string behind a style value or prop, if the **whole** value is one."""
    if value is None:
        return None
    literal = getattr(value, "_var_value", value)
    return literal if isinstance(literal, str) else None


#: A double-quoted token inside a rendered ``Var`` expression. ``rx.cond`` renders as
#: ``(test ? "a" : "b")`` and nests, so every branch appears as one of these.
_RENDERED_STRING = re.compile(r'"([^"\\]*)"')


def _colour_literals(value) -> list[str]:
    """Every colour a colour prop can resolve to — **including each branch of a cond**.

    🚨 This guard's first version reduced a colour with ``_literal`` alone and returned
    ``None`` for anything that was not a plain string — and ``None`` read as *"fine"* at both
    call sites below. That is a skip, and a skip is the same colour as a pass, in a guard
    whose entire subject is which colour text is painted.

    It was not hypothetical. ``getting_started_checklist`` painted a completed item's label
    ``rx.cond(done, "var(--slate-10)", "var(--slate-12)")`` — a step-10 on real ``rx.text``,
    exactly the class this file exists to stop — and the guard passed. Measured on the census
    rather than argued: **7** of the coloured components reachable from the page factories
    carry a composite colour (the checklist's five item labels and ``/dashboard``'s two usage
    percents), and all 7 were invisible.

    A ``Var`` renders its whole expression, so reading the quoted tokens out of it checks
    *every* branch instead of none of them, and a nested cond renders all of its branches too.
    ``test_it_saw_colours_written_as_a_cond`` asserts the census still meets composite
    colours, so this cannot quietly stop being exercised.
    """
    if value is None:
        return []
    literal = _literal(value)
    if literal is not None:
        return [literal]
    return _RENDERED_STRING.findall(str(value))


def _is_css_grey(colour: str) -> bool:
    return colour.strip().lower() in _CSS_GREYS


def _is_background_step(colour: str) -> bool:
    match = _GREY_STEP.fullmatch(colour.strip())
    return bool(match) and int(match.group(2)) < 11


def is_painted_css_grey(component) -> bool:
    colours = _colour_literals((getattr(component, "style", None) or {}).get("color"))
    return any(_is_css_grey(colour) for colour in colours)


def is_text_on_a_background_step(component) -> bool:
    """``rx.text`` painted with a grey step below 11. Icons are not text, and are not checked."""
    if type(component).__name__ != "Text":
        return False
    colours = _colour_literals((getattr(component, "style", None) or {}).get("color"))
    return any(_is_background_step(colour) for colour in colours)


def is_link_in_a_sentence_without_a_cue(component, parent) -> bool:
    if type(component).__name__ != "Link" or type(parent).__name__ != "Text":
        return False
    return _literal(getattr(component, "underline", None)) != "always"


def _census():
    grey, low_step, uncued, links_in_text, coloured, composite = [], [], [], 0, 0, 0
    for module, attr in FACTORIES:
        with redirect_stdout(io.StringIO()):
            tree = getattr(importlib.import_module(f"datanika.ui.pages.{module}"), attr)()
        for component, parent in _walk(tree):
            painted = (getattr(component, "style", None) or {}).get("color")
            if painted is not None:
                coloured += 1
                if _literal(painted) is None:
                    composite += 1
            if is_painted_css_grey(component):
                grey.append(f"{module}.{attr}: {type(component).__name__}")
            if is_text_on_a_background_step(component):
                low_step.append(f"{module}.{attr}")
            if type(component).__name__ == "Link" and type(parent).__name__ == "Text":
                links_in_text += 1
            if is_link_in_a_sentence_without_a_cue(component, parent):
                uncued.append(f"{module}.{attr}")
    return grey, low_step, uncued, links_in_text, coloured, composite


GREY, LOW_STEP, UNCUED, LINKS_IN_TEXT, COLOURED, COMPOSITE = _census()


class TestTheCensusSawTheSubjects:
    def test_it_saw_coloured_text(self):
        assert COLOURED >= 50, COLOURED

    def test_it_saw_links_inside_sentences(self):
        """Five today (signup ×3, oauth consent, forgot password)."""
        assert LINKS_IN_TEXT >= 4, LINKS_IN_TEXT

    def test_it_saw_colours_written_as_a_cond(self):
        """Seven today — the checklist's five item labels and ``/dashboard``'s two percents.

        Every one of these was invisible to this file's first version. If this reaches 0,
        ``_colour_literals``' branch reading is exercised by no real page and the two checks
        below have silently narrowed back to plain literals — passing all the way down.
        """
        assert COMPOSITE >= 2, COMPOSITE


class TestTextIsLegible:
    def test_no_text_is_painted_a_css_grey(self):
        """Use ``var(--gray-11)`` (or ``color_scheme="gray"``), never the CSS named colour."""
        assert not sorted(set(GREY)), sorted(set(GREY))

    def test_no_text_is_painted_a_background_step(self):
        """Grey text uses step 11 (or 12). Steps 8-10 are for borders and solid fills."""
        assert not sorted(set(LOW_STEP)), sorted(set(LOW_STEP))

    def test_a_link_inside_a_sentence_is_underlined(self):
        """``link-in-text-block``: colour alone does not make a link findable."""
        assert not sorted(set(UNCUED)), sorted(set(UNCUED))


class TestTheChecksTellTheShapesApart:
    def test_css_grey_is_caught_and_the_radix_step_is_not(self):
        assert is_painted_css_grey(rx.text("x", color="gray"))
        assert is_painted_css_grey(rx.text("x", color="grey"))
        assert not is_painted_css_grey(rx.text("x", color="var(--gray-11)"))

    def test_a_background_step_on_text_is_caught_and_step_11_and_icons_are_not(self):
        assert is_text_on_a_background_step(rx.text("x", color="var(--gray-9)"))
        assert is_text_on_a_background_step(rx.text("x", color="var(--slate-10)"))
        assert not is_text_on_a_background_step(rx.text("x", color="var(--gray-11)"))
        assert not is_text_on_a_background_step(rx.icon("bell", color="var(--slate-8)"))

    def test_a_background_step_hidden_in_a_cond_is_caught(self):
        """The shape this file shipped blind to, and ``/dashboard``'s legitimate one beside it.

        A ``Var`` is not a ``str``, so reducing the colour with ``_literal`` alone returned
        ``None`` and both checks answered *no offence* — for a real step-10 on real text.
        """
        done = rx.Var.create(True)
        assert is_text_on_a_background_step(
            rx.text("x", color=rx.cond(done, "var(--slate-10)", "var(--slate-12)"))
        )
        assert is_painted_css_grey(rx.text("x", color=rx.cond(done, "gray", "var(--gray-11)")))
        # /dashboard's usage percents: every branch is step 11, and must stay acceptable.
        assert not is_text_on_a_background_step(
            rx.text("x", color=rx.cond(done, "var(--red-11)", "var(--green-11)"))
        )

    def test_the_branch_reader_returns_every_branch(self):
        """Anti-vacuity for the test above, which passes just as well on one branch.

        A reader that returned only the *first* branch would catch the assertions above and
        miss a step-9 hiding in an else, so the count is asserted rather than the verdict.
        """
        done = rx.Var.create(True)
        assert _colour_literals(rx.cond(done, "var(--slate-10)", "var(--slate-12)")) == [
            "var(--slate-10)",
            "var(--slate-12)",
        ]
        assert _colour_literals(
            rx.cond(done, "var(--red-11)", rx.cond(done, "gray", "var(--gray-11)"))
        ) == ["var(--red-11)", "gray", "var(--gray-11)"]
        assert _colour_literals("var(--gray-11)") == ["var(--gray-11)"]
        assert _colour_literals(None) == []

    def test_an_uncued_link_in_a_sentence_is_caught_and_an_underlined_one_is_not(self):
        sentence = rx.text("Read the ", rx.link("terms", href="/t"))
        cued = rx.text("Read the ", rx.link("terms", href="/t", underline="always"))
        assert is_link_in_a_sentence_without_a_cue(sentence.children[1], sentence)
        assert not is_link_in_a_sentence_without_a_cue(cued.children[1], cued)
