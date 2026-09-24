"""Every button's label is legible against its own background (core#1409).

This is the guard `docs/specs/SPEC_BUTTON_CONTRAST.md` §9 specifies, and the last of #1409's
six violation classes — the one blocking core#720's graduation.

**What it asserts.** For every ``rx.button``/``rx.icon_button`` in the component tree, resolve
``(variant, color_scheme, high_contrast)`` to the two tokens ``components.css`` actually paints
with, look those tokens up in ``@radix-ui/themes`` **at the version Reflex pins**, and require
the label to clear WCAG AA's 4.5:1 against its own background. That is the invariant
(`WORKFLOW_RULES` §5a), not today's instance: a scale nobody has used yet is graded the first
time somebody uses it, and a Radix version bump that moves a value goes red at PR time rather
than drifting.

🚨 **The spec forbids the shape this guard could so easily have taken.** *"No source line says
``color_scheme="red"`` with a solid variant"* is the absence-of-the-wrong-word assertion
`WORKFLOW_RULES` §4 bans: it is satisfied by deleting the button, and it cannot see a scale added
next month. This grades what gets painted.

### Why it walks the component tree and not the source

Two populations are invisible to the obvious instruments, and both were measured rather than
argued:

* **axe cannot see a dialog.** ``rx.alert_dialog.content`` is not mounted in the DOM until the
  dialog opens, so the #1409 sweep scored one ``color-contrast`` node on ``/settings`` where ten
  exist. The component tree has no such blind spot — it holds the whole dialog — and
  ``test_it_saw_the_buttons_inside_a_dialog`` pins that this guard keeps reaching them. **That
  population is 62 buttons, and it is exactly the confirm/cancel pair a user reads before doing
  something irreversible.**
* **a source-level reducer cannot see a computed colour.** ``color_scheme`` is not always a
  literal: the Run button on ``/pipelines`` and ``/uploads`` takes
  ``run_in_progress_color(...)``, an ``rx.match`` that renders to a JavaScript ``switch``. The
  previous version of this file's sibling — ``test_text_is_legible.py`` — returned ``None`` for
  any non-``str`` colour, and ``None`` read as *no offence* at its call sites, so **7 of 7
  composite colours were invisible while the guard was green**. :func:`_branches` reads every
  arm, and **raises** on an expression shape it cannot read, because a reducer that returns
  nothing for what it does not understand hands that shape to its caller as clean.

### Arms, each of which a weaker guard would have omitted

1. **Anti-vacuity** — a tree walk whose match is wrong finds no buttons and passes trivially.
   The counts are asserted, including the dialog-only and computed-colour sub-populations.
2. **A positive control** — the arithmetic reproduces two published pairs and Radix's own
   alpha-compositing design property before any verdict here is worth reading.
3. **Seen failing** — :class:`TestTheGradeTellsTheShapesApart` drives the grader with the shape
   the spec bans and the shape it mandates, and requires them to come out *differently*. A guard
   that refuses everything is not discriminating.
4. **The accent is read from the app, not from this file.** If ``rx.App`` stopped carrying the
   theme, every grade here would be computed against an accent production does not render — a
   green describing a different product. ``TestTheAppCarriesTheTheme`` closes that.
"""

from __future__ import annotations

import importlib
import io
import itertools
import json
import pathlib
import re
from contextlib import redirect_stdout

import pytest
import reflex as rx
from reflex.components.radix.themes.base import RadixThemesComponent

import datanika.ui.pages
from datanika.ui.theme import ACCENT_COLOR, GRAY_COLOR
from tests.test_ui.test_every_page_constructs import FACTORIES

#: WCAG 2.1 AA for body text. Button labels render at ``size="1"``/``size="2"``, which is not
#: "large text", so 3:1 — the threshold Radix step 9 is designed against — does not apply.
MIN_RATIO = 4.5

#: The version every value in :data:`TOKENS` was read from. Asserted against the pin Reflex
#: actually ships (``RadixThemesComponent.library``) so a bump goes red here instead of leaving
#: this table describing a package nobody installs any more. That red is the signal to re-read
#: SPEC_BUTTON_CONTRAST §7, not to adjust the table.
RADIX_VERSION = "3.2.1"

#: Button-relevant tokens, read from ``tokens/colors/<scale>.css`` in @radix-ui/themes 3.2.1.
#:
#: ⚠️ **Only the first top-level block** (``:root, .light, .light-theme``) — reading past it
#: gives the ``.dark`` value, which is a different colour and looks perfectly plausible.
#: ``<scale>-contrast`` lives in a trailing bare ``:root``.
#: :func:`test_the_vendored_tokens_match_the_installed_package` re-reads all of it wherever the
#: frontend is installed, so this table cannot quietly diverge from the package.
TOKENS: dict[str, dict[str, str]] = {
    "violet": {
        "1": "#fdfcfe",
        "9": "#6e56cf",
        "12": "#2f265f",
        "a3": "#4400ee0f",
        "a11": "#1f0099af",
        "contrast": "white",
    },
    "amber": {
        "1": "#fefdfb",
        "9": "#ffc53d",
        "12": "#4f3422",
        "a3": "#ffde003d",
        "a11": "#ab6400",
        "contrast": "#21201c",
    },
    "yellow": {
        "1": "#fdfdf9",
        "9": "#ffe629",
        "12": "#473b1f",
        "a3": "#ffee0047",
        "a11": "#9e6c00",
        "contrast": "#21201c",
    },
    "red": {
        "1": "#fffcfc",
        "9": "#e5484d",
        "12": "#641723",
        "a3": "#f3000d14",
        "a11": "#c40006d3",
        "contrast": "white",
    },
    "gray": {
        "1": "#fcfcfc",
        "9": "#8d8d8d",
        "12": "#202020",
        "a3": "#0000000f",
        "a11": "#0000009b",
        "contrast": "white",
    },
    "mauve": {
        "1": "#fdfcfd",
        "9": "#8e8c99",
        "12": "#211f26",
        "a3": "#30004010",
        "a11": "#0400119c",
        "contrast": "white",
    },
    "slate": {
        "1": "#fcfcfd",
        "9": "#8b8d98",
        "12": "#1c2024",
        "a3": "#0000330f",
        "a11": "#0007149f",
        "contrast": "white",
    },
    "green": {
        "1": "#fbfefc",
        "9": "#30a46c",
        "12": "#193b2d",
        "a3": "#00a43319",
        "a11": "#00713fde",
        "contrast": "white",
    },
    "blue": {
        "1": "#fbfdff",
        "9": "#0090ff",
        "12": "#113264",
        "a3": "#008ff519",
        "a11": "#006dcbf2",
        "contrast": "white",
    },
    "orange": {
        "1": "#fefcfb",
        "9": "#f76b15",
        "12": "#582d1d",
        "a3": "#ff9c0029",
        "a11": "#cc4e00",
        "contrast": "white",
    },
}

#: What ``ghost`` and ``outline`` buttons sit on. The app declares no ``appearance`` and no dark
#: mode, so the page is Radix's light ``--color-background``. Cards are the same white, which is
#: why one value covers both — if a tinted surface is ever introduced, this becomes a per-button
#: question and the spec's §7 flip conditions apply.
PAGE_BACKGROUND = "#ffffff"

#: Radix's own default for a button that names no variant (``base-button.props.js``:
#: ``variant: {... default: "solid"}``). 25 call sites rely on it, so reading it as "no variant"
#: rather than "solid" would make a quarter of the population invisible.
DEFAULT_VARIANT = "solid"

#: ``components.css``, read rather than remembered → (background token, label token) per
#: ``(variant, high_contrast)``. ``None`` as a background means the page shows through.
VARIANT_PAINT: dict[tuple[str, bool], tuple[str | None, str]] = {
    ("solid", False): ("{scale}-9", "{scale}-contrast"),
    ("solid", True): ("{scale}-12", "{gray}-1"),
    ("classic", False): ("{scale}-9", "{scale}-contrast"),
    ("classic", True): ("{scale}-12", "{gray}-1"),
    ("soft", False): ("{scale}-a3", "{scale}-a11"),
    ("soft", True): ("{scale}-a3", "{scale}-12"),
    ("ghost", False): (None, "{scale}-a11"),
    ("ghost", True): (None, "{scale}-12"),
    ("outline", False): (None, "{scale}-a11"),
    ("outline", True): (None, "{scale}-12"),
}


class UnreadablePropError(AssertionError):
    """A prop this guard cannot resolve.

    Raised, never swallowed. A colour a guard cannot read is a colour it is not grading, and
    the failure direction of a silent skip is a green that covers nothing — the defect that cost
    ``test_text_is_legible.py`` 7 of 7 composite colours.
    """


# --------------------------------------------------------------------------- colour arithmetic


def _rgba(token: str) -> tuple[float, float, float, float]:
    """``white`` / ``#rgb`` / ``#rrggbb`` / ``#rrggbbaa`` → channels in 0-255 plus alpha 0-1."""
    value = token.strip().lower()
    if value == "white":
        return (255.0, 255.0, 255.0, 1.0)
    if value == "black":
        return (0.0, 0.0, 0.0, 1.0)
    if not value.startswith("#"):
        raise UnreadablePropError(f"not a colour this guard can read: {token!r}")
    digits = value[1:]
    if len(digits) == 3:
        digits = "".join(c * 2 for c in digits)
    if len(digits) not in (6, 8):
        raise UnreadablePropError(f"not a colour this guard can read: {token!r}")
    red, green, blue = (int(digits[i : i + 2], 16) for i in (0, 2, 4))
    alpha = int(digits[6:8], 16) / 255 if len(digits) == 8 else 1.0
    return (float(red), float(green), float(blue), alpha)


def _composite(token: str, background: str) -> str:
    """``token`` painted over ``background``, as an opaque ``#rrggbb``.

    Radix's soft fills and low-contrast labels are alpha tokens, so the literal hex is never
    what the eye sees. Checked against Radix's own design property — ``--red-a3`` over white is
    ``--red-3`` — in :func:`test_the_compositing_matches_radix_own_design`.
    """
    fr, fg_, fb, fa = _rgba(token)
    br, bg_, bb, _ = _rgba(background)
    mix = (fr * fa + br * (1 - fa), fg_ * fa + bg_ * (1 - fa), fb * fa + bb * (1 - fa))
    return "#" + "".join(f"{round(c):02x}" for c in mix)


def _luminance(token: str) -> float:
    """WCAG relative luminance of an **opaque** colour."""

    def channel(value: float) -> float:
        srgb = value / 255
        return srgb / 12.92 if srgb <= 0.03928 else ((srgb + 0.055) / 1.055) ** 2.4

    red, green, blue, _ = _rgba(token)
    return 0.2126 * channel(red) + 0.7152 * channel(green) + 0.0722 * channel(blue)


def ratio(foreground: str, background: str) -> float:
    """WCAG contrast ratio. ``foreground`` is composited over ``background`` first."""
    opaque = _composite(foreground, background)
    light, dark = sorted((_luminance(opaque), _luminance(background)), reverse=True)
    return (light + 0.05) / (dark + 0.05)


# ------------------------------------------------------------------------------ prop resolving

#: ``rx.match`` renders to a JavaScript ``switch`` whose arms are ``return ("value")``.
_MATCH_ARM = re.compile(r'return\s*\(\s*"([^"]*)"\s*\)')
#: ``rx.cond`` renders to a ternary. Both halves are branches; the *test* is not.
#:
#: 🔴 **These were ONE pattern matching a flat pair, and a NESTED cond defeated it silently
#: (core#1535).** ``rx.cond(a, "gray", rx.cond(b, "green", "red"))`` renders
#: ``(a ? "gray" : (b ? "green" : "red"))``; the flat pattern matched only the inner pair and
#: returned ``['green', 'red']`` — **dropping ``"gray"`` with no error**. It degraded with depth:
#: a three-deep cond returned only the innermost pair, losing two arms of four.
#:
#: 🔑 **The fail-loud design had a hole exactly where the shape is recognisable but incomplete.**
#: The docstring below is right that returning ``[]`` would read as *"nothing to grade here"* — but
#: a *partial* list is worse, because it reads as a complete grading of a smaller population. The
#: real case is `/connections`' test verdict, whose third state (`gray`, core#821) was invisible.
_TERNARY_CONDITION = re.compile(r'\?\s*[("]')
_TERNARY_ARM = re.compile(r'[?:]\s*"([^"]*)"')


def _branches(value: object) -> list[object]:
    """Every value a prop can resolve to — **each branch of a cond or a match, not the first**.

    Raises :class:`UnreadablePropError` on a shape it does not recognise. That is deliberate and is
    the whole lesson of the composite-colour miss: the alternative is returning ``[]``, which
    every caller below would read as *"nothing to grade here"*.

    ⚠️ **A ternary chain must yield exactly one more arm than it has conditions.** That count is
    what makes a *partial* read loud: an arm that is itself a Var rather than a literal leaves the
    totals disagreeing, and this raises instead of grading a subset and calling it the whole.
    """
    if value is None:
        return [None]
    literal = getattr(value, "_var_value", value)
    if isinstance(literal, (str, bool)):
        return [literal]
    rendered = str(value)
    arms = _MATCH_ARM.findall(rendered)
    if arms:
        return sorted(set(arms))
    conditions = len(_TERNARY_CONDITION.findall(rendered))
    if conditions:
        ternary_arms = _TERNARY_ARM.findall(rendered)
        if len(ternary_arms) != conditions + 1:
            raise UnreadablePropError(
                f"{conditions} ternary condition(s) but {len(ternary_arms)} literal arm(s) — "
                f"at least one branch is not a literal, so grading these would grade a subset "
                f"and call it the whole: {rendered[:200]}"
            )
        return sorted(set(ternary_arms))
    raise UnreadablePropError(f"cannot read the branches of: {rendered[:200]}")


def _scale_of(color_scheme: str | None) -> str:
    """The Radix scale a ``color_scheme`` paints with.

    ``None`` is the accent — that is the whole point of setting one. ``"gray"`` is **not** the
    literal gray scale: Radix aliases ``--gray-*`` to the theme's ``gray_color``, which
    SPEC_BUTTON_CONTRAST §4 pins to ``mauve``. Grading ``"gray"`` against the ``gray`` scale
    would read a colour the product never paints.
    """
    if color_scheme is None:
        return ACCENT_COLOR
    if color_scheme == "gray":
        return GRAY_COLOR
    return color_scheme


def grade(variant: str | None, color_scheme: str | None, high_contrast: bool | None) -> float:
    """The contrast ratio Radix will paint this button's label at."""
    resolved_variant = DEFAULT_VARIANT if variant is None else variant
    paint = VARIANT_PAINT.get((resolved_variant, bool(high_contrast)))
    if paint is None:
        raise UnreadablePropError(
            f"no painting rule for variant={resolved_variant!r} "
            f"high_contrast={bool(high_contrast)!r}; read components.css and add it to "
            "VARIANT_PAINT rather than letting this button go ungraded"
        )
    scale = _scale_of(color_scheme)
    if scale not in TOKENS:
        raise UnreadablePropError(
            f"no tokens for scale {scale!r}; add its values from "
            f"@radix-ui/themes@{RADIX_VERSION} tokens/colors/{scale}.css"
        )
    background_token, label_token = paint
    fields = {"scale": scale, "gray": GRAY_COLOR}
    if background_token is None:
        background = PAGE_BACKGROUND
    else:
        background = _composite(
            TOKENS[scale][background_token.format(**fields).split("-", 1)[1]], PAGE_BACKGROUND
        )
    label_scale, label_step = label_token.format(**fields).split("-", 1)
    return ratio(TOKENS[label_scale][label_step], background)


# ------------------------------------------------------------------------------------- the walk


def _walk(component, path: tuple[str, ...] = ()):
    here = path + (type(component).__name__,)
    yield component, here
    for child in getattr(component, "children", []) or []:
        yield from _walk(child, here)


def _is_static(value: object) -> bool:
    """Whether a prop is a plain literal a call site could simply have omitted."""
    return value is None or isinstance(getattr(value, "_var_value", value), (str, bool))


def _census():
    buttons, in_dialog, computed = [], 0, 0
    for module, attr in FACTORIES:
        with redirect_stdout(io.StringIO()):
            tree = getattr(importlib.import_module(f"datanika.ui.pages.{module}"), attr)()
        for component, path in _walk(tree):
            if type(component).__name__ not in ("Button", "IconButton"):
                continue
            raw_scheme = getattr(component, "color_scheme", None)
            variants = _branches(getattr(component, "variant", None))
            schemes = _branches(raw_scheme)
            contrasts = _branches(getattr(component, "high_contrast", None))
            if not _is_static(raw_scheme) or len(variants) > 1:
                computed += 1
            if any("DialogContent" in name for name in path):
                in_dialog += 1
            buttons.append(
                (f"{module}.{attr}", variants, schemes, contrasts, _is_static(raw_scheme))
            )
    return buttons, in_dialog, computed


BUTTONS, IN_DIALOG, COMPUTED = _census()


def _offences() -> list[str]:
    """Every (site, variant, scheme, high_contrast) combination below AA, worst first."""
    out = []
    for site, variants, schemes, contrasts, _static in BUTTONS:
        for variant, scheme, contrast in itertools.product(variants, schemes, contrasts):
            value = grade(variant, scheme, contrast)
            if value < MIN_RATIO:
                out.append(
                    f"{site}: variant={variant or DEFAULT_VARIANT!r} color_scheme={scheme!r} "
                    f"high_contrast={bool(contrast)!r} -> {value:.2f}:1"
                )
    return sorted(set(out))


def _rule_three_offences() -> list[str]:
    """Non-accent, non-solid buttons that do not carry ``high_contrast=True`` (spec §0 rule 3)."""
    out = []
    for site, variants, schemes, contrasts, _static in BUTTONS:
        for variant, scheme, contrast in itertools.product(variants, schemes, contrasts):
            resolved = DEFAULT_VARIANT if variant is None else variant
            if resolved in ("solid", "classic"):
                continue
            if _scale_of(scheme) == ACCENT_COLOR:
                continue
            if not bool(contrast):
                out.append(f"{site}: variant={resolved!r} color_scheme={scheme!r}")
    return sorted(set(out))


class TestTheCensusSawTheButtons:
    """Anti-vacuity. A walk that matched nothing would make every assertion below trivially true."""

    def test_it_saw_the_buttons(self):
        """473 in the tree today across 60 page factories, from 136 call sites."""
        assert len(BUTTONS) >= 400, len(BUTTONS)

    def test_it_saw_the_buttons_inside_a_dialog(self):
        """🔑 The population axe structurally cannot score, because it is not in the DOM.

        62 today — every Delete/Cancel pair behind an ``rx.alert_dialog``. If this reaches 0 the
        guard has quietly narrowed to what a browser sweep already covers, which is the one thing
        it exists to do better, and every assertion here would still be green.
        """
        assert IN_DIALOG >= 40, IN_DIALOG

    def test_it_saw_a_computed_colour_scheme(self):
        """The Run buttons take ``rx.match`` over the last run's status — 4 of them today.

        If this reaches 0, :func:`_branches`' arm reading is exercised by no real page and the
        grade has silently narrowed back to plain literals, passing all the way down.
        """
        assert COMPUTED >= 2, COMPUTED


class TestTheArithmeticIsArmed:
    """A contrast table with no control is a table of plausible numbers."""

    def test_it_reproduces_published_contrast_pairs(self):
        assert round(ratio("#000000", "#ffffff"), 2) == 21.00
        assert round(ratio("#767676", "#ffffff"), 2) == 4.54
        assert round(ratio("#ffffff", "#ffffff"), 2) == 1.00

    def test_the_compositing_matches_radix_own_design(self):
        """``--red-a3`` over white is ``--red-3`` by construction — Radix's own property."""
        assert _composite(TOKENS["red"]["a3"], "#ffffff") == "#feebec"

    def test_it_reproduces_the_spec_table(self):
        """The rows SPEC_BUTTON_CONTRAST §5 decided on, recomputed here rather than copied."""
        assert round(grade("solid", None, None), 2) == 5.39  # violet accent
        assert round(grade("solid", "red", None), 2) == 3.91  # the shape the spec bans
        assert round(grade("soft", "red", True), 2) == 10.84  # the shape it mandates
        assert round(grade("solid", "amber", None), 2) == 10.33
        assert round(grade("solid", "yellow", None), 2) == 12.89
        assert round(grade("ghost", "red", None), 2) == 5.21
        assert round(grade("solid", "gray", None), 2) == 3.30  # resolves through mauve

    def test_the_plain_soft_column_differs_from_the_spec_by_0_06_and_why(self):
        """🔑 A measured disagreement with the merged spec, recorded rather than rounded away.

        The spec's plain-``soft`` column reads 4.54 for red; this grader says **4.60**. Neither
        is a mistake: ``components.css`` paints a soft label with ``var(--accent-a11)``, an
        *alpha* colour, so the browser composites it over the soft fill — while the spec used the
        opaque ``--accent-11``, which Radix designs to be the equivalent of exactly that. The
        residue is Radix's own rounding in that design property (a11 over ``red-3`` lands on
        ``#ce292e``; ``--red-11`` is ``#ce2c31``).

        **It changes no decision.** All 32 cells of §5 agree on pass/fail under both models, and
        the spec rejects plain soft red on its margin regardless. This grader keeps the literal
        model because it is what the browser paints — but the number is written down here so the
        next person to compare the two does not spend a round deciding which is broken.
        """
        assert round(grade("soft", "red", None), 2) == 4.60
        assert round(ratio("#ce2c31", _composite(TOKENS["red"]["a3"], PAGE_BACKGROUND)), 2) == 4.54
        for variant, scheme, contrast in [
            ("soft", "amber", None),
            ("soft", "yellow", None),
            ("soft", "green", None),
            ("soft", "blue", None),
            ("soft", "orange", None),
            ("soft", "red", None),
            ("soft", "gray", None),
            ("soft", None, None),
        ]:
            spec_says_pass = scheme in ("red", "gray", None)
            assert (grade(variant, scheme, contrast) >= MIN_RATIO) is spec_says_pass, scheme


class TestButtonsAreLegible:
    def test_every_button_label_clears_aa(self):
        """AC1 and AC2: no button label sits below 4.5:1 against its own background."""
        assert not _offences(), "\n".join(_offences())

    def test_every_non_accent_non_solid_button_is_high_contrast(self):
        """Spec §0 rule 3 — ``high_contrast`` is *always*, not "where needed".

        Plain ``soft`` fails outright on amber, yellow, green, blue and orange, and clears by
        +0.04 on red and +0.66 on the gray; the exceptions are not where intuition puts them, so
        the rule is stated structurally instead of left to each author's arithmetic.
        """
        assert not _rule_three_offences(), "\n".join(_rule_three_offences())

    def test_no_button_hand_writes_the_accent_it_would_get_anyway(self):
        """AC5. Redundant today, and actively wrong the day the accent changes.

        Phrased against ``ACCENT_COLOR`` rather than the literal ``"violet"``, so it keeps
        meaning the same thing after a future accent decision. Nine such overrides existed
        before the theme was set; the app had been out-voting Reflex's blue by hand at its most
        prominent calls to action, which is most of the evidence that violet was the right pick.

        ⚠️ **Static props only, and the exemption is structural rather than a concession.** A
        *computed* scheme cannot express "leave it to the theme" — you cannot conditionally omit
        a keyword argument — so ``run_in_progress_color`` has to name the accent for its idle
        arm. Flagging that would be demanding an impossible edit, and the usual repair for an
        impossible demand is to delete the guard.
        """
        named = sorted(
            {
                site
                for site, _variants, schemes, _contrasts, static in BUTTONS
                if static and ACCENT_COLOR in [s for s in schemes if isinstance(s, str)]
            }
        )
        assert not named, named


class TestTheAppCarriesTheTheme:
    """Without this, every grade above is computed against an accent production may not render.

    ``reflex/app.py`` defaults ``rx.App`` to ``accent_color="blue"``, so *not* passing a theme is
    not an absence — it is Reflex's choice, made for us. A guard that read the accent from
    :mod:`datanika.ui.theme` alone would stay green through a revert of the one line that makes
    any of it true.
    """

    def test_the_app_passes_the_chosen_accent_and_gray(self):
        app = importlib.import_module("datanika.datanika").app
        assert app.theme is not None, "rx.App carries no theme — Reflex will default it to blue"
        assert getattr(app.theme.accent_color, "_var_value", None) == ACCENT_COLOR
        assert getattr(app.theme.gray_color, "_var_value", None) == GRAY_COLOR

    def test_the_gray_is_pinned_rather_than_left_to_auto(self):
        """§4: ``gray_color`` defaults to ``"auto"``, which maps violet → mauve behind your back.

        Same value, but chosen by the accent rather than by us — *a value implied by another
        value is a value nothing reviews*.
        """
        assert GRAY_COLOR != "auto"


class TestTheGradeTellsTheShapesApart:
    """Seen failing, and seen *discriminating* — a guard that refuses everything is not a guard."""

    def test_the_banned_shape_fails_and_the_mandated_one_passes(self):
        assert grade("solid", "red", None) < MIN_RATIO
        assert grade("soft", "red", True) >= MIN_RATIO
        assert grade("solid", None, None) >= MIN_RATIO

    def test_every_scale_the_spec_rejects_for_solid_is_rejected(self):
        for scale in ("red", "gray", "green", "blue", "orange"):
            assert grade("solid", scale, None) < MIN_RATIO, scale
        for scale in (None, "amber", "yellow"):
            assert grade("solid", scale, None) >= MIN_RATIO, scale

    def test_a_bad_branch_hidden_in_a_match_is_caught(self):
        """The Run button's real shape: solid, with the scale computed from the run status.

        A grader reading only the first arm would pass this, because the first arm is fine.
        """
        status = rx.Var.create("running")
        good = rx.button("run", color_scheme=rx.match(status, ("running", "yellow"), ACCENT_COLOR))
        bad = rx.button("run", color_scheme=rx.match(status, ("running", "yellow"), "gray"))
        assert all(grade("solid", s, None) >= MIN_RATIO for s in _branches(good.color_scheme))
        assert any(grade("solid", s, None) < MIN_RATIO for s in _branches(bad.color_scheme))

    def test_the_branch_reader_returns_every_branch_not_just_one(self):
        """Anti-vacuity for the test above, which a one-branch reader would also satisfy."""
        status = rx.Var.create("running")
        assert _branches(rx.match(status, ("running", "yellow"), "gray")) == ["gray", "yellow"]
        assert _branches(rx.cond(rx.Var.create(True), "red", "green")) == ["green", "red"]
        assert _branches(None) == [None]
        assert _branches("soft") == ["soft"]

    def test_a_shape_it_cannot_read_raises_rather_than_passing(self):
        """🔑 The failure direction that matters.

        A reducer that returns ``None``/``[]`` for what it does not understand hands that shape
        to its caller as clean — how 7 of 7 composite colours stayed invisible under a green
        guard. Unknown variant, unknown scale and unreadable expression all raise.
        """
        with pytest.raises(UnreadablePropError):
            grade("surface", "red", False)
        with pytest.raises(UnreadablePropError):
            grade("solid", "chartreuse", False)
        with pytest.raises(UnreadablePropError):
            _branches(rx.Var.create(["a", "b"]).length())


class TestTheTokensDescribeThePackageWeShip:
    def test_the_vendored_version_is_the_one_reflex_pins(self):
        """§7's flip condition, mechanised: a bump reds here instead of drifting silently."""
        assert RadixThemesComponent.library == f"@radix-ui/themes@{RADIX_VERSION}"

    def test_the_vendored_tokens_match_the_installed_package(self):
        """Wherever the frontend is installed, re-read every value rather than trusting the table.

        Skipped where ``.web/node_modules`` is absent (CI's unit job never builds the frontend).
        The version assertion above is what covers that case: the values are an artifact of the
        version, so an unchanged pin means unchanged values.
        """
        package = pathlib.Path(datanika.ui.pages.__file__).parents[3] / (
            ".web/node_modules/@radix-ui/themes"
        )
        if not package.is_dir():
            pytest.skip(f"frontend not installed at {package}")
        assert json.loads((package / "package.json").read_text())["version"] == RADIX_VERSION
        top = re.compile(r":root,\s*\.light,\s*\.light-theme\s*\{(.*?)\}", re.S)
        bare = re.compile(r"^\s*:root\s*\{(.*?)\}", re.S | re.M)
        for scale, wanted in TOKENS.items():
            text = (package / "tokens/colors" / f"{scale}.css").read_text(encoding="utf-8")
            blocks = [m.group(1) for m in top.finditer(text)][:1] + bare.findall(text)
            found: dict[str, str] = {}
            for block in blocks:
                for name, value in re.findall(r"--([a-z0-9-]+):\s*([^;]+);", block):
                    found.setdefault(name, value.strip())
            for step, value in wanted.items():
                assert found.get(f"{scale}-{step}") == value, f"--{scale}-{step}"


# ------------------------------------------------------------------------ callouts (§11, #1535)

#: 🚨 **Radix's default variant for `Callout.Root` is `soft`, NOT the `solid` above.**
#: ``DEFAULT_VARIANT`` is read from ``base-button.props.js`` and is a fact about *buttons*.
#: Grading a callout with it silently paints the wrong thing: every scheme comes out ~1.1 low
#: (green 3.16 instead of 4.28, red 3.91 instead of 4.60), which is alarming rather than
#: flattering — but wrong either way, and it would have put this census ~1.1 away from
#: SPEC_BUTTON_CONTRAST §11's own measured figures with no visible reason.
#:
#: 🔑 The soft column is what reproduces the spec's oracle, which is how this was settled rather
#: than argued: see :meth:`TestTheCalloutDefaultIsSoftNotSolid.test_soft_reproduces_the_spec`.
CALLOUT_DEFAULT_VARIANT = "soft"


def grade_callout(variant: str | None, color_scheme: str | None, high_contrast: bool | None):
    """:func:`grade`, with the callout's own default variant rather than the button's."""
    resolved = CALLOUT_DEFAULT_VARIANT if variant is None else variant
    return grade(resolved, color_scheme, high_contrast)


def _callout_census():
    """Every ``CalloutRoot`` the page factories reach, with each branch of every cond-valued prop.

    ⚠️ **The component is ``CalloutRoot``, not ``Callout``.** Asking the walk for ``Callout``
    returns **0** on a tree full of them, which reads exactly like a population the walk cannot
    reach — the wrong diagnosis, and the expensive one, since the obvious next move is to rewrite
    the walk. ``_walk`` already follows ``rx.cond`` branches: a Cond exposes them as children.
    """
    rows, computed = [], 0
    for module, attr in FACTORIES:
        with redirect_stdout(io.StringIO()):
            tree = getattr(importlib.import_module(f"datanika.ui.pages.{module}"), attr)()
        for component, _path in _walk(tree):
            if type(component).__name__ != "CalloutRoot":
                continue
            raw_scheme = getattr(component, "color_scheme", None)
            variants = _branches(getattr(component, "variant", None))
            schemes = _branches(raw_scheme)
            contrasts = _branches(getattr(component, "high_contrast", None))
            if not _is_static(raw_scheme):
                computed += 1
            rows.append((f"{module}.{attr}", variants, schemes, contrasts, _is_static(raw_scheme)))
    return rows, computed


CALLOUTS, CALLOUTS_COMPUTED = _callout_census()


def _callout_offences() -> list[str]:
    out = []
    for site, variants, schemes, contrasts, _static in CALLOUTS:
        for variant, scheme, contrast in itertools.product(variants, schemes, contrasts):
            value = grade_callout(variant, scheme, contrast)
            if value < MIN_RATIO:
                out.append(
                    f"{site}: variant={variant or CALLOUT_DEFAULT_VARIANT!r} "
                    f"color_scheme={scheme!r} high_contrast={bool(contrast)!r} -> {value:.2f}:1"
                )
    return sorted(set(out))


def _callouts_naming_a_scheme_without_high_contrast() -> list[str]:
    """§11's rule: every callout that NAMES a ``color_scheme`` carries ``high_contrast=True``."""
    out = []
    for site, _variants, schemes, contrasts, _static in CALLOUTS:
        if all(scheme is None for scheme in schemes):
            continue  # inherits the accent — §11 leaves these alone
        if not all(bool(c) for c in contrasts):
            out.append(f"{site}: color_scheme={schemes!r} high_contrast={contrasts!r}")
    return sorted(set(out))


class TestTheCensusSawTheCallouts:
    """Anti-vacuity. Coverage and sensitivity are different properties."""

    def test_it_saw_the_callouts(self):
        """175 instances today from 62 call sites across 60 page factories."""
        assert len(CALLOUTS) >= 120, len(CALLOUTS)

    def test_it_saw_a_computed_colour_scheme(self):
        """`/connections`' test verdict picks its scheme with a nested ``rx.cond``.

        🔑 **This is the site §11 says matters most, and a source-literal guard misses it** —
        there is no ``color_scheme="green"`` anywhere in `connections.py` to grep for. If this
        reaches 0, the census has silently narrowed to literals and would stay green.
        """
        assert CALLOUTS_COMPUTED >= 1, CALLOUTS_COMPUTED

    def test_the_component_is_named_callout_root(self):
        """The name that cost a wrong diagnosis. ``Callout`` matches nothing; this records why."""
        names = set()
        with redirect_stdout(io.StringIO()):
            page = importlib.import_module("datanika.ui.pages.connections").connections_page()
        for component, _ in _walk(page):
            names.add(type(component).__name__)
        assert "CalloutRoot" in names
        assert "Callout" not in names


class TestTheCalloutDefaultIsSoftNotSolid:
    """The grader is shared with buttons; the default variant is not."""

    def test_soft_reproduces_the_spec(self):
        """SPEC_BUTTON_CONTRAST §11's measured column, recomputed rather than copied.

        Those figures were read off the **running app's served stylesheet**. Reproducing them
        from the vendored tokens is what establishes that this grader is looking at the same
        paint the browser applies.
        """
        assert round(grade_callout(None, "green", None), 2) == 4.28  # §11 says 4.27
        assert round(grade_callout(None, "orange", None), 2) == 3.99  # §11 says 3.99
        assert round(grade_callout(None, "amber", None), 2) == 4.25  # §11 says 4.25
        assert round(grade_callout(None, "blue", None), 2) == 4.25  # §11 says 4.26
        assert round(grade_callout(None, None, None), 2) == 5.81  # violet accent; §11 says 5.80

    def test_the_button_default_would_grade_a_different_paint(self):
        """The negative control. Without it, "soft is right" is an assertion, not a measurement."""
        assert round(grade(DEFAULT_VARIANT, "green", None), 2) == 3.16
        assert grade(DEFAULT_VARIANT, "green", None) != grade_callout(None, "green", None)

    def test_high_contrast_is_what_clears_aa_for_every_scheme(self):
        """§11's remedy, asserted as the reason the rule is 'always' rather than 'where needed'."""
        for scheme in ("green", "red", "orange", "amber", "blue", "gray"):
            assert grade_callout(None, scheme, None) < 10, scheme
            assert grade_callout(None, scheme, True) >= MIN_RATIO, scheme


class TestCalloutsAreLegible:
    def test_every_callout_clears_aa(self):
        """AC1/AC2, on every branch of every cond-valued prop."""
        assert not _callout_offences(), "\n".join(_callout_offences())

    def test_every_callout_naming_a_scheme_is_high_contrast(self):
        """§11's rule stated positively.

        ⚠️ Written as *"names a scheme, therefore carries high_contrast"*, **not** as *"no line
        says rx.callout without high_contrast"* — that form is satisfied by deleting the callout
        and goes red on the correct un-schemed ones.
        """
        offenders = _callouts_naming_a_scheme_without_high_contrast()
        assert not offenders, "\n".join(offenders)

    def test_the_connection_test_verdict_is_legible_in_all_three_states(self):
        """§11's headline instance, and the one the browser sweep structurally cannot score.

        `/connections` renders the verdict inside ``rx.cond(ConnectionState.test_message, …)`` and
        ``a11y-sweep.spec.ts`` performs **zero clicks**, so a green sweep carries no information
        about it. **The SUCCESS case was the failing one** — the user who cannot read the green
        verdict is the one who cannot tell it from the neutral *not tested* state core#821 added
        precisely to keep those two apart.

        All three states are asserted by name, so an edit that drops one reds here rather than
        quietly grading a smaller population.
        """
        verdicts = [row for row in CALLOUTS if row[0].startswith("connections.") and not row[4]]
        assert verdicts, "the computed verdict callout is no longer in the census"
        for _site, variants, schemes, contrasts, _static in verdicts:
            assert set(schemes) == {"gray", "green", "red"}, (
                f"expected core#821's three states, got {schemes!r} — a nested rx.cond that "
                "loses an arm reads as a complete grading of a smaller population"
            )
            for variant, scheme, contrast in itertools.product(variants, schemes, contrasts):
                assert grade_callout(variant, scheme, contrast) >= MIN_RATIO, scheme
