"""A table placeholder that is not an empty table (core#872).

## Why this exists

`connections == []` and `models == []` each mean two different things — *"this
org has none"* and *"the websocket has not delivered them yet"* — and on
production the second lasted **5–17 seconds** while rendering pixel-identically
to the first. `/models` was worse: it showed its *"no models yet"* callout on the
first paint, telling a user who has data, in words, that they have none.

That ambiguity is what made core#869 hard: one `/models` poll stayed empty for a
full 30 s and **that one was a real emptiness**. Until the not-yet-loaded case is
separable, an honest empty cannot be read as honest.

## Why a spinner and not a skeleton

A skeleton (grey placeholder rows) reads better, and choosing it is a Product
call about how many rows to fake and how they should look. `rx.spinner` is what
this codebase already uses on `auth_complete`, `model_detail` and
`oauth_consent`, so it is the existing precedent rather than a new design
decision arriving inside a bug fix. Swapping the internals here upgrades every
caller at once — which is the point of putting it in one place.

## The one thing a caller must not do

Render this *instead of the empty-state message* only while the flag is False.
A page that shows it whenever the row list is empty has simply replaced one
permanent lie with another: a user with genuinely no connections would wait
forever on a spinner.
"""

from __future__ import annotations

import reflex as rx

from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def table_loading(min_height: str = "120px") -> rx.Component:
    """The 'not loaded yet' third state for a table.

    Carries `aria-busy` and a `status` role so the distinction this component
    exists to make is available to a screen reader too — for which the original
    defect was not "looks the same" but "announces nothing at all".
    """
    return rx.center(
        rx.hstack(
            rx.spinner(size="2"),
            rx.text(_t["common.loading"], size="2", color="var(--gray-9)"),
            spacing="3",
            align="center",
        ),
        width="100%",
        min_height=min_height,
        role="status",
        aria_busy="true",
    )
