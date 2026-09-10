"""Contextual tooltip for complex concepts on form labels.

⚠️ **``TOOLTIP_KEYS`` describes what THIS COMPONENT renders — not every key named
``tooltip.*``.** The three ``write_disposition_{append,replace,merge}`` strings
left this registry in core#1242: they are rendered as an inline ``rx.match`` hint
under the select (``uploads.py``), because a tooltip inside an open dropdown is
hover-only and touch-hostile. They are still ``tooltip.*`` keys and still live in
all nine locales; they are simply not this component's content.
"""

import reflex as rx

from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations

TOOLTIP_KEYS = (
    "tooltip.write_disposition",
    "tooltip.incremental_cursor",
    "tooltip.schema_contract",
    "tooltip.load_mode",
    "tooltip.materialization",
    "tooltip.rows_loaded",
)

_KEYS_FOR_SCANNER = (
    _t["tooltip.write_disposition"],
    _t["tooltip.incremental_cursor"],
    _t["tooltip.schema_contract"],
    _t["tooltip.load_mode"],
    _t["tooltip.materialization"],
    _t["tooltip.rows_loaded"],
)


def info_tooltip(i18n_key: str) -> rx.Component:
    return rx.tooltip(
        # `help-circle` was lucide's OLD name for this glyph; the current one is
        # `circle_help`. Reflex does not raise on an unknown tag — it prints
        # "Invalid icon tag" to stdout, where nothing is listening in a
        # container, and substitutes a different icon (core#701). Here the
        # substitute happened to be `circle_help` itself, so every tooltip
        # rendered correctly by luck of the substitution target. The next bad
        # tag will not be a rename, and it will look exactly as healthy.
        rx.icon("circle_help", size=14, color="var(--slate-8)", cursor="help"),
        content=_t[i18n_key],
        side="right",
        max_width="320px",
    )
