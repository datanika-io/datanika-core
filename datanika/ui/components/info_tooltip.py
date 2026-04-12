"""Contextual tooltip for complex concepts on form labels."""

import reflex as rx

from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations

TOOLTIP_KEYS = (
    "tooltip.write_disposition",
    "tooltip.write_disposition_append",
    "tooltip.write_disposition_replace",
    "tooltip.write_disposition_merge",
    "tooltip.incremental_cursor",
    "tooltip.schema_contract",
    "tooltip.load_mode",
    "tooltip.materialization",
)

_KEYS_FOR_SCANNER = (
    _t["tooltip.write_disposition"],
    _t["tooltip.write_disposition_append"],
    _t["tooltip.write_disposition_replace"],
    _t["tooltip.write_disposition_merge"],
    _t["tooltip.incremental_cursor"],
    _t["tooltip.schema_contract"],
    _t["tooltip.load_mode"],
    _t["tooltip.materialization"],
)


def info_tooltip(i18n_key: str) -> rx.Component:
    return rx.tooltip(
        rx.icon("help-circle", size=14, color="var(--slate-8)", cursor="help"),
        content=_t[i18n_key],
        side="right",
        max_width="320px",
    )
