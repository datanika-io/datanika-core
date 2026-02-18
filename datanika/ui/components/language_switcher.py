"""Compact language switcher for the sidebar."""

import reflex as rx

from datanika.i18n import SUPPORTED_LOCALES
from datanika.ui.state.i18n_state import I18nState


def language_switcher() -> rx.Component:
    return rx.select(
        SUPPORTED_LOCALES,
        value=I18nState.locale,
        on_change=I18nState.set_locale,
        size="1",
        width="100%",
    )
