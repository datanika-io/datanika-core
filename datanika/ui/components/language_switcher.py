"""Compact language switcher for the sidebar."""

import reflex as rx

from datanika.i18n import SUPPORTED_LOCALES
from datanika.ui.components.searchable_select import searchable_select
from datanika.ui.state.i18n_state import I18nState


def language_switcher() -> rx.Component:
    return searchable_select(
        SUPPORTED_LOCALES,
        value=I18nState.locale,
        on_change=I18nState.set_locale,
        size="1",
        width="100%",
        search_placeholder="Language...",
    )
