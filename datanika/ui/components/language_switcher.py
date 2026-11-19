"""Compact language switcher for the sidebar and the pre-auth pages."""

import reflex as rx

from datanika.i18n import LOCALE_NAMES, SUPPORTED_LOCALES
from datanika.ui.components.searchable_select import searchable_select
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def language_switcher(*, suppress_autofill: bool = True) -> rx.Component:
    # The sidebar keeps the default: that filter shares a document with
    # connector secrets (core#618). The four pre-auth pages pass False,
    # because no_autofill_attrs is forbidden there (core#672).
    return searchable_select(
        [LOCALE_NAMES[code] for code in SUPPORTED_LOCALES],
        value=I18nState.locale_label,
        on_change=I18nState.set_locale,
        size="1",
        width="100%",
        search_placeholder=_t["locale.search"],
        suppress_autofill=suppress_autofill,
    )
