"""i18n state — locale selection and reactive translations dict."""

import reflex as rx

from datanika.i18n import SUPPORTED_LOCALES, get_translations


class I18nState(rx.State):
    locale: str = "en"
    translations: dict[str, str] = get_translations("en")

    def set_locale(self, locale: str):
        if locale not in SUPPORTED_LOCALES:
            return
        self.locale = locale
        self.translations = get_translations(locale)
