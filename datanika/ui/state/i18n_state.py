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

    def ensure_loaded(self):
        """Re-fetch translations for the current locale.

        Picks up keys registered after import time (e.g. cloud plugin).
        Called from check_auth so every protected page has fresh translations.
        """
        self.translations = get_translations(self.locale)
