"""i18n state — locale selection and reactive translations dict."""

import reflex as rx

from datanika.i18n import DEFAULT_LOCALE, LOCALE_NAMES, SUPPORTED_LOCALES, get_translations

# One year. The reset link is opened later, often after the tab that chose the
# language is gone, and a users.locale column does not exist yet on /signup.
_LOCALE_MAX_AGE = 60 * 60 * 24 * 365
_NAME_TO_CODE = {name: code for code, name in LOCALE_NAMES.items()}


def locale_code(value: str) -> str | None:
    if value in SUPPORTED_LOCALES:
        return value
    return _NAME_TO_CODE.get(value)


class I18nState(rx.State):
    # The signed-in app and the signed-out pages read this one field.
    locale: str = rx.Cookie(
        "en",
        name="datanika_locale",
        path="/",
        max_age=_LOCALE_MAX_AGE,
        same_site="lax",
    )
    translations: dict[str, str] = get_translations(DEFAULT_LOCALE)

    @rx.var
    def locale_label(self) -> str:
        return LOCALE_NAMES.get(self.locale, LOCALE_NAMES[DEFAULT_LOCALE])

    def set_locale(self, locale: str) -> None:
        code = locale_code(locale)
        if code is None:
            return
        self.locale = code
        self.translations = get_translations(code)

    def ensure_loaded(self) -> None:
        """Re-fetch translations for the current locale.

        Picks up keys registered after import time (e.g. cloud plugin).
        Called from check_auth so every protected page has fresh translations,
        and from the pre-auth pages so a stored choice is what they render.
        An invalid stored value falls back to English and is written back.
        A valid one is not reassigned, so a visit does not set the cookie.
        """
        code = self.locale if self.locale in SUPPORTED_LOCALES else DEFAULT_LOCALE
        if code != self.locale:
            self.locale = code
        self.translations = get_translations(code)
