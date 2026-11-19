"""i18n translation loader with English fallback."""

import json
from pathlib import Path

SUPPORTED_LOCALES = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]
DEFAULT_LOCALE = "en"

# Each language in that language. A translated label would be English until the
# visitor had already found the control (core#696 AC5).
LOCALE_NAMES = {
    "en": "English",
    "ru": "Русский",
    "el": "Ελληνικά",
    "de": "Deutsch",
    "fr": "Français",
    "es": "Español",
    "zh": "中文",
    "ar": "العربية",
    "sr": "Српски",
}

_cache: dict[str, dict[str, str]] = {}
_dir = Path(__file__).parent


def load_all() -> None:
    """Load all JSON translation files into the module cache."""
    for locale in SUPPORTED_LOCALES:
        path = _dir / f"{locale}.json"
        with open(path, encoding="utf-8") as f:
            _cache[locale] = json.load(f)


def register_translations(translations: dict[str, dict[str, str]]) -> None:
    """Merge extra translations from a plugin into the cache.

    ``translations`` maps locale codes to key→value dicts.  Only locales
    already present in the cache are updated.
    """
    if not _cache:
        load_all()
    for locale, keys in translations.items():
        if locale in _cache:
            _cache[locale].update(keys)


def get_translations(locale: str) -> dict[str, str]:
    """Return merged translations: English base + target locale overrides.

    Guarantees every English key is present. Falls back to English for
    unsupported locales or missing keys.
    """
    if not _cache:
        load_all()

    en = _cache.get(DEFAULT_LOCALE, {})
    if locale not in SUPPORTED_LOCALES or locale == DEFAULT_LOCALE:
        return dict(en)

    target = _cache.get(locale, {})
    merged = dict(en)
    merged.update(target)
    return merged
