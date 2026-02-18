"""i18n translation loader with English fallback."""

import json
from pathlib import Path

SUPPORTED_LOCALES = ["en", "ru", "el", "de", "fr", "es", "zh", "ar"]
DEFAULT_LOCALE = "en"

_cache: dict[str, dict[str, str]] = {}
_dir = Path(__file__).parent


def load_all() -> None:
    """Load all JSON translation files into the module cache."""
    for locale in SUPPORTED_LOCALES:
        path = _dir / f"{locale}.json"
        with open(path, encoding="utf-8") as f:
            _cache[locale] = json.load(f)


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
