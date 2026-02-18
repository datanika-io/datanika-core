"""Tests for the i18n translation system."""

import json

from datanika.i18n import (
    DEFAULT_LOCALE,
    SUPPORTED_LOCALES,
    _cache,
    _dir,
    get_translations,
    load_all,
)


class TestI18nConfig:
    def test_default_locale_is_english(self):
        assert DEFAULT_LOCALE == "en"

    def test_supported_locales_count(self):
        assert len(SUPPORTED_LOCALES) == 6

    def test_supported_locales_contains_expected(self):
        for locale in ["en", "ru", "el", "de", "fr", "es"]:
            assert locale in SUPPORTED_LOCALES


class TestLoadAll:
    def test_load_all_populates_cache(self):
        _cache.clear()
        load_all()
        assert len(_cache) == len(SUPPORTED_LOCALES)
        for locale in SUPPORTED_LOCALES:
            assert locale in _cache
            assert isinstance(_cache[locale], dict)
            assert len(_cache[locale]) > 0

    def test_all_locales_parse(self):
        """Verify every JSON file is valid JSON."""
        for locale in SUPPORTED_LOCALES:
            path = _dir / f"{locale}.json"
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            assert isinstance(data, dict)
            assert len(data) > 0


class TestGetTranslations:
    def test_english_has_all_keys(self):
        en = get_translations("en")
        assert "app.name" in en
        assert "nav.dashboard" in en
        assert "auth.sign_in" in en
        assert "common.edit" in en

    def test_all_locales_have_same_keys(self):
        en_keys = set(get_translations("en").keys())
        for locale in SUPPORTED_LOCALES:
            locale_keys = set(get_translations(locale).keys())
            missing = en_keys - locale_keys
            assert not missing, f"{locale} missing keys: {missing}"

    def test_fallback_to_english(self):
        """get_translations merges English base + target, so all keys present."""
        for locale in SUPPORTED_LOCALES:
            t = get_translations(locale)
            en = get_translations("en")
            for key in en:
                assert key in t, f"{locale} missing key after merge: {key}"

    def test_unsupported_locale_returns_english(self):
        en = get_translations("en")
        result = get_translations("xx")
        assert result == en

    def test_translations_values_are_strings(self):
        for locale in SUPPORTED_LOCALES:
            t = get_translations(locale)
            for key, value in t.items():
                assert isinstance(value, str), f"{locale}.{key} is not a string: {type(value)}"

    def test_non_english_locale_has_different_values(self):
        """At least some values should differ from English in other locales."""
        en = get_translations("en")
        for locale in SUPPORTED_LOCALES:
            if locale == "en":
                continue
            t = get_translations(locale)
            differences = sum(1 for k in en if t.get(k) != en[k])
            assert differences > 10, (
                f"{locale} has only {differences} different values from English"
            )
