"""Tests for the i18n translation system."""

import json
import re
from pathlib import Path

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
        assert len(SUPPORTED_LOCALES) == 9

    def test_supported_locales_contains_expected(self):
        for locale in ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]:
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


# ---------------------------------------------------------------------------
# Code ↔ JSON sync tests: every _t["key"] in UI code must exist in every
# locale file, and every JSON key should be referenced in UI code.
# ---------------------------------------------------------------------------

_UI_ROOT = Path(__file__).resolve().parent.parent.parent / "datanika" / "ui"
_KEY_RE = re.compile(r'_t\["([^"]+)"\]')
# Second usage channel, added with core#804's delete toasts. A component tree
# reads translations reactively as `_t["key"]`, but a *state handler* producing
# a string in Python reads the same dict directly:
#
#     i18n = await self.get_state(I18nState)
#     yield rx.toast.success(i18n.translations.get("connections.deleted_toast", …))
#
# Without this pattern those keys look like orphans, and the obvious response to
# `test_no_orphan_keys_in_json` is to delete the key — which silently drops the
# translation and leaves the fallback English string for all nine locales.
_STATE_KEY_RE = re.compile(r'translations(?:\.get\(|\[)"([^"]+)"')
# core#851. Nine delete handlers now reach their toast string through
# `BaseState._deleted_toast("<key>", "<fallback>")` rather than by touching
# `translations` themselves, so `_STATE_KEY_RE` no longer sees them. Left
# unfixed, `test_no_orphan_keys_in_json` reports every `*.deleted_toast` key as
# unused — and the documented remedy for an orphan is to *delete the key*, which
# would have silently dropped nine translations across nine locales and left
# every non-English user reading the English fallback. A key-usage scanner is
# only as wide as the idioms it knows; adding an indirection is adding an idiom.
_TOAST_KEY_RE = re.compile(r'_deleted_toast\(\s*"([^"]+)"')
# core#862 adds a second indirection: `BaseState._translated(key, fallback)`,
# used for save-time refusals that must reach the user in their own language.
# Same lesson as the line above, one release later — **a key-usage scanner is
# only as wide as the idioms it knows, and the documented remedy for a false
# orphan is to DELETE the key**, which would silently drop the translation in
# all nine locales. Add the pattern when you add the helper.
_TRANSLATED_KEY_RE = re.compile(r'_translated\(\s*"([^"]+)"')
# core#872 adds the constructive twin, `BaseState._saved_toast`. Third instance
# of the lesson two comments above, and the first two were written by people who
# had just been bitten by it: **adding an indirection is adding an idiom.** All
# thirteen new keys read as orphans until this line existed, and the documented
# remedy for a false orphan is to DELETE the key — which would have dropped the
# translation in all nine locales while every check stayed green.
#
# ⚠️ The pattern captures ONE literal per call, so a call site that picks its key
# with a ternary hides the second branch from this scanner. The two handlers that
# do both create and update therefore use explicit if/else branches rather than
# `"a" if edit else "b"` — readable, and visible to the tooling.
_SAVED_TOAST_KEY_RE = re.compile(r'_saved_toast\(\s*"([^"]+)"')
# core#978 / core#979 add a FOURTH indirection, and this file's own lesson —
# **adding an indirection is adding an idiom** — is now on its fourth instance,
# each written by somebody who had just been bitten by the previous one.
#
# The new idiom is a **mapping constant whose values are keys**.
# `ConnectionState._VERDICT_KEYS` maps a service-side verdict `reason` to the
# key the UI looks up. It exists because a *service* must not carry i18n keys
# (`BaseState._translated`: "services have no locale and no business having
# one") and because this scanner walks `datanika/ui` only — so a key whose sole
# literal lived in `services/` would read as an orphan, and the documented
# remedy for a false orphan is to DELETE the key.
#
# ⚠️ Anchored to a whole line with a **dotted** value on the right, so it reads
# `"file_found": "connections.test_file_found",` and not an arbitrary dict.
# Measured when added: 5 matches, all real keys, **0 false matches anywhere in
# `datanika/ui`** — a pattern that matched extra values would make a genuinely
# orphaned key look used, which is this check failing in the silent direction.
_KEY_MAP_VALUE_RE = re.compile(r'^\s*"[a-z_]+"\s*:\s*"([a-z_]+\.[a-z_.]+)"\s*,?\s*$', re.M)


def _collect_keys_from_code() -> set[str]:
    """Scan all .py files under datanika/ui/ for translation-key references."""
    keys: set[str] = set()
    for py_file in _UI_ROOT.rglob("*.py"):
        text = py_file.read_text(encoding="utf-8")
        keys.update(_KEY_RE.findall(text))
        keys.update(_STATE_KEY_RE.findall(text))
        keys.update(_TOAST_KEY_RE.findall(text))
        keys.update(_TRANSLATED_KEY_RE.findall(text))
        keys.update(_SAVED_TOAST_KEY_RE.findall(text))
        keys.update(_KEY_MAP_VALUE_RE.findall(text))
    return keys


def _collect_keys_from_json() -> dict[str, set[str]]:
    """Return {locale: set_of_keys} for every locale JSON file."""
    result: dict[str, set[str]] = {}
    for locale in SUPPORTED_LOCALES:
        path = _dir / f"{locale}.json"
        with open(path, encoding="utf-8") as f:
            result[locale] = set(json.load(f).keys())
    return result


class TestTheScannerSeesEachIdiom:
    """Each key-usage idiom, asserted to be reachable by the scanner.

    🚨 **This is the control the previous three additions did not have.** Every
    one of `_TOAST_KEY_RE`, `_TRANSLATED_KEY_RE` and `_SAVED_TOAST_KEY_RE` was
    added *after* the idiom it matches shipped and reported every one of its keys
    as an orphan — three times, with the comment above each pattern warning the
    next person. A pattern that silently stops matching (a rename, a reformat, a
    helper that grows a keyword argument) puts the scanner straight back into
    that state, and the symptom is a **false orphan**, whose documented remedy is
    to delete the key.

    So each idiom gets one live example asserted by name. A test that says
    "these regexes find something" would be satisfied by any one of them.
    """

    def test_every_idiom_contributes_at_least_one_key(self):
        found = _collect_keys_from_code()
        for idiom, example in [
            ('_t["..."]', "app.name"),
            ("_translated(...)", "connections.deleted_toast"),
            ("_VERDICT_KEYS mapping", "connections.test_file_missing"),
        ]:
            assert example in found, (
                f"the {idiom} idiom no longer reaches the scanner — {example!r} was not "
                "collected, so every key that only appears through it now reads as an "
                "orphan, and the documented remedy for an orphan is to delete it"
            )

    def test_the_key_map_pattern_does_not_over_match(self):
        """The other direction, and it fails silently.

        A pattern that scoops up extra dict values makes a genuinely orphaned key
        look used. Measured at 0 false matches when added; asserted so a later
        loosening has to be deliberate.
        """
        en = _collect_keys_from_json()["en"]
        matched: set[str] = set()
        for py_file in _UI_ROOT.rglob("*.py"):
            matched.update(_KEY_MAP_VALUE_RE.findall(py_file.read_text(encoding="utf-8")))
        assert matched <= en, (
            "the key-map pattern matched values that are not translation keys, so it "
            f"can now hide a real orphan: {sorted(matched - en)}"
        )


class TestCodeJsonSync:
    """Ensure translation keys referenced in UI code match JSON files."""

    def test_all_code_keys_exist_in_english(self):
        """Every _t['key'] used in UI code must be defined in en.json."""
        code_keys = _collect_keys_from_code()
        en_keys = _collect_keys_from_json()["en"]
        missing = code_keys - en_keys
        assert not missing, f"Keys used in code but missing from en.json: {sorted(missing)}"

    def test_all_code_keys_exist_in_every_locale(self):
        """Every _t['key'] used in UI code must be present in all locale files."""
        code_keys = _collect_keys_from_code()
        locale_keys = _collect_keys_from_json()
        for locale, keys in locale_keys.items():
            missing = code_keys - keys
            assert not missing, (
                f"Keys used in code but missing from {locale}.json: {sorted(missing)}"
            )

    def test_no_orphan_keys_in_json(self):
        """Every key in en.json should be referenced in at least one UI file."""
        code_keys = _collect_keys_from_code()
        en_keys = _collect_keys_from_json()["en"]
        orphans = en_keys - code_keys
        assert not orphans, f"Keys in en.json but never used in code: {sorted(orphans)}"

    def test_code_references_at_least_one_key(self):
        """Sanity: the regex scanner should find a reasonable number of keys."""
        code_keys = _collect_keys_from_code()
        assert len(code_keys) >= 50, (
            f"Expected >=50 translation keys in UI code, found {len(code_keys)}"
        )


# ---------------------------------------------------------------------------
# Regression: doubled asterisk on required-field labels (core#368).
# Connection-config fields that append a literal " *" in code must NOT also
# carry a trailing " *" in their i18n value — otherwise the label renders
# "Label * *" (e.g. "Instance URL * *", "Store Name * *", "Jira Domain * *").
# ---------------------------------------------------------------------------

_CCF = (
    Path(__file__).resolve().parent.parent.parent
    / "datanika"
    / "ui"
    / "components"
    / "connection_config_fields.py"
)
_STAR_APPEND_RE = re.compile(r'_t\["([^"]+)"\],\s*" \*"')


class TestNoDoubledAsterisk:
    """Guard against the doubled-asterisk render bug (core#368)."""

    def test_star_appended_fields_have_no_baked_asterisk(self):
        code = _CCF.read_text(encoding="utf-8")
        appended_keys = set(_STAR_APPEND_RE.findall(code))
        assert appended_keys, (
            "expected to find fields that append ' *' in connection_config_fields.py"
        )
        for locale in SUPPORTED_LOCALES:
            t = get_translations(locale)
            offenders = {k: t[k] for k in appended_keys if k in t and t[k].rstrip().endswith("*")}
            assert not offenders, (
                f"{locale}: fields append ' *' in code AND already end in '*' in i18n "
                f"(renders a doubled asterisk): {offenders}"
            )
