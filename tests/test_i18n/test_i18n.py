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

# ---------------------------------------------------------------------------
# Keys that are DELIBERATELY unreferenced, and must NOT be deleted.
#
# Every comment above this one is about a *false* orphan — a key the scanner
# could not see through a new indirection — and each ends with the same
# warning: the documented remedy for an orphan is to DELETE the key, which
# silently drops the translation in all nine locales. This constant is for the
# other case, which those four comments do not cover and which #1540 produced:
# a key that is **genuinely** unreferenced on purpose.
#
# `volume_quota_modal.py` painted two templates raw. Neither can be filled from
# anything the UI holds — one needs the size of the run being refused, the other
# needs the NEXT tier's name and allowance — and substituting only the fillable
# half renders a confident sentence with a rendering bug inside it. So the modal
# renders placeholder-free keys and these two stay translated, unused, for
# whenever a producer exists.
#
# 🔑 This is a REPOINT, not a loophole (WORKFLOW_RULES §5a rule 2). The
# invariant `test_no_orphan_keys_in_json` protects is *"no key is dead weight
# nobody can account for"*, and an entry here accounts for one. It is held from
# BOTH sides by `test_placeholders_are_substituted.py`, which fails if a
# reserved key gains a reference (stale exemption) or stops existing (stale
# exemption) — so an exemption cannot quietly outlive its reason, which is the
# only thing that would make this a hole.
RESERVED_UNUSED_KEYS = {
    "quota.volume_quota_reached_body": (
        "Needs {needed} — the projected size of the run being refused. No producer exists: "
        "BaseState carries is_quota_error and quota_metric and nothing else about the run. "
        "Substituting {plan} and {remaining} and leaving {needed} would paint a confident "
        "sentence with a rendering bug in the middle of it, which is worse than the template. "
        "The modal renders the placeholder-free quota.upgrade_hint instead; the key stays "
        "translated in all nine locales for when a producer exists. #1540."
    ),
    "quota.upgrade_to_next_tier": (
        "Needs the NEXT tier's {plan} and {gb}. DashboardState.plan_name is the CURRENT plan, so "
        "substituting it renders 'Upgrade to Free (... included)' to a Free user — worse than "
        "braces. No next-tier data reaches the UI. The modal renders the placeholder-free "
        "quota.upgrade_button instead. #1540."
    ),
}


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
        """Every key in en.json is referenced in a UI file, or declared reserved.

        ⚠️ **Before deleting a key this reports, read the four comments above
        `_collect_keys_from_code`.** Four times now the answer has been that the
        scanner could not see a new indirection, and the "obvious" remedy would
        have dropped nine translations while every check stayed green.
        """
        code_keys = _collect_keys_from_code()
        en_keys = _collect_keys_from_json()["en"]
        orphans = en_keys - code_keys - set(RESERVED_UNUSED_KEYS)
        assert not orphans, (
            f"Keys in en.json but never used in code: {sorted(orphans)}.\n"
            "Three possibilities, in the order they have actually occurred here:\n"
            "  1. The scanner cannot see a new indirection — add the pattern above, do NOT "
            "delete the key (this has been the answer four times).\n"
            "  2. The key is deliberately reserved for a producer that does not exist yet — "
            "add it to RESERVED_UNUSED_KEYS with the reason.\n"
            "  3. It really is dead — delete it from all nine locale files."
        )

    def test_reserved_keys_are_still_unreferenced(self):
        """A reserved key that gained a reference has a stale exemption.

        The exemption exists because nothing paints the key. The moment something
        does, the reason is gone and the entry must go with it — otherwise an
        exemption outlives what justified it, which is the only way this becomes
        a hole rather than a repoint.
        """
        code_keys = _collect_keys_from_code()
        stale = sorted(k for k in RESERVED_UNUSED_KEYS if k in code_keys)
        assert not stale, (
            f"RESERVED_UNUSED_KEYS entries that are now referenced in code: {stale}. "
            "Delete the entry — the key is live again."
        )

    def test_reserved_keys_still_exist_and_carry_a_reason(self):
        en_keys = _collect_keys_from_json()["en"]
        missing = sorted(k for k in RESERVED_UNUSED_KEYS if k not in en_keys)
        assert not missing, (
            f"RESERVED_UNUSED_KEYS entries no longer in en.json: {missing}. Delete the entry."
        )
        for key, reason in RESERVED_UNUSED_KEYS.items():
            assert len(reason) > 80, f"{key}: an exemption without a reason is just a hole"

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
