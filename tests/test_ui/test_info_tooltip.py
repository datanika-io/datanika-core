"""Tests for the contextual tooltip component and its i18n key registry.

⚠️ **`EXPECTED_KEYS` is a deliberate second source and stays.** It catches a key
vanishing from `TOOLTIP_KEYS`, which a self-derived check never could.

🚨 **What did NOT stay is `test_exactly_eight_tooltips`, which asserted
`len(TOOLTIP_KEYS) == 8`.** A literal count is not an invariant — it records
which change shipped last, and it went red on core#1170 AC4.3 adding a ninth
tooltip *with* its key and its nine translations, i.e. on exactly the thing this
file exists to require. Replaced by an assertion tied to `EXPECTED_KEYS`, so the
two sources still have to agree and neither is a number someone has to remember
to bump.
"""

import json
import pathlib

import pytest

import datanika.ui
from datanika.ui.components.info_tooltip import TOOLTIP_KEYS, info_tooltip

LOCALES = ("en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr")
I18N_DIR = pathlib.Path(datanika.ui.__file__).parent.parent / "i18n"

EXPECTED_KEYS = (
    "tooltip.write_disposition",
    "tooltip.write_disposition_append",
    "tooltip.write_disposition_replace",
    "tooltip.write_disposition_merge",
    "tooltip.incremental_cursor",
    "tooltip.schema_contract",
    "tooltip.load_mode",
    "tooltip.materialization",
    "tooltip.rows_loaded",
)


class TestTooltipKeys:
    def test_all_expected_keys_registered(self):
        for key in EXPECTED_KEYS:
            assert key in TOOLTIP_KEYS, f"missing tooltip key: {key}"

    def test_no_extra_keys(self):
        assert set(TOOLTIP_KEYS) == set(EXPECTED_KEYS)

    @pytest.mark.parametrize("key", EXPECTED_KEYS)
    def test_keys_use_tooltip_namespace(self, key):
        assert key.startswith("tooltip.")

    def test_the_registry_has_no_duplicates_and_matches_the_expected_set(self):
        """Replaces `len(TOOLTIP_KEYS) == 8`. No literal to remember to bump."""
        assert len(TOOLTIP_KEYS) == len(set(TOOLTIP_KEYS)), "a key is registered twice"
        assert len(TOOLTIP_KEYS) == len(EXPECTED_KEYS)

    @pytest.mark.parametrize("locale", LOCALES)
    def test_every_registered_tooltip_resolves_in_every_locale(self, locale):
        """The invariant the count never carried: a registered key must SAY something.

        A tooltip whose key is missing or empty renders a blank bubble, which is
        worse than no tooltip — the user hovers deliberately and is told nothing.
        """
        data = json.loads((I18N_DIR / f"{locale}.json").read_text(encoding="utf-8"))
        en = json.loads((I18N_DIR / "en.json").read_text(encoding="utf-8"))
        for key in TOOLTIP_KEYS:
            assert key in data, f"{locale}.json is missing {key}"
            assert data[key].strip(), f"{locale}.json has an empty {key}"
            if locale != "en":
                assert data[key] != en[key], f"{locale}.json's {key} is the English verbatim"


class TestInfoTooltipFactory:
    def test_returns_component(self):
        component = info_tooltip("tooltip.write_disposition")
        assert component is not None

    @pytest.mark.parametrize("key", EXPECTED_KEYS)
    def test_renders_for_every_registered_key(self, key):
        assert info_tooltip(key) is not None
