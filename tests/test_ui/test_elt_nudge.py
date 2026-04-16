"""Tests for the ELT nudge card component."""

import json
from pathlib import Path

import pytest


class TestEltNudgeI18nKeys:
    """Verify nudge.* i18n keys exist across all 9 locales."""

    REQUIRED_KEYS = [
        "nudge.title",
        "nudge.message",
        "nudge.savings_hint",
        "nudge.switch_cta",
        "nudge.learn_more",
        "nudge.dismiss",
    ]
    LOCALES = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]

    @pytest.mark.parametrize("locale", LOCALES)
    def test_nudge_keys_in_locale(self, locale: str):
        i18n_dir = Path(__file__).resolve().parent.parent.parent / "datanika" / "i18n"
        path = i18n_dir / f"{locale}.json"
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for key in self.REQUIRED_KEYS:
            assert key in data, f"Missing {key} in {locale}.json"
            assert isinstance(data[key], str), f"{key} in {locale}.json is not a string"
            assert len(data[key]) > 0, f"{key} in {locale}.json is empty"


class TestEltNudgeComponent:
    """Verify the component module imports cleanly."""

    def test_import(self):
        from datanika.ui.components.elt_nudge_card import NUDGE_KEYS

        assert len(NUDGE_KEYS) == 6
        assert "nudge.title" in NUDGE_KEYS

    def test_factory_returns_component(self):
        from datanika.ui.components.elt_nudge_card import elt_nudge_card

        component = elt_nudge_card()
        assert component is not None
