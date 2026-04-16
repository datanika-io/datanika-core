"""Tests for the cost estimator card component."""

import json
from pathlib import Path

import pytest


class TestCostEstimatorI18nKeys:
    """Verify cost.* i18n keys exist across all 9 locales."""

    REQUIRED_KEYS = [
        "cost.title",
        "cost.estimate_label",
        "cost.per_gb_rate",
        "cost.total_estimate",
        "cost.mode_split_hint",
        "cost.unknown",
        "cost.free_included",
        "cost.learn_more",
    ]
    LOCALES = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]

    @pytest.mark.parametrize("locale", LOCALES)
    def test_cost_keys_in_locale(self, locale: str):
        i18n_dir = Path(__file__).resolve().parent.parent.parent / "datanika" / "i18n"
        path = i18n_dir / f"{locale}.json"
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for key in self.REQUIRED_KEYS:
            assert key in data, f"Missing {key} in {locale}.json"
            assert isinstance(data[key], str), f"{key} in {locale}.json is not a string"
            assert len(data[key]) > 0, f"{key} in {locale}.json is empty"


class TestCostEstimatorComponent:
    """Verify the component module imports cleanly and exposes the right keys."""

    def test_import(self):
        from datanika.ui.components.cost_estimator_card import COST_KEYS

        assert len(COST_KEYS) == 8
        assert "cost.title" in COST_KEYS

    def test_factory_returns_component(self):
        from datanika.ui.components.cost_estimator_card import cost_estimator_card

        component = cost_estimator_card()
        assert component is not None
