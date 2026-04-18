"""Tests for the billing preview modal component."""

import json
from pathlib import Path

import pytest


class TestBillingPreviewI18nKeys:
    """Verify billing.* i18n keys exist across all 9 locales."""

    REQUIRED_KEYS = [
        "billing.preview_title",
        "billing.plan_label",
        "billing.cycle_label",
        "billing.volume_used",
        "billing.volume_included",
        "billing.overage_gb",
        "billing.overage_cost",
        "billing.projected_total",
        "billing.no_overage",
        "billing.close",
        # V2 P5 Option B additions
        "billing.cycle.closes_on",
        "billing.cycle.closes_in_days",
        "billing.cycle.closes_today",
        "billing.projected_overage.heading",
        "billing.projected_overage.see_notification",
        # charge_incoming notification
        "notifications.charge_incoming.title",
        "notifications.charge_incoming.body",
    ]
    LOCALES = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]

    @pytest.mark.parametrize("locale", LOCALES)
    def test_billing_keys_in_locale(self, locale: str):
        i18n_dir = Path(__file__).resolve().parent.parent.parent / "datanika" / "i18n"
        path = i18n_dir / f"{locale}.json"
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        for key in self.REQUIRED_KEYS:
            assert key in data, f"Missing {key} in {locale}.json"
            assert isinstance(data[key], str), f"{key} in {locale}.json is not a string"
            assert len(data[key]) > 0, f"{key} in {locale}.json is empty"


class TestBillingPreviewComponent:
    """Verify the component module imports cleanly."""

    def test_import(self):
        from datanika.ui.components.billing_preview_modal import BILLING_KEYS

        # 10 original + 5 V2 P5 Option B additions (cycle close + projected overage)
        assert len(BILLING_KEYS) == 15
        assert "billing.preview_title" in BILLING_KEYS
        assert "billing.cycle.closes_on" in BILLING_KEYS
        assert "billing.projected_overage.heading" in BILLING_KEYS

    def test_factory_returns_component(self):
        from datanika.ui.components.billing_preview_modal import billing_preview_modal

        component = billing_preview_modal()
        assert component is not None
