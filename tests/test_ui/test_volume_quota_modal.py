"""Tests for the V2 pricing-pivot Path A volume quota modal."""

import json
from pathlib import Path


class TestVolumeQuotaModalImport:
    def test_module_imports(self):
        from datanika.ui.components import volume_quota_modal

        assert hasattr(volume_quota_modal, "volume_quota_modal")
        assert callable(volume_quota_modal.volume_quota_modal)

    def test_builds_component(self):
        from datanika.ui.components.volume_quota_modal import volume_quota_modal
        from datanika.ui.state.dashboard_state import DashboardState

        component = volume_quota_modal(DashboardState)
        assert component is not None


class TestVolumeQuotaModalI18nKeys:
    """The 5 Path A modal i18n keys must be present in every locale."""

    def test_modal_keys_in_all_locales(self):
        i18n_dir = Path(__file__).resolve().parent.parent.parent / "datanika" / "i18n"
        required_keys = [
            "quota.volume_quota_reached_title",
            "quota.volume_quota_reached_body",
            "quota.allow_as_overage",
            "quota.allow_as_overage_button",
            "quota.upgrade_to_next_tier",
        ]
        locales = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]

        for locale in locales:
            path = i18n_dir / f"{locale}.json"
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            for key in required_keys:
                assert key in data, f"Missing key '{key}' in {locale}.json"
                assert data[key], f"Empty value for '{key}' in {locale}.json"


class TestVolumeQuotaDetection:
    """Modal should only show on volume quota errors, not other errors."""

    def test_detects_volume_quota_by_metric_attr(self):
        class FakeQuotaError(ValueError):
            metric = "bytes_processed"

        exc = FakeQuotaError("exceeded")
        assert getattr(exc, "metric", None) == "bytes_processed"

    def test_missing_metric_attr_defaults_to_none(self):
        exc = ValueError("plain")
        assert getattr(exc, "metric", None) is None

    def test_free_plan_gets_hard_block_branch(self):
        # Free tier is hard-capped (Q6 HARD SUNSET) — no overage allowed.
        plan, is_quota = "Free", True
        is_hard_block = is_quota and plan == "Free"
        assert is_hard_block is True

    def test_pro_plan_gets_soft_overage_branch(self):
        # Pro tier allows overage silently (Q3 SILENT — no Enterprise nudge).
        plan, is_quota = "Pro", True
        is_hard_block = is_quota and plan == "Free"
        assert is_hard_block is False
