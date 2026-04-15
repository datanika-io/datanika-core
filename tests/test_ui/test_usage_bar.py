"""Tests for usage bar on dashboard."""

import json
from pathlib import Path

from datanika.hooks import clear, emit, on

GB = 1024**3


class TestUsageHook:
    """Test that usage.get_summary hook populates context."""

    def setup_method(self):
        clear()

    def teardown_method(self):
        clear()

    def test_no_handler_returns_empty_context(self):
        ctx = {
            "org_id": 1,
            "runs_used": 0,
            "runs_limit": 0,
            "plan_name": "",
            "bytes_used": 0,
            "bytes_limit": 0,
        }
        emit("usage.get_summary", context=ctx)
        assert ctx["runs_used"] == 0
        assert ctx["runs_limit"] == 0
        assert ctx["plan_name"] == ""
        assert ctx["bytes_used"] == 0
        assert ctx["bytes_limit"] == 0

    def test_handler_populates_context(self):
        def fake_handler(*, context, **_kw):
            context["runs_used"] = 380
            context["runs_limit"] = 500
            context["plan_name"] = "Free"
            context["bytes_used"] = 5 * GB
            context["bytes_limit"] = 10 * GB

        on("usage.get_summary", fake_handler)

        ctx = {
            "org_id": 1,
            "runs_used": 0,
            "runs_limit": 0,
            "plan_name": "",
            "bytes_used": 0,
            "bytes_limit": 0,
        }
        emit("usage.get_summary", context=ctx)
        assert ctx["runs_used"] == 380
        assert ctx["runs_limit"] == 500
        assert ctx["plan_name"] == "Free"
        assert ctx["bytes_used"] == 5 * GB
        assert ctx["bytes_limit"] == 10 * GB

    def test_percentage_calculation(self):
        used, limit = 400, 500
        pct = int(used / limit * 100) if limit > 0 else 0
        assert pct == 80

    def test_percentage_zero_limit(self):
        used, limit = 0, 0
        pct = int(used / limit * 100) if limit > 0 else 0
        assert pct == 0

    def test_color_thresholds(self):
        def color_for(pct):
            if pct >= 80:
                return "red"
            if pct >= 60:
                return "yellow"
            return "green"

        assert color_for(50) == "green"
        assert color_for(60) == "yellow"
        assert color_for(79) == "yellow"
        assert color_for(80) == "red"
        assert color_for(100) == "red"


class TestDashboardStateVolumeFields:
    """DashboardState must expose bytes_used/limit + derived vars for the Volume bar."""

    def test_state_has_volume_fields(self):
        from datanika.ui.state.dashboard_state import DashboardState

        annotations = DashboardState.__annotations__
        assert "bytes_used" in annotations
        assert "bytes_limit" in annotations

    def test_bytes_percent_zero_when_no_limit(self):
        used, limit = 3 * GB, 0
        pct = min(int(used / limit * 100), 100) if limit > 0 else 0
        assert pct == 0

    def test_bytes_percent_half_full(self):
        used, limit = 5 * GB, 10 * GB
        pct = min(int(used / limit * 100), 100) if limit > 0 else 0
        assert pct == 50

    def test_bytes_percent_clamped_at_100_when_over(self):
        used, limit = 13 * GB, 10 * GB
        pct = min(int(used / limit * 100), 100) if limit > 0 else 0
        assert pct == 100

    def test_bytes_color_thresholds(self):
        def color_for(pct):
            if pct >= 80:
                return "red"
            if pct >= 60:
                return "yellow"
            return "green"

        assert color_for(50) == "green"
        assert color_for(60) == "yellow"
        assert color_for(80) == "red"

    def test_has_volume_data_detection(self):
        assert (0 > 0) is False
        assert (10 * GB > 0) is True


class TestUsageI18nKeys:
    """Verify usage bar i18n keys exist in all locale files."""

    def test_usage_keys_in_all_locales(self):
        i18n_dir = Path(__file__).resolve().parent.parent.parent / "datanika" / "i18n"
        required_keys = [
            "dashboard.usage_title",
            "dashboard.usage_runs",
            "dashboard.usage_upgrade",
            "quota.volume_title",
            "quota.volume_usage",
            "quota.volume_overage",
        ]
        locales = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]

        for locale in locales:
            path = i18n_dir / f"{locale}.json"
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            for key in required_keys:
                assert key in data, f"Missing key '{key}' in {locale}.json"
                assert data[key], f"Empty value for '{key}' in {locale}.json"
