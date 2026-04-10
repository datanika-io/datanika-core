"""Tests for usage bar on dashboard."""

import json
from pathlib import Path

from datanika.hooks import clear, emit, on


class TestUsageHook:
    """Test that usage.get_summary hook populates context."""

    def setup_method(self):
        clear()

    def teardown_method(self):
        clear()

    def test_no_handler_returns_empty_context(self):
        ctx = {"org_id": 1, "runs_used": 0, "runs_limit": 0, "plan_name": ""}
        emit("usage.get_summary", context=ctx)
        assert ctx["runs_used"] == 0
        assert ctx["runs_limit"] == 0
        assert ctx["plan_name"] == ""

    def test_handler_populates_context(self):
        def fake_handler(*, context, **_kw):
            context["runs_used"] = 380
            context["runs_limit"] = 500
            context["plan_name"] = "Free"

        on("usage.get_summary", fake_handler)

        ctx = {"org_id": 1, "runs_used": 0, "runs_limit": 0, "plan_name": ""}
        emit("usage.get_summary", context=ctx)
        assert ctx["runs_used"] == 380
        assert ctx["runs_limit"] == 500
        assert ctx["plan_name"] == "Free"

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


class TestUsageI18nKeys:
    """Verify usage bar i18n keys exist in all locale files."""

    def test_usage_keys_in_all_locales(self):
        i18n_dir = Path(__file__).resolve().parent.parent.parent / "datanika" / "i18n"
        required_keys = [
            "dashboard.usage_title",
            "dashboard.usage_runs",
            "dashboard.usage_upgrade",
        ]
        locales = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]

        for locale in locales:
            path = i18n_dir / f"{locale}.json"
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            for key in required_keys:
                assert key in data, f"Missing key '{key}' in {locale}.json"
                assert data[key], f"Empty value for '{key}' in {locale}.json"
