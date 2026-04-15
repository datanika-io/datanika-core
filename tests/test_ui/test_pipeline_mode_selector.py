"""Tests for the V2 pricing-pivot pipeline mode selector component."""

import json
from pathlib import Path


class TestModeSelectorImport:
    def test_module_imports(self):
        from datanika.ui.components import pipeline_mode_selector

        assert hasattr(pipeline_mode_selector, "pipeline_mode_selector")
        assert callable(pipeline_mode_selector.pipeline_mode_selector)

    def test_builds_component(self):
        from datanika.ui.components.pipeline_mode_selector import pipeline_mode_selector

        component = pipeline_mode_selector()
        assert component is not None


class TestModeSelectorI18nKeys:
    """The 17 mode-selector i18n keys must be present in every locale."""

    def test_mode_keys_in_all_locales(self):
        i18n_dir = Path(__file__).resolve().parent.parent.parent / "datanika" / "i18n"
        required_keys = [
            "pipelines.mode_label",
            "pipelines.mode_auto",
            "pipelines.mode_auto_hint",
            "pipelines.mode_etl",
            "pipelines.mode_etl_hint",
            "pipelines.mode_elt",
            "pipelines.mode_elt_hint",
            "pipelines.mode_advanced_toggle",
            "pipelines.mode_tooltip",
            "pipelines.mode_resolved_prefix",
            "pipelines.mode_elt_requires_warehouse",
            "pipelines.mode_elt_source_unsupported",
            "pipelines.mode_change_confirm_title",
            "pipelines.mode_change_confirm_body",
            "pipelines.volume_estimate_label",
            "pipelines.volume_estimate_placeholder",
            "pipelines.volume_estimate_hint",
        ]
        locales = ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"]

        for locale in locales:
            path = i18n_dir / f"{locale}.json"
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            for key in required_keys:
                assert key in data, f"Missing key '{key}' in {locale}.json"
                assert data[key], f"Empty value for '{key}' in {locale}.json"
