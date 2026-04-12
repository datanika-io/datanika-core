"""Tests for the contextual tooltip component and its i18n key registry."""

import pytest

from datanika.ui.components.info_tooltip import TOOLTIP_KEYS, info_tooltip

EXPECTED_KEYS = (
    "tooltip.write_disposition",
    "tooltip.write_disposition_append",
    "tooltip.write_disposition_replace",
    "tooltip.write_disposition_merge",
    "tooltip.incremental_cursor",
    "tooltip.schema_contract",
    "tooltip.load_mode",
    "tooltip.materialization",
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

    def test_exactly_eight_tooltips(self):
        assert len(TOOLTIP_KEYS) == 8


class TestInfoTooltipFactory:
    def test_returns_component(self):
        component = info_tooltip("tooltip.write_disposition")
        assert component is not None

    @pytest.mark.parametrize("key", EXPECTED_KEYS)
    def test_renders_for_every_registered_key(self, key):
        assert info_tooltip(key) is not None
