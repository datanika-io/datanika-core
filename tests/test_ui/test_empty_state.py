"""Tests for the reusable EmptyState component and its preset table.

The component itself is a Reflex render function — we don't walk its
internal tree here. What we can test in pure Python is the preset
dictionary that drives it: every page has a preset, every preset uses
the `empty.<page>.<field>` i18n key shape, and the factory returns a
Reflex component without raising.
"""

import pytest

from datanika.ui.components.empty_state import (
    EMPTY_STATE_PRESETS,
    EmptyStatePreset,
    empty_state,
)

EXPECTED_PAGES = ("connections", "pipelines", "transformations")


class TestPresetsCoverThreePages:
    def test_all_three_pages_have_presets(self):
        for page in EXPECTED_PAGES:
            assert page in EMPTY_STATE_PRESETS, f"missing preset for {page}"

    def test_no_extra_presets(self):
        # If someone adds a 4th page, they need to update the test and
        # confirm the i18n keys exist for it.
        assert set(EMPTY_STATE_PRESETS.keys()) == set(EXPECTED_PAGES)


class TestPresetShape:
    @pytest.mark.parametrize("page", EXPECTED_PAGES)
    def test_preset_has_required_fields(self, page):
        preset = EMPTY_STATE_PRESETS[page]
        assert isinstance(preset, EmptyStatePreset)
        assert preset.icon, f"{page} preset is missing an icon"
        assert preset.title_key, f"{page} preset is missing title_key"
        assert preset.body_key, f"{page} preset is missing body_key"
        assert preset.cta_key, f"{page} preset is missing cta_key"
        assert preset.cta_href, f"{page} preset is missing cta_href"

    @pytest.mark.parametrize("page", EXPECTED_PAGES)
    def test_i18n_keys_use_empty_namespace(self, page):
        preset = EMPTY_STATE_PRESETS[page]
        assert preset.title_key == f"empty.{page}.title"
        assert preset.body_key == f"empty.{page}.body"
        assert preset.cta_key == f"empty.{page}.cta"


class TestFactory:
    def test_empty_state_returns_component(self):
        component = empty_state("connections")
        assert component is not None

    def test_empty_state_unknown_key_raises(self):
        with pytest.raises(KeyError):
            empty_state("bogus-page")

    @pytest.mark.parametrize("page", EXPECTED_PAGES)
    def test_empty_state_renders_for_every_page(self, page):
        assert empty_state(page) is not None
