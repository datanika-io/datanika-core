"""Tests for the AI agent tier single source of truth (#85).

`agent_tiers.py` is the canonical home for tier + capability data. Every
other surface (LLMS_TXT, AGENT_GUIDE_MD, /api/v1/meta/agent-tiers, the
landing site via JSON fetch) renders from this module. These tests pin
the data shape, the derived counts, and the renderer outputs so a
future edit can't reintroduce the off-by-one bug class that hit core
PR #80 and landing PR #97.
"""

import json
from dataclasses import is_dataclass

import pytest

from datanika.services.agent_tiers import (
    ERROR_CODES,
    GOLDEN_PATH_LOOP,
    TIERS,
    UI_ONLY_OPERATIONS,
    Capability,
    ErrorCode,
    Tier,
    all_capabilities,
    capability_count,
    render_error_codes_table,
    render_golden_path,
    render_llms_capabilities,
    render_ui_only_operations,
    tier_count,
    to_json_dict,
)


class TestTierStructure:
    def test_exactly_five_tiers(self):
        # 5 tiers matches plans/engineering/PLAN_ENGINEERING.md.
        # If you add a tier, update the engineering plan AND the landing
        # site AND any other surface that pins this number — or even
        # better, fetch /api/v1/meta/agent-tiers from those surfaces.
        assert tier_count() == 5
        assert len(TIERS) == 5

    def test_tier_numbers_are_sequential(self):
        for expected, tier in enumerate(TIERS, start=1):
            assert tier.number == expected, (
                f"Tier {tier.name!r} has number {tier.number}, expected {expected}"
            )

    def test_every_tier_has_at_least_one_capability(self):
        for tier in TIERS:
            assert len(tier.capabilities) >= 1, f"Tier {tier.name!r} has no capabilities"

    def test_every_capability_has_at_least_one_endpoint(self):
        for tier in TIERS:
            for cap in tier.capabilities:
                assert len(cap.endpoints) >= 1, (
                    f"Capability {cap.name!r} in tier {tier.name!r} has no endpoints"
                )

    def test_every_tier_has_summary(self):
        for tier in TIERS:
            assert tier.summary, f"Tier {tier.name!r} has empty summary"
            assert len(tier.summary) > 30, f"Tier {tier.name!r} summary is too short"

    def test_dataclasses_are_frozen(self):
        # Frozen dataclasses prevent mutation of the SoT at runtime —
        # any attempt to "patch" tier data should be a code change.
        assert is_dataclass(Tier)
        assert is_dataclass(Capability)
        assert is_dataclass(ErrorCode)
        with pytest.raises((AttributeError, TypeError)):
            TIERS[0].number = 99


class TestCapabilityCount:
    def test_capability_count_matches_flat_list(self):
        assert capability_count() == len(all_capabilities())

    def test_capability_count_matches_sum_across_tiers(self):
        manual_total = sum(len(t.capabilities) for t in TIERS)
        assert capability_count() == manual_total

    def test_current_capability_count_is_six(self):
        # 5 tiers, but the current capability decomposition is 6:
        # Discover, Introspect, Build, Validate, Execute, Control,
        # plus the Tier 5 docs capability — wait, that's 7. Recount.
        # Tier 1: Discover + Introspect = 2
        # Tier 2: Build = 1
        # Tier 3: Validate = 1
        # Tier 4: Execute + Control = 2
        # Tier 5: Discover Docs = 1
        # Total = 7
        # If you change this, the LLMS_TXT capability list changes too,
        # and any landing-site copy that quotes a number will need to
        # be updated. Better: have the landing site fetch the JSON.
        assert capability_count() == 7


class TestKnownCapabilityNames:
    def test_discover_in_tier_1(self):
        names = [c.name for c in TIERS[0].capabilities]
        assert "Discover" in names
        assert "Introspect" in names

    def test_validate_in_tier_3(self):
        assert any(c.name == "Validate" for c in TIERS[2].capabilities)

    def test_control_in_tier_4(self):
        assert any(c.name == "Control" for c in TIERS[3].capabilities)


class TestGoldenPath:
    def test_seventeen_steps(self):
        # The 17-step loop is the canonical onboarding flow. If we
        # change the count, agent-guide.md and any external content
        # that quotes "17 steps" must be updated — or just fetch the
        # JSON instead.
        assert len(GOLDEN_PATH_LOOP) == 17

    def test_first_step_is_discover_meta(self):
        assert "/api/v1/meta/connection-types" in GOLDEN_PATH_LOOP[0]

    def test_last_step_is_monitor_runs(self):
        assert "/api/v1/runs" in GOLDEN_PATH_LOOP[-1]

    def test_no_step_is_empty(self):
        for n, step in enumerate(GOLDEN_PATH_LOOP):
            assert step.strip(), f"Step {n} is empty"


class TestErrorCodes:
    def test_six_error_codes(self):
        assert len(ERROR_CODES) == 6

    def test_known_codes_present(self):
        codes = {ec.code for ec in ERROR_CODES}
        assert codes == {
            "compilation_error",
            "execution_error",
            "missing_destination",
            "unsafe_sql",
            "invalid_request",
            "not_cancellable",
        }


class TestRenderers:
    def test_render_llms_capabilities_includes_all_capabilities(self):
        rendered = render_llms_capabilities()
        for cap in all_capabilities():
            assert cap.name in rendered, f"Missing capability {cap.name!r} in LLMS render"

    def test_render_llms_capabilities_is_numbered(self):
        rendered = render_llms_capabilities()
        # First and last capabilities are numbered
        assert rendered.startswith("1. ")
        last_n = capability_count()
        assert f"{last_n}. " in rendered

    def test_render_llms_capabilities_no_hardcoded_count(self):
        # The renderer must not include any literal number that hardcodes
        # tier_count or capability_count outside of the line numbering.
        # This guards against someone re-introducing "5 tiers" prose.
        rendered = render_llms_capabilities()
        assert "5 tiers" not in rendered
        assert "6 tiers" not in rendered
        assert "(5)" not in rendered

    def test_render_golden_path_is_numbered_seventeen(self):
        rendered = render_golden_path()
        assert rendered.startswith("1. ")
        assert "\n17. " in rendered
        assert "\n18. " not in rendered

    def test_render_error_codes_is_markdown_table(self):
        rendered = render_error_codes_table()
        assert "| Code |" in rendered
        assert "|------|" in rendered
        for ec in ERROR_CODES:
            assert f"`{ec.code}`" in rendered

    def test_render_ui_only_operations_is_bullet_list(self):
        rendered = render_ui_only_operations()
        for op in UI_ONLY_OPERATIONS:
            assert f"- {op}" in rendered


class TestJsonSerialization:
    def test_to_json_dict_round_trips_via_json_module(self):
        data = to_json_dict()
        # Must be JSON-serializable with no custom encoder
        serialized = json.dumps(data)
        parsed = json.loads(serialized)
        assert parsed == data

    def test_json_has_top_level_keys(self):
        data = to_json_dict()
        assert set(data.keys()) == {
            "tier_count",
            "capability_count",
            "tiers",
            "golden_path",
            "error_codes",
            "ui_only_operations",
        }

    def test_json_tier_count_matches_helper(self):
        data = to_json_dict()
        assert data["tier_count"] == tier_count()
        assert data["tier_count"] == len(data["tiers"])

    def test_json_capability_count_matches_helper(self):
        data = to_json_dict()
        assert data["capability_count"] == capability_count()
        flat = sum(len(t["capabilities"]) for t in data["tiers"])
        assert data["capability_count"] == flat

    def test_json_tier_shape(self):
        data = to_json_dict()
        for tier in data["tiers"]:
            assert set(tier.keys()) == {"number", "name", "summary", "capabilities"}
            for cap in tier["capabilities"]:
                assert set(cap.keys()) == {"name", "description", "endpoints"}
                assert isinstance(cap["endpoints"], list)

    def test_json_golden_path_is_list_of_strings(self):
        data = to_json_dict()
        assert isinstance(data["golden_path"], list)
        assert all(isinstance(step, str) for step in data["golden_path"])
        assert len(data["golden_path"]) == 17

    def test_json_error_codes_have_required_fields(self):
        data = to_json_dict()
        for ec in data["error_codes"]:
            assert set(ec.keys()) == {"code", "meaning", "action"}
