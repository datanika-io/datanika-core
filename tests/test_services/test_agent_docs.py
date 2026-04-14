"""Tests for /llms.txt, /api/v1/agent-guide.md, /api/v1/meta/agent-tiers (#64, #85, #124)."""

from pathlib import Path

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.services.agent_docs import (
    AGENT_GUIDE_MD,
    DEFAULT_ASSETS_DIR,
    LLMS_TXT,
    agent_doc_routes,
    write_llms_txt_asset,
)
from datanika.services.agent_tiers import (
    all_capabilities,
    capability_count,
    tier_count,
)


@pytest.fixture
def client():
    app = Starlette(routes=agent_doc_routes)
    return TestClient(app)


class TestLlmsTxt:
    def test_returns_200(self, client):
        resp = client.get("/llms.txt")
        assert resp.status_code == 200

    def test_content_type_is_text_plain(self, client):
        resp = client.get("/llms.txt")
        assert "text/plain" in resp.headers["content-type"]

    def test_contains_openapi_spec_url(self, client):
        resp = client.get("/llms.txt")
        assert "openapi.json" in resp.text

    def test_contains_base_url(self, client):
        resp = client.get("/llms.txt")
        assert "https://app.datanika.io/api/v1/" in resp.text

    def test_contains_agent_guide_link(self, client):
        resp = client.get("/llms.txt")
        assert "agent-guide.md" in resp.text

    def test_contains_auth_instructions(self, client):
        resp = client.get("/llms.txt")
        assert "Bearer" in resp.text
        assert "API key" in resp.text

    def test_contains_capabilities(self, client):
        resp = client.get("/llms.txt")
        assert "Discover" in resp.text
        assert "Introspect" in resp.text
        assert "Validate" in resp.text
        assert "Execute" in resp.text

    def test_capabilities_header_has_no_tier_count(self, client):
        # Regression for #80: header used to say "Capabilities (5 tiers)"
        # while listing 6 items. The SoT renderer in agent_tiers.py never
        # emits a hardcoded count — this assertion guards against either
        # the old "5 tiers" or any new "N tiers" label being re-introduced.
        resp = client.get("/llms.txt")
        assert "## Capabilities" in resp.text
        assert "5 tiers" not in resp.text
        assert "6 tiers" not in resp.text
        assert "(5 tiers)" not in resp.text

    def test_lists_every_capability_from_sot(self, client):
        # Every capability defined in agent_tiers.TIERS must appear in
        # the rendered LLMS_TXT. If you add a new capability to the
        # SoT, this test passes automatically — no llms.txt edit needed.
        resp = client.get("/llms.txt")
        for cap in all_capabilities():
            assert cap.name in resp.text, f"Capability {cap.name!r} missing from /llms.txt"

    def test_advertises_agent_tiers_json_endpoint(self, client):
        resp = client.get("/llms.txt")
        assert "/api/v1/meta/agent-tiers" in resp.text

    def test_no_auth_required(self, client):
        resp = client.get("/llms.txt")
        assert resp.status_code == 200


class TestAgentGuide:
    def test_returns_200(self, client):
        resp = client.get("/api/v1/agent-guide.md")
        assert resp.status_code == 200

    def test_content_type_is_markdown(self, client):
        resp = client.get("/api/v1/agent-guide.md")
        assert "text/markdown" in resp.headers["content-type"]

    def test_contains_golden_path_endpoints(self, client):
        resp = client.get("/api/v1/agent-guide.md")
        text = resp.text
        assert "/meta/connection-types" in text
        assert "/connections/{id}/introspect" in text
        assert "/uploads" in text
        assert "/transformations/{id}/compile" in text
        assert "/transformations/{id}/preview" in text
        assert "/schedules" in text
        assert "/catalog" in text

    def test_contains_error_codes(self, client):
        resp = client.get("/api/v1/agent-guide.md")
        for code in [
            "compilation_error",
            "execution_error",
            "missing_destination",
            "unsafe_sql",
            "not_cancellable",
        ]:
            assert code in resp.text, f"Missing error code: {code}"

    def test_contains_idempotency_section(self, client):
        resp = client.get("/api/v1/agent-guide.md")
        assert "Idempotency-Key" in resp.text

    def test_contains_what_api_cannot_do(self, client):
        resp = client.get("/api/v1/agent-guide.md")
        assert "Cannot Do" in resp.text
        assert "User management" in resp.text
        assert "File uploads" in resp.text

    def test_contains_wait_true_pattern(self, client):
        resp = client.get("/api/v1/agent-guide.md")
        assert "?wait=true" in resp.text

    def test_no_auth_required(self, client):
        resp = client.get("/api/v1/agent-guide.md")
        assert resp.status_code == 200


class TestAgentTiersJson:
    def test_returns_200(self, client):
        resp = client.get("/api/v1/meta/agent-tiers")
        assert resp.status_code == 200

    def test_content_type_is_json(self, client):
        resp = client.get("/api/v1/meta/agent-tiers")
        assert "application/json" in resp.headers["content-type"]

    def test_no_auth_required(self, client):
        # Same posture as /llms.txt — pre-auth discovery surface.
        resp = client.get("/api/v1/meta/agent-tiers")
        assert resp.status_code == 200

    def test_top_level_shape(self, client):
        data = client.get("/api/v1/meta/agent-tiers").json()
        assert set(data.keys()) == {
            "tier_count",
            "capability_count",
            "tiers",
            "golden_path",
            "error_codes",
            "ui_only_operations",
        }

    def test_counts_match_sot(self, client):
        data = client.get("/api/v1/meta/agent-tiers").json()
        assert data["tier_count"] == tier_count()
        assert data["capability_count"] == capability_count()

    def test_tier_count_is_five(self, client):
        data = client.get("/api/v1/meta/agent-tiers").json()
        assert data["tier_count"] == 5
        assert len(data["tiers"]) == 5

    def test_tiers_are_numbered_sequentially(self, client):
        data = client.get("/api/v1/meta/agent-tiers").json()
        for expected, tier in enumerate(data["tiers"], start=1):
            assert tier["number"] == expected


class TestSoTWiring:
    """Guard rails ensuring agent_docs.py renders from the SoT, not hardcoded text."""

    def test_llms_txt_module_constant_is_rendered_from_sot(self):
        # Every capability name should appear in the module-level constant.
        # If a future edit hardcodes the markdown back, the SoT addition
        # of a new capability would fail this test.
        for cap in all_capabilities():
            assert cap.name in LLMS_TXT, f"Capability {cap.name!r} missing from LLMS_TXT"

    def test_agent_guide_md_includes_all_error_codes(self):
        from datanika.services.agent_tiers import ERROR_CODES

        for ec in ERROR_CODES:
            assert ec.code in AGENT_GUIDE_MD, f"Error code {ec.code!r} missing from agent guide"

    def test_agent_guide_md_includes_all_golden_path_steps(self):
        from datanika.services.agent_tiers import GOLDEN_PATH_LOOP

        for step in GOLDEN_PATH_LOOP:
            # Step is a backtick-wrapped endpoint plus prose; the
            # endpoint substring is the stable part to assert on.
            endpoint = step.split("`")[1] if "`" in step else step
            assert endpoint in AGENT_GUIDE_MD, f"Golden path step {endpoint!r} missing"


class TestLlmsTxtStaticAsset:
    """Regression for issue #124.

    `/llms.txt` is referenced from published landing content, blog
    posts, and four comparison pages. It must be reachable at
    `https://app.datanika.io/llms.txt` — which means it has to be
    served by the Reflex frontend (port 3000), not the Starlette
    backend, because nginx routes the root to the frontend. We
    materialise the rendered LLMS_TXT into the Reflex assets dir at
    app startup; these tests pin that contract so the fix can't drift.
    """

    def test_write_llms_txt_asset_creates_file(self, tmp_path):
        target = write_llms_txt_asset(assets_dir=tmp_path)
        assert target.exists()
        assert target == tmp_path / "llms.txt"

    def test_asset_content_matches_module_constant(self, tmp_path):
        target = write_llms_txt_asset(assets_dir=tmp_path)
        assert target.read_text(encoding="utf-8") == LLMS_TXT

    def test_asset_is_non_empty_and_has_openapi_url(self, tmp_path):
        target = write_llms_txt_asset(assets_dir=tmp_path)
        content = target.read_text(encoding="utf-8")
        assert len(content) > 500
        assert "openapi.json" in content

    def test_asset_is_idempotent(self, tmp_path):
        """Calling twice leaves the same file, same content."""
        first = write_llms_txt_asset(assets_dir=tmp_path)
        second = write_llms_txt_asset(assets_dir=tmp_path)
        assert first == second
        assert first.read_text(encoding="utf-8") == LLMS_TXT

    def test_default_assets_dir_is_project_root_assets(self):
        """The default must resolve to a sibling `assets/` directory
        next to the `datanika/` package, not a subdir inside it."""
        assert DEFAULT_ASSETS_DIR.name == "assets"
        assert DEFAULT_ASSETS_DIR.is_absolute()
        # The package dir `datanika/` must live alongside, not above.
        assert (DEFAULT_ASSETS_DIR.parent / "datanika").is_dir()

    def test_app_bootstrap_writes_the_asset(self):
        """datanika.datanika imports and calls write_llms_txt_asset()
        so the file exists after app startup. This is a source-level
        assertion — a runtime check would require booting the full
        Reflex app, which is out of scope for unit tests."""
        src = Path(__file__).resolve().parents[2] / "datanika" / "datanika.py"
        text = src.read_text(encoding="utf-8")
        assert "write_llms_txt_asset" in text, (
            "datanika/datanika.py must import and call write_llms_txt_asset() "
            "at app bootstrap — see issue #124."
        )
        assert "write_llms_txt_asset()" in text, (
            "datanika/datanika.py imports write_llms_txt_asset but never "
            "calls it — the asset won't be materialised on startup."
        )
