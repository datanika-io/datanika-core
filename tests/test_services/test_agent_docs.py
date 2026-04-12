"""Tests for /llms.txt and /api/v1/agent-guide.md (#64)."""

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.services.agent_docs import agent_doc_routes


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
