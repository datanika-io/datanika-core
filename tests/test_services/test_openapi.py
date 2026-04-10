"""Tests for OpenAPI spec and Swagger UI."""

import json

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.services.openapi import build_openapi_spec, openapi_routes


@pytest.fixture
def client():
    app = Starlette(routes=openapi_routes)
    return TestClient(app)


class TestOpenAPISpec:
    def test_spec_is_valid_openapi(self):
        spec = build_openapi_spec()
        assert spec["openapi"] == "3.0.3"
        assert spec["info"]["title"] == "Datanika API"
        assert "paths" in spec
        assert "components" in spec

    def test_spec_has_all_resource_paths(self):
        spec = build_openapi_spec()
        paths = spec["paths"]
        for resource in [
            "connections", "uploads", "pipelines",
            "transformations", "schedules", "runs",
        ]:
            assert f"/api/v1/{resource}" in paths, f"Missing {resource} list path"

    def test_spec_has_notification_channels(self):
        spec = build_openapi_spec()
        assert "/api/v1/notifications/channels" in spec["paths"]

    def test_spec_has_trigger_endpoints(self):
        spec = build_openapi_spec()
        for resource in ["uploads", "pipelines", "transformations"]:
            path = f"/api/v1/{resource}/{{id}}/run"
            assert path in spec["paths"], f"Missing trigger for {resource}"
            assert "post" in spec["paths"][path]

    def test_spec_has_auth_scheme(self):
        spec = build_openapi_spec()
        schemes = spec["components"]["securitySchemes"]
        assert "BearerApiKey" in schemes
        assert schemes["BearerApiKey"]["scheme"] == "bearer"

    def test_spec_has_all_schemas(self):
        spec = build_openapi_spec()
        schemas = spec["components"]["schemas"]
        for name in [
            "Connection", "Upload", "Pipeline", "Transformation",
            "Schedule", "Run", "NotificationChannel", "Error",
        ]:
            assert name in schemas, f"Missing schema {name}"

    def test_runs_path_has_query_params(self):
        spec = build_openapi_spec()
        runs_get = spec["paths"]["/api/v1/runs"]["get"]
        param_names = [p["name"] for p in runs_get["parameters"]]
        assert "target_type" in param_names
        assert "status" in param_names
        assert "limit" in param_names


class TestOpenAPIEndpoints:
    def test_openapi_json_returns_200(self, client):
        resp = client.get("/api/v1/openapi.json")
        assert resp.status_code == 200
        data = resp.json()
        assert data["openapi"] == "3.0.3"

    def test_swagger_ui_returns_html(self, client):
        resp = client.get("/api/v1/docs")
        assert resp.status_code == 200
        assert "swagger-ui" in resp.text
        assert "openapi.json" in resp.text
