"""Tests for OpenAPI spec and Swagger UI."""

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
            "connections",
            "uploads",
            "pipelines",
            "transformations",
            "schedules",
            "runs",
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
            "Connection",
            "Upload",
            "Pipeline",
            "Transformation",
            "Schedule",
            "Run",
            "NotificationChannel",
            "Error",
        ]:
            assert name in schemas, f"Missing schema {name}"

    def test_spec_has_tier_2_compile_preview_paths(self):
        """Tier 2 Agent Compatibility endpoints must be in OpenAPI spec (#52)."""
        spec = build_openapi_spec()
        compile_path = "/api/v1/transformations/{id}/compile"
        preview_path = "/api/v1/transformations/{id}/preview"
        assert compile_path in spec["paths"]
        assert preview_path in spec["paths"]
        assert "post" in spec["paths"][compile_path]
        assert "post" in spec["paths"][preview_path]

        # Preview accepts a request body with optional limit
        preview_post = spec["paths"][preview_path]["post"]
        assert "requestBody" in preview_post
        # 200 responses reference typed schemas (no {type: object} placeholder)
        compile_200 = spec["paths"][compile_path]["post"]["responses"]["200"]
        compile_ref = compile_200["content"]["application/json"]["schema"]["$ref"]
        assert compile_ref.endswith("CompileResult")
        preview_200 = preview_post["responses"]["200"]
        preview_ref = preview_200["content"]["application/json"]["schema"]["$ref"]
        assert preview_ref.endswith("PreviewResult")
        # 400 responses reference typed error schemas with string error codes
        compile_400 = spec["paths"][compile_path]["post"]["responses"]["400"]
        assert compile_400["content"]["application/json"]["schema"]["$ref"].endswith("CompileError")

    def test_tier_2_schemas_have_required_fields(self):
        spec = build_openapi_spec()
        schemas = spec["components"]["schemas"]
        for name in [
            "CompileResult",
            "CompileError",
            "PreviewRequest",
            "PreviewResult",
            "PreviewColumn",
            "PreviewError",
        ]:
            assert name in schemas, f"Missing Tier 2 schema {name}"

        compile_result = schemas["CompileResult"]
        assert "compiled_sql" in compile_result["properties"]
        assert "node" in compile_result["properties"]
        assert "compiled_sql" in compile_result["required"]

        preview_result = schemas["PreviewResult"]
        for field in ("columns", "rows", "row_count", "truncated"):
            assert field in preview_result["properties"]

    def test_runs_path_has_query_params(self):
        spec = build_openapi_spec()
        runs_get = spec["paths"]["/api/v1/runs"]["get"]
        param_names = [p["name"] for p in runs_get["parameters"]]
        assert "target_type" in param_names
        assert "status" in param_names
        assert "limit" in param_names


# ---------------------------------------------------------------------------
# Tier 3: inlined per-type schemas (#57)
# ---------------------------------------------------------------------------


class TestTier3InlinedSchemas:
    """Verify ConnectionCreate and UploadCreate use discriminator-based
    oneOf instead of opaque {type: object} placeholders."""

    def test_spec_round_trips_through_json(self):
        """Spec must serialize and deserialize without loss."""
        import json

        spec = build_openapi_spec()
        raw = json.dumps(spec)
        parsed = json.loads(raw)
        assert parsed["openapi"] == "3.0.3"
        assert "ConnectionCreate" in parsed["components"]["schemas"]

    def test_connection_create_is_oneof_with_discriminator(self):
        spec = build_openapi_spec()
        cc = spec["components"]["schemas"]["ConnectionCreate"]
        assert "oneOf" in cc, "ConnectionCreate should be a oneOf"
        assert cc["discriminator"]["propertyName"] == "connection_type"
        assert len(cc["oneOf"]) >= 27  # at least 27 source types

    def test_every_connection_type_has_a_branch(self):
        """…except a withdrawn one, which has no config schema to describe.

        A withdrawn type keeps its enum member so stored connections resolve,
        but is not creatable, so the spec has nothing to say about how to create
        one (`WITHDRAWN_SOURCE_TYPES`, core#555). Advertising a branch here
        would tell an API client to POST a config that no longer validates.
        """
        from datanika.models.connection import ConnectionType
        from datanika.services.connection_service import WITHDRAWN_SOURCE_TYPES

        spec = build_openapi_spec()
        cc = spec["components"]["schemas"]["ConnectionCreate"]
        mapping = cc["discriminator"]["mapping"]
        for ct in ConnectionType:
            if ct.value in WITHDRAWN_SOURCE_TYPES:
                assert ct.value not in mapping, (
                    f"{ct.value} is withdrawn but still has a ConnectionCreate branch"
                )
                continue
            assert ct.value in mapping, (
                f"ConnectionType.{ct.name} ({ct.value}) missing from discriminator mapping"
            )

    def test_per_type_config_schemas_have_properties_and_required(self):
        from datanika.services.connection_schemas import CONFIG_SCHEMAS
        from datanika.services.openapi_inline import _to_pascal

        spec = build_openapi_spec()
        schemas = spec["components"]["schemas"]
        for slug in CONFIG_SCHEMAS:
            config_name = f"{_to_pascal(slug)}ConnectionConfig"
            assert config_name in schemas, f"Missing schema {config_name}"
            schema = schemas[config_name]
            assert "properties" in schema
            assert "required" in schema
            assert schema["type"] == "object"

    def test_per_type_create_schemas_have_example(self):
        from datanika.services.connection_schemas import CONFIG_SCHEMAS
        from datanika.services.openapi_inline import _to_pascal

        spec = build_openapi_spec()
        schemas = spec["components"]["schemas"]
        for slug in CONFIG_SCHEMAS:
            create_name = f"Create{_to_pascal(slug)}Connection"
            assert create_name in schemas, f"Missing {create_name}"
            assert "example" in schemas[create_name], f"{create_name} missing example"
            example = schemas[create_name]["example"]
            assert example["connection_type"] == slug

    def test_no_type_object_placeholder_in_connection_create(self):
        """The old `config: {type: object}` must be gone."""
        import json

        spec = build_openapi_spec()
        cc_raw = json.dumps(spec["components"]["schemas"]["ConnectionCreate"])
        # The top-level ConnectionCreate has no `properties.config` anymore —
        # it's a oneOf parent. And each branch references a typed schema.
        assert '"config":{"type":"object"}' not in cc_raw

    def test_connection_type_is_enum_on_read_schema(self):
        from datanika.models.connection import ConnectionType

        spec = build_openapi_spec()
        conn = spec["components"]["schemas"]["Connection"]
        ct_prop = conn["properties"]["connection_type"]
        assert "enum" in ct_prop
        assert set(ct_prop["enum"]) == {ct.value for ct in ConnectionType}

    def test_upload_create_dlt_config_is_ref_to_dlt_config(self):
        spec = build_openapi_spec()
        uc = spec["components"]["schemas"]["UploadCreate"]
        dlt_ref = uc["properties"]["dlt_config"].get("$ref", "")
        assert dlt_ref.endswith("DltConfig")

    def test_dlt_config_has_discriminator_on_mode(self):
        spec = build_openapi_spec()
        dc = spec["components"]["schemas"]["DltConfig"]
        assert "oneOf" in dc
        assert dc["discriminator"]["propertyName"] == "mode"
        mapping = dc["discriminator"]["mapping"]
        assert "single_table" in mapping
        assert "full_database" in mapping

    def test_single_table_config_has_table_field(self):
        spec = build_openapi_spec()
        st = spec["components"]["schemas"]["SingleTableDltConfig"]
        assert "table" in st["properties"]
        assert "table" in st["required"]
        assert st["properties"]["mode"]["enum"] == ["single_table"]

    def test_full_database_config_has_table_names(self):
        spec = build_openapi_spec()
        fd = spec["components"]["schemas"]["FullDatabaseDltConfig"]
        assert "table_names" in fd["properties"]
        assert fd["properties"]["mode"]["enum"] == ["full_database"]

    def test_no_type_object_placeholder_in_upload_create(self):
        import json

        spec = build_openapi_spec()
        uc_raw = json.dumps(spec["components"]["schemas"]["UploadCreate"])
        assert '"dlt_config":{"type":"object"}' not in uc_raw

    def test_upload_create_has_example(self):
        spec = build_openapi_spec()
        uc = spec["components"]["schemas"]["UploadCreate"]
        assert "example" in uc
        assert uc["example"]["dlt_config"]["mode"] == "single_table"

    def test_sensitive_fields_have_password_format(self):
        """Spot-check that password/API key fields keep format: password."""

        spec = build_openapi_spec()
        schemas = spec["components"]["schemas"]
        pg = schemas["PostgresConnectionConfig"]
        assert pg["properties"]["password"]["format"] == "password"

        stripe = schemas["StripeConnectionConfig"]
        assert stripe["properties"]["api_key"]["format"] == "password"


class TestStabilityTags:
    """#137 — every operation in the OpenAPI spec must carry an
    ``x-stability`` extension so enterprise buyers can distinguish
    stable, beta, and experimental endpoints at a glance.
    """

    def test_all_operations_have_x_stability(self):
        spec = build_openapi_spec()
        for path_key, methods in spec["paths"].items():
            for method, op in methods.items():
                if not isinstance(op, dict) or "tags" not in op:
                    continue
                assert "x-stability" in op, (
                    f"{method.upper()} {path_key} is missing x-stability tag. "
                    "See docs/api_versioning.md and #137."
                )
                assert op["x-stability"] in ("stable", "beta", "experimental"), (
                    f"{method.upper()} {path_key} has invalid x-stability "
                    f"value {op['x-stability']!r}"
                )

    def test_crud_endpoints_are_stable(self):
        spec = build_openapi_spec()
        for resource in ("connections", "uploads", "pipelines", "transformations", "schedules"):
            path = f"/api/v1/{resource}"
            for method, op in spec["paths"][path].items():
                if isinstance(op, dict) and "tags" in op:
                    assert op["x-stability"] == "stable", (
                        f"{method.upper()} {path} should be stable"
                    )

    def test_catalog_endpoints_are_beta(self):
        spec = build_openapi_spec()
        for path_key, methods in spec["paths"].items():
            if not path_key.startswith("/api/v1/catalog"):
                continue
            for method, op in methods.items():
                if isinstance(op, dict) and "tags" in op:
                    assert op["x-stability"] == "beta", (
                        f"{method.upper()} {path_key} should be beta"
                    )

    def test_at_least_three_stable_endpoints(self):
        spec = build_openapi_spec()
        stable_count = sum(
            1
            for methods in spec["paths"].values()
            for op in methods.values()
            if isinstance(op, dict) and op.get("x-stability") == "stable"
        )
        assert stable_count >= 3, f"Only {stable_count} stable endpoints found"

    def test_at_least_one_beta_endpoint(self):
        spec = build_openapi_spec()
        beta_count = sum(
            1
            for methods in spec["paths"].values()
            for op in methods.values()
            if isinstance(op, dict) and op.get("x-stability") == "beta"
        )
        assert beta_count >= 1, "No beta endpoints found — catalog should be beta"


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
