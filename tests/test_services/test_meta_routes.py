"""Tests for /api/v1/meta/* discovery endpoints."""

from unittest.mock import MagicMock, patch

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.models.connection import ConnectionType
from datanika.services.meta_routes import meta_routes
from datanika.services.rate_limit_service import RateLimitResult


@pytest.fixture
def fake_api_key():
    key = MagicMock()
    key.id = 1
    key.org_id = 10
    key.scopes = None
    return key


@pytest.fixture
def rate_limit_ok():
    return RateLimitResult(
        allowed=True,
        current_count=1,
        limit=60,
        remaining=59,
        retry_after=0,
        reset_at=9999999999,
    )


@pytest.fixture
def client():
    app = Starlette(routes=meta_routes)
    return TestClient(app)


def _auth_headers():
    return {"Authorization": "Bearer etf_test"}


def _patch_auth(fake_api_key, rate_limit_ok):
    """Patch middleware auth + rate limiting."""
    import contextlib

    @contextlib.contextmanager
    def fake_session():
        yield MagicMock()

    return [
        patch("datanika.services.api_middleware._api_key_svc"),
        patch("datanika.services.api_middleware._rate_limit_svc"),
        patch("datanika.services.api_middleware._get_session", fake_session),
    ]


def _setup_mocks(stack, fake_api_key, rate_limit_ok):
    """Configure mocks after entering the patch stack."""
    mocks = [s.__enter__() for s in stack]
    mocks[0].authenticate_api_key.return_value = fake_api_key
    mocks[1].get_limit_for_org.return_value = 60
    mocks[1].check_rate_limit.return_value = rate_limit_ok
    return mocks


class TestAuth:
    def test_no_auth_returns_401(self, client):
        resp = client.get("/api/v1/meta/connection-types")
        assert resp.status_code == 401


class TestConnectionTypes:
    def test_lists_all_connection_types(self, client, fake_api_key, rate_limit_ok):
        stack = _patch_auth(fake_api_key, rate_limit_ok)
        try:
            _setup_mocks(stack, fake_api_key, rate_limit_ok)
            resp = client.get(
                "/api/v1/meta/connection-types",
                headers=_auth_headers(),
            )
            assert resp.status_code == 200
            items = resp.json()["items"]
            slugs = {item["type"] for item in items}
            # Every ConnectionType enum value must be present
            for ct in ConnectionType:
                assert ct.value in slugs, f"Missing {ct.value}"
        finally:
            for s in reversed(stack):
                s.__exit__(None, None, None)

    def test_each_item_has_required_fields(
        self,
        client,
        fake_api_key,
        rate_limit_ok,
    ):
        stack = _patch_auth(fake_api_key, rate_limit_ok)
        try:
            _setup_mocks(stack, fake_api_key, rate_limit_ok)
            resp = client.get(
                "/api/v1/meta/connection-types",
                headers=_auth_headers(),
            )
            for item in resp.json()["items"]:
                assert "type" in item
                assert "direction" in item
                assert "is_source" in item
                assert "is_destination" in item
                assert "config_schema" in item
                schema = item["config_schema"]
                assert schema["type"] == "object"
                assert "properties" in schema
                assert "required" in schema
        finally:
            for s in reversed(stack):
                s.__exit__(None, None, None)

    def test_postgres_schema_has_expected_fields(
        self,
        client,
        fake_api_key,
        rate_limit_ok,
    ):
        stack = _patch_auth(fake_api_key, rate_limit_ok)
        try:
            _setup_mocks(stack, fake_api_key, rate_limit_ok)
            resp = client.get(
                "/api/v1/meta/connection-types/postgres",
                headers=_auth_headers(),
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["type"] == "postgres"
            schema = data["config_schema"]
            assert "host" in schema["properties"]
            assert "database" in schema["properties"]
            assert "password" in schema["properties"]
            assert schema["properties"]["password"]["format"] == "password"
            assert "host" in schema["required"]
        finally:
            for s in reversed(stack):
                s.__exit__(None, None, None)

    def test_unknown_type_returns_404(
        self,
        client,
        fake_api_key,
        rate_limit_ok,
    ):
        stack = _patch_auth(fake_api_key, rate_limit_ok)
        try:
            _setup_mocks(stack, fake_api_key, rate_limit_ok)
            resp = client.get(
                "/api/v1/meta/connection-types/nonexistent",
                headers=_auth_headers(),
            )
            assert resp.status_code == 404
        finally:
            for s in reversed(stack):
                s.__exit__(None, None, None)


class TestDltConfigSchema:
    def test_returns_schema(self, client, fake_api_key, rate_limit_ok):
        stack = _patch_auth(fake_api_key, rate_limit_ok)
        try:
            _setup_mocks(stack, fake_api_key, rate_limit_ok)
            resp = client.get(
                "/api/v1/meta/dlt-config-schema",
                headers=_auth_headers(),
            )
            assert resp.status_code == 200
            schema = resp.json()
            assert schema["type"] == "object"
            props = schema["properties"]
            assert "mode" in props
            assert "write_disposition" in props
            assert "incremental" in props
            assert "merge_config" in props
            assert "filters" in props
            assert schema["properties"]["mode"]["enum"] == [
                "full_database",
                "single_table",
            ]
            assert "examples" in schema
        finally:
            for s in reversed(stack):
                s.__exit__(None, None, None)


class TestDbtTests:
    def test_lists_all_test_types(self, client, fake_api_key, rate_limit_ok):
        stack = _patch_auth(fake_api_key, rate_limit_ok)
        try:
            _setup_mocks(stack, fake_api_key, rate_limit_ok)
            resp = client.get(
                "/api/v1/meta/dbt-tests",
                headers=_auth_headers(),
            )
            assert resp.status_code == 200
            items = resp.json()["items"]
            names = {t["name"] for t in items}
            assert names == {
                "not_null",
                "unique",
                "accepted_values",
                "relationships",
            }
            for t in items:
                assert "description" in t
                assert "params_schema" in t
                assert "example" in t
        finally:
            for s in reversed(stack):
                s.__exit__(None, None, None)

    def test_matches_validation_logic(
        self,
        client,
        fake_api_key,
        rate_limit_ok,
    ):
        """The exposed test types must match what TransformationService accepts."""
        from datanika.services.transformation_service import TransformationService

        stack = _patch_auth(fake_api_key, rate_limit_ok)
        try:
            _setup_mocks(stack, fake_api_key, rate_limit_ok)
            resp = client.get(
                "/api/v1/meta/dbt-tests",
                headers=_auth_headers(),
            )
            exposed = {t["name"] for t in resp.json()["items"]}
            assert exposed == TransformationService.VALID_GENERIC_TESTS
        finally:
            for s in reversed(stack):
                s.__exit__(None, None, None)


class TestMaterializations:
    def test_lists_all_materializations(
        self,
        client,
        fake_api_key,
        rate_limit_ok,
    ):
        from datanika.models.transformation import Materialization

        stack = _patch_auth(fake_api_key, rate_limit_ok)
        try:
            _setup_mocks(stack, fake_api_key, rate_limit_ok)
            resp = client.get(
                "/api/v1/meta/materializations",
                headers=_auth_headers(),
            )
            assert resp.status_code == 200
            items = resp.json()["items"]
            names = {m["name"] for m in items}
            # Must cover all enum values
            assert names == {m.value for m in Materialization}
            for m in items:
                assert "description" in m
        finally:
            for s in reversed(stack):
                s.__exit__(None, None, None)
