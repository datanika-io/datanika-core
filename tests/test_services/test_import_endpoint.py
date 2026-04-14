"""Tests for POST /api/v1/import — bulk resource creation (#131)."""

from unittest.mock import MagicMock, patch

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

import datanika.models.invitation  # noqa: F401
import datanika.models.notification_channel  # noqa: F401
import datanika.models.sso_config  # noqa: F401
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.rate_limit_service import RateLimitResult


@pytest.fixture
def fake_api_key():
    key = MagicMock()
    key.id = 1
    key.org_id = 10
    key.user_id = 1
    key.name = "Test Key"
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
def app():
    return Starlette(routes=api_v1_routes)


@pytest.fixture
def client(app):
    return TestClient(app)


def _auth_headers():
    return {"Authorization": "Bearer etf_testkey"}


def _patch_auth(fake_api_key, rate_limit_ok):
    import contextlib

    from cryptography.fernet import Fernet
    from sqlalchemy import create_engine
    from sqlalchemy.orm import Session as SASession

    from datanika.models.base import Base
    from datanika.services.connection_service import ConnectionService
    from datanika.services.encryption import EncryptionService
    from datanika.services.pipeline_service import PipelineService
    from datanika.services.schedule_service import ScheduleService
    from datanika.services.transformation_service import TransformationService
    from datanika.services.upload_service import UploadService

    @contextlib.contextmanager
    def _ctx():
        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=__import__("sqlalchemy.pool", fromlist=["StaticPool"]).StaticPool,
        )
        Base.metadata.create_all(engine)
        session = SASession(engine)

        enc = EncryptionService(Fernet.generate_key().decode())
        conn_svc = ConnectionService(enc)
        upload_svc = UploadService(conn_svc)
        pipeline_svc = PipelineService()
        transform_svc = TransformationService()
        schedule_svc = ScheduleService(upload_svc, transform_svc, pipeline_service=pipeline_svc)

        @contextlib.contextmanager
        def fake_session():
            yield session

        import datanika.services.api_v1_routes as routes_mod

        with (
            patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
            patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
            patch("datanika.services.api_middleware._get_session", fake_session),
            patch.object(routes_mod, "_get_conn_svc", return_value=conn_svc),
            patch.object(routes_mod, "_get_upload_svc", return_value=upload_svc),
            patch.object(routes_mod, "_get_schedule_svc", return_value=schedule_svc),
        ):
            mock_svc.authenticate_api_key.return_value = fake_api_key
            mock_rl.get_limit_for_org.return_value = 60
            mock_rl.check_rate_limit.return_value = rate_limit_ok

            yield session

        session.close()
        engine.dispose()

    return _ctx()


# ---------------------------------------------------------------------------
# Fixtures — common payloads
# ---------------------------------------------------------------------------

_PG_CONN = {
    "name": "My PG",
    "connection_type": "postgres",
    "config": {"host": "localhost", "port": 5432, "user": "u", "password": "p", "database": "db"},
}

_MYSQL_CONN = {
    "name": "My MySQL",
    "connection_type": "mysql",
    "config": {"host": "mysql.local", "port": 3306, "user": "u", "password": "p", "database": "db"},
}


def _full_payload():
    """Return a valid version-2 import payload with all sections."""
    return {
        "version": 2,
        "connections": [_PG_CONN, _MYSQL_CONN],
        "uploads": [
            {
                "name": "Load orders",
                "source_connection_name": "My MySQL",
                "destination_connection_name": "My PG",
                "dlt_config": {"load_mode": "single_table", "table_name": "orders"},
            }
        ],
        "pipelines": [
            {
                "name": "Build analytics",
                "destination_connection_name": "My PG",
                "command": "build",
            }
        ],
        "transformations": [
            {
                "name": "stg_orders",
                "sql_body": "SELECT * FROM {{ source('raw', 'orders') }}",
                "materialization": "view",
            }
        ],
    }


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestImportEndpointHappyPath:
    def test_full_import_creates_all_resources(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post("/api/v1/import", json=_full_payload(), headers=_auth_headers())
            assert resp.status_code == 201, resp.json()
            data = resp.json()
            assert "created" in data
            assert len(data["created"]["connections"]) == 2
            assert len(data["created"]["uploads"]) == 1
            assert len(data["created"]["pipelines"]) == 1
            assert len(data["created"]["transformations"]) == 1

    def test_connections_only(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/import",
                json={"version": 2, "connections": [_PG_CONN]},
                headers=_auth_headers(),
            )
            assert resp.status_code == 201
            data = resp.json()
            assert len(data["created"]["connections"]) == 1
            assert data["created"]["uploads"] == []
            assert data["created"]["pipelines"] == []
            assert data["created"]["transformations"] == []

    def test_uploads_referencing_existing_connections(self, client, fake_api_key, rate_limit_ok):
        """Uploads can reference connections already in the org, not just in the payload."""
        with _patch_auth(fake_api_key, rate_limit_ok):
            # Pre-create connections via the normal endpoint
            client.post(
                "/api/v1/connections",
                json=_PG_CONN,
                headers=_auth_headers(),
            )
            client.post(
                "/api/v1/connections",
                json=_MYSQL_CONN,
                headers=_auth_headers(),
            )

            # Import an upload referencing those existing connections
            resp = client.post(
                "/api/v1/import",
                json={
                    "version": 2,
                    "uploads": [
                        {
                            "name": "Load orders",
                            "source_connection_name": "My MySQL",
                            "destination_connection_name": "My PG",
                            "dlt_config": {"load_mode": "single_table", "table_name": "t"},
                        }
                    ],
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 201
            assert len(resp.json()["created"]["uploads"]) == 1

    def test_transformations_only(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/import",
                json={
                    "version": 2,
                    "transformations": [
                        {
                            "name": "stg_users",
                            "sql_body": "SELECT * FROM users",
                            "materialization": "view",
                        }
                    ],
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 201
            assert len(resp.json()["created"]["transformations"]) == 1

    def test_empty_sections_are_fine(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/import",
                json={"version": 2},
                headers=_auth_headers(),
            )
            assert resp.status_code == 201
            data = resp.json()
            for key in ("connections", "uploads", "pipelines", "transformations"):
                assert data["created"][key] == []

    def test_response_contains_created_ids(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post("/api/v1/import", json=_full_payload(), headers=_auth_headers())
            data = resp.json()
            for section in ("connections", "uploads", "pipelines", "transformations"):
                for item_id in data["created"][section]:
                    assert isinstance(item_id, int)


# ---------------------------------------------------------------------------
# Validation errors
# ---------------------------------------------------------------------------


class TestImportValidation:
    def test_missing_version_returns_400(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/import",
                json={"connections": [_PG_CONN]},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400

    def test_wrong_version_returns_400(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/import",
                json={"version": 1, "connections": [_PG_CONN]},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400

    def test_connection_missing_name(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/import",
                json={
                    "version": 2,
                    "connections": [
                        {"connection_type": "postgres", "config": {"host": "localhost"}}
                    ],
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 400
            assert "errors" in resp.json()

    def test_connection_invalid_type(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/import",
                json={
                    "version": 2,
                    "connections": [{"name": "Bad", "connection_type": "no_such_db", "config": {}}],
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 400
            errors = resp.json()["errors"]
            assert any("INVALID_CONNECTION_TYPE" in str(e) for e in errors)

    def test_duplicate_connection_names(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            conn = {**_PG_CONN}
            resp = client.post(
                "/api/v1/import",
                json={"version": 2, "connections": [conn, conn]},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400
            errors = resp.json()["errors"]
            assert any("DUPLICATE_NAME" in str(e) for e in errors)

    def test_upload_unknown_connection_ref(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/import",
                json={
                    "version": 2,
                    "uploads": [
                        {
                            "name": "Bad ref",
                            "source_connection_name": "Does Not Exist",
                            "destination_connection_name": "Also Missing",
                            "dlt_config": {},
                        }
                    ],
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 400
            errors = resp.json()["errors"]
            assert any("UNKNOWN_CONNECTION_REF" in str(e) for e in errors)

    def test_pipeline_unknown_connection_ref(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/import",
                json={
                    "version": 2,
                    "pipelines": [
                        {
                            "name": "Bad pipeline",
                            "destination_connection_name": "Nope",
                            "command": "run",
                        }
                    ],
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 400
            errors = resp.json()["errors"]
            assert any("UNKNOWN_CONNECTION_REF" in str(e) for e in errors)

    def test_transformation_missing_sql_body(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/import",
                json={
                    "version": 2,
                    "transformations": [{"name": "bad"}],
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 400

    def test_multiple_errors_returned_at_once(self, client, fake_api_key, rate_limit_ok):
        """All validation errors collected, not just the first one."""
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/import",
                json={
                    "version": 2,
                    "connections": [
                        {"connection_type": "postgres", "config": {}},  # missing name
                        {"name": "Bad", "connection_type": "no_such", "config": {}},
                    ],
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 400
            errors = resp.json()["errors"]
            assert len(errors) >= 2

    def test_nothing_created_on_validation_failure(self, client, fake_api_key, rate_limit_ok):
        """If validation fails, no resources are created (atomic)."""
        with _patch_auth(fake_api_key, rate_limit_ok):
            # Send a payload with a valid connection but an invalid upload
            resp = client.post(
                "/api/v1/import",
                json={
                    "version": 2,
                    "connections": [_PG_CONN],
                    "uploads": [
                        {
                            "name": "Bad",
                            "source_connection_name": "Nonexistent",
                            "destination_connection_name": "My PG",
                            "dlt_config": {},
                        }
                    ],
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 400

            # Verify the connection was NOT created
            resp2 = client.get("/api/v1/connections", headers=_auth_headers())
            assert resp2.status_code == 200
            assert resp2.json()["items"] == []


# ---------------------------------------------------------------------------
# Auth & routing
# ---------------------------------------------------------------------------


class TestImportAuth:
    def test_no_auth_returns_401(self, client):
        resp = client.post("/api/v1/import", json={"version": 2})
        assert resp.status_code == 401

    def test_empty_body_returns_400(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post("/api/v1/import", json={}, headers=_auth_headers())
            assert resp.status_code == 400


# ---------------------------------------------------------------------------
# OpenAPI spec
# ---------------------------------------------------------------------------


class TestImportOpenAPISpec:
    def test_import_path_in_spec(self):
        from datanika.services.openapi import build_openapi_spec

        spec = build_openapi_spec()
        assert "/api/v1/import" in spec["paths"]
        assert "post" in spec["paths"]["/api/v1/import"]

    def test_import_is_stable(self):
        from datanika.services.openapi import build_openapi_spec

        spec = build_openapi_spec()
        post_op = spec["paths"]["/api/v1/import"]["post"]
        assert post_op.get("x-stability") == "stable"

    def test_import_schemas_exist(self):
        from datanika.services.openapi import build_openapi_spec

        spec = build_openapi_spec()
        schemas = spec["components"]["schemas"]
        assert "ImportRequest" in schemas
        assert "ImportResult" in schemas
