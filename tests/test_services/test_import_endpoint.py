"""Tests for POST /api/v1/import — bulk resource creation (#131)."""

from unittest.mock import MagicMock, patch

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

import datanika.models.invitation  # noqa: F401
import datanika.models.notification_channel  # noqa: F401
import datanika.models.sso_config  # noqa: F401
from datanika.services.api_v1_routes import (
    MAX_IMPORT_OBJECTS,
    _validate_import_payload,
    api_v1_routes,
)
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

        # core#681: the key's owner must be a real member -- these routes call services that
        # resolve the actor's CURRENT role. Without it every mutating route here answers 403
        # `insufficient_role`, which is the system working, not the test being wrong.
        from datanika.models.user import MemberRole, Membership, Organization
        from tests.factories import make_user

        session.add(Organization(id=fake_api_key.org_id, name="Test Org", slug="test-org-imp"))
        session.flush()
        _actor = make_user(session, email="owner-imp@test.io", password_hash="x")
        session.add(
            Membership(user_id=_actor.id, org_id=fake_api_key.org_id, role=MemberRole.ADMIN)
        )
        session.flush()
        fake_api_key.user_id = _actor.id

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


class TestImportScopes:
    """core#681. The import route creates resources in four subsystems, so it declares no single
    scope -- and a key's scopes must still bound what it creates. Each refusal sits beside the same
    request by a key that carries the scope, which is served."""

    _TRANSFORMATION_ONLY = {
        "version": 2,
        "transformations": [{"name": "scoped_t", "sql_body": "select 1"}],
    }

    def test_a_key_without_the_write_scope_creates_nothing(
        self, client, fake_api_key, rate_limit_ok
    ):
        from datanika.models.transformation import Transformation

        fake_api_key.scopes = ["catalog:read", "transformations:read"]
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            resp = client.post(
                "/api/v1/import", json=self._TRANSFORMATION_ONLY, headers=_auth_headers()
            )
            count = session.query(Transformation).count()

        assert resp.status_code == 403, resp.text
        err = resp.json()["error"]
        assert err["code"] == "insufficient_scope"
        assert err["required_scopes"] == ["transformations:write"]
        assert count == 0

    def test_the_refusal_names_only_the_scopes_that_are_missing(
        self, client, fake_api_key, rate_limit_ok
    ):
        fake_api_key.scopes = ["connections:write"]
        payload = {**self._TRANSFORMATION_ONLY, "connections": [_PG_CONN]}
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post("/api/v1/import", json=payload, headers=_auth_headers())

        assert resp.status_code == 403, resp.text
        assert resp.json()["error"]["required_scopes"] == ["transformations:write"]

    def test_a_key_carrying_the_scope_is_served(self, client, fake_api_key, rate_limit_ok):
        fake_api_key.scopes = ["transformations:write"]
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/import", json=self._TRANSFORMATION_ONLY, headers=_auth_headers()
            )

        assert resp.status_code == 201, resp.text


# ---------------------------------------------------------------------------
# Per-call object cap (#1549)
# ---------------------------------------------------------------------------


class TestImportObjectCap:
    """One call used to create an unbounded number of objects across four sequential phases.

    The cap is checked in ``_validate_import_payload`` -- i.e. before phase 1 creates anything --
    because the phases are sequential and a refusal in phase 3 would leave phases 1 and 2 already
    applied.
    """

    @staticmethod
    def _n(section: str, n: int) -> dict:
        """``n`` structurally empty items. Empty on purpose: each one is missing several required
        fields, so without the short-circuit the response would carry thousands of per-item errors
        rather than one."""
        return {"version": 2, section: [{} for _ in range(n)]}

    def test_an_oversized_payload_is_refused_and_creates_nothing(
        self, client, fake_api_key, rate_limit_ok
    ):
        """AC1/AC2, end to end: refused whole, nothing part-applied."""
        over = MAX_IMPORT_OBJECTS + 1
        payload = {
            "version": 2,
            "connections": [{**_PG_CONN, "name": f"conn {i}"} for i in range(over)],
        }
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post("/api/v1/import", json=payload, headers=_auth_headers())
            assert resp.status_code == 400, resp.text
            assert any(e["code"] == "IMPORT_TOO_LARGE" for e in resp.json()["errors"])

            listed = client.get("/api/v1/connections", headers=_auth_headers())
            assert listed.status_code == 200
            assert listed.json()["items"] == []

    def test_the_refusal_names_the_limit_and_the_count_received(self):
        """AC3. 'Too large' without the two numbers does not tell the caller how to split."""
        over = MAX_IMPORT_OBJECTS + 1
        errors = _validate_import_payload(self._n("connections", over), {})
        message = errors[0]["message"]
        assert str(MAX_IMPORT_OBJECTS) in message
        assert str(over) in message

    def test_the_cap_short_circuits_so_the_error_response_stays_bounded(self):
        """The refusal replaces per-item validation rather than joining it.

        Each item here is missing name, connection_type and config, so per-item validation would
        answer with thousands of errors -- an unbounded response to an unbounded request, which is
        the same defect one layer up.
        """
        errors = _validate_import_payload(self._n("connections", MAX_IMPORT_OBJECTS + 1), {})
        assert len(errors) == 1
        assert errors[0]["code"] == "IMPORT_TOO_LARGE"

    def test_the_cap_counts_every_section_not_only_connections(self):
        """A payload can exceed the cap with no single section exceeding it.

        Counting connections alone would leave the three unquota'd sections unbounded -- and
        connections are the one section datanika-cloud already caps by plan.
        """
        per = MAX_IMPORT_OBJECTS // 4 + 1
        data = {
            "version": 2,
            "connections": [{} for _ in range(per)],
            "uploads": [{} for _ in range(per)],
            "pipelines": [{} for _ in range(per)],
            "transformations": [{} for _ in range(per)],
        }
        errors = _validate_import_payload(data, {})
        assert errors[0]["code"] == "IMPORT_TOO_LARGE"
        assert str(per * 4) in errors[0]["message"]

    def test_a_payload_at_the_limit_is_not_refused_by_the_cap(self):
        """The control, and it is the point of the test class.

        A guard that refuses everything is not discriminating, and the obvious repair for 'it
        refuses everything' is to loosen it. Drive it with both populations and require different
        answers. Asserts only that the CAP is silent -- other validation errors are another test's
        subject.
        """
        data = {
            "version": 2,
            "connections": [{**_PG_CONN, "name": f"conn {i}"} for i in range(MAX_IMPORT_OBJECTS)],
        }
        errors = _validate_import_payload(data, {})
        assert not any(e["code"] == "IMPORT_TOO_LARGE" for e in errors)

    def test_phase_1_emits_the_connection_quota_hook_once_per_connection(
        self, client, fake_api_key, rate_limit_ok
    ):
        """AC4, measured on the real dispatch rather than read off the source.

        datanika-cloud enforces ``max_connections`` by subscribing to ``connection.before_create``
        and raising. This establishes that bulk import reaches that hook AND that it fires once per
        connection -- so a plan cap can refuse partway through phase 1, which is what makes the
        partial-application question real rather than hypothetical. It is recorded here because the
        cloud tree is not installed in this suite, so the assertion is about the emit, not about the
        refusal.
        """
        from datanika import hooks

        seen: list[dict] = []

        def _spy(**kwargs):
            seen.append(kwargs)

        hooks.on("connection.before_create", _spy)
        try:
            payload = {
                "version": 2,
                "connections": [{**_PG_CONN, "name": f"conn {i}"} for i in range(3)],
            }
            with _patch_auth(fake_api_key, rate_limit_ok):
                resp = client.post("/api/v1/import", json=payload, headers=_auth_headers())
                assert resp.status_code == 201, resp.text
        finally:
            hooks.off("connection.before_create", _spy)

        assert len(seen) == 3
        assert all(kw.get("org_id") == fake_api_key.org_id for kw in seen)
