"""Tests for transformation_compile service and API endpoints (#52).

Covers:
- compile_transformation: success, Jinja error, missing transformation,
  missing destination (compile is lenient — preview is strict).
- preview_transformation: limit clamping, safe-SQL guard, warehouse
  error propagation, compilation-error short-circuit.
- /api/v1/transformations/{id}/compile API endpoint: 401, 404, 200,
  tenant isolation.
- /api/v1/transformations/{id}/preview API endpoint: default limit,
  max limit clamp, 404.

Real dbt compile is exercised via fixture SQL; the warehouse is mocked.
"""

from __future__ import annotations

import contextlib
from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SASession
from sqlalchemy.pool import StaticPool
from starlette.applications import Starlette
from starlette.testclient import TestClient

# Register all model tables so Base.metadata.create_all sees them.
import datanika.models.invitation  # noqa: F401
import datanika.models.notification_channel  # noqa: F401
import datanika.models.sso_config  # noqa: F401
from datanika.models.base import Base
from datanika.models.connection import Connection, ConnectionType
from datanika.models.transformation import Materialization, Transformation
from datanika.models.user import Organization
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.encryption import EncryptionService
from datanika.services.rate_limit_service import RateLimitResult
from datanika.services.transformation_compile import (
    DEFAULT_PREVIEW_LIMIT,
    MAX_PREVIEW_LIMIT,
    CompileResult,
    MissingDestinationError,
    PreviewResult,
    TransformationNotFoundError,
    compile_transformation,
    preview_transformation,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def fernet_key():
    return Fernet.generate_key().decode()


@pytest.fixture
def engine():
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def db_session(engine):
    session = SASession(engine)
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def org(db_session):
    org = Organization(name="Test Org", slug="test-org")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def duckdb_connection(db_session, org, fernet_key, tmp_path):
    """Create a DuckDB destination connection with a seeded 'orders' table.

    DuckDB is used instead of SQLite because `dbt-duckdb` is installed
    as a project dependency, so real `dbt compile` works against it.
    """
    import duckdb

    db_path = tmp_path / "dest.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT, total DOUBLE)")
    con.executemany(
        "INSERT INTO orders (id, name, total) VALUES (?, ?, ?)",
        [(1, "Alice", 10.5), (2, "Bob", 20.0), (3, "Carol", 15.75)],
    )
    con.close()

    enc = EncryptionService(fernet_key)
    conn = Connection(
        org_id=org.id,
        name="Test DuckDB",
        connection_type=ConnectionType.DUCKDB,
        direction="both",
        config_encrypted=enc.encrypt({"path": str(db_path)}),
    )
    db_session.add(conn)
    db_session.flush()
    return conn


@pytest.fixture
def transformation(db_session, org, duckdb_connection):
    t = Transformation(
        org_id=org.id,
        name="stg_orders",
        sql_body="SELECT id, name, total FROM orders",
        materialization=Materialization.VIEW,
        schema_name="staging",
        destination_connection_id=duckdb_connection.id,
        tests_config={},
        tags=[],
    )
    db_session.add(t)
    db_session.flush()
    return t


@pytest.fixture
def dbt_dir(tmp_path):
    """Temp directory for dbt projects during the test."""
    return str(tmp_path / "dbt_projects")


@pytest.fixture(autouse=True)
def patch_encryption_key(fernet_key):
    """Make the shared EncryptionService instantiations in the compile
    service see the same fernet key used to seed the fixture connection."""
    with patch(
        "datanika.services.transformation_compile.settings",
    ) as mock_settings:
        mock_settings.credential_encryption_key = fernet_key
        mock_settings.dbt_projects_dir = ""  # will be overridden per-call
        yield


# ---------------------------------------------------------------------------
# compile_transformation — service layer
# ---------------------------------------------------------------------------


class TestCompileTransformationService:
    def test_not_found_raises(self, db_session, org, dbt_dir):
        with pytest.raises(TransformationNotFoundError):
            compile_transformation(db_session, org.id, 9999, dbt_projects_dir=dbt_dir)

    def test_cross_org_not_found(self, db_session, transformation, dbt_dir):
        # Different org_id should not be able to compile this transformation
        with pytest.raises(TransformationNotFoundError):
            compile_transformation(db_session, 9999, transformation.id, dbt_projects_dir=dbt_dir)

    def test_success_returns_compiled_sql(self, db_session, org, transformation, dbt_dir):
        result = compile_transformation(
            db_session, org.id, transformation.id, dbt_projects_dir=dbt_dir
        )
        assert isinstance(result, CompileResult)
        assert result.success is True
        assert "SELECT" in result.compiled_sql.upper()
        assert result.node.startswith(f"model.tenant_{org.id}.")
        assert result.error_message == ""

    def test_jinja_error_returns_failure_with_message(
        self, db_session, org, transformation, dbt_dir
    ):
        transformation.sql_body = "SELECT {{ ref('nonexistent_model') }}.id"
        db_session.flush()
        result = compile_transformation(
            db_session, org.id, transformation.id, dbt_projects_dir=dbt_dir
        )
        assert result.success is False
        assert result.error_message != ""


# ---------------------------------------------------------------------------
# preview_transformation — service layer
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_warehouse():
    """Mock ConnectionService.execute_query so preview tests don't need
    a live warehouse connection. Returns a context manager yielding a
    MagicMock so individual tests can tweak the return value."""
    with patch("datanika.services.transformation_compile.ConnectionService.execute_query") as mock:
        mock.return_value = (
            ["id", "name", "total"],
            [[1, "Alice", 10.5], [2, "Bob", 20.0], [3, "Carol", 15.75]],
        )
        yield mock


class TestPreviewTransformationService:
    def test_preview_success_returns_rows(
        self, db_session, org, transformation, dbt_dir, mock_warehouse
    ):
        result = preview_transformation(
            db_session, org.id, transformation.id, dbt_projects_dir=dbt_dir
        )
        assert isinstance(result, PreviewResult)
        assert result.success is True
        assert len(result.rows) == 3
        assert [c["name"] for c in result.columns] == ["id", "name", "total"]
        assert result.row_count == 3
        assert result.truncated is False
        # Verify the query sent to the warehouse was wrapped in a
        # bounded SELECT and includes the clamped LIMIT.
        query_sent = mock_warehouse.call_args.args[2]
        assert "SELECT * FROM" in query_sent.upper()
        assert "LIMIT 100" in query_sent.upper()

    def test_limit_is_clamped_to_max(
        self, db_session, org, transformation, dbt_dir, mock_warehouse
    ):
        result = preview_transformation(
            db_session,
            org.id,
            transformation.id,
            limit=99999,
            dbt_projects_dir=dbt_dir,
        )
        assert result.success is True
        # Query should have been clamped to MAX_PREVIEW_LIMIT (1000)
        query_sent = mock_warehouse.call_args.args[2]
        assert "LIMIT 1000" in query_sent.upper()

    def test_limit_floor_is_one(self, db_session, org, transformation, dbt_dir, mock_warehouse):
        result = preview_transformation(
            db_session,
            org.id,
            transformation.id,
            limit=0,  # should clamp to 1 (not raise)
            dbt_projects_dir=dbt_dir,
        )
        assert result.success is True
        query_sent = mock_warehouse.call_args.args[2]
        assert "LIMIT 1" in query_sent.upper()

    def test_warehouse_error_surfaces_as_execution_error(
        self, db_session, org, transformation, dbt_dir
    ):
        with patch(
            "datanika.services.transformation_compile.ConnectionService.execute_query"
        ) as mock:
            mock.side_effect = RuntimeError("relation orders does not exist")
            result = preview_transformation(
                db_session, org.id, transformation.id, dbt_projects_dir=dbt_dir
            )
        assert result.success is False
        assert result.error_code == "execution_error"
        assert "relation orders does not exist" in result.error_message

    def test_compilation_error_short_circuits(self, db_session, org, transformation, dbt_dir):
        transformation.sql_body = "{% if %}broken{% endif %}"
        db_session.flush()
        result = preview_transformation(
            db_session, org.id, transformation.id, dbt_projects_dir=dbt_dir
        )
        assert result.success is False
        assert result.error_code == "compilation_error"
        assert result.rows == []

    def test_not_found_raises(self, db_session, org, dbt_dir):
        with pytest.raises(TransformationNotFoundError):
            preview_transformation(db_session, org.id, 9999, dbt_projects_dir=dbt_dir)

    def test_missing_destination_raises(self, db_session, org, transformation, dbt_dir):
        transformation.destination_connection_id = None
        db_session.flush()
        with pytest.raises(MissingDestinationError):
            preview_transformation(db_session, org.id, transformation.id, dbt_projects_dir=dbt_dir)

    def test_constants(self):
        assert DEFAULT_PREVIEW_LIMIT == 100
        assert MAX_PREVIEW_LIMIT == 1000


# ---------------------------------------------------------------------------
# API endpoints — /api/v1/transformations/{id}/compile and /preview
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_api_key():
    key = MagicMock()
    key.id = 1
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
def client():
    return TestClient(Starlette(routes=api_v1_routes))


def _headers():
    return {"Authorization": "Bearer etf_test"}


@contextlib.contextmanager
def _patch_api_auth(fake_api_key, rate_limit_ok, db_session, org_id, dbt_dir, fernet_key):
    """Patch the middleware + compile-service settings so the API endpoint
    sees the test session and fixture dbt dir."""
    fake_api_key.org_id = org_id

    @contextlib.contextmanager
    def fake_session():
        yield db_session

    with (
        patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
        patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
        patch("datanika.services.api_middleware._get_session", fake_session),
        patch("datanika.services.transformation_compile.settings") as mock_settings,
    ):
        mock_svc.authenticate_api_key.return_value = fake_api_key
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = rate_limit_ok
        mock_settings.credential_encryption_key = fernet_key
        mock_settings.dbt_projects_dir = dbt_dir
        yield


class TestCompileEndpoint:
    def test_401_without_auth(self, client, transformation):
        resp = client.post(f"/api/v1/transformations/{transformation.id}/compile")
        assert resp.status_code == 401

    def test_success(
        self,
        client,
        fake_api_key,
        rate_limit_ok,
        db_session,
        org,
        transformation,
        dbt_dir,
        fernet_key,
    ):
        with _patch_api_auth(fake_api_key, rate_limit_ok, db_session, org.id, dbt_dir, fernet_key):
            resp = client.post(
                f"/api/v1/transformations/{transformation.id}/compile",
                headers=_headers(),
            )
        assert resp.status_code == 200
        data = resp.json()
        assert "compiled_sql" in data
        assert "SELECT" in data["compiled_sql"].upper()
        assert data["node"].startswith(f"model.tenant_{org.id}.")

    def test_404_on_wrong_org(
        self,
        client,
        fake_api_key,
        rate_limit_ok,
        db_session,
        transformation,
        dbt_dir,
        fernet_key,
    ):
        # API key belongs to a different org than the transformation
        with _patch_api_auth(
            fake_api_key,
            rate_limit_ok,
            db_session,
            999,
            dbt_dir,
            fernet_key,
        ):
            resp = client.post(
                f"/api/v1/transformations/{transformation.id}/compile",
                headers=_headers(),
            )
        assert resp.status_code == 404

    def test_compilation_error_returns_400(
        self,
        client,
        fake_api_key,
        rate_limit_ok,
        db_session,
        org,
        transformation,
        dbt_dir,
        fernet_key,
    ):
        transformation.sql_body = "{% if broken %}oops"
        db_session.flush()
        with _patch_api_auth(fake_api_key, rate_limit_ok, db_session, org.id, dbt_dir, fernet_key):
            resp = client.post(
                f"/api/v1/transformations/{transformation.id}/compile",
                headers=_headers(),
            )
        assert resp.status_code == 400
        body = resp.json()
        assert body["error"]["code"] == "compilation_error"
        assert body["error"]["message"]


class TestPreviewEndpoint:
    def test_401_without_auth(self, client, transformation):
        resp = client.post(f"/api/v1/transformations/{transformation.id}/preview")
        assert resp.status_code == 401

    def test_success_default_limit(
        self,
        client,
        fake_api_key,
        rate_limit_ok,
        db_session,
        org,
        transformation,
        dbt_dir,
        fernet_key,
        mock_warehouse,
    ):
        with _patch_api_auth(fake_api_key, rate_limit_ok, db_session, org.id, dbt_dir, fernet_key):
            resp = client.post(
                f"/api/v1/transformations/{transformation.id}/preview",
                headers=_headers(),
                json={},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 3
        assert [c["name"] for c in data["columns"]] == ["id", "name", "total"]
        assert data["truncated"] is False

    def test_limit_clamped(
        self,
        client,
        fake_api_key,
        rate_limit_ok,
        db_session,
        org,
        transformation,
        dbt_dir,
        fernet_key,
        mock_warehouse,
    ):
        with _patch_api_auth(fake_api_key, rate_limit_ok, db_session, org.id, dbt_dir, fernet_key):
            resp = client.post(
                f"/api/v1/transformations/{transformation.id}/preview",
                headers=_headers(),
                json={"limit": 99999},
            )
        assert resp.status_code == 200
        # Confirm the endpoint passed the clamped limit through
        query_sent = mock_warehouse.call_args.args[2]
        assert "LIMIT 1000" in query_sent.upper()

    def test_404_on_wrong_org(
        self,
        client,
        fake_api_key,
        rate_limit_ok,
        db_session,
        transformation,
        dbt_dir,
        fernet_key,
    ):
        with _patch_api_auth(
            fake_api_key,
            rate_limit_ok,
            db_session,
            999,
            dbt_dir,
            fernet_key,
        ):
            resp = client.post(
                f"/api/v1/transformations/{transformation.id}/preview",
                headers=_headers(),
                json={},
            )
        assert resp.status_code == 404

    def test_missing_destination_returns_400(
        self,
        client,
        fake_api_key,
        rate_limit_ok,
        db_session,
        org,
        transformation,
        dbt_dir,
        fernet_key,
    ):
        transformation.destination_connection_id = None
        db_session.flush()
        with _patch_api_auth(fake_api_key, rate_limit_ok, db_session, org.id, dbt_dir, fernet_key):
            resp = client.post(
                f"/api/v1/transformations/{transformation.id}/preview",
                headers=_headers(),
                json={},
            )
        assert resp.status_code == 400
