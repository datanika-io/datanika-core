"""Tests for REST API v1 endpoints."""

from unittest.mock import MagicMock, patch

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

import datanika.models.invitation  # noqa: F401

# Import all models so Base.metadata knows about all tables
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
    key.scopes = None  # no scope restriction
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


def _auth_headers(key="etf_testkey"):
    return {"Authorization": f"Bearer {key}"}


def _patch_auth(fake_api_key, rate_limit_ok):
    """Return a context manager that patches auth + rate limiting."""
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

        # Build properly initialized services for tests
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
# Auth tests
# ---------------------------------------------------------------------------


class TestApiAuth:
    def test_no_auth_returns_401(self, client):
        resp = client.get("/api/v1/connections")
        assert resp.status_code == 401

    def test_bad_key_returns_401(self, client):
        with patch("datanika.services.api_middleware._api_key_svc") as mock_svc:
            mock_svc.authenticate_api_key.return_value = None
            with patch("datanika.services.api_middleware._get_session") as ms:
                ms.return_value.__enter__ = lambda s: MagicMock()
                ms.return_value.__exit__ = lambda s, *a: None
                resp = client.get("/api/v1/connections", headers=_auth_headers())
                assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Connection endpoints
# ---------------------------------------------------------------------------


class TestConnectionEndpoints:
    def test_list_connections_empty(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.get("/api/v1/connections", headers=_auth_headers())
            assert resp.status_code == 200
            assert resp.json()["items"] == []

    def test_create_connection_missing_fields(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/connections",
                json={"name": "test"},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400

    def test_create_and_get_connection(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            # Create
            resp = client.post(
                "/api/v1/connections",
                json={
                    "name": "Test PG",
                    "connection_type": "postgres",
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "user": "u",
                        "password": "p",
                        "database": "db",
                    },
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 201
            data = resp.json()
            assert data["name"] == "Test PG"
            assert data["connection_type"] == "postgres"
            conn_id = data["id"]

            # Get
            resp = client.get(f"/api/v1/connections/{conn_id}", headers=_auth_headers())
            assert resp.status_code == 200
            assert resp.json()["id"] == conn_id

    def test_update_connection(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/connections",
                json={
                    "name": "Old Name",
                    "connection_type": "postgres",
                    "config": {"host": "localhost"},
                },
                headers=_auth_headers(),
            )
            conn_id = resp.json()["id"]

            resp = client.put(
                f"/api/v1/connections/{conn_id}",
                json={"name": "New Name"},
                headers=_auth_headers(),
            )
            assert resp.status_code == 200
            assert resp.json()["name"] == "New Name"

    def test_delete_connection(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/connections",
                json={
                    "name": "To Delete",
                    "connection_type": "postgres",
                    "config": {"host": "localhost"},
                },
                headers=_auth_headers(),
            )
            conn_id = resp.json()["id"]

            resp = client.delete(f"/api/v1/connections/{conn_id}", headers=_auth_headers())
            assert resp.status_code == 200
            assert resp.json()["deleted"] is True

            # Should be gone
            resp = client.get(f"/api/v1/connections/{conn_id}", headers=_auth_headers())
            assert resp.status_code == 404

    def test_get_nonexistent_connection(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.get("/api/v1/connections/999", headers=_auth_headers())
            assert resp.status_code == 404

    def test_invalid_connection_type(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/connections",
                json={"name": "bad", "connection_type": "invalid_db", "config": {}},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400


class TestConnectionIntrospection:
    """Tests for introspect/columns/preview/query endpoints (#39)."""

    def _create_sqlite_conn_with_table(self, client, db_path):
        """Create a SQLite connection and seed it with a test table."""
        # Seed the SQLite file
        import sqlite3

        c = sqlite3.connect(db_path)
        c.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT, total REAL)")
        c.executemany(
            "INSERT INTO orders (name, total) VALUES (?, ?)",
            [("Alice", 10.5), ("Bob", 20.0), ("Carol", 15.75)],
        )
        c.commit()
        c.close()
        # Create connection via API
        resp = client.post(
            "/api/v1/connections",
            json={
                "name": "Sample SQLite",
                "connection_type": "sqlite",
                "config": {"path": str(db_path)},
            },
            headers=_auth_headers(),
        )
        assert resp.status_code == 201
        return resp.json()["id"]

    def test_introspect_lists_tables(self, client, fake_api_key, rate_limit_ok, tmp_path):
        with _patch_auth(fake_api_key, rate_limit_ok):
            db = tmp_path / "test.db"
            conn_id = self._create_sqlite_conn_with_table(client, db)
            resp = client.post(
                f"/api/v1/connections/{conn_id}/introspect",
                json={},
                headers=_auth_headers(),
            )
            assert resp.status_code == 200
            tables = resp.json()["tables"]
            names = [t["name"] for t in tables]
            assert "orders" in names

    def test_columns_lists_columns(self, client, fake_api_key, rate_limit_ok, tmp_path):
        with _patch_auth(fake_api_key, rate_limit_ok):
            db = tmp_path / "test.db"
            conn_id = self._create_sqlite_conn_with_table(client, db)
            resp = client.post(
                f"/api/v1/connections/{conn_id}/columns",
                json={"table": "orders"},
                headers=_auth_headers(),
            )
            assert resp.status_code == 200
            cols = resp.json()["columns"]
            names = [c["name"] for c in cols]
            assert names == ["id", "name", "total"]

    def test_columns_requires_table(self, client, fake_api_key, rate_limit_ok, tmp_path):
        with _patch_auth(fake_api_key, rate_limit_ok):
            db = tmp_path / "test.db"
            conn_id = self._create_sqlite_conn_with_table(client, db)
            resp = client.post(
                f"/api/v1/connections/{conn_id}/columns",
                json={},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400

    def test_preview_returns_rows(self, client, fake_api_key, rate_limit_ok, tmp_path):
        with _patch_auth(fake_api_key, rate_limit_ok):
            db = tmp_path / "test.db"
            conn_id = self._create_sqlite_conn_with_table(client, db)
            resp = client.post(
                f"/api/v1/connections/{conn_id}/preview",
                json={"table": "orders", "limit": 2},
                headers=_auth_headers(),
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["columns"] == ["id", "name", "total"]
            assert len(data["rows"]) == 2

    def test_query_executes_select(self, client, fake_api_key, rate_limit_ok, tmp_path):
        with _patch_auth(fake_api_key, rate_limit_ok):
            db = tmp_path / "test.db"
            conn_id = self._create_sqlite_conn_with_table(client, db)
            resp = client.post(
                f"/api/v1/connections/{conn_id}/query",
                json={"query": "SELECT name FROM orders ORDER BY id"},
                headers=_auth_headers(),
            )
            assert resp.status_code == 200
            data = resp.json()
            assert data["columns"] == ["name"]
            assert data["rows"] == [["Alice"], ["Bob"], ["Carol"]]

    def test_query_rejects_drop(self, client, fake_api_key, rate_limit_ok, tmp_path):
        with _patch_auth(fake_api_key, rate_limit_ok):
            db = tmp_path / "test.db"
            conn_id = self._create_sqlite_conn_with_table(client, db)
            resp = client.post(
                f"/api/v1/connections/{conn_id}/query",
                json={"query": "DROP TABLE orders"},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400

    def test_query_rejects_insert(self, client, fake_api_key, rate_limit_ok, tmp_path):
        with _patch_auth(fake_api_key, rate_limit_ok):
            db = tmp_path / "test.db"
            conn_id = self._create_sqlite_conn_with_table(client, db)
            resp = client.post(
                f"/api/v1/connections/{conn_id}/query",
                json={"query": "INSERT INTO orders (name, total) VALUES ('X', 1)"},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400

    def test_query_rejects_multiple_statements(self, client, fake_api_key, rate_limit_ok, tmp_path):
        with _patch_auth(fake_api_key, rate_limit_ok):
            db = tmp_path / "test.db"
            conn_id = self._create_sqlite_conn_with_table(client, db)
            resp = client.post(
                f"/api/v1/connections/{conn_id}/query",
                json={"query": "SELECT 1; DROP TABLE orders"},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400

    def test_introspect_404_for_unknown_connection(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/connections/9999/introspect",
                json={},
                headers=_auth_headers(),
            )
            assert resp.status_code == 404

    def test_introspect_rejects_non_db_connection(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/connections",
                json={
                    "name": "Stripe",
                    "connection_type": "stripe",
                    "config": {"api_key": "sk_test"},
                },
                headers=_auth_headers(),
            )
            conn_id = resp.json()["id"]
            resp = client.post(
                f"/api/v1/connections/{conn_id}/introspect",
                json={},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400


class TestCatalogEndpoints:
    """Tests for /api/v1/catalog endpoints (#40)."""

    def _seed_catalog(self, session, org_id, **overrides):
        from datanika.models.catalog_entry import CatalogEntry, CatalogEntryType
        from datanika.models.dependency import NodeType

        defaults = {
            "org_id": org_id,
            "entry_type": CatalogEntryType.SOURCE_TABLE,
            "origin_type": NodeType.UPLOAD,
            "origin_id": 1,
            "schema_name": "raw",
            "table_name": "orders",
            "dataset_name": "raw_data",
            "columns": [{"name": "id", "data_type": "INTEGER"}],
        }
        defaults.update(overrides)
        entry = CatalogEntry(**defaults)
        session.add(entry)
        session.flush()
        return entry

    def test_list_empty(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.get("/api/v1/catalog", headers=_auth_headers())
            assert resp.status_code == 200
            assert resp.json()["items"] == []

    def test_list_returns_seeded_entries(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            self._seed_catalog(session, fake_api_key.org_id, table_name="orders")
            self._seed_catalog(session, fake_api_key.org_id, table_name="customers")
            session.commit()

            resp = client.get("/api/v1/catalog", headers=_auth_headers())
            assert resp.status_code == 200
            items = resp.json()["items"]
            names = {e["table_name"] for e in items}
            assert names == {"orders", "customers"}

    def test_list_filters_by_schema(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            self._seed_catalog(session, fake_api_key.org_id, schema_name="raw", table_name="orders")
            self._seed_catalog(
                session, fake_api_key.org_id, schema_name="staging", table_name="stg_orders"
            )
            session.commit()

            resp = client.get("/api/v1/catalog?schema=staging", headers=_auth_headers())
            items = resp.json()["items"]
            assert len(items) == 1
            assert items[0]["schema_name"] == "staging"

    def test_list_filters_by_entry_type(self, client, fake_api_key, rate_limit_ok):
        from datanika.models.catalog_entry import CatalogEntryType

        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            self._seed_catalog(
                session,
                fake_api_key.org_id,
                entry_type=CatalogEntryType.SOURCE_TABLE,
                table_name="orders",
            )
            self._seed_catalog(
                session,
                fake_api_key.org_id,
                entry_type=CatalogEntryType.DBT_MODEL,
                table_name="stg_orders",
            )
            session.commit()

            resp = client.get("/api/v1/catalog?entry_type=dbt_model", headers=_auth_headers())
            items = resp.json()["items"]
            assert len(items) == 1
            assert items[0]["entry_type"] == "dbt_model"

    def test_list_invalid_entry_type(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.get("/api/v1/catalog?entry_type=bogus", headers=_auth_headers())
            assert resp.status_code == 400

    def test_list_filters_by_connection_id(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            self._seed_catalog(session, fake_api_key.org_id, table_name="a", connection_id=42)
            self._seed_catalog(session, fake_api_key.org_id, table_name="b", connection_id=99)
            session.commit()

            resp = client.get("/api/v1/catalog?connection_id=42", headers=_auth_headers())
            items = resp.json()["items"]
            assert len(items) == 1
            assert items[0]["table_name"] == "a"

    def test_get_returns_entry(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            entry = self._seed_catalog(session, fake_api_key.org_id, table_name="orders")
            session.commit()

            resp = client.get(f"/api/v1/catalog/{entry.id}", headers=_auth_headers())
            assert resp.status_code == 200
            data = resp.json()
            assert data["id"] == entry.id
            assert data["table_name"] == "orders"
            assert data["columns"] == [{"name": "id", "data_type": "INTEGER"}]

    def test_get_404_for_unknown_entry(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.get("/api/v1/catalog/9999", headers=_auth_headers())
            assert resp.status_code == 404

    def test_tenant_isolation(self, client, fake_api_key, rate_limit_ok):
        """Org A cannot read org B's catalog entry."""
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            other_entry = self._seed_catalog(session, org_id=999, table_name="other_org")
            session.commit()

            # fake_api_key.org_id is 10, not 999
            resp = client.get(f"/api/v1/catalog/{other_entry.id}", headers=_auth_headers())
            assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Transformation endpoints
# ---------------------------------------------------------------------------


class TestTransformationEndpoints:
    def test_list_transformations_empty(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.get("/api/v1/transformations", headers=_auth_headers())
            assert resp.status_code == 200
            assert resp.json()["items"] == []

    def test_create_transformation(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/transformations",
                json={
                    "name": "stg_orders",
                    "sql_body": "SELECT * FROM raw.orders",
                    "materialization": "view",
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 201
            assert resp.json()["name"] == "stg_orders"

    def test_create_transformation_missing_sql(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/transformations",
                json={"name": "test"},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400

    def test_update_transformation(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/transformations",
                json={
                    "name": "my_model",
                    "sql_body": "SELECT 1",
                    "materialization": "table",
                },
                headers=_auth_headers(),
            )
            tid = resp.json()["id"]

            resp = client.put(
                f"/api/v1/transformations/{tid}",
                json={"sql_body": "SELECT 2"},
                headers=_auth_headers(),
            )
            assert resp.status_code == 200
            assert resp.json()["sql_body"] == "SELECT 2"

    def test_delete_transformation(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/transformations",
                json={
                    "name": "to_del",
                    "sql_body": "SELECT 1",
                },
                headers=_auth_headers(),
            )
            tid = resp.json()["id"]
            resp = client.delete(f"/api/v1/transformations/{tid}", headers=_auth_headers())
            assert resp.status_code == 200
            assert resp.json()["deleted"] is True


# ---------------------------------------------------------------------------
# Pipeline endpoints
# ---------------------------------------------------------------------------


class TestPipelineEndpoints:
    def _create_destination(self, client):
        resp = client.post(
            "/api/v1/connections",
            json={
                "name": "Dest PG",
                "connection_type": "postgres",
                "config": {"host": "localhost"},
            },
            headers=_auth_headers(),
        )
        return resp.json()["id"]

    def test_list_pipelines_empty(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.get("/api/v1/pipelines", headers=_auth_headers())
            assert resp.status_code == 200
            assert resp.json()["items"] == []

    def test_create_pipeline(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            dest_id = self._create_destination(client)
            resp = client.post(
                "/api/v1/pipelines",
                json={
                    "name": "Analytics Pipeline",
                    "destination_connection_id": dest_id,
                    "command": "run",
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 201
            assert resp.json()["name"] == "Analytics Pipeline"

    def test_create_pipeline_missing_name(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/pipelines",
                json={"destination_connection_id": 1},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Schedule endpoints
# ---------------------------------------------------------------------------


class TestScheduleEndpoints:
    def test_list_schedules_empty(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.get("/api/v1/schedules", headers=_auth_headers())
            assert resp.status_code == 200
            assert resp.json()["items"] == []

    def test_create_schedule_missing_fields(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/schedules",
                json={"cron_expression": "*/5 * * * *"},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400

    def test_create_schedule_invalid_target_type(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/schedules",
                json={
                    "target_type": "invalid",
                    "target_id": 1,
                    "cron_expression": "*/5 * * * *",
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Run endpoints
# ---------------------------------------------------------------------------


class TestRunEndpoints:
    def test_list_runs_empty(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.get("/api/v1/runs", headers=_auth_headers())
            assert resp.status_code == 200
            assert resp.json()["items"] == []

    def test_get_run_not_found(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.get("/api/v1/runs/999", headers=_auth_headers())
            assert resp.status_code == 404

    def test_list_runs_with_invalid_filter(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.get(
                "/api/v1/runs?target_type=invalid",
                headers=_auth_headers(),
            )
            assert resp.status_code == 400


# ---------------------------------------------------------------------------
# Notification channel endpoints
# ---------------------------------------------------------------------------


class TestNotificationChannelEndpoints:
    def test_list_channels_empty(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.get("/api/v1/notifications/channels", headers=_auth_headers())
            assert resp.status_code == 200
            assert resp.json()["items"] == []

    def test_create_slack_channel(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/notifications/channels",
                json={
                    "name": "Slack alerts",
                    "channel_type": "slack",
                    "config": {"webhook_url": "https://hooks.slack.com/test"},
                    "events": ["run_failure"],
                },
                headers=_auth_headers(),
            )
            assert resp.status_code == 201
            data = resp.json()
            assert data["name"] == "Slack alerts"
            assert data["channel_type"] == "slack"

    def test_create_channel_invalid_type(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/notifications/channels",
                json={"name": "bad", "channel_type": "pigeon"},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400

    def test_create_channel_missing_name(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/notifications/channels",
                json={"channel_type": "slack"},
                headers=_auth_headers(),
            )
            assert resp.status_code == 400

    def test_update_channel(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/notifications/channels",
                json={
                    "name": "test-ch",
                    "channel_type": "webhook",
                    "config": {"url": "https://example.com/hook"},
                    "events": ["run_failure"],
                },
                headers=_auth_headers(),
            )
            cid = resp.json()["id"]

            resp = client.put(
                f"/api/v1/notifications/channels/{cid}",
                json={"name": "updated-ch", "events": ["run_failure", "run_success"]},
                headers=_auth_headers(),
            )
            assert resp.status_code == 200
            assert resp.json()["name"] == "updated-ch"

    def test_delete_channel(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok):
            resp = client.post(
                "/api/v1/notifications/channels",
                json={
                    "name": "to-del",
                    "channel_type": "telegram",
                    "config": {"token": "123:abc", "chat_id": "456"},
                    "events": ["run_success"],
                },
                headers=_auth_headers(),
            )
            cid = resp.json()["id"]
            resp = client.delete(
                f"/api/v1/notifications/channels/{cid}",
                headers=_auth_headers(),
            )
            assert resp.status_code == 200
            assert resp.json()["deleted"] is True


# ---------------------------------------------------------------------------
# Atomicity of a rejected update (core#790)
# ---------------------------------------------------------------------------


class HarnessError(RuntimeError):
    """Raised when a test's SETUP does not hold.

    Deliberately not an `AssertionError`. The two tests below are
    `xfail(strict=True, raises=AssertionError)`, and a bare `assert` in their
    setup would be **absorbed by the marker** — a broken harness would report as
    a satisfied expected-failure, which is the exact trap `docs/QA_RULES.md` §5
    describes. With this, only the one assertion under test can xfail; anything
    else fails loudly.
    """


def _require(condition, message: str) -> None:
    if not condition:
        raise HarnessError(message)


class TestRejectedUpdateIsAtomic:
    """A request the API rejects must leave the row exactly as it was.

    `api_endpoint` commits when the handler **returns** and rolls back only when
    it **raises** — and `_error(400, ...)` is a return. The update services
    assign as they go and validate in place, so a validator that raises part-way
    down commits everything assigned above it. The API reports a clean 400 and
    the earlier fields are durable.

    🔑 **The rollback in `_name_in_db` is the assertion, not tidying.** Reading
    the attribute straight back proves nothing: a merely-dirty value in the
    identity map reads identically to a committed one, and two handles on the
    same identity-mapped row move together. The first version of this probe
    printed *"clean: nothing persisted"* over data showing the opposite, for
    exactly that reason. `session.rollback()` is the only step that tells the
    two apart.
    """

    @staticmethod
    def _name_in_db(session, model, row_id) -> str:
        from sqlalchemy import select

        # An uncommitted dirty attribute does not survive this; a committed one
        # does. That difference is the whole test.
        session.rollback()
        session.expire_all()
        return str(session.execute(select(model.name).where(model.id == row_id)).scalar_one())

    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason="core#790 — api_endpoint commits on a RETURN, so a 400 keeps the "
        "fields assigned before the guard that rejected the request",
    )
    def test_rejected_transformation_update_does_not_rename(
        self, client, fake_api_key, rate_limit_ok
    ):
        from datanika.models.transformation import Transformation

        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            created = client.post(
                "/api/v1/transformations",
                json={"name": "stg_orders", "sql_body": "SELECT 1"},
                headers=_auth_headers(),
            )
            _require(created.status_code == 201, f"setup: create failed {created.text}")
            tid = created.json()["id"]

            # `name` is assigned before `validate_tests_config` runs.
            resp = client.put(
                f"/api/v1/transformations/{tid}",
                json={"name": "renamed_by_a_rejected_request", "tests_config": {"columns": "x"}},
                headers=_auth_headers(),
            )
            _require(
                resp.status_code == 400, f"setup: expected 400, got {resp.status_code} {resp.text}"
            )

            assert self._name_in_db(session, Transformation, tid) == "stg_orders", (
                "the API rejected this update with 400 and committed the rename anyway"
            )

    @pytest.mark.xfail(
        strict=True,
        raises=AssertionError,
        reason="core#790 — same defect on the pipeline update path",
    )
    def test_rejected_pipeline_update_does_not_rename(self, client, fake_api_key, rate_limit_ok):
        from datanika.models.pipeline import Pipeline

        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            dest = client.post(
                "/api/v1/connections",
                json={
                    "name": "Dest PG",
                    "connection_type": "postgres",
                    "config": {"host": "localhost"},
                },
                headers=_auth_headers(),
            )
            _require(dest.status_code == 201, f"setup: connection create failed {dest.text}")
            created = client.post(
                "/api/v1/pipelines",
                json={"name": "Analytics", "destination_connection_id": dest.json()["id"]},
                headers=_auth_headers(),
            )
            _require(created.status_code == 201, f"setup: create failed {created.text}")
            pid = created.json()["id"]

            # `name` is assigned before the destination_connection_id guard.
            resp = client.put(
                f"/api/v1/pipelines/{pid}",
                json={
                    "name": "renamed_by_a_rejected_request",
                    "destination_connection_id": 999999,
                },
                headers=_auth_headers(),
            )
            _require(
                resp.status_code == 400, f"setup: expected 400, got {resp.status_code} {resp.text}"
            )

            assert self._name_in_db(session, Pipeline, pid) == "Analytics", (
                "the API rejected this update with 400 and committed the rename anyway"
            )

    def test_a_rejection_with_nothing_assigned_first_is_clean(
        self, client, fake_api_key, rate_limit_ok
    ):
        """CONTROL — and the two xfails mean nothing without it.

        This sends *only* the field that fails validation, so the service
        assigns nothing before raising. It passes today. If a fix makes the two
        above pass by rolling back indiscriminately, this still has to hold —
        and if this one ever fails, the harness is broken rather than the app,
        because there is no write for a 400 to have leaked.
        """
        from datanika.models.transformation import Transformation

        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            created = client.post(
                "/api/v1/transformations",
                json={"name": "stg_clean", "sql_body": "SELECT 1"},
                headers=_auth_headers(),
            )
            assert created.status_code == 201, created.text
            tid = created.json()["id"]

            resp = client.put(
                f"/api/v1/transformations/{tid}",
                json={"tests_config": {"columns": "x"}},
                headers=_auth_headers(),
            )
            _require(
                resp.status_code == 400, f"setup: expected 400, got {resp.status_code} {resp.text}"
            )

            assert self._name_in_db(session, Transformation, tid) == "stg_clean"
