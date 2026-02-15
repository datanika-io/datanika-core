"""TDD tests for connection management service."""

import pytest
from cryptography.fernet import Fernet

from etlfabric.models.connection import Connection, ConnectionDirection, ConnectionType
from etlfabric.models.user import Organization
from etlfabric.services.connection_service import ConnectionService
from etlfabric.services.encryption import EncryptionService


@pytest.fixture
def encryption():
    key = Fernet.generate_key().decode()
    return EncryptionService(key)


@pytest.fixture
def svc(encryption):
    return ConnectionService(encryption)


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-conn-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def other_org(db_session):
    org = Organization(name="OtherCo", slug="other-conn-svc")
    db_session.add(org)
    db_session.flush()
    return org


class TestCreateConnection:
    def test_returns_connection(self, svc, db_session, org):
        conn = svc.create_connection(
            db_session,
            org.id,
            "My DB",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {"host": "localhost", "port": 5432},
        )
        assert isinstance(conn, Connection)
        assert isinstance(conn.id, int)
        assert conn.name == "My DB"
        assert conn.org_id == org.id

    def test_config_is_encrypted(self, svc, db_session, org, encryption):
        conn = svc.create_connection(
            db_session,
            org.id,
            "My DB",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {"host": "localhost"},
        )
        # The raw encrypted field should not contain the plaintext
        assert "localhost" not in conn.config_encrypted
        # But decrypting it should return the original
        decrypted = encryption.decrypt(conn.config_encrypted)
        assert decrypted == {"host": "localhost"}

    def test_sets_org_id(self, svc, db_session, org):
        conn = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.MYSQL,
            ConnectionDirection.DESTINATION,
            {},
        )
        assert conn.org_id == org.id

    def test_sets_type_and_direction(self, svc, db_session, org):
        conn = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.REST_API,
            ConnectionDirection.BOTH,
            {},
        )
        assert conn.connection_type == ConnectionType.REST_API
        assert conn.direction == ConnectionDirection.BOTH


class TestGetConnection:
    def test_existing(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        fetched = svc.get_connection(db_session, org.id, created.id)
        assert fetched is not None
        assert fetched.id == created.id

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_connection(db_session, org.id, 99999) is None

    def test_wrong_org(self, svc, db_session, org, other_org):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        assert svc.get_connection(db_session, other_org.id, created.id) is None

    def test_soft_deleted(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        svc.delete_connection(db_session, org.id, created.id)
        assert svc.get_connection(db_session, org.id, created.id) is None


class TestGetConnectionConfig:
    def test_decrypts_roundtrip(self, svc, db_session, org):
        config = {"host": "db.example.com", "port": 5432, "password": "s3cret"}
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            config,
        )
        result = svc.get_connection_config(db_session, org.id, created.id)
        assert result == config

    def test_nonexistent(self, svc, db_session, org):
        assert svc.get_connection_config(db_session, org.id, 99999) is None


class TestListConnections:
    def test_empty(self, svc, db_session, org):
        result = svc.list_connections(db_session, org.id)
        assert result == []

    def test_multiple(self, svc, db_session, org):
        svc.create_connection(
            db_session,
            org.id,
            "A",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        svc.create_connection(
            db_session,
            org.id,
            "B",
            ConnectionType.MYSQL,
            ConnectionDirection.DESTINATION,
            {},
        )
        result = svc.list_connections(db_session, org.id)
        assert len(result) == 2

    def test_excludes_deleted(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "A",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        svc.create_connection(
            db_session,
            org.id,
            "B",
            ConnectionType.MYSQL,
            ConnectionDirection.DESTINATION,
            {},
        )
        svc.delete_connection(db_session, org.id, created.id)
        result = svc.list_connections(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "B"

    def test_filters_by_org(self, svc, db_session, org, other_org):
        svc.create_connection(
            db_session,
            org.id,
            "A",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        svc.create_connection(
            db_session,
            other_org.id,
            "B",
            ConnectionType.MYSQL,
            ConnectionDirection.DESTINATION,
            {},
        )
        result = svc.list_connections(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "A"


class TestUpdateConnection:
    def test_update_name(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "Old",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        updated = svc.update_connection(db_session, org.id, created.id, name="New")
        assert updated is not None
        assert updated.name == "New"

    def test_re_encrypts_config(self, svc, db_session, org, encryption):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {"host": "old"},
        )
        old_encrypted = created.config_encrypted
        updated = svc.update_connection(
            db_session,
            org.id,
            created.id,
            config={"host": "new"},
        )
        assert updated.config_encrypted != old_encrypted
        assert encryption.decrypt(updated.config_encrypted) == {"host": "new"}

    def test_nonexistent(self, svc, db_session, org):
        assert svc.update_connection(db_session, org.id, 99999, name="X") is None

    def test_preserves_unchanged_fields(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "Keep",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {"host": "keep"},
        )
        old_encrypted = created.config_encrypted
        updated = svc.update_connection(db_session, org.id, created.id, name="Changed")
        assert updated.name == "Changed"
        assert updated.config_encrypted == old_encrypted


class TestDeleteConnection:
    def test_sets_deleted_at(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        result = svc.delete_connection(db_session, org.id, created.id)
        assert result is True
        # Verify deleted_at is set by querying directly
        db_session.refresh(created)
        assert created.deleted_at is not None

    def test_nonexistent(self, svc, db_session, org):
        result = svc.delete_connection(db_session, org.id, 99999)
        assert result is False

    def test_idempotent(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
            ConnectionDirection.SOURCE,
            {},
        )
        svc.delete_connection(db_session, org.id, created.id)
        result = svc.delete_connection(db_session, org.id, created.id)
        assert result is False


class TestTestConnection:
    def test_empty_config_fails(self, svc):
        ok, msg = svc.test_connection({}, ConnectionType.POSTGRES)
        assert ok is False
        assert "empty" in msg.lower()

    def test_sqlite_real_connection(self, svc):
        """Real in-memory SQLite — no mocks needed."""
        ok, msg = svc.test_connection({"path": ":memory:"}, ConnectionType.SQLITE)
        assert ok is True
        assert msg == "Connected successfully"

    def test_postgres_connection_mocked(self, svc):
        """Mock create_engine to verify SELECT 1 is executed."""
        from unittest.mock import MagicMock, patch

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch(
            "etlfabric.services.connection_service.create_engine", return_value=mock_engine
        ):
            ok, msg = svc.test_connection(
                {"host": "localhost", "port": 5432, "user": "u", "password": "p", "database": "d"},
                ConnectionType.POSTGRES,
            )
        assert ok is True
        assert msg == "Connected successfully"
        mock_conn.execute.assert_called_once()
        executed_sql = str(mock_conn.execute.call_args[0][0])
        assert "SELECT 1" in executed_sql

    def test_connection_failure(self, svc):
        """Mock engine raising OperationalError."""
        from unittest.mock import MagicMock, patch

        from sqlalchemy.exc import OperationalError

        mock_engine = MagicMock()
        mock_engine.connect.side_effect = OperationalError("stmt", {}, Exception("refused"))

        with patch(
            "etlfabric.services.connection_service.create_engine", return_value=mock_engine
        ):
            ok, msg = svc.test_connection(
                {"host": "badhost", "user": "u", "password": "p", "database": "d"},
                ConnectionType.POSTGRES,
            )
        assert ok is False
        assert len(msg) > 0

    def test_missing_driver(self, svc):
        """Mock create_engine raising ImportError."""
        from unittest.mock import patch

        with patch(
            "etlfabric.services.connection_service.create_engine",
            side_effect=ImportError("No module named 'pymysql'"),
        ):
            ok, msg = svc.test_connection(
                {"host": "localhost", "user": "u", "password": "p", "database": "d"},
                ConnectionType.MYSQL,
            )
        assert ok is False
        assert "Driver not installed" in msg

    def test_non_db_type_skipped_s3(self, svc):
        ok, msg = svc.test_connection({"bucket_url": "s3://my-bucket"}, ConnectionType.S3)
        assert ok is True
        assert "not applicable" in msg.lower()

    def test_non_db_type_skipped_csv(self, svc):
        ok, msg = svc.test_connection({"bucket_url": "/data"}, ConnectionType.CSV)
        assert ok is True
        assert "not applicable" in msg.lower()

    def test_non_db_type_skipped_rest_api(self, svc):
        config = {"base_url": "https://api.example.com"}
        ok, msg = svc.test_connection(config, ConnectionType.REST_API)
        assert ok is True
        assert "not applicable" in msg.lower()


class TestBuildSaUrl:
    def test_postgres_url(self):
        from etlfabric.services.connection_service import _build_sa_url

        config = {
            "host": "db.example.com", "port": 5432,
            "user": "admin", "password": "s3c", "database": "mydb",
        }
        url = _build_sa_url(config, ConnectionType.POSTGRES)
        assert url.startswith("postgresql+psycopg2://")
        assert "admin" in url
        assert "db.example.com" in url
        assert "5432" in url
        assert "mydb" in url

    def test_redshift_url(self):
        from etlfabric.services.connection_service import _build_sa_url

        config = {
            "host": "cluster.redshift.amazonaws.com",
            "user": "u", "password": "p", "database": "dw",
        }
        url = _build_sa_url(config, ConnectionType.REDSHIFT)
        assert url.startswith("postgresql+psycopg2://")
        assert "5439" in url  # default redshift port

    def test_mysql_url(self):
        from etlfabric.services.connection_service import _build_sa_url

        url = _build_sa_url(
            {"host": "mysql.local", "user": "root", "password": "pw", "database": "app"},
            ConnectionType.MYSQL,
        )
        assert url.startswith("mysql+pymysql://")
        assert "3306" in url

    def test_mssql_url(self):
        from etlfabric.services.connection_service import _build_sa_url

        url = _build_sa_url(
            {"host": "sql.local", "user": "sa", "password": "pw", "database": "master"},
            ConnectionType.MSSQL,
        )
        assert url.startswith("mssql+pymssql://")
        assert "1433" in url

    def test_sqlite_url(self):
        from etlfabric.services.connection_service import _build_sa_url

        url = _build_sa_url({"path": "/tmp/test.db"}, ConnectionType.SQLITE)
        assert url == "sqlite:////tmp/test.db"

    def test_sqlite_memory_url(self):
        from etlfabric.services.connection_service import _build_sa_url

        url = _build_sa_url({"path": ":memory:"}, ConnectionType.SQLITE)
        assert url == "sqlite:///:memory:"

    def test_snowflake_url(self):
        from etlfabric.services.connection_service import _build_sa_url

        url = _build_sa_url(
            {
                "account": "xy12345.us-east-1",
                "user": "etl_user",
                "password": "secret",
                "database": "ANALYTICS",
                "schema": "PUBLIC",
                "warehouse": "COMPUTE_WH",
                "role": "LOADER",
            },
            ConnectionType.SNOWFLAKE,
        )
        assert url.startswith("snowflake://")
        assert "xy12345.us-east-1" in url
        assert "warehouse=COMPUTE_WH" in url
        assert "role=LOADER" in url

    def test_bigquery_url(self):
        from etlfabric.services.connection_service import _build_sa_url

        url = _build_sa_url(
            {"project": "my-project", "dataset": "raw_data"},
            ConnectionType.BIGQUERY,
        )
        assert url == "bigquery://my-project/raw_data"

    def test_special_chars_encoded(self):
        from etlfabric.services.connection_service import _build_sa_url

        config = {
            "host": "db.example.com", "user": "admin@corp",
            "password": "p@ss/word!", "database": "db",
        }
        url = _build_sa_url(config, ConnectionType.POSTGRES)
        assert "admin%40corp" in url
        assert "p%40ss%2Fword%21" in url

    def test_unsupported_type_raises(self):
        from etlfabric.services.connection_service import _build_sa_url

        with pytest.raises(ValueError, match="Unsupported"):
            _build_sa_url({"bucket": "x"}, ConnectionType.S3)
