"""TDD tests for connection management service."""

from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet

from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.models.user import Organization
from datanika.services.connection_service import ConnectionService, infer_direction
from datanika.services.egress_guard import EgressValidationError
from datanika.services.encryption import EncryptionService


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
            {},
        )
        assert conn.org_id == org.id

    def test_sets_type_and_direction(self, svc, db_session, org):
        conn = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.REST_API,
            {},
        )
        assert conn.connection_type == ConnectionType.REST_API
        # REST_API is source-only, so direction should be SOURCE
        assert conn.direction == ConnectionDirection.SOURCE

    # --- SSRF create-time egress gate (core#338) -------------------------------
    def test_rejects_rest_api_base_url_resolving_to_private(self, svc, db_session, org):
        """A rest_api base_url that resolves to a private IP is refused at create."""
        with (
            patch("socket.getaddrinfo", return_value=[(2, 1, 6, "", ("10.0.0.5", 0))]),
            pytest.raises(EgressValidationError),
        ):
            svc.create_connection(
                db_session,
                org.id,
                "Internal API",
                ConnectionType.REST_API,
                {"base_url": "http://internal.corp.example"},
            )

    def test_rejects_base_url_resolving_to_metadata_ip(self, svc, db_session, org):
        with (
            patch("socket.getaddrinfo", return_value=[(2, 1, 6, "", ("169.254.169.254", 0))]),
            pytest.raises(EgressValidationError),
        ):
            svc.create_connection(
                db_session,
                org.id,
                "Metadata Grab",
                ConnectionType.REST_API,
                {"base_url": "http://169.254.169.254/latest/meta-data/"},
            )

    def test_allows_rest_api_public_base_url(self, svc, db_session, org):
        with patch("socket.getaddrinfo", return_value=[(2, 1, 6, "", ("93.184.216.34", 0))]):
            conn = svc.create_connection(
                db_session,
                org.id,
                "Public API",
                ConnectionType.REST_API,
                {"base_url": "https://api.public.example"},
            )
        assert conn.connection_type == ConnectionType.REST_API

    def test_guard_skipped_when_no_base_url(self, svc, db_session, org):
        """DB connectors (no base_url) never hit the egress guard."""
        with patch("datanika.services.connection_service.validate_egress_host") as mock_guard:
            svc.create_connection(
                db_session, org.id, "My DB", ConnectionType.POSTGRES, {"host": "localhost"}
            )
        mock_guard.assert_not_called()


class TestGetConnection:
    def test_existing(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
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
            {},
        )
        assert svc.get_connection(db_session, other_org.id, created.id) is None

    def test_soft_deleted(self, svc, db_session, org):
        created = svc.create_connection(
            db_session,
            org.id,
            "X",
            ConnectionType.POSTGRES,
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
            {},
        )
        svc.create_connection(
            db_session,
            org.id,
            "B",
            ConnectionType.MYSQL,
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
            {},
        )
        svc.create_connection(
            db_session,
            org.id,
            "B",
            ConnectionType.MYSQL,
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
            {},
        )
        svc.create_connection(
            db_session,
            other_org.id,
            "B",
            ConnectionType.MYSQL,
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

        with patch("datanika.services.connection_service.create_engine", return_value=mock_engine):
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

        with patch("datanika.services.connection_service.create_engine", return_value=mock_engine):
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
            "datanika.services.connection_service.create_engine",
            side_effect=ImportError("No module named 'pymysql'"),
        ):
            ok, msg = svc.test_connection(
                {"host": "localhost", "user": "u", "password": "p", "database": "d"},
                ConnectionType.MYSQL,
            )
        assert ok is False
        assert "Driver not installed" in msg

    def test_file_types_are_no_longer_skipped_s3(self, svc):
        """Contract change (core#493): file sources are testable, so they get tested.

        These two used to assert `(True, "not applicable")` — which is exactly
        how a wrong path came to test identically to a right one, and why a
        typo'd `bucket_url` was first noticed as a green run with zero rows.
        An unreachable bucket must not report success.
        """
        ok, msg = svc.test_connection({"bucket_url": "s3://no-such-bucket"}, ConnectionType.S3)
        assert ok is False
        assert "not applicable" not in msg.lower()

    def test_file_types_are_no_longer_skipped_csv(self, svc):
        ok, msg = svc.test_connection({"bucket_url": "/data"}, ConnectionType.CSV)
        assert ok is False
        assert "not applicable" not in msg.lower()

    def test_rest_api_is_reported_as_not_tested(self, svc):
        """core#821: this used to answer `(True, "Test not applicable")`.

        `True` paints the message green, so a connector nobody had checked
        read as verified. `rest_api` still cannot be probed — a base URL plus
        arbitrary auth offers no endpoint we know is safe to call — but the
        honest answer to "did it work?" is *not tested*, not *yes*.
        """
        config = {"base_url": "https://api.example.com"}
        ok, msg = svc.test_connection(config, ConnectionType.REST_API)
        assert ok is None, f"expected the neutral verdict, got {ok!r}"
        assert "not tested" in msg.lower()

    def test_mongodb_connection_mocked(self, svc):
        """Mock pymongo.MongoClient to verify server_info() is called."""
        from unittest.mock import MagicMock, patch

        mock_client_cls = MagicMock()
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        with patch.dict("sys.modules", {"pymongo": MagicMock(MongoClient=mock_client_cls)}):
            ok, msg = svc.test_connection(
                {"host": "localhost", "port": 27017, "database": "testdb"},
                ConnectionType.MONGODB,
            )
        assert ok is True
        assert msg == "Connected successfully"
        mock_client.server_info.assert_called_once()
        mock_client.close.assert_called_once()

    def test_mongodb_connection_failure(self, svc):
        """Mock pymongo.MongoClient raising an exception."""
        from unittest.mock import MagicMock, patch

        mock_client_cls = MagicMock()
        mock_client_cls.return_value.server_info.side_effect = Exception("refused")

        with patch.dict("sys.modules", {"pymongo": MagicMock(MongoClient=mock_client_cls)}):
            ok, msg = svc.test_connection(
                {"host": "badhost", "database": "testdb"},
                ConnectionType.MONGODB,
            )
        assert ok is False
        assert len(msg) > 0

    def test_clickhouse_connection_mocked(self, svc):
        """Mock create_engine to verify SELECT 1 is executed for ClickHouse."""
        from unittest.mock import MagicMock, patch

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch("datanika.services.connection_service.create_engine", return_value=mock_engine):
            ok, msg = svc.test_connection(
                {
                    "host": "localhost",
                    "port": 8123,
                    "user": "default",
                    "password": "",
                    "database": "default",
                },
                ConnectionType.CLICKHOUSE,
            )
        assert ok is True
        assert msg == "Connected successfully"


class TestTestConnectionConnectArgs:
    """Verify each connection type passes the correct timeout kwarg to create_engine."""

    _DB_CONFIG = {"host": "h", "port": 1234, "user": "u", "password": "p", "database": "d"}

    def _run(self, svc, connection_type, config=None):
        from unittest.mock import MagicMock, patch

        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)

        with patch(
            "datanika.services.connection_service.create_engine", return_value=mock_engine
        ) as mock_create:
            svc.test_connection(config or self._DB_CONFIG, connection_type)

        return mock_create.call_args

    def test_mssql_uses_login_timeout(self, svc):
        call = self._run(svc, ConnectionType.MSSQL)
        assert call.kwargs["connect_args"] == {"login_timeout": 5}

    def test_postgres_uses_connect_timeout(self, svc):
        call = self._run(svc, ConnectionType.POSTGRES)
        assert call.kwargs["connect_args"] == {"connect_timeout": 5}

    def test_mysql_uses_connect_timeout(self, svc):
        call = self._run(svc, ConnectionType.MYSQL)
        assert call.kwargs["connect_args"] == {"connect_timeout": 5}

    def test_redshift_uses_connect_timeout(self, svc):
        call = self._run(svc, ConnectionType.REDSHIFT)
        assert call.kwargs["connect_args"] == {"connect_timeout": 5}

    def test_clickhouse_uses_connect_timeout(self, svc):
        call = self._run(svc, ConnectionType.CLICKHOUSE)
        assert call.kwargs["connect_args"] == {"connect_timeout": 5}

    def test_snowflake_uses_connect_timeout(self, svc):
        config = {
            "account": "acct",
            "user": "u",
            "password": "p",
            "database": "d",
            "schema": "s",
        }
        call = self._run(svc, ConnectionType.SNOWFLAKE, config)
        assert call.kwargs["connect_args"] == {"connect_timeout": 5}

    def test_sqlite_uses_no_connect_args(self, svc):
        call = self._run(svc, ConnectionType.SQLITE, {"path": ":memory:"})
        assert call.kwargs["connect_args"] == {}


class TestBuildSaUrl:
    def test_postgres_url(self):
        from datanika.services.connection_service import _build_sa_url

        config = {
            "host": "db.example.com",
            "port": 5432,
            "user": "admin",
            "password": "s3c",
            "database": "mydb",
        }
        url = _build_sa_url(config, ConnectionType.POSTGRES)
        assert url.startswith("postgresql+psycopg2://")
        assert "admin" in url
        assert "db.example.com" in url
        assert "5432" in url
        assert "mydb" in url

    def test_redshift_url(self):
        from datanika.services.connection_service import _build_sa_url

        config = {
            "host": "cluster.redshift.amazonaws.com",
            "user": "u",
            "password": "p",
            "database": "dw",
        }
        url = _build_sa_url(config, ConnectionType.REDSHIFT)
        assert url.startswith("postgresql+psycopg2://")
        assert "5439" in url  # default redshift port

    def test_mysql_url(self):
        from datanika.services.connection_service import _build_sa_url

        url = _build_sa_url(
            {"host": "mysql.local", "user": "root", "password": "pw", "database": "app"},
            ConnectionType.MYSQL,
        )
        assert url.startswith("mysql+pymysql://")
        assert "3306" in url

    def test_mssql_url(self):
        from datanika.services.connection_service import _build_sa_url

        url = _build_sa_url(
            {"host": "sql.local", "user": "sa", "password": "pw", "database": "master"},
            ConnectionType.MSSQL,
        )
        assert url.startswith("mssql+pymssql://")
        assert "1433" in url

    def test_sqlite_url(self):
        from datanika.services.connection_service import _build_sa_url

        url = _build_sa_url({"path": "/tmp/test.db"}, ConnectionType.SQLITE)
        assert url == "sqlite:////tmp/test.db"

    def test_sqlite_memory_url(self):
        from datanika.services.connection_service import _build_sa_url

        url = _build_sa_url({"path": ":memory:"}, ConnectionType.SQLITE)
        assert url == "sqlite:///:memory:"

    def test_snowflake_url(self):
        from datanika.services.connection_service import _build_sa_url

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
        from datanika.services.connection_service import _build_sa_url

        url = _build_sa_url(
            {"project": "my-project", "dataset": "raw_data"},
            ConnectionType.BIGQUERY,
        )
        assert url == "bigquery://my-project/raw_data"

    def test_clickhouse_url(self):
        from datanika.services.connection_service import _build_sa_url

        config = {
            "host": "ch.example.com",
            "port": 8123,
            "user": "default",
            "password": "secret",
            "database": "analytics",
        }
        url = _build_sa_url(config, ConnectionType.CLICKHOUSE)
        assert url.startswith("clickhousedb+connect://")
        assert "ch.example.com" in url
        assert "8123" in url
        assert "analytics" in url

    def test_special_chars_encoded(self):
        from datanika.services.connection_service import _build_sa_url

        config = {
            "host": "db.example.com",
            "user": "admin@corp",
            "password": "p@ss/word!",
            "database": "db",
        }
        url = _build_sa_url(config, ConnectionType.POSTGRES)
        assert "admin%40corp" in url
        assert "p%40ss%2Fword%21" in url

    def test_unsupported_type_raises(self):
        from datanika.services.connection_service import _build_sa_url

        with pytest.raises(ValueError, match="Unsupported"):
            _build_sa_url({"bucket": "x"}, ConnectionType.S3)

    def test_mongodb_url_raises(self):
        from datanika.services.connection_service import _build_sa_url

        with pytest.raises(ValueError, match="Unsupported"):
            _build_sa_url({"host": "localhost"}, ConnectionType.MONGODB)


class TestConnectionNameValidation:
    def test_valid_name(self, svc, db_session, org):
        conn = svc.create_connection(
            db_session,
            org.id,
            "My DB 1",
            ConnectionType.POSTGRES,
            {},
        )
        assert conn.name == "My DB 1"

    def test_empty_name_rejected(self, svc, db_session, org):
        with pytest.raises(ValueError, match="Connection name cannot be empty"):
            svc.create_connection(
                db_session,
                org.id,
                "   ",
                ConnectionType.POSTGRES,
                {},
            )

    def test_special_chars_rejected(self, svc, db_session, org):
        with pytest.raises(ValueError, match="alphanumeric"):
            svc.create_connection(
                db_session,
                org.id,
                "My-DB!",
                ConnectionType.POSTGRES,
                {},
            )

    def test_create_rejects_invalid(self, svc, db_session, org):
        with pytest.raises(ValueError):
            svc.create_connection(
                db_session,
                org.id,
                "",
                ConnectionType.POSTGRES,
                {},
            )

    def test_update_rejects_invalid_name(self, svc, db_session, org):
        conn = svc.create_connection(
            db_session,
            org.id,
            "Valid",
            ConnectionType.POSTGRES,
            {},
        )
        with pytest.raises(ValueError, match="alphanumeric"):
            svc.update_connection(db_session, org.id, conn.id, name="bad@name")


class TestInferDirection:
    def test_source_only_type(self):
        assert infer_direction("rest_api") == ConnectionDirection.SOURCE
        assert infer_direction("s3") == ConnectionDirection.SOURCE
        assert infer_direction("stripe") == ConnectionDirection.SOURCE

    def test_destination_only_type(self):
        assert infer_direction("bigquery") == ConnectionDirection.DESTINATION
        assert infer_direction("snowflake") == ConnectionDirection.DESTINATION
        assert infer_direction("redshift") == ConnectionDirection.DESTINATION

    def test_both_direction_type(self):
        assert infer_direction("postgres") == ConnectionDirection.BOTH
        assert infer_direction("mysql") == ConnectionDirection.BOTH
        assert infer_direction("clickhouse") == ConnectionDirection.BOTH

    def test_accepts_connection_type_enum(self):
        assert infer_direction(ConnectionType.POSTGRES) == ConnectionDirection.BOTH
        assert infer_direction(ConnectionType.BIGQUERY) == ConnectionDirection.DESTINATION
        assert infer_direction(ConnectionType.S3) == ConnectionDirection.SOURCE

    def test_auto_direction_on_create(self, svc, db_session, org):
        conn = svc.create_connection(db_session, org.id, "PG", ConnectionType.POSTGRES, {})
        assert conn.direction == ConnectionDirection.BOTH

        conn2 = svc.create_connection(db_session, org.id, "BQ", ConnectionType.BIGQUERY, {})
        assert conn2.direction == ConnectionDirection.DESTINATION

    def test_auto_direction_on_update_type(self, svc, db_session, org):
        conn = svc.create_connection(db_session, org.id, "X", ConnectionType.POSTGRES, {})
        assert conn.direction == ConnectionDirection.BOTH
        svc.update_connection(db_session, org.id, conn.id, connection_type=ConnectionType.BIGQUERY)
        assert conn.direction == ConnectionDirection.DESTINATION


class TestExecuteQueryRejection:
    def test_mongodb_rejected(self):
        with pytest.raises(ValueError, match="Cannot execute SQL"):
            ConnectionService.execute_query(
                {"host": "localhost"}, ConnectionType.MONGODB, "SELECT 1"
            )


class TestIsSelectOnly:
    """Validation for the read-only SQL guard used by /connections/{id}/query."""

    def test_simple_select(self):
        assert ConnectionService.is_select_only("SELECT 1")
        assert ConnectionService.is_select_only("select * from t")
        assert ConnectionService.is_select_only("SELECT * FROM t WHERE id = 1")

    def test_select_with_trailing_semicolon(self):
        assert ConnectionService.is_select_only("SELECT 1;")
        assert ConnectionService.is_select_only("SELECT 1 ; ")

    def test_with_cte(self):
        assert ConnectionService.is_select_only("WITH x AS (SELECT 1) SELECT * FROM x")

    def test_rejects_drop(self):
        assert not ConnectionService.is_select_only("DROP TABLE t")
        assert not ConnectionService.is_select_only("drop table t")

    def test_rejects_insert(self):
        assert not ConnectionService.is_select_only("INSERT INTO t (a) VALUES (1)")

    def test_rejects_update(self):
        assert not ConnectionService.is_select_only("UPDATE t SET a = 1")

    def test_rejects_delete(self):
        assert not ConnectionService.is_select_only("DELETE FROM t")

    def test_rejects_create(self):
        assert not ConnectionService.is_select_only("CREATE TABLE t (a INT)")

    def test_rejects_alter(self):
        assert not ConnectionService.is_select_only("ALTER TABLE t ADD b INT")

    def test_rejects_truncate(self):
        assert not ConnectionService.is_select_only("TRUNCATE TABLE t")

    def test_rejects_multiple_statements(self):
        assert not ConnectionService.is_select_only("SELECT 1; SELECT 2")
        assert not ConnectionService.is_select_only("SELECT 1; DROP TABLE t")

    def test_rejects_cte_with_mutation(self):
        assert not ConnectionService.is_select_only(
            "WITH x AS (DELETE FROM t RETURNING *) SELECT * FROM x"
        )

    def test_rejects_empty(self):
        assert not ConnectionService.is_select_only("")
        assert not ConnectionService.is_select_only("   ")
        assert not ConnectionService.is_select_only(None)

    def test_strips_line_comments(self):
        assert ConnectionService.is_select_only("-- comment\nSELECT 1")

    def test_strips_block_comments(self):
        assert ConnectionService.is_select_only("/* hi */ SELECT 1")


class TestSourceTemplateSlug:
    """#93 — ``connections.source_template_slug`` records the Pipeline
    Template a connection was created from, so the Plausible funnel
    (``template_selected`` → ``template_prefill_applied`` →
    ``template_first_run_triggered``) can attribute activation to the
    right template across sessions.
    """

    def test_default_source_template_slug_is_none(self, svc, db_session, org):
        conn = svc.create_connection(db_session, org.id, "Plain", ConnectionType.POSTGRES, {})
        assert conn.source_template_slug is None

    def test_stores_source_template_slug(self, svc, db_session, org):
        conn = svc.create_connection(
            db_session,
            org.id,
            "Shopify",
            ConnectionType.SHOPIFY,
            {},
            source_template_slug="shopify-to-postgres",
        )
        assert conn.source_template_slug == "shopify-to-postgres"

    def test_empty_slug_normalises_to_none(self, svc, db_session, org):
        # ``ConnectionState.selected_template_slug`` defaults to "" so the
        # service must treat the empty string the same as NULL — otherwise
        # analytics queries for "template-sourced connections" would need
        # a secondary `!= ""` filter and every non-template connection
        # would show up as a phantom template-origin row.
        conn = svc.create_connection(
            db_session, org.id, "X", ConnectionType.MYSQL, {}, source_template_slug=""
        )
        assert conn.source_template_slug is None


class TestConsumeTemplateFirstRun:
    """#93 — ``consume_template_first_run`` is the idempotency latch. The
    first call for a template-sourced connection returns the slug and
    stamps ``template_first_run_fired_at``; every subsequent call
    returns None. The Plausible event must fire exactly once per
    connection lifetime.
    """

    def _create(self, svc, db_session, org, slug=None):
        return svc.create_connection(
            db_session,
            org.id,
            "Conn",
            ConnectionType.POSTGRES,
            {"host": "localhost"},
            source_template_slug=slug,
        )

    def test_returns_slug_on_first_call(self, svc, db_session, org):
        conn = self._create(svc, db_session, org, slug="shopify-to-pg")
        assert svc.consume_template_first_run(db_session, org.id, conn.id) == "shopify-to-pg"

    def test_sets_fired_at_on_first_call(self, svc, db_session, org):
        conn = self._create(svc, db_session, org, slug="x")
        assert conn.template_first_run_fired_at is None
        svc.consume_template_first_run(db_session, org.id, conn.id)
        db_session.refresh(conn)
        assert conn.template_first_run_fired_at is not None

    def test_returns_none_on_second_call(self, svc, db_session, org):
        conn = self._create(svc, db_session, org, slug="x")
        first = svc.consume_template_first_run(db_session, org.id, conn.id)
        second = svc.consume_template_first_run(db_session, org.id, conn.id)
        assert first == "x"
        assert second is None

    def test_returns_none_when_no_slug(self, svc, db_session, org):
        conn = self._create(svc, db_session, org, slug=None)
        assert svc.consume_template_first_run(db_session, org.id, conn.id) is None

    def test_returns_none_for_missing_connection(self, svc, db_session, org):
        assert svc.consume_template_first_run(db_session, org.id, 999_999) is None

    def test_scoped_to_org(self, svc, db_session, org, other_org):
        """A connection in another org is invisible. MUST NOT fire for the
        wrong tenant, AND must remain eligible from the right tenant.
        """
        conn = self._create(svc, db_session, org, slug="x")
        assert svc.consume_template_first_run(db_session, other_org.id, conn.id) is None
        assert svc.consume_template_first_run(db_session, org.id, conn.id) == "x"


def _bq_keyfile() -> str:
    """A service-account JSON whose base64 encoding contains ``+``.

    The ``~`` is load-bearing, not decoration. Base64 of pure-ASCII text can
    only produce ``+`` when a byte at offset ``≡ 2 (mod 3)`` is ``>`` or ``~``;
    with neither, ``test_base64_is_percent_encoded`` would pass no matter how
    the URL was built. ``test_fixture_actually_exercises_the_plus_case`` fails
    loudly if an edit ever removes that property.
    """
    import json

    return json.dumps(
        {
            "type": "service_account",
            "project_id": "my-project",
            "private_key": "-----BEGIN PRIVATE KEY-----\n~\n-----END PRIVATE KEY-----\n",
            "client_email": "qa-smoke@datanika.iam.gserviceaccount.com",
        }
    )


class TestBigQueryCatalogUrlCarriesCredentials:
    """[#869] The BigQuery catalog/preview URL must carry the service account.

    Regression: ``_build_sa_url`` returned a bare ``bigquery://project/dataset``,
    so sqlalchemy-bigquery fell back to Application Default Credentials, which
    do not exist in the container. The *load* still succeeded, because dlt
    receives the key from the connection config directly -- so rows landed in
    BigQuery and then never appeared under Models/Catalog.

    These assert on the URL actually produced, which is why they are safe to
    run anywhere: with ADC present the unfixed code returns an *empty table
    list* rather than raising, so a test that called the catalog would pass on
    a developer box and fail only in production.
    """

    def _url(self, **over):
        from datanika.services.connection_service import _build_sa_url

        config = {
            "project": "my-project",
            "dataset": "raw_data",
            "keyfile_json": _bq_keyfile(),
        }
        config.update(over)
        return _build_sa_url(config, ConnectionType.BIGQUERY)

    def test_fixture_actually_exercises_the_plus_case(self):
        import base64

        assert "+" in base64.b64encode(_bq_keyfile().encode()).decode()

    def test_url_carries_the_service_account(self):
        assert "credentials_base64=" in self._url()

    def test_project_and_dataset_are_preserved(self):
        assert self._url().startswith("bigquery://my-project/raw_data")

    def test_credentials_round_trip_to_the_original_keyfile(self):
        import base64

        from sqlalchemy.engine import make_url

        value = make_url(self._url()).query["credentials_base64"]
        assert base64.b64decode(value).decode() == _bq_keyfile()

    def test_base64_is_percent_encoded(self):
        """``+`` in a query string decodes to a space.

        An unencoded payload is silently corrupted for exactly those keys whose
        encoding contains ``+`` -- the fix would work for some service accounts
        and not others, which is worse than failing outright.
        """
        assert "+" not in self._url().split("credentials_base64=", 1)[1]

    def test_the_real_dialect_extracts_the_credentials(self):
        """Validate against the consumer, not our model of it."""
        import base64

        from sqlalchemy.engine import make_url
        from sqlalchemy_bigquery.base import parse_url as bq_parse_url

        expected = base64.b64encode(_bq_keyfile().encode()).decode()
        assert bq_parse_url(make_url(self._url()))[5] == expected

    def test_service_account_json_is_accepted_too(self):
        """core#565: the form writes ``keyfile_json``; the runner accepts both."""
        url = self._url(keyfile_json=None, service_account_json=_bq_keyfile())
        assert "credentials_base64=" in url

    def test_connection_without_credentials_is_unchanged(self):
        assert self._url(keyfile_json=None) == "bigquery://my-project/raw_data"

    def test_other_destinations_are_untouched(self):
        from datanika.services.connection_service import _build_sa_url

        url = _build_sa_url(
            {
                "host": "db.example.com",
                "port": 5432,
                "user": "admin",
                "password": "s3c",
                "database": "mydb",
            },
            ConnectionType.POSTGRES,
        )
        assert "credentials_base64" not in url
