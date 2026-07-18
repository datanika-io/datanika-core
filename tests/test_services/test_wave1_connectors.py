"""Wave-1 connectors — Oracle (SQL) + Pipedrive / Freshdesk / Asana (SaaS REST).

Mirrors the existing connector conventions:
- SQL URL / connect_args → ``test_connection_service.py`` (TestBuildSaUrl, connect_args)
- SaaS ``_build_saas_source`` → ``test_dlt_runner.py`` (TestStripeSaasSource, …)

All mocked — no live creds, no real driver import (create_engine / rest_api_source
are patched). Live connectivity is covered by QA/Infra smoke against real containers.

Regression + feature tests for datanika-core#309.
"""

from unittest.mock import patch

import pytest

from datanika.models.connection import ConnectionType
from datanika.services.connection_schemas import CONFIG_SCHEMAS, list_connection_types
from datanika.services.connection_service import (
    _NON_DB_TYPES,
    DESTINATION_TYPES,
    SOURCE_TYPES,
    ConnectionService,
    _build_sa_url,
)
from datanika.services.dlt_runner import (
    _RENAME_USER_TYPES,
    SOURCE_DRIVERNAME_MAP,
    SUPPORTED_SAAS_TYPES,
    DltRunnerError,
    DltRunnerService,
)
from datanika.services.ir.builder import SAAS_TYPES, SQL_TYPES


@pytest.fixture
def svc():
    return DltRunnerService()


# --------------------------------------------------------------------------- #
# Enum
# --------------------------------------------------------------------------- #


class TestWave1Enum:
    def test_new_connection_types_exist(self):
        assert ConnectionType.ORACLE == "oracle"
        assert ConnectionType.PIPEDRIVE == "pipedrive"
        assert ConnectionType.FRESHDESK == "freshdesk"
        assert ConnectionType.ASANA == "asana"


# --------------------------------------------------------------------------- #
# Oracle (SQL, source-only)
# --------------------------------------------------------------------------- #


class TestOracleConnector:
    def test_build_sa_url(self):
        url = _build_sa_url(
            {
                "host": "db.example.com",
                "port": 1521,
                "user": "scott",
                "password": "tiger",
                "database": "XEPDB1",
            },
            ConnectionType.ORACLE,
        )
        assert url == "oracle+oracledb://scott:tiger@db.example.com:1521/?service_name=XEPDB1"

    def test_build_sa_url_default_port(self):
        url = _build_sa_url(
            {"host": "h", "user": "u", "password": "p", "database": "SVC"},
            ConnectionType.ORACLE,
        )
        assert "@h:1521/?service_name=SVC" in url

    def test_build_sa_url_quotes_special_chars(self):
        url = _build_sa_url(
            {"host": "h", "user": "u", "password": "p@ss/word", "database": "SVC"},
            ConnectionType.ORACLE,
        )
        assert "p%40ss%2Fword" in url

    def test_build_sa_url_sid_mode(self):
        # use_sid=True → legacy SID connect (the URL path is the SID).
        url = _build_sa_url(
            {"host": "h", "user": "u", "password": "p", "database": "ORCL", "use_sid": True},
            ConnectionType.ORACLE,
        )
        assert url == "oracle+oracledb://u:p@h:1521/ORCL"

    def test_dsn_uses_service_name_not_sid(self):
        # #329: guard the *resolved* DSN semantics, not just the URL string — the
        # bug was a correct-looking path URL that SQLAlchemy resolves to a SID.
        from sqlalchemy import create_engine
        from sqlalchemy.engine import make_url

        cases = [
            (
                {"host": "h", "user": "u", "password": "p", "database": "XEPDB1"},
                "SERVICE_NAME=XEPDB1",
                "(SID=",
            ),
            (
                {"host": "h", "user": "u", "password": "p", "database": "ORCL", "use_sid": True},
                "SID=ORCL",
                "SERVICE_NAME=",
            ),
        ]
        for cfg, expect, forbid in cases:
            url = make_url(_build_sa_url(cfg, ConnectionType.ORACLE))
            dsn = create_engine(url).dialect.create_connect_args(url)[1]["dsn"]
            assert expect in dsn, dsn
            assert forbid not in dsn, dsn

    def test_is_source_not_destination(self):
        assert "oracle" in SOURCE_TYPES
        assert "oracle" not in DESTINATION_TYPES
        assert ConnectionType.ORACLE not in _NON_DB_TYPES  # SQL type → SELECT 1 works

    def test_supported_as_dlt_source_only(self):
        assert "oracle" in DltRunnerService.SUPPORTED_SOURCE_TYPES
        assert "oracle" not in DltRunnerService.SUPPORTED_DESTINATION_TYPES

    def test_driver_maps(self):
        assert SOURCE_DRIVERNAME_MAP["oracle"] == "oracle+oracledb"
        assert "oracle" in _RENAME_USER_TYPES

    def test_to_dlt_credentials_service_name(self):
        # Default: the dlt extract path must also use service_name, not SID (#329).
        creds = DltRunnerService._to_dlt_credentials(
            "oracle", {"host": "h", "user": "u", "password": "p", "database": "SVC"}
        )
        assert creds["drivername"] == "oracle+oracledb"
        assert creds["username"] == "u"
        assert "user" not in creds
        assert creds.get("query") == {"service_name": "SVC"}
        assert "database" not in creds  # moved into the service_name query

    def test_to_dlt_credentials_sid_mode(self):
        creds = DltRunnerService._to_dlt_credentials(
            "oracle",
            {"host": "h", "user": "u", "password": "p", "database": "ORCL", "use_sid": True},
        )
        assert creds["database"] == "ORCL"  # SID stays in the URL path
        assert "service_name" not in (creds.get("query") or {})
        assert "use_sid" not in creds  # our flag never leaks into dlt credentials

    def test_in_ir_sql_types(self):
        assert "oracle" in SQL_TYPES
        assert "oracle" not in SAAS_TYPES

    def test_config_schema(self):
        schema = CONFIG_SCHEMAS["oracle"]
        props = schema["properties"]
        assert {"host", "port", "database", "user", "password"} <= set(props)
        assert schema["required"] == ["host", "database", "user", "password"]
        assert props["password"].get("format") == "password"
        # #329: optional legacy-SID toggle (default = service name)
        assert props["use_sid"]["type"] == "boolean"
        assert "use_sid" not in schema["required"]

    @patch("datanika.services.connection_service.create_engine")
    def test_connect_args_avoids_connect_timeout(self, mock_ce):
        # oracledb's DBAPI rejects ``connect_timeout``; it must not be passed.
        ok, _msg = ConnectionService.test_connection(
            {"host": "h", "user": "u", "password": "p", "database": "SVC"},
            ConnectionType.ORACLE,
        )
        connect_args = mock_ce.call_args.kwargs["connect_args"]
        assert "connect_timeout" not in connect_args
        assert connect_args == {"tcp_connect_timeout": 5}
        assert ok is True

    @patch("datanika.services.connection_service.create_engine")
    def test_probe_query_uses_dual_on_oracle(self, mock_ce):
        # #329: Oracle rejects a bare `SELECT 1` (ORA-00923); the connectivity
        # probe must use FROM DUAL. Mocks can't connect, but they lock the SQL
        # text — the gap that let the SID/SELECT-1 bugs pass unit tests but fail
        # the live Oracle smoke.
        ConnectionService.test_connection(
            {"host": "h", "user": "u", "password": "p", "database": "SVC"},
            ConnectionType.ORACLE,
        )
        conn = mock_ce.return_value.connect.return_value.__enter__.return_value
        assert str(conn.execute.call_args[0][0]) == "SELECT 1 FROM DUAL"

    @patch("datanika.services.connection_service.create_engine")
    def test_preview_uses_fetch_first_on_oracle(self, mock_ce):
        # #329: Oracle has no LIMIT clause (ORA-00933) — use FETCH FIRST.
        engine = mock_ce.return_value
        engine.dialect.identifier_preparer.quote = lambda x: f'"{x}"'
        conn = engine.connect.return_value.__enter__.return_value
        conn.execute.return_value.keys.return_value = ["id"]
        conn.execute.return_value.fetchall.return_value = []
        ConnectionService.preview_table(
            {"host": "h", "user": "u", "password": "p", "database": "SVC"},
            ConnectionType.ORACLE,
            "T329",
            limit=5,
        )
        sql = str(conn.execute.call_args[0][0])
        assert "FETCH FIRST 5 ROWS ONLY" in sql
        assert "LIMIT" not in sql

    @patch("datanika.services.dlt_runner.sql_table")
    def test_build_source_normalizes_oracle_table(self, mock_sql_table):
        # #347: the Oracle table name must be normalized (lower-cased) before dlt
        # reflects it — else SQLAlchemy treats an UPPERCASE name as a
        # case-sensitive quoted identifier and reflection misses the table
        # (NoSuchTableError). Verified live against Oracle XE.
        DltRunnerService().build_source(
            "oracle",
            {"host": "h", "user": "u", "password": "p", "database": "SVC"},
            {"mode": "single_table", "table": "QA_E2E_WAVE1"},
        )
        assert mock_sql_table.call_args.kwargs["table"] == "qa_e2e_wave1"

    @patch("datanika.services.dlt_runner.sql_database")
    def test_build_source_normalizes_oracle_table_names(self, mock_sql_db):
        DltRunnerService().build_source(
            "oracle",
            {"host": "h", "user": "u", "password": "p", "database": "SVC"},
            {"mode": "full_database", "table_names": ["QA_E2E_WAVE1", "OTHER_TBL"]},
        )
        assert mock_sql_db.call_args.kwargs["table_names"] == ["qa_e2e_wave1", "other_tbl"]


# --------------------------------------------------------------------------- #
# SaaS REST connectors
# --------------------------------------------------------------------------- #


class TestPipedriveConnector:
    def test_registered_as_saas_source(self):
        assert "pipedrive" in SUPPORTED_SAAS_TYPES
        assert "pipedrive" in SOURCE_TYPES
        assert ConnectionType.PIPEDRIVE in _NON_DB_TYPES
        assert "pipedrive" in SAAS_TYPES

    @patch("datanika.services.dlt_runner.rest_api_source")
    def test_builds_rest_api_source(self, mock_rest, svc):
        mock_rest.return_value = "pd_src"
        result = svc._build_saas_source("pipedrive", {"api_key": "tok123"}, {})
        assert result == "pd_src"
        cfg = mock_rest.call_args[0][0]
        assert cfg["client"]["base_url"] == "https://api.pipedrive.com/v1/"
        assert len(cfg["resources"]) >= 3
        # Pipedrive authenticates with an api_token query param
        assert cfg["client"]["auth"]["location"] == "query"
        assert cfg["client"]["auth"]["name"] == "api_token"

    def test_requires_api_key(self, svc):
        with pytest.raises(DltRunnerError, match="api"):
            svc._build_saas_source("pipedrive", {}, {})

    def test_config_schema(self):
        assert CONFIG_SCHEMAS["pipedrive"]["required"] == ["api_key"]
        assert CONFIG_SCHEMAS["pipedrive"]["properties"]["api_key"]["format"] == "password"


class TestFreshdeskConnector:
    def test_registered_as_saas_source(self):
        assert "freshdesk" in SUPPORTED_SAAS_TYPES
        assert "freshdesk" in SOURCE_TYPES
        assert ConnectionType.FRESHDESK in _NON_DB_TYPES
        assert "freshdesk" in SAAS_TYPES

    @patch("datanika.services.dlt_runner.rest_api_source")
    def test_builds_rest_api_source(self, mock_rest, svc):
        mock_rest.return_value = "fd_src"
        result = svc._build_saas_source("freshdesk", {"api_key": "k", "domain": "acme"}, {})
        assert result == "fd_src"
        cfg = mock_rest.call_args[0][0]
        assert cfg["client"]["base_url"] == "https://acme.freshdesk.com/api/v2/"
        assert len(cfg["resources"]) >= 3
        # Freshdesk uses HTTP basic (api_key as username)
        assert cfg["client"]["auth"]["type"] == "http_basic"
        assert cfg["client"]["auth"]["username"] == "k"

    def test_requires_api_key_and_domain(self, svc):
        with pytest.raises(DltRunnerError, match="api_key|domain"):
            svc._build_saas_source("freshdesk", {}, {})
        with pytest.raises(DltRunnerError, match="domain"):
            svc._build_saas_source("freshdesk", {"api_key": "k"}, {})

    def test_config_schema(self):
        assert set(CONFIG_SCHEMAS["freshdesk"]["required"]) == {"domain", "api_key"}


class TestAsanaConnector:
    def test_registered_as_saas_source(self):
        assert "asana" in SUPPORTED_SAAS_TYPES
        assert "asana" in SOURCE_TYPES
        assert ConnectionType.ASANA in _NON_DB_TYPES
        assert "asana" in SAAS_TYPES

    @patch("datanika.services.dlt_runner.rest_api_source")
    def test_builds_rest_api_source(self, mock_rest, svc):
        mock_rest.return_value = "as_src"
        result = svc._build_saas_source("asana", {"api_key": "pat123"}, {})
        assert result == "as_src"
        cfg = mock_rest.call_args[0][0]
        assert cfg["client"]["base_url"] == "https://app.asana.com/api/1.0/"
        assert len(cfg["resources"]) >= 3
        assert cfg["client"]["auth"] == {"type": "bearer", "token": "pat123"}

    def test_requires_api_key(self, svc):
        with pytest.raises(DltRunnerError, match="api|token"):
            svc._build_saas_source("asana", {}, {})

    def test_config_schema(self):
        assert CONFIG_SCHEMAS["asana"]["required"] == ["api_key"]


# --------------------------------------------------------------------------- #
# Discovery — all 4 flow through list_connection_types()
# --------------------------------------------------------------------------- #


class TestWave1Discovery:
    @pytest.mark.parametrize("slug", ["oracle", "pipedrive", "freshdesk", "asana"])
    def test_in_config_schemas(self, slug):
        assert slug in CONFIG_SCHEMAS

    @pytest.mark.parametrize("slug", ["oracle", "pipedrive", "freshdesk", "asana"])
    def test_listed_by_discovery_endpoint(self, slug):
        listed = {item["type"] for item in list_connection_types()}
        assert slug in listed
