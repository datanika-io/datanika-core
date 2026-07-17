"""Connection management service — CRUD with encrypted credentials."""

import re
from datetime import UTC, datetime
from functools import partial
from urllib.parse import quote_plus

from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.orm import Session

from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.services.encryption import EncryptionService
from datanika.services.naming import validate_name

validate_connection_name = partial(validate_name, entity_label="Connection")

# Types that can serve as sources (databases + files + rest_api + sheets)
SOURCE_TYPES = {
    "postgres",
    "mysql",
    "mssql",
    "oracle",
    "sqlite",
    "rest_api",
    "s3",
    "csv",
    "json",
    "parquet",
    "google_sheets",
    "mongodb",
    "clickhouse",
    "duckdb",
    "stripe",
    "github",
    "hubspot",
    "salesforce",
    "shopify",
    "jira",
    "slack",
    "google_analytics",
    "google_ads",
    "facebook_ads",
    "zendesk",
    "airtable",
    "notion",
    "pipedrive",
    "freshdesk",
    "asana",
    "kafka",
}

# Types that can serve as destinations (databases + cloud warehouses)
DESTINATION_TYPES = {
    "postgres",
    "mysql",
    "mssql",
    "sqlite",
    "bigquery",
    "snowflake",
    "redshift",
    "clickhouse",
    "duckdb",
    "databricks",
    "synapse",
}


def infer_direction(connection_type: str | ConnectionType) -> ConnectionDirection:
    """Derive direction from connection type."""
    ct = connection_type.value if isinstance(connection_type, ConnectionType) else connection_type
    is_src = ct in SOURCE_TYPES
    is_dst = ct in DESTINATION_TYPES
    if is_src and is_dst:
        return ConnectionDirection.BOTH
    if is_dst:
        return ConnectionDirection.DESTINATION
    return ConnectionDirection.SOURCE


# Connection types that don't support SQL queries (SELECT 1 testing or execute_query)
_NON_DB_TYPES = {
    ConnectionType.S3,
    ConnectionType.CSV,
    ConnectionType.JSON,
    ConnectionType.PARQUET,
    ConnectionType.REST_API,
    ConnectionType.GOOGLE_SHEETS,
    ConnectionType.MONGODB,
    ConnectionType.STRIPE,
    ConnectionType.GITHUB,
    ConnectionType.HUBSPOT,
    ConnectionType.SALESFORCE,
    ConnectionType.SHOPIFY,
    ConnectionType.JIRA,
    ConnectionType.SLACK,
    ConnectionType.GOOGLE_ANALYTICS,
    ConnectionType.GOOGLE_ADS,
    ConnectionType.FACEBOOK_ADS,
    ConnectionType.ZENDESK,
    ConnectionType.AIRTABLE,
    ConnectionType.NOTION,
    ConnectionType.PIPEDRIVE,
    ConnectionType.FRESHDESK,
    ConnectionType.ASANA,
    ConnectionType.KAFKA,
}


def _build_sa_url(config: dict, connection_type: ConnectionType) -> str:
    """Build a SQLAlchemy connection URL from config dict and connection type."""
    if connection_type in (ConnectionType.POSTGRES, ConnectionType.REDSHIFT):
        driver = "postgresql+psycopg2"
        port = config.get("port", 5432 if connection_type == ConnectionType.POSTGRES else 5439)
        return (
            f"{driver}://{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('host', 'localhost')}:{port}/"
            f"{config.get('database', '')}"
        )

    if connection_type == ConnectionType.MYSQL:
        port = config.get("port", 3306)
        return (
            f"mysql+pymysql://{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('host', 'localhost')}:{port}/"
            f"{config.get('database', '')}"
        )

    if connection_type == ConnectionType.MSSQL:
        port = config.get("port", 1433)
        return (
            f"mssql+pymssql://{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('host', 'localhost')}:{port}/"
            f"{config.get('database', '')}"
        )

    if connection_type == ConnectionType.SQLITE:
        path = config.get("path", ":memory:")
        return f"sqlite:///{path}"

    if connection_type == ConnectionType.SNOWFLAKE:
        url = (
            f"snowflake://{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('account', '')}"
            f"/{config.get('database', '')}"
            f"/{config.get('schema', '')}"
        )
        params = []
        if config.get("warehouse"):
            params.append(f"warehouse={quote_plus(config['warehouse'])}")
        if config.get("role"):
            params.append(f"role={quote_plus(config['role'])}")
        if params:
            url += "?" + "&".join(params)
        return url

    if connection_type == ConnectionType.BIGQUERY:
        project = config.get("project", "")
        dataset = config.get("dataset", "")
        return f"bigquery://{project}/{dataset}"

    if connection_type == ConnectionType.DATABRICKS:
        host = config.get("host", "")
        http_path = config.get("http_path", "")
        token = config.get("token", config.get("password", ""))
        catalog = config.get("catalog", config.get("database", ""))
        return (
            f"databricks://token:{quote_plus(token)}@{host}"
            f"?http_path={quote_plus(http_path)}&catalog={quote_plus(catalog)}"
        )

    if connection_type == ConnectionType.SYNAPSE:
        port = config.get("port", 1433)
        return (
            f"mssql+pymssql://{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('host', 'localhost')}:{port}/"
            f"{config.get('database', '')}"
        )

    if connection_type == ConnectionType.DUCKDB:
        path = config.get("path", config.get("database", ":memory:"))
        return f"duckdb:///{path}"

    if connection_type == ConnectionType.CLICKHOUSE:
        port = config.get("port", 8123)
        secure_qs = "?secure=1" if config.get("secure", False) else ""
        return (
            f"clickhousedb+connect://{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('host', 'localhost')}:{port}/"
            f"{config.get('database', '')}{secure_qs}"
        )

    if connection_type == ConnectionType.ORACLE:
        port = config.get("port", 1521)
        userinfo = (
            f"{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('host', 'localhost')}:{port}/"
        )
        if config.get("use_sid"):
            # Legacy SID connect (classic single-instance): the URL path is the SID.
            return f"oracle+oracledb://{userinfo}{config.get('database', '')}"
        # Default: service-name connect (PDB / RAC / Autonomous). SQLAlchemy's
        # oracledb dialect reads the service name from the ?service_name= query;
        # a value in the URL path is treated as a SID (#329).
        return f"oracle+oracledb://{userinfo}?service_name={quote_plus(config.get('database', ''))}"

    raise ValueError(f"Unsupported connection type for URL building: {connection_type}")


class ConnectionService:
    def __init__(self, encryption: EncryptionService):
        self._encryption = encryption

    def create_connection(
        self,
        session: Session,
        org_id: int,
        name: str,
        connection_type: ConnectionType,
        config: dict,
        source_template_slug: str | None = None,
    ) -> Connection:
        from datanika.hooks import emit

        emit("connection.before_create", session=session, org_id=org_id)
        validate_connection_name(name)
        direction = infer_direction(connection_type)
        # Normalise "" → None so analytics queries can filter on a single
        # NULL check. ConnectionState.selected_template_slug defaults to ""
        # when a user creates a connection outside the template flow. #93.
        conn = Connection(
            org_id=org_id,
            name=name,
            connection_type=connection_type,
            direction=direction,
            config_encrypted=self._encryption.encrypt(config),
            source_template_slug=source_template_slug or None,
        )
        session.add(conn)
        session.flush()
        return conn

    def consume_template_first_run(self, session: Session, org_id: int, conn_id: int) -> str | None:
        """Return the template slug on the connection's first eligible run,
        then mark it so subsequent calls return None. Idempotent.

        Feeds the ``template_first_run_triggered`` Plausible event (#93).
        The caller (``PipelineState.run_pipeline`` / ``UploadState.run_upload``)
        emits an ``rx.call_script`` when this returns a slug, and nothing
        when it returns None — so a connection fires the funnel step 3
        event exactly once over its lifetime, regardless of how many
        runs it sees.
        """
        from datetime import UTC, datetime

        conn = self.get_connection(session, org_id, conn_id)
        if conn is None or not conn.source_template_slug:
            return None
        if conn.template_first_run_fired_at is not None:
            return None
        conn.template_first_run_fired_at = datetime.now(UTC)
        session.flush()
        return conn.source_template_slug

    def get_connection(self, session: Session, org_id: int, conn_id: int) -> Connection | None:
        stmt = select(Connection).where(
            Connection.id == conn_id,
            Connection.org_id == org_id,
            Connection.deleted_at.is_(None),
        )
        return session.execute(stmt).scalar_one_or_none()

    def get_connection_config(self, session: Session, org_id: int, conn_id: int) -> dict | None:
        conn = self.get_connection(session, org_id, conn_id)
        if conn is None:
            return None
        return self._encryption.decrypt(conn.config_encrypted)

    def list_connections(self, session: Session, org_id: int) -> list[Connection]:
        stmt = (
            select(Connection)
            .where(Connection.org_id == org_id, Connection.deleted_at.is_(None))
            .order_by(Connection.created_at.desc())
        )
        return list(session.execute(stmt).scalars().all())

    def update_connection(
        self, session: Session, org_id: int, conn_id: int, **kwargs
    ) -> Connection | None:
        conn = self.get_connection(session, org_id, conn_id)
        if conn is None:
            return None

        if "name" in kwargs:
            validate_connection_name(kwargs["name"])
            conn.name = kwargs["name"]
        if "connection_type" in kwargs:
            conn.connection_type = kwargs["connection_type"]
            conn.direction = infer_direction(kwargs["connection_type"])
        if "config" in kwargs:
            conn.config_encrypted = self._encryption.encrypt(kwargs["config"])

        session.flush()
        return conn

    def delete_connection(self, session: Session, org_id: int, conn_id: int) -> bool:
        conn = self.get_connection(session, org_id, conn_id)
        if conn is None:
            return False
        conn.deleted_at = datetime.now(UTC)
        session.flush()
        return True

    @staticmethod
    def execute_query(
        config: dict,
        connection_type: ConnectionType,
        query: str,
    ) -> tuple[list[str], list[list]]:
        """Execute a read-only SQL query. Returns (column_names, rows)."""
        if connection_type in _NON_DB_TYPES:
            raise ValueError(f"Cannot execute SQL on {connection_type.value} connections")
        url = _build_sa_url(config, connection_type)
        engine = create_engine(url)
        try:
            with engine.connect() as conn:
                result = conn.execute(text(query))
                columns = list(result.keys())
                rows = [list(row) for row in result.fetchall()]
                return columns, rows
        finally:
            engine.dispose()

    @staticmethod
    def _test_mongodb(config: dict) -> tuple[bool, str]:
        """Test MongoDB connectivity via server_info(). Returns (success, message)."""
        try:
            from pymongo import MongoClient
        except ImportError:
            return False, "Driver not installed for mongodb"

        host = config.get("host", "localhost")
        port = config.get("port", 27017)
        user = config.get("user", "")
        password = config.get("password", "")
        database = config.get("database", "")

        if user:
            uri = f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}:{port}/{database}"
        else:
            uri = f"mongodb://{host}:{port}/{database}"

        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.server_info()
            client.close()
            return True, "Connected successfully"
        except Exception:
            return False, "Connection failed — check your credentials and network settings"

    @staticmethod
    def test_connection(config: dict, connection_type: ConnectionType) -> tuple[bool, str]:
        """Test real database connectivity via SELECT 1. Returns (success, message)."""
        if not config:
            return False, "Configuration is empty"

        if connection_type == ConnectionType.MONGODB:
            return ConnectionService._test_mongodb(config)

        if connection_type in _NON_DB_TYPES:
            return True, "Test not applicable for this type"

        try:
            url = _build_sa_url(config, connection_type)
        except ValueError as e:
            return False, str(e)

        connect_args: dict = {}
        if connection_type == ConnectionType.MSSQL:
            connect_args = {"login_timeout": 5}
        elif connection_type == ConnectionType.ORACLE:
            # oracledb's DBAPI does not accept ``connect_timeout``.
            connect_args = {"tcp_connect_timeout": 5}
        elif connection_type != ConnectionType.SQLITE:
            connect_args = {"connect_timeout": 5}

        try:
            engine = create_engine(url, connect_args=connect_args)
        except ImportError:
            return False, f"Driver not installed for {connection_type.value}"

        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True, "Connected successfully"
        except ImportError:
            return False, f"Driver not installed for {connection_type.value}"
        except Exception:
            return False, "Connection failed — check your credentials and network settings"
        finally:
            engine.dispose()

    @staticmethod
    def is_select_only(query: str) -> bool:
        """Return True if a query is a single read-only SELECT statement.

        Rejects DDL/DML, multiple statements, and CTE-prefixed mutations.
        Comments and leading/trailing whitespace are ignored.
        """
        if not query or not query.strip():
            return False
        # Strip line and block comments
        cleaned_lines = []
        in_block_comment = False
        for line in query.splitlines():
            i = 0
            line_chars = []
            while i < len(line):
                if in_block_comment:
                    if line[i : i + 2] == "*/":
                        in_block_comment = False
                        i += 2
                    else:
                        i += 1
                elif line[i : i + 2] == "/*":
                    in_block_comment = True
                    i += 2
                elif line[i : i + 2] == "--":
                    break
                else:
                    line_chars.append(line[i])
                    i += 1
            cleaned_lines.append("".join(line_chars))
        cleaned = " ".join(cleaned_lines).strip().rstrip(";").strip()
        if not cleaned:
            return False
        # Reject multiple statements
        if ";" in cleaned:
            return False
        upper = cleaned.upper()
        forbidden_re = re.compile(
            r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|"
            r"GRANT|REVOKE|MERGE)\b"
        )
        if upper.startswith("SELECT"):
            return forbidden_re.search(upper) is None
        if upper.startswith("WITH "):
            # CTE — must contain SELECT and no mutating keywords anywhere
            if forbidden_re.search(upper):
                return False
            return re.search(r"\bSELECT\b", upper) is not None
        return False

    @staticmethod
    def list_tables(
        config: dict, connection_type: ConnectionType, schema: str | None = None
    ) -> list[dict]:
        """List tables in a SQL connection. Returns [{schema, name}].

        Raises ValueError for non-SQL connection types.
        """
        if connection_type in _NON_DB_TYPES:
            raise ValueError(f"Cannot list tables for {connection_type.value} connections")
        url = _build_sa_url(config, connection_type)
        engine = create_engine(url)
        try:
            insp = inspect(engine)
            schemas = [schema] if schema else insp.get_schema_names()
            tables = []
            for sch in schemas:
                # Skip system schemas
                if sch in ("information_schema", "pg_catalog", "sys"):
                    continue
                try:
                    for name in insp.get_table_names(schema=sch):
                        tables.append({"schema": sch, "name": name})
                except Exception:
                    continue
            return tables
        finally:
            engine.dispose()

    @staticmethod
    def list_columns(
        config: dict,
        connection_type: ConnectionType,
        table: str,
        schema: str | None = None,
    ) -> list[dict]:
        """List columns of a table. Returns [{name, type, nullable}]."""
        if connection_type in _NON_DB_TYPES:
            raise ValueError(f"Cannot list columns for {connection_type.value} connections")
        url = _build_sa_url(config, connection_type)
        engine = create_engine(url)
        try:
            insp = inspect(engine)
            cols = insp.get_columns(table, schema=schema)
            return [
                {
                    "name": c["name"],
                    "type": str(c["type"]),
                    "nullable": c.get("nullable", True),
                }
                for c in cols
            ]
        finally:
            engine.dispose()

    @staticmethod
    def preview_table(
        config: dict,
        connection_type: ConnectionType,
        table: str,
        schema: str | None = None,
        limit: int = 100,
    ) -> tuple[list[str], list[list]]:
        """Return the first N rows of a table. Returns (columns, rows)."""
        limit = max(1, min(int(limit), 1000))
        # Quote identifiers safely with the engine's preparer
        url = _build_sa_url(config, connection_type)
        engine = create_engine(url)
        try:
            preparer = engine.dialect.identifier_preparer
            qualified = preparer.quote(table)
            if schema:
                qualified = f"{preparer.quote_schema(schema)}.{qualified}"
            query = f"SELECT * FROM {qualified} LIMIT {limit}"
            with engine.connect() as conn:
                result = conn.execute(text(query))
                columns = list(result.keys())
                rows = [list(row) for row in result.fetchall()]
                return columns, rows
        finally:
            engine.dispose()
