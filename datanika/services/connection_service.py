"""Connection management service — CRUD with encrypted credentials."""

from datetime import UTC, datetime
from functools import partial
from urllib.parse import quote_plus

from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from datanika.models.connection import Connection, ConnectionDirection, ConnectionType
from datanika.services.encryption import EncryptionService
from datanika.services.naming import validate_name

validate_connection_name = partial(validate_name, entity_label="Connection")

# Connection types that don't support SQL queries (SELECT 1 testing or execute_query)
_NON_DB_TYPES = {
    ConnectionType.S3,
    ConnectionType.CSV,
    ConnectionType.JSON,
    ConnectionType.PARQUET,
    ConnectionType.REST_API,
    ConnectionType.GOOGLE_SHEETS,
    ConnectionType.MONGODB,
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

    if connection_type == ConnectionType.CLICKHOUSE:
        port = config.get("port", 8123)
        return (
            f"clickhousedb+connect://{quote_plus(config.get('user', ''))}:"
            f"{quote_plus(config.get('password', ''))}@"
            f"{config.get('host', 'localhost')}:{port}/"
            f"{config.get('database', '')}"
        )

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
        direction: ConnectionDirection,
        config: dict,
    ) -> Connection:
        validate_connection_name(name)
        conn = Connection(
            org_id=org_id,
            name=name,
            connection_type=connection_type,
            direction=direction,
            config_encrypted=self._encryption.encrypt(config),
        )
        session.add(conn)
        session.flush()
        return conn

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
        if "direction" in kwargs:
            conn.direction = kwargs["direction"]
        if "connection_type" in kwargs:
            conn.connection_type = kwargs["connection_type"]
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
        config: dict, connection_type: ConnectionType, query: str,
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
            uri = (
                f"mongodb://{quote_plus(user)}:{quote_plus(password)}"
                f"@{host}:{port}/{database}"
            )
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

        try:
            engine = create_engine(url, connect_args={"connect_timeout": 5}
                                   if connection_type not in (ConnectionType.SQLITE,)
                                   else {})
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
