"""Connection management service — CRUD with encrypted credentials."""

from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from etlfabric.models.connection import Connection, ConnectionDirection, ConnectionType
from etlfabric.services.encryption import EncryptionService


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
    def test_connection(config: dict, connection_type: ConnectionType) -> tuple[bool, str]:
        """Basic connectivity validation. Returns (success, message)."""
        if not config:
            return False, "Configuration is empty"
        return True, "Connection parameters look valid"
