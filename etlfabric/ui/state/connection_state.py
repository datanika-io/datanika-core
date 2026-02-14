"""Connection state for Reflex UI."""

import reflex as rx

from etlfabric.config import settings
from etlfabric.models.connection import ConnectionDirection, ConnectionType
from etlfabric.services.connection_service import ConnectionService
from etlfabric.services.encryption import EncryptionService
from etlfabric.ui.state.base_state import BaseState, get_sync_session


class ConnectionItem(rx.Base):
    id: int = 0
    name: str = ""
    connection_type: str = ""
    direction: str = ""


class ConnectionState(BaseState):
    connections: list[ConnectionItem] = []
    form_name: str = ""
    form_type: str = "postgres"
    form_direction: str = "source"
    form_config: str = "{}"

    def load_connections(self):
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            rows = svc.list_connections(session, self.org_id)
            self.connections = [
                ConnectionItem(
                    id=c.id,
                    name=c.name,
                    connection_type=c.connection_type.value,
                    direction=c.direction.value,
                )
                for c in rows
            ]
        self.error_message = ""

    def create_connection(self):
        import json

        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        try:
            config = json.loads(self.form_config)
        except json.JSONDecodeError:
            self.error_message = "Invalid JSON in config"
            return
        try:
            with get_sync_session() as session:
                svc.create_connection(
                    session,
                    self.org_id,
                    self.form_name,
                    ConnectionType(self.form_type),
                    ConnectionDirection(self.form_direction),
                    config,
                )
                session.commit()
        except Exception as e:
            self.error_message = str(e)
            return
        self.form_name = ""
        self.form_config = "{}"
        self.error_message = ""
        self.load_connections()

    def delete_connection(self, conn_id: int):
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            svc.delete_connection(session, self.org_id, conn_id)
            session.commit()
        self.load_connections()
