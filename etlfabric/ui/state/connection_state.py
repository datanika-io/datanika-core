"""Connection state for Reflex UI."""

from pydantic import BaseModel

from etlfabric.config import settings
from etlfabric.models.connection import ConnectionDirection, ConnectionType
from etlfabric.services.connection_service import ConnectionService
from etlfabric.services.encryption import EncryptionService
from etlfabric.ui.state.base_state import BaseState, get_sync_session


class ConnectionItem(BaseModel):
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

    def set_form_name(self, value: str):
        self.form_name = value

    def set_form_type(self, value: str):
        self.form_type = value

    def set_form_direction(self, value: str):
        self.form_direction = value

    def set_form_config(self, value: str):
        self.form_config = value

    async def load_connections(self):
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            rows = svc.list_connections(session, org_id)
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

    async def create_connection(self):
        import json

        org_id = await self._get_org_id()
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
                    org_id,
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
        await self.load_connections()

    async def delete_connection(self, conn_id: int):
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            svc.delete_connection(session, org_id, conn_id)
            session.commit()
        await self.load_connections()
