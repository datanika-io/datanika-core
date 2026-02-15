"""Connection state for Reflex UI."""

import json

from pydantic import BaseModel

from etlfabric.config import settings
from etlfabric.models.connection import ConnectionDirection, ConnectionType
from etlfabric.services.connection_service import ConnectionService
from etlfabric.services.encryption import EncryptionService
from etlfabric.ui.state.base_state import BaseState, get_sync_session

# Types that can serve as sources (databases + files + rest_api)
SOURCE_TYPES = {"postgres", "mysql", "mssql", "sqlite", "rest_api", "s3", "csv", "json", "parquet"}
# Types that can serve as destinations (databases + cloud warehouses)
DESTINATION_TYPES = {"postgres", "mysql", "mssql", "sqlite", "bigquery", "snowflake", "redshift"}

# Default ports for database connection types
_DEFAULT_PORTS: dict[str, str] = {
    "postgres": "5432",
    "mysql": "3306",
    "mssql": "1433",
    "redshift": "5439",
}

# Connection types that use the SQL database form group
_DB_TYPES = {"postgres", "mysql", "mssql", "redshift"}


def _infer_direction(connection_type: str) -> ConnectionDirection:
    """Infer direction from connection type."""
    is_source = connection_type in SOURCE_TYPES
    is_dest = connection_type in DESTINATION_TYPES
    if is_source and is_dest:
        return ConnectionDirection.BOTH
    if is_dest:
        return ConnectionDirection.DESTINATION
    return ConnectionDirection.SOURCE


class ConnectionItem(BaseModel):
    id: int = 0
    name: str = ""
    connection_type: str = ""


class ConnectionState(BaseState):
    connections: list[ConnectionItem] = []
    form_name: str = ""
    form_type: str = "postgres"
    form_config: str = "{}"
    form_use_raw_json: bool = False

    # SQL database fields (postgres, mysql, mssql, redshift)
    form_host: str = ""
    form_port: str = "5432"
    form_user: str = ""
    form_password: str = ""
    form_database: str = ""
    form_schema: str = ""

    # SQLite
    form_path: str = ""

    # BigQuery
    form_project: str = ""
    form_dataset: str = ""
    form_keyfile_json: str = ""

    # Snowflake (also uses form_user, form_password, form_database, form_schema)
    form_account: str = ""
    form_warehouse: str = ""
    form_role: str = ""

    # S3 (also used by csv/json/parquet for bucket_url)
    form_bucket_url: str = ""
    form_aws_access_key_id: str = ""
    form_aws_secret_access_key: str = ""
    form_region_name: str = ""
    form_endpoint_url: str = ""

    # REST API
    form_base_url: str = ""
    form_api_key: str = ""
    form_extra_headers: str = ""

    def set_form_name(self, value: str):
        self.form_name = value

    def set_form_type(self, value: str):
        self.form_type = value
        self.form_port = _DEFAULT_PORTS.get(value, "")

    def set_form_config(self, value: str):
        self.form_config = value

    def set_form_use_raw_json(self, value: bool):
        self.form_use_raw_json = value

    def set_form_host(self, value: str):
        self.form_host = value

    def set_form_port(self, value: str):
        self.form_port = value

    def set_form_user(self, value: str):
        self.form_user = value

    def set_form_password(self, value: str):
        self.form_password = value

    def set_form_database(self, value: str):
        self.form_database = value

    def set_form_schema(self, value: str):
        self.form_schema = value

    def set_form_path(self, value: str):
        self.form_path = value

    def set_form_project(self, value: str):
        self.form_project = value

    def set_form_dataset(self, value: str):
        self.form_dataset = value

    def set_form_keyfile_json(self, value: str):
        self.form_keyfile_json = value

    def set_form_account(self, value: str):
        self.form_account = value

    def set_form_warehouse(self, value: str):
        self.form_warehouse = value

    def set_form_role(self, value: str):
        self.form_role = value

    def set_form_bucket_url(self, value: str):
        self.form_bucket_url = value

    def set_form_aws_access_key_id(self, value: str):
        self.form_aws_access_key_id = value

    def set_form_aws_secret_access_key(self, value: str):
        self.form_aws_secret_access_key = value

    def set_form_region_name(self, value: str):
        self.form_region_name = value

    def set_form_endpoint_url(self, value: str):
        self.form_endpoint_url = value

    def set_form_base_url(self, value: str):
        self.form_base_url = value

    def set_form_api_key(self, value: str):
        self.form_api_key = value

    def set_form_extra_headers(self, value: str):
        self.form_extra_headers = value

    def _build_config(self) -> dict:
        """Build connection config dict from structured form fields."""
        if self.form_use_raw_json:
            return json.loads(self.form_config)

        config: dict = {}
        t = self.form_type

        if t in _DB_TYPES:
            if self.form_host:
                config["host"] = self.form_host
            if self.form_port:
                config["port"] = int(self.form_port)
            if self.form_user:
                config["user"] = self.form_user
            if self.form_password:
                config["password"] = self.form_password
            if self.form_database:
                config["database"] = self.form_database
            if self.form_schema:
                config["schema"] = self.form_schema

        elif t == "sqlite":
            if self.form_path:
                config["path"] = self.form_path

        elif t == "bigquery":
            if self.form_project:
                config["project"] = self.form_project
            if self.form_dataset:
                config["dataset"] = self.form_dataset
            if self.form_keyfile_json:
                config["keyfile_json"] = self.form_keyfile_json

        elif t == "snowflake":
            if self.form_account:
                config["account"] = self.form_account
            if self.form_user:
                config["user"] = self.form_user
            if self.form_password:
                config["password"] = self.form_password
            if self.form_database:
                config["database"] = self.form_database
            if self.form_warehouse:
                config["warehouse"] = self.form_warehouse
            if self.form_role:
                config["role"] = self.form_role
            if self.form_schema:
                config["schema"] = self.form_schema

        elif t == "s3":
            if self.form_bucket_url:
                config["bucket_url"] = self.form_bucket_url
            if self.form_aws_access_key_id:
                config["aws_access_key_id"] = self.form_aws_access_key_id
            if self.form_aws_secret_access_key:
                config["aws_secret_access_key"] = self.form_aws_secret_access_key
            if self.form_region_name:
                config["region_name"] = self.form_region_name
            if self.form_endpoint_url:
                config["endpoint_url"] = self.form_endpoint_url

        elif t in ("csv", "json", "parquet"):
            if self.form_bucket_url:
                config["bucket_url"] = self.form_bucket_url

        elif t == "rest_api":
            if self.form_base_url:
                config["base_url"] = self.form_base_url
            if self.form_api_key:
                config["api_key"] = self.form_api_key
            if self.form_extra_headers:
                config["extra_headers"] = self.form_extra_headers

        return config

    def _reset_form_fields(self):
        """Clear all typed form fields after successful creation."""
        self.form_name = ""
        self.form_config = "{}"
        self.form_use_raw_json = False
        self.form_host = ""
        self.form_port = _DEFAULT_PORTS.get(self.form_type, "")
        self.form_user = ""
        self.form_password = ""
        self.form_database = ""
        self.form_schema = ""
        self.form_path = ""
        self.form_project = ""
        self.form_dataset = ""
        self.form_keyfile_json = ""
        self.form_account = ""
        self.form_warehouse = ""
        self.form_role = ""
        self.form_bucket_url = ""
        self.form_aws_access_key_id = ""
        self.form_aws_secret_access_key = ""
        self.form_region_name = ""
        self.form_endpoint_url = ""
        self.form_base_url = ""
        self.form_api_key = ""
        self.form_extra_headers = ""
        self.error_message = ""

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
                )
                for c in rows
            ]
        self.error_message = ""

    async def create_connection(self):
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        try:
            config = self._build_config()
        except (json.JSONDecodeError, ValueError) as e:
            self.error_message = f"Invalid config: {e}"
            return
        direction = _infer_direction(self.form_type)
        try:
            with get_sync_session() as session:
                svc.create_connection(
                    session,
                    org_id,
                    self.form_name,
                    ConnectionType(self.form_type),
                    direction,
                    config,
                )
                session.commit()
        except Exception as e:
            self.error_message = str(e)
            return
        self._reset_form_fields()
        await self.load_connections()

    async def delete_connection(self, conn_id: int):
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            svc.delete_connection(session, org_id, conn_id)
            session.commit()
        await self.load_connections()
