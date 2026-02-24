"""Connection state for Reflex UI."""

import json
import re

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.connection import ConnectionDirection, ConnectionType
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.ui.state.base_state import BaseState, get_sync_session

# Types that can serve as sources (databases + files + rest_api + sheets)
SOURCE_TYPES = {
    "postgres",
    "mysql",
    "mssql",
    "sqlite",
    "rest_api",
    "s3",
    "csv",
    "json",
    "parquet",
    "google_sheets",
    "mongodb",
    "clickhouse",
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
}

# Default ports for database connection types
_DEFAULT_PORTS: dict[str, str] = {
    "postgres": "5432",
    "mysql": "3306",
    "mssql": "1433",
    "redshift": "5439",
    "mongodb": "27017",
    "clickhouse": "8123",
}

# Connection types that use the SQL database form group (host/port/user/pass/db/schema)
_DB_TYPES = {"postgres", "mysql", "mssql", "redshift", "clickhouse"}


def _validate_connection_form(
    name: str,
    conn_type: str,
    use_raw_json: bool,
    *,
    host: str = "",
    port: str = "",
    database: str = "",
    path: str = "",
    project: str = "",
    dataset: str = "",
    account: str = "",
    user: str = "",
    bucket_url: str = "",
    base_url: str = "",
    uploaded_file_id: int = 0,
    spreadsheet_url: str = "",
    service_account_json: str = "",
) -> str:
    """Return an error message if required fields are missing, or '' if valid."""
    if not name.strip():
        return "Connection name is required"

    if use_raw_json:
        return ""

    if conn_type in _DB_TYPES:
        if not host.strip():
            return "Host is required"
        if not port.strip():
            return "Port is required"
        if not database.strip():
            return "Database is required"
    elif conn_type == "sqlite":
        if not path.strip():
            return "Database path is required"
    elif conn_type == "bigquery":
        if not project.strip():
            return "GCP Project ID is required"
        if not dataset.strip():
            return "Dataset is required"
    elif conn_type == "snowflake":
        if not account.strip():
            return "Account is required"
        if not user.strip():
            return "User is required"
        if not database.strip():
            return "Database is required"
    elif conn_type == "s3":
        if not bucket_url.strip():
            return "Bucket URL is required"
    elif conn_type in ("csv", "json", "parquet"):
        if not bucket_url.strip() and not uploaded_file_id:
            return "File upload or file path is required"
    elif conn_type == "mongodb":
        if not host.strip():
            return "Host is required"
        if not database.strip():
            return "Database is required"
    elif conn_type == "google_sheets":
        if not spreadsheet_url.strip():
            return "Spreadsheet URL is required"
        if not service_account_json.strip():
            return "Service Account JSON is required"
    elif conn_type == "rest_api":
        if not base_url.strip():
            return "Base URL is required"
    return ""


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
    test_status: str = ""  # "" = untested, "ok" = success, "fail" = failure


class ConnectionState(BaseState):
    connections: list[ConnectionItem] = []
    form_name: str = ""
    form_type: str = "postgres"
    form_config: str = "{}"
    form_use_raw_json: bool = False

    # 0 = creating new, >0 = editing existing connection
    editing_conn_id: int = 0

    # Test connection feedback
    test_message: str = ""
    test_success: bool = False

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

    # File upload (csv/json/parquet)
    form_uploaded_file_id: int = 0
    form_uploaded_file_name: str = ""

    # Google Sheets
    form_spreadsheet_url: str = ""
    form_service_account_json: str = ""

    def set_form_name(self, value: str):
        self.form_name = re.sub(r"[^a-zA-Z0-9 ]", "", value)

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

    def set_form_spreadsheet_url(self, value: str):
        self.form_spreadsheet_url = value

    def set_form_service_account_json(self, value: str):
        self.form_service_account_json = value

    async def handle_file_upload(self, files: list):
        """Receive uploaded file, call FileUploadService.save_file, store ID."""
        from datanika.services.file_upload_service import FileUploadService

        if not files:
            return
        file = files[0]
        upload_data = await file.read()
        filename = file.filename

        org_id = await self._get_org_id()
        file_svc = FileUploadService(settings.file_uploads_dir)
        try:
            with get_sync_session() as session:
                record = file_svc.save_file(session, org_id, filename, upload_data)
                session.commit()
                self.form_uploaded_file_id = record.id
                self.form_uploaded_file_name = record.original_name
                self.error_message = ""
        except ValueError as e:
            self.error_message = str(e)

    def _validate_form(self) -> str:
        """Return an error message if required fields are missing, or '' if valid."""
        return _validate_connection_form(
            name=self.form_name,
            conn_type=self.form_type,
            use_raw_json=self.form_use_raw_json,
            host=self.form_host,
            port=self.form_port,
            database=self.form_database,
            path=self.form_path,
            project=self.form_project,
            dataset=self.form_dataset,
            account=self.form_account,
            user=self.form_user,
            bucket_url=self.form_bucket_url,
            base_url=self.form_base_url,
            uploaded_file_id=self.form_uploaded_file_id,
            spreadsheet_url=self.form_spreadsheet_url,
            service_account_json=self.form_service_account_json,
        )

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
            if self.form_uploaded_file_id:
                config["uploaded_file_id"] = self.form_uploaded_file_id
            if self.form_bucket_url:
                config["bucket_url"] = self.form_bucket_url

        elif t == "google_sheets":
            if self.form_spreadsheet_url:
                config["spreadsheet_url"] = self.form_spreadsheet_url
            if self.form_service_account_json:
                config["service_account_json"] = self.form_service_account_json

        elif t == "rest_api":
            if self.form_base_url:
                config["base_url"] = self.form_base_url
            if self.form_api_key:
                config["api_key"] = self.form_api_key
            if self.form_extra_headers:
                config["extra_headers"] = self.form_extra_headers

        elif t == "mongodb":
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

        return config

    def _reset_form_fields(self):
        """Clear all typed form fields and exit edit mode."""
        self.editing_conn_id = 0
        self.form_name = ""
        self.form_type = "postgres"
        self.form_config = "{}"
        self.form_use_raw_json = False
        self.form_host = ""
        self.form_port = _DEFAULT_PORTS.get("postgres", "")
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
        self.form_uploaded_file_id = 0
        self.form_uploaded_file_name = ""
        self.form_spreadsheet_url = ""
        self.form_service_account_json = ""
        self.error_message = ""
        self.test_message = ""
        self.test_success = False

    def _populate_form_from_config(self, name: str, conn_type: str, config: dict):
        """Fill form fields from a decrypted config dict."""
        self.form_name = name
        self.form_type = conn_type
        self.form_use_raw_json = False
        self.error_message = ""
        self.test_message = ""

        # Reset all type-specific fields first
        self.form_host = ""
        self.form_port = _DEFAULT_PORTS.get(conn_type, "")
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
        self.form_uploaded_file_id = 0
        self.form_uploaded_file_name = ""
        self.form_spreadsheet_url = ""
        self.form_service_account_json = ""

        if conn_type in _DB_TYPES:
            self.form_host = config.get("host", "")
            self.form_port = str(config.get("port", _DEFAULT_PORTS.get(conn_type, "")))
            self.form_user = config.get("user", "")
            self.form_password = config.get("password", "")
            self.form_database = config.get("database", "")
            self.form_schema = config.get("schema", "")
        elif conn_type == "sqlite":
            self.form_path = config.get("path", "")
        elif conn_type == "bigquery":
            self.form_project = config.get("project", "")
            self.form_dataset = config.get("dataset", "")
            self.form_keyfile_json = config.get("keyfile_json", "")
        elif conn_type == "snowflake":
            self.form_account = config.get("account", "")
            self.form_user = config.get("user", "")
            self.form_password = config.get("password", "")
            self.form_database = config.get("database", "")
            self.form_warehouse = config.get("warehouse", "")
            self.form_role = config.get("role", "")
            self.form_schema = config.get("schema", "")
        elif conn_type == "s3":
            self.form_bucket_url = config.get("bucket_url", "")
            self.form_aws_access_key_id = config.get("aws_access_key_id", "")
            self.form_aws_secret_access_key = config.get("aws_secret_access_key", "")
            self.form_region_name = config.get("region_name", "")
            self.form_endpoint_url = config.get("endpoint_url", "")
        elif conn_type in ("csv", "json", "parquet"):
            self.form_bucket_url = config.get("bucket_url", "")
            self.form_uploaded_file_id = config.get("uploaded_file_id", 0)
            if self.form_uploaded_file_id:
                self.form_uploaded_file_name = config.get("uploaded_file_name", "uploaded file")
        elif conn_type == "google_sheets":
            self.form_spreadsheet_url = config.get("spreadsheet_url", "")
            self.form_service_account_json = config.get("service_account_json", "")
        elif conn_type == "rest_api":
            self.form_base_url = config.get("base_url", "")
            self.form_api_key = config.get("api_key", "")
            self.form_extra_headers = config.get("extra_headers", "")
        elif conn_type == "mongodb":
            self.form_host = config.get("host", "")
            self.form_port = str(config.get("port", _DEFAULT_PORTS.get("mongodb", "")))
            self.form_user = config.get("user", "")
            self.form_password = config.get("password", "")
            self.form_database = config.get("database", "")

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

    async def save_connection(self):
        """Create a new connection or update an existing one."""
        validation_error = self._validate_form()
        if validation_error:
            self.error_message = validation_error
            return
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        try:
            config = self._build_config()
        except (json.JSONDecodeError, ValueError) as e:
            self.error_message = f"Invalid config: {e}"
            return
        try:
            with get_sync_session() as session:
                if self.editing_conn_id:
                    svc.update_connection(
                        session,
                        org_id,
                        self.editing_conn_id,
                        name=self.form_name,
                        connection_type=ConnectionType(self.form_type),
                        direction=_infer_direction(self.form_type),
                        config=config,
                    )
                else:
                    svc.create_connection(
                        session,
                        org_id,
                        self.form_name,
                        ConnectionType(self.form_type),
                        _infer_direction(self.form_type),
                        config,
                    )
                session.commit()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to save connection")
            return
        self._reset_form_fields()
        await self.load_connections()

    async def edit_connection(self, conn_id: int):
        """Load a saved connection into the form for editing."""
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            conn = svc.get_connection(session, org_id, conn_id)
            config = svc.get_connection_config(session, org_id, conn_id)
        if conn is None or config is None:
            self.error_message = "Connection not found"
            return
        self._populate_form_from_config(conn.name, conn.connection_type.value, config)
        self.editing_conn_id = conn_id

    async def copy_connection(self, conn_id: int):
        """Load a saved connection into the form as a new copy."""
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            conn = svc.get_connection(session, org_id, conn_id)
            config = svc.get_connection_config(session, org_id, conn_id)
        if conn is None or config is None:
            self.error_message = "Connection not found"
            return
        self._populate_form_from_config(f"{conn.name} copy", conn.connection_type.value, config)
        self.editing_conn_id = 0

    def cancel_edit(self):
        """Cancel editing and reset the form."""
        self._reset_form_fields()

    async def delete_connection(self, conn_id: int):
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            svc.delete_connection(session, org_id, conn_id)
            session.commit()
        await self.load_connections()

    async def test_connection_from_form(self):
        """Test connectivity using the current form fields (before saving)."""
        validation_error = self._validate_form()
        if validation_error:
            self.test_success = False
            self.test_message = validation_error
            return
        try:
            config = self._build_config()
        except (json.JSONDecodeError, ValueError) as e:
            self.test_success = False
            self.test_message = f"Invalid config: {e}"
            return
        ok, msg = ConnectionService.test_connection(config, ConnectionType(self.form_type))
        self.test_success = ok
        self.test_message = msg

    async def test_saved_connection(self, conn_id: int):
        """Test connectivity for an already-saved connection."""
        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        svc = ConnectionService(encryption)
        with get_sync_session() as session:
            config = svc.get_connection_config(session, org_id, conn_id)
            conn = svc.get_connection(session, org_id, conn_id)
        if config is None or conn is None:
            self._set_row_test_status(conn_id, "fail")
            return
        ok, _msg = ConnectionService.test_connection(config, conn.connection_type)
        self._set_row_test_status(conn_id, "ok" if ok else "fail")

    def _set_row_test_status(self, conn_id: int, status: str):
        """Update test_status for a specific connection row."""
        self.connections = [
            item.model_copy(update={"test_status": status}) if item.id == conn_id else item
            for item in self.connections
        ]
