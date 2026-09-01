"""Upload state for Reflex UI."""

import json
import re

import reflex as rx
from pydantic import BaseModel

from datanika.config import settings
from datanika.models.dependency import NodeType
from datanika.services.connection_service import DESTINATION_TYPES, SOURCE_TYPES, ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.upload_service import UploadService
from datanika.tasks.upload_tasks import run_upload_task
from datanika.ui.state.base_state import BaseState, get_sync_session
from datanika.ui.state.connection_state import (
    FILE_SOURCE_TYPES,
    NON_SQL_SOURCE_TYPES,
    SAAS_DEFAULT_ENDPOINTS,
    SAAS_SOURCE_TYPES,
)


class UploadItem(BaseModel):
    id: int = 0
    name: str = ""
    description: str = ""
    status: str = ""
    source_connection_id: int = 0
    destination_connection_id: int = 0
    source_connection_name: str = ""
    destination_connection_name: str = ""
    last_run_status: str = ""
    #: Whether either end of this upload points at a soft-deleted connection
    #: (core#805). Derived on every load, never persisted, so restoring the
    #: connection restores the row's display and its Run button with no other
    #: repair.
    source_connection_deleted: bool = False
    destination_connection_deleted: bool = False
    is_blocked: bool = False
    #: Colour for the status badge. Computed here rather than as nested
    #: ``rx.cond`` in a prop so the three-way choice stays readable.
    status_color: str = "gray"


class UploadState(BaseState):
    uploads: list[UploadItem] = []
    source_conn_options: list[str] = []
    dest_conn_options: list[str] = []
    form_name: str = ""
    form_description: str = ""
    form_source_id: str = ""
    form_dest_id: str = ""
    # Structured mode fields
    form_mode: str = "full_database"
    form_write_disposition: str = "append"
    form_primary_key: str = ""
    form_table: str = ""
    form_source_schema: str = ""
    form_table_names: str = ""
    form_batch_size: str = ""
    form_enable_incremental: bool = False
    form_cursor_path: str = ""
    form_initial_value: str = ""
    form_row_order: str = ""
    form_merge_config: str = ""
    # Schema contract
    form_sc_tables: str = ""
    form_sc_columns: str = ""
    form_sc_data_type: str = ""
    # SaaS endpoint selection
    form_available_endpoints: list[str] = []
    form_selected_endpoints: list[str] = []
    form_is_saas_source: bool = False
    form_is_non_sql_source: bool = False

    # Google Sheets specific
    form_sheet_names: str = ""

    # MongoDB specific
    form_collection_names: str = ""

    # File source specific (csv/json/parquet/s3)
    form_file_glob: str = ""
    form_is_file_source: bool = False
    # Format knobs the runner reads from dlt_config (core#499). Without a UI
    # these were reachable only through raw JSON config, which left a semicolon
    # CSV loading as one fused column and an `s3` connection on the default `*`
    # glob failing with an error naming a setting the user could not set.
    form_file_format: str = ""
    form_delimiter: str = ""
    form_encoding: str = ""

    # Raw JSON fallback
    form_config: str = "{}"
    form_use_raw_json: bool = False
    # 0 = creating new, >0 = editing existing upload
    editing_upload_id: int = 0

    def set_form_name(self, value: str):
        self.form_name = re.sub(r"[^a-zA-Z0-9 ]", "", value)

    def set_form_description(self, value: str):
        self.form_description = value

    def set_form_source_id(self, value: str):
        self.form_source_id = value
        conn_type = self._extract_conn_type(value)
        self.form_is_non_sql_source = conn_type in NON_SQL_SOURCE_TYPES
        self.form_is_file_source = conn_type in FILE_SOURCE_TYPES
        if conn_type in SAAS_SOURCE_TYPES:
            self.form_is_saas_source = True
            self.form_available_endpoints = SAAS_DEFAULT_ENDPOINTS.get(conn_type, [])
            self.form_selected_endpoints = list(self.form_available_endpoints)
        else:
            self.form_is_saas_source = False
            self.form_available_endpoints = []
            self.form_selected_endpoints = []

    @staticmethod
    def _extract_conn_type(option_str: str) -> str:
        """Extract connection type from formatted option string like '1 — MyConn (stripe)'."""
        if "(" in option_str and ")" in option_str:
            return option_str.rsplit("(", 1)[-1].rstrip(")")
        return ""

    def toggle_endpoint(self, endpoint: str):
        """Toggle an endpoint in the selected list."""
        if endpoint in self.form_selected_endpoints:
            self.form_selected_endpoints = [
                e for e in self.form_selected_endpoints if e != endpoint
            ]
        else:
            self.form_selected_endpoints = self.form_selected_endpoints + [endpoint]

    def set_form_dest_id(self, value: str):
        self.form_dest_id = value

    def set_form_mode(self, value: str):
        self.form_mode = value
        if value == "full_database":
            self.form_primary_key = ""
        elif value == "single_table":
            self.form_merge_config = ""

    def set_form_write_disposition(self, value: str):
        self.form_write_disposition = value

    def set_form_primary_key(self, value: str):
        self.form_primary_key = value

    def set_form_merge_config(self, value: str):
        self.form_merge_config = value

    def set_form_table(self, value: str):
        self.form_table = value

    def set_form_source_schema(self, value: str):
        self.form_source_schema = value

    def set_form_table_names(self, value: str):
        self.form_table_names = value

    def set_form_batch_size(self, value: str):
        self.form_batch_size = value

    def set_form_enable_incremental(self, value: bool):
        self.form_enable_incremental = value

    def set_form_cursor_path(self, value: str):
        self.form_cursor_path = value

    def set_form_initial_value(self, value: str):
        self.form_initial_value = value

    def set_form_row_order(self, value: str):
        self.form_row_order = value

    def set_form_sc_tables(self, value: str):
        self.form_sc_tables = value

    def set_form_sc_columns(self, value: str):
        self.form_sc_columns = value

    def set_form_sc_data_type(self, value: str):
        self.form_sc_data_type = value

    def set_form_file_format(self, value: str):
        self.form_file_format = value

    def set_form_delimiter(self, value: str):
        self.form_delimiter = value

    def set_form_encoding(self, value: str):
        self.form_encoding = value

    def set_form_config(self, value: str):
        self.form_config = value

    def set_form_use_raw_json(self, value: bool):
        self.form_use_raw_json = value

    def _get_services(self):
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        return upload_svc, conn_svc

    def _build_config(self) -> dict:
        """Build dlt_config from structured form fields."""
        if self.form_use_raw_json:
            return json.loads(self.form_config)

        config: dict = {}
        config["mode"] = self.form_mode

        if self.form_write_disposition:
            config["write_disposition"] = self.form_write_disposition
        if self.form_write_disposition == "merge":
            if self.form_mode == "single_table" and self.form_primary_key:
                config["primary_key"] = self.form_primary_key
            elif self.form_mode == "full_database" and self.form_merge_config:
                config["merge_config"] = json.loads(self.form_merge_config)

        if self.form_source_schema:
            config["source_schema"] = self.form_source_schema

        if self.form_batch_size:
            config["batch_size"] = int(self.form_batch_size)

        if self.form_mode == "single_table":
            if self.form_table:
                config["table"] = self.form_table
            if self.form_enable_incremental and self.form_cursor_path:
                inc: dict = {"cursor_path": self.form_cursor_path}
                if self.form_initial_value:
                    inc["initial_value"] = self.form_initial_value
                if self.form_row_order:
                    inc["row_order"] = self.form_row_order
                config["incremental"] = inc
        else:  # full_database
            if self.form_table_names:
                names = [t.strip() for t in self.form_table_names.split(",") if t.strip()]
                if names:
                    config["table_names"] = names

        # Schema contract
        sc: dict = {}
        if self.form_sc_tables:
            sc["tables"] = self.form_sc_tables
        if self.form_sc_columns:
            sc["columns"] = self.form_sc_columns
        if self.form_sc_data_type:
            sc["data_type"] = self.form_sc_data_type
        if sc:
            config["schema_contract"] = sc

        # SaaS endpoint selection → stored as "endpoints" in dlt_config
        if self.form_is_saas_source and self.form_selected_endpoints:
            config["endpoints"] = self.form_selected_endpoints

        # Google Sheets: sheet names
        if self.form_sheet_names:
            names = [s.strip() for s in self.form_sheet_names.split(",") if s.strip()]
            if names:
                config["sheet_names"] = names

        # MongoDB: collection names
        if self.form_collection_names:
            names = [c.strip() for c in self.form_collection_names.split(",") if c.strip()]
            if names:
                config["collection_names"] = names

        # File sources: glob pattern
        if self.form_file_glob:
            config["file_glob"] = self.form_file_glob

        # File sources: format knobs (core#499). Each is written only when set —
        # an empty string would override the runner's inference with nothing,
        # and "auto" is the UI's word for "infer", not a format.
        if self.form_file_format and self.form_file_format != "auto":
            config["file_format"] = self.form_file_format
        if self.form_delimiter:
            config["delimiter"] = self.form_delimiter
        if self.form_encoding:
            config["encoding"] = self.form_encoding

        return config

    async def load_uploads(self):
        org_id = await self._get_org_id()
        upload_svc, conn_svc = self._get_services()
        exec_svc = ExecutionService()
        with get_sync_session() as session:
            # Include retired connections in the *name* map only (core#805).
            # Without this a soft-deleted connection fell through to the
            # `#{id}` fallback and the row showed a raw internal identifier —
            # not localized, in no i18n key, and indistinguishable from a
            # connection that exists with an empty name.
            all_conns = conn_svc.list_connections(session, org_id, include_deleted=True)
            conn_names = {c.id: f"{c.name} ({c.connection_type.value})" for c in all_conns}
            deleted_conn_ids = {c.id for c in all_conns if c.deleted_at is not None}
            conns = [c for c in all_conns if c.deleted_at is None]
            rows = upload_svc.list_uploads(session, org_id)
            items = []
            for p in rows:
                runs = exec_svc.list_runs(
                    session, org_id, target_type=NodeType.UPLOAD, target_id=p.id, limit=1
                )
                last_status = runs[0].status.value if runs else ""
                src_deleted = p.source_connection_id in deleted_conn_ids
                dst_deleted = p.destination_connection_id in deleted_conn_ids
                blocked = src_deleted or dst_deleted
                if blocked:
                    status_color = "red"
                elif p.status.value == "active":
                    status_color = "green"
                else:
                    status_color = "gray"
                items.append(
                    UploadItem(
                        id=p.id,
                        name=p.name,
                        description=p.description or "",
                        status=p.status.value,
                        source_connection_id=p.source_connection_id,
                        destination_connection_id=p.destination_connection_id,
                        source_connection_name=conn_names.get(
                            p.source_connection_id, f"#{p.source_connection_id}"
                        ),
                        destination_connection_name=conn_names.get(
                            p.destination_connection_id, f"#{p.destination_connection_id}"
                        ),
                        last_run_status=last_status,
                        source_connection_deleted=src_deleted,
                        destination_connection_deleted=dst_deleted,
                        is_blocked=blocked,
                        status_color=status_color,
                    )
                )
            self.uploads = items
            # Load connections filtered by capability
            self.source_conn_options = [
                f"{c.id} — {c.name} ({c.connection_type.value})"
                for c in conns
                if c.connection_type.value in SOURCE_TYPES
            ]
            self.dest_conn_options = [
                f"{c.id} — {c.name} ({c.connection_type.value})"
                for c in conns
                if c.connection_type.value in DESTINATION_TYPES
            ]
        self.error_message = ""

    async def save_upload(self):
        if not await self._check_role("editor"):
            return
        if not self.form_name.strip():
            self.error_message = "Upload name cannot be empty"
            return
        from datanika.ui.state.auth_state import AuthState

        auth_state = await self.get_state(AuthState)
        org_id = auth_state.current_org.id
        user_id = auth_state.current_user.id
        upload_svc, _ = self._get_services()
        try:
            config = self._build_config()
        except (json.JSONDecodeError, ValueError) as e:
            self.error_message = f"Invalid config: {e}"
            return
        try:
            src_id = int(self.form_source_id.split(" — ")[0])
            dst_id = int(self.form_dest_id.split(" — ")[0])
        except (ValueError, IndexError):
            self.error_message = "Please select source and destination connections"
            return
        try:
            with get_sync_session() as session:
                if self.editing_upload_id:
                    upload_svc.update_upload(
                        session,
                        org_id,
                        self.editing_upload_id,
                        name=self.form_name,
                        description=self.form_description or None,
                        source_connection_id=src_id,
                        destination_connection_id=dst_id,
                        dlt_config=config,
                    )
                    self._audit(
                        session,
                        org_id,
                        user_id,
                        "update",
                        "upload",
                        resource_id=self.editing_upload_id,
                        new_values={
                            "name": self.form_name,
                            "source": str(src_id),
                            "destination": str(dst_id),
                        },
                    )
                else:
                    upload = upload_svc.create_upload(
                        session,
                        org_id,
                        self.form_name,
                        self.form_description or None,
                        src_id,
                        dst_id,
                        config,
                    )
                    self._audit(
                        session,
                        org_id,
                        user_id,
                        "create",
                        "upload",
                        resource_id=upload.id,
                        new_values={
                            "name": self.form_name,
                            "source": str(src_id),
                            "destination": str(dst_id),
                        },
                    )
                session.commit()
        except Exception as e:
            self._set_error(e, "Failed to save upload")
            return
        self._reset_form()
        await self.load_uploads()

    def _reset_form(self):
        self.editing_upload_id = 0
        self.form_name = ""
        self.form_description = ""
        self.form_source_id = ""
        self.form_dest_id = ""
        self.form_mode = "full_database"
        self.form_write_disposition = "append"
        self.form_primary_key = ""
        self.form_merge_config = ""
        self.form_table = ""
        self.form_source_schema = ""
        self.form_table_names = ""
        self.form_batch_size = ""
        self.form_enable_incremental = False
        self.form_cursor_path = ""
        self.form_initial_value = ""
        self.form_row_order = ""
        self.form_sc_tables = ""
        self.form_sc_columns = ""
        self.form_sc_data_type = ""
        self.form_config = "{}"
        self.form_use_raw_json = False
        self.form_is_saas_source = False
        self.form_is_non_sql_source = False
        self.form_available_endpoints = []
        self.form_selected_endpoints = []
        self.form_sheet_names = ""
        self.form_collection_names = ""
        self.form_file_glob = ""
        self.form_is_file_source = False
        self.form_file_format = ""
        self.form_delimiter = ""
        self.form_encoding = ""
        self.error_message = ""

    def _populate_form_from_upload(self, upload, conn_options_src, conn_options_dst):
        """Fill form fields from an upload object."""
        self.form_name = upload.name
        self.form_description = upload.description or ""
        self.error_message = ""

        # Find matching connection option strings
        src_prefix = f"{upload.source_connection_id} — "
        self.form_source_id = next((o for o in conn_options_src if o.startswith(src_prefix)), "")
        dst_prefix = f"{upload.destination_connection_id} — "
        self.form_dest_id = next((o for o in conn_options_dst if o.startswith(dst_prefix)), "")

        # Populate from dlt_config
        config = upload.dlt_config or {}
        self.form_mode = config.get("mode", "full_database")
        self.form_write_disposition = config.get("write_disposition", "append")
        self.form_primary_key = config.get("primary_key", "")
        merge_config = config.get("merge_config")
        self.form_merge_config = json.dumps(merge_config, indent=2) if merge_config else ""
        self.form_source_schema = config.get("source_schema", "")
        self.form_batch_size = str(config["batch_size"]) if "batch_size" in config else ""
        self.form_table = config.get("table", "")
        table_names = config.get("table_names", [])
        self.form_table_names = ", ".join(table_names) if table_names else ""

        inc = config.get("incremental", {})
        self.form_enable_incremental = bool(inc)
        self.form_cursor_path = inc.get("cursor_path", "") if inc else ""
        self.form_initial_value = inc.get("initial_value", "") if inc else ""
        self.form_row_order = inc.get("row_order", "") if inc else ""

        sc = config.get("schema_contract", {})
        self.form_sc_tables = sc.get("tables", "") if sc else ""
        self.form_sc_columns = sc.get("columns", "") if sc else ""
        self.form_sc_data_type = sc.get("data_type", "") if sc else ""

        # Restore source-type-specific fields
        conn_type = self._extract_conn_type(self.form_source_id)
        self.form_is_non_sql_source = conn_type in NON_SQL_SOURCE_TYPES
        self.form_is_file_source = conn_type in FILE_SOURCE_TYPES
        if conn_type in SAAS_SOURCE_TYPES:
            self.form_is_saas_source = True
            self.form_available_endpoints = SAAS_DEFAULT_ENDPOINTS.get(conn_type, [])
            saved_endpoints = config.get("endpoints", [])
            self.form_selected_endpoints = (
                saved_endpoints if saved_endpoints else list(self.form_available_endpoints)
            )
        else:
            self.form_is_saas_source = False
            self.form_available_endpoints = []
            self.form_selected_endpoints = []

        # Google Sheets
        sheet_names = config.get("sheet_names", [])
        self.form_sheet_names = ", ".join(sheet_names) if sheet_names else ""

        # MongoDB
        collection_names = config.get("collection_names", [])
        self.form_collection_names = ", ".join(collection_names) if collection_names else ""

        # File sources
        self.form_file_glob = config.get("file_glob", "")
        self.form_file_format = config.get("file_format", "")
        self.form_delimiter = config.get("delimiter", "")
        self.form_encoding = config.get("encoding", "")

        # Last, because it asks the structured form what it just produced.
        self._restore_raw_json_fallback(config)

    def _restore_raw_json_fallback(self, config: dict) -> None:
        """Re-enter raw-JSON mode when the structured form cannot hold ``config``.

        This used to be an unconditional ``form_use_raw_json = False`` /
        ``form_config = "{}"``, so merely *opening* the edit form on an upload
        built with **Use raw JSON config** discarded it — and for a ``rest_api``
        source that made the upload unrunnable, since the runner requires a
        ``resources`` list (core#803).

        The test is ``_build_config()`` itself rather than a list of "advanced"
        keys. ``_build_config`` is the only definition of what the structured
        form can produce, so running it answers the question exactly, stays
        correct when a connector adds a key, and catches unrepresentable
        *values* (an integer ``initial_value`` that a text field would
        stringify) as well as unrepresentable keys.

        Extra keys the rebuild *adds* — an explicit ``write_disposition`` where
        the stored config relied on the default — are not a loss, so the check
        is one-directional. Must be called after every form field is populated:
        ``_build_config`` reads them all.
        """
        self.form_use_raw_json = False
        self.form_config = "{}"
        if not config:
            return
        try:
            rebuilt = self._build_config()
        except Exception:
            # A config that reached the database by another route (API, MCP,
            # a hand edit) can hold a value the structured form cannot parse
            # back. Failing towards "keep it" is the only safe direction.
            rebuilt = None
        if rebuilt is None or any(k not in rebuilt or rebuilt[k] != v for k, v in config.items()):
            self.form_use_raw_json = True
            self.form_config = json.dumps(config, indent=2)

    async def edit_upload(self, upload_id: int):
        """Load an upload into the form for editing."""
        org_id = await self._get_org_id()
        upload_svc, conn_svc = self._get_services()
        with get_sync_session() as session:
            upload = upload_svc.get_upload(session, org_id, upload_id)
            if upload is None:
                self.error_message = "Upload not found"
                return
            # Ensure connection options are loaded
            conns = conn_svc.list_connections(session, org_id)
            src_opts = [
                f"{c.id} — {c.name} ({c.connection_type.value})"
                for c in conns
                if c.connection_type.value in SOURCE_TYPES
            ]
            dst_opts = [
                f"{c.id} — {c.name} ({c.connection_type.value})"
                for c in conns
                if c.connection_type.value in DESTINATION_TYPES
            ]
            self.source_conn_options = src_opts
            self.dest_conn_options = dst_opts
            self._populate_form_from_upload(upload, src_opts, dst_opts)
        self.editing_upload_id = upload_id

    async def copy_upload(self, upload_id: int):
        """Load an upload into the form as a new copy."""
        org_id = await self._get_org_id()
        upload_svc, conn_svc = self._get_services()
        with get_sync_session() as session:
            upload = upload_svc.get_upload(session, org_id, upload_id)
            if upload is None:
                self.error_message = "Upload not found"
                return
            conns = conn_svc.list_connections(session, org_id)
            src_opts = [
                f"{c.id} — {c.name} ({c.connection_type.value})"
                for c in conns
                if c.connection_type.value in SOURCE_TYPES
            ]
            dst_opts = [
                f"{c.id} — {c.name} ({c.connection_type.value})"
                for c in conns
                if c.connection_type.value in DESTINATION_TYPES
            ]
            self.source_conn_options = src_opts
            self.dest_conn_options = dst_opts
            self._populate_form_from_upload(upload, src_opts, dst_opts)
        self.form_name = f"{self.form_name} copy"
        self.editing_upload_id = 0

    def cancel_edit(self):
        """Cancel editing and reset the form."""
        self._reset_form()

    async def delete_upload(self, upload_id: int):
        if not await self._check_role("admin"):
            return
        from datanika.ui.state.auth_state import AuthState

        auth_state = await self.get_state(AuthState)
        org_id = auth_state.current_org.id
        user_id = auth_state.current_user.id
        upload_svc, _ = self._get_services()
        with get_sync_session() as session:
            upload = upload_svc.get_upload(session, org_id, upload_id)
            old_values = {"name": upload.name} if upload else {}
            upload_svc.delete_upload(session, org_id, upload_id)
            self._audit(
                session,
                org_id,
                user_id,
                "delete",
                "upload",
                resource_id=upload_id,
                old_values=old_values,
            )
            session.commit()
        await self.load_uploads()
        yield await self._deleted_toast("uploads.deleted_toast", "Upload deleted")

    async def run_upload(self, upload_id: int):
        if not await self._check_role("editor"):
            return
        from datanika.ui.state.auth_state import AuthState

        auth_state = await self.get_state(AuthState)
        org_id = auth_state.current_org.id
        user_id = auth_state.current_user.id
        exec_svc = ExecutionService()
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        template_slug: str | None = None
        with get_sync_session() as session:
            upload = upload_svc.get_upload(session, org_id, upload_id)
            # Refuse rather than queue a run that cannot succeed (core#805).
            # `get_connection` filters `deleted_at`, so the source lookup
            # inside the task returns nothing and the failure surfaces only in
            # the run — at 03:00 with nobody watching, for a scheduled upload.
            # The disabled button on the row is a claim the client makes; this
            # is the one that holds.
            if upload is not None:
                missing = [
                    conn_id
                    for conn_id in (
                        upload.source_connection_id,
                        upload.destination_connection_id,
                    )
                    if conn_svc.get_connection(session, org_id, conn_id) is None
                ]
                if missing:
                    self.error_message = (
                        "This upload's source or destination connection was deleted. "
                        "Restore the connection to run it again."
                    )
                    return
            run = exec_svc.create_run(session, org_id, NodeType.UPLOAD, upload_id)
            self._audit(
                session,
                org_id,
                user_id,
                "run",
                "upload",
                resource_id=upload_id,
                new_values={"target_type": "upload", "target_id": upload_id},
            )
            # Check both ends of the upload — the template flow could have
            # created either the source or the destination. First eligible
            # connection fires ``template_first_run_triggered``; subsequent
            # runs see fired_at set and return None. #93.
            if upload is not None:
                for conn_id in (upload.source_connection_id, upload.destination_connection_id):
                    template_slug = conn_svc.consume_template_first_run(session, org_id, conn_id)
                    if template_slug:
                        break
            session.commit()
            run_id = run.id
        run_upload_task.delay(run_id=run_id, org_id=org_id)
        self.error_message = ""
        yield rx.toast("Run triggered", position="top-right")
        if template_slug:
            import json

            yield rx.call_script(
                "if(window.plausible){window.plausible('template_first_run_triggered',"
                f"{{props:{{slug:{json.dumps(template_slug)}}}}})}}"
            )
