"""Model detail state for Reflex UI — view/edit a single catalog entry."""

import json
import logging

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.catalog_entry import CatalogEntryType
from datanika.models.dependency import NodeType
from datanika.services.catalog_service import CatalogService
from datanika.services.connection_service import ConnectionService
from datanika.services.dbt_project import DbtProjectService
from datanika.services.encryption import EncryptionService
from datanika.services.naming import to_snake_case
from datanika.services.pipeline_service import PipelineService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService
from datanika.ui.state.base_state import BaseState, get_sync_session

_log = logging.getLogger(__name__)

_VALID_STRING_TESTS = {"not_null", "unique"}
_VALID_DICT_TESTS = {"accepted_values", "relationships"}
_DBT_UTILS_PREFIX = "dbt_utils."


def _validate_column_tests(tests: list) -> str | None:
    """Validate that all tests are recognized dbt test formats.

    Returns an error message string if invalid, or None if all valid.
    """
    for t in tests:
        if isinstance(t, str):
            if t not in _VALID_STRING_TESTS:
                return f"Unknown test: {t}"
        elif isinstance(t, dict):
            key = next(iter(t), "")
            if key not in _VALID_DICT_TESTS and not key.startswith(_DBT_UTILS_PREFIX):
                return f"Unknown test: {key}"
        else:
            return f"Invalid test format: {t}"
    return None


def _recompute_columns(columns: list["ColumnItem"]) -> list["ColumnItem"]:
    """Scan tests list on each column and populate computed display fields."""
    result = []
    for col in columns:
        has_not_null = False
        has_unique = False
        accepted_values_csv = ""
        relationship_to = ""
        relationship_field = ""
        additional_tests: list[str] = []

        for t in col.tests:
            if t == "not_null":
                has_not_null = True
            elif t == "unique":
                has_unique = True
            elif isinstance(t, dict):
                key = next(iter(t), "")
                if key == "accepted_values":
                    values = t["accepted_values"].get("values", [])
                    accepted_values_csv = ", ".join(str(v) for v in values)
                elif key == "relationships":
                    relationship_to = t["relationships"].get("to", "")
                    relationship_field = t["relationships"].get("field", "")
                additional_tests.append(key)

        result.append(
            col.model_copy(
                update={
                    "has_not_null": has_not_null,
                    "has_unique": has_unique,
                    "accepted_values_csv": accepted_values_csv,
                    "relationship_to": relationship_to,
                    "relationship_field": relationship_field,
                    "additional_tests": additional_tests,
                }
            )
        )
    return result


class ColumnItem(BaseModel):
    name: str = ""
    data_type: str = ""
    description: str = ""
    tests: list = []
    # Computed display fields (populated by _recompute_columns)
    has_not_null: bool = False
    has_unique: bool = False
    accepted_values_csv: str = ""
    relationship_to: str = ""
    relationship_field: str = ""
    additional_tests: list[str] = []  # display keys for non-standard dict tests


PREVIEW_ROW_LIMIT = 100
_PREVIEW_CELL_MAX = 200  # truncate long cell values in the UI


def _stringify_cell(value) -> str:
    """Render a SQL row value as a safe, truncated string for display."""
    if value is None:
        return ""
    try:
        rendered = str(value)
    except Exception:
        rendered = repr(value)
    if len(rendered) > _PREVIEW_CELL_MAX:
        return rendered[:_PREVIEW_CELL_MAX] + "…"
    return rendered


class ModelDetailState(BaseState):
    entry_id: int = 0
    entry_type: str = ""
    origin_name: str = ""
    table_name: str = ""
    schema_name: str = ""
    dataset_name: str = ""
    connection_id: int = 0
    columns: list[ColumnItem] = []
    form_description: str = ""
    form_dbt_config: str = "{}"
    form_alias: str = ""
    form_tags: str = ""

    # Column editing state
    expanded_column: str = ""
    adding_test_column: str = ""
    custom_test_type: str = ""
    custom_test_expression: str = ""
    custom_test_min_value: str = ""
    custom_test_max_value: str = ""
    custom_test_proportion: str = ""

    # Data preview — first 100 rows of the underlying table fetched from
    # the destination connection on demand. Column types come from the
    # stored catalog metadata (``columns`` above); rows come from the
    # live SQL query via ``ConnectionService.preview_table``.
    preview_loaded: bool = False
    preview_loading: bool = False
    preview_error: str = ""
    preview_columns: list[str] = []
    preview_rows: list[list[str]] = []

    @property
    def can_preview(self) -> bool:
        """Preview requires a destination connection. Entries without a
        ``connection_id`` (legacy rows or pure dbt models not yet
        materialized) cannot be previewed."""
        return self.connection_id > 0 and bool(self.table_name)

    async def load_model_detail(self):
        raw_id = self.router.page.params.get("id", "0")
        try:
            self.entry_id = int(raw_id)
        except (ValueError, TypeError):
            self.error_message = "Invalid model ID"
            return

        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        org_id = auth.current_org.id or 0
        user_id = auth.current_user.id or 0
        if org_id == 0 or user_id == 0:
            return

        catalog_svc = CatalogService()
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        transform_svc = TransformationService()

        with get_sync_session() as session:
            entry = catalog_svc.get_entry(session, org_id, self.entry_id)
            if entry is None:
                self.error_message = "Model not found"
                return

            self.entry_type = entry.entry_type.value
            self.table_name = entry.table_name
            self.schema_name = entry.schema_name
            self.dataset_name = entry.dataset_name
            self.connection_id = entry.connection_id or 0
            self.form_description = entry.description or ""
            dbt_cfg = entry.dbt_config or {}
            self.form_alias = dbt_cfg.get("alias", "")
            tags = dbt_cfg.get("tags", [])
            self.form_tags = ", ".join(tags) if isinstance(tags, list) else ""
            self.form_dbt_config = json.dumps(dbt_cfg, indent=2)

            # Resolve origin name
            if entry.origin_type == NodeType.UPLOAD:
                uploads = upload_svc.list_uploads(session, org_id)
                names = {u.id: u.name for u in uploads}
                self.origin_name = names.get(entry.origin_id, f"Upload #{entry.origin_id}")
            elif entry.origin_type == NodeType.PIPELINE:
                pipeline_svc = PipelineService()
                pipelines = pipeline_svc.list_pipelines(session, org_id)
                names = {p.id: p.name for p in pipelines}
                self.origin_name = names.get(entry.origin_id, f"Pipeline #{entry.origin_id}")
            else:
                transformations = transform_svc.list_transformations(session, org_id)
                names = {t.id: t.name for t in transformations}
                self.origin_name = names.get(entry.origin_id, f"Transformation #{entry.origin_id}")

            # Populate columns with computed display fields
            raw_cols = [
                ColumnItem(
                    name=c.get("name", ""),
                    data_type=c.get("data_type", ""),
                    description=c.get("description", ""),
                    tests=c.get("tests", []),
                )
                for c in (entry.columns or [])
            ]
            self.columns = _recompute_columns(raw_cols)
        # Reset preview state on navigation between entries so stale
        # rows from a previous table don't bleed through.
        self.preview_loaded = False
        self.preview_loading = False
        self.preview_error = ""
        self.preview_columns = []
        self.preview_rows = []
        self.error_message = ""

    async def load_preview(self):
        """Fetch the first ``PREVIEW_ROW_LIMIT`` rows from the underlying
        destination connection.

        Requires ``connection_id > 0`` (set on load). Swallows DB errors
        into ``preview_error`` so a broken destination doesn't kill the
        detail page — users can still edit metadata offline.
        """
        if not self.can_preview:
            self.preview_error = "Preview requires a destination connection on this entry."
            self.preview_loaded = True
            return

        org_id = await self._get_org_id()
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)

        self.preview_loading = True
        self.preview_error = ""
        try:
            with get_sync_session() as session:
                conn = conn_svc.get_connection(session, org_id, self.connection_id)
                if conn is None:
                    self.preview_error = "Destination connection not found or no longer accessible."
                    return
                config = conn_svc.get_connection_config(session, org_id, self.connection_id)
                if config is None:
                    self.preview_error = "Connection credentials could not be decrypted."
                    return
                try:
                    columns, rows = ConnectionService.preview_table(
                        config,
                        conn.connection_type,
                        self.table_name,
                        schema=self.schema_name or None,
                        limit=PREVIEW_ROW_LIMIT,
                    )
                except Exception as exc:  # noqa: BLE001 — surface DB errors as UI-safe text
                    self.preview_error = f"Could not load preview: {exc}"
                    return
                self.preview_columns = list(columns)
                self.preview_rows = [[_stringify_cell(cell) for cell in row] for row in rows]
        finally:
            self.preview_loading = False
            self.preview_loaded = True

    def set_form_description(self, value: str):
        self.form_description = value

    def set_form_dbt_config(self, value: str):
        self.form_dbt_config = value

    def set_form_alias(self, value: str):
        self.form_alias = value

    def set_form_tags(self, value: str):
        self.form_tags = value

    def set_column_description(self, index: int, value: str):
        if 0 <= index < len(self.columns):
            updated = self.columns[index].model_copy(update={"description": value})
            self.columns[index] = updated

    # -- Column expand/collapse --

    def toggle_column_expand(self, col_name: str):
        self.expanded_column = "" if self.expanded_column == col_name else col_name
        self.adding_test_column = ""

    # -- Column description by name --

    def set_column_description_by_name(self, col_name: str, value: str):
        self.columns = [
            col.model_copy(update={"description": value}) if col.name == col_name else col
            for col in self.columns
        ]

    # -- Toggle not_null / unique --

    def toggle_column_not_null(self, col_name: str, checked: bool):
        self.columns = _recompute_columns(
            [
                self._toggle_test(col, "not_null", checked) if col.name == col_name else col
                for col in self.columns
            ]
        )

    def toggle_column_unique(self, col_name: str, checked: bool):
        self.columns = _recompute_columns(
            [
                self._toggle_test(col, "unique", checked) if col.name == col_name else col
                for col in self.columns
            ]
        )

    @staticmethod
    def _toggle_test(col: ColumnItem, test_name: str, on: bool) -> ColumnItem:
        tests = [t for t in col.tests if t != test_name]
        if on:
            tests.append(test_name)
        return col.model_copy(update={"tests": tests})

    # -- Custom test form --

    def open_custom_test_form(self, col_name: str):
        self.adding_test_column = col_name
        self.custom_test_type = ""
        self.custom_test_expression = ""
        self.custom_test_min_value = ""
        self.custom_test_max_value = ""
        self.custom_test_proportion = ""

    def cancel_custom_test_form(self):
        self.adding_test_column = ""

    def set_custom_test_type(self, value: str):
        self.custom_test_type = value

    def set_custom_test_expression(self, value: str):
        self.custom_test_expression = value

    def set_custom_test_min_value(self, value: str):
        self.custom_test_min_value = value

    def set_custom_test_max_value(self, value: str):
        self.custom_test_max_value = value

    def set_custom_test_proportion(self, value: str):
        self.custom_test_proportion = value

    def add_custom_test(self):
        col_name = self.adding_test_column
        if not col_name or not self.custom_test_type:
            return

        test_entry = self._build_custom_test_entry()
        if test_entry is None:
            return

        new_key = next(iter(test_entry), "")
        self.columns = _recompute_columns(
            [
                self._replace_or_add_test(col, new_key, test_entry) if col.name == col_name else col
                for col in self.columns
            ]
        )
        self.adding_test_column = ""

    @staticmethod
    def _replace_or_add_test(
        col: "ColumnItem",
        new_key: str,
        test_entry: dict,
    ) -> "ColumnItem":
        """Remove any existing test with the same key, then append the new one."""
        tests = [t for t in col.tests if not (isinstance(t, dict) and next(iter(t), "") == new_key)]
        tests.append(test_entry)
        return col.model_copy(update={"tests": tests})

    def _build_custom_test_entry(self) -> dict | None:
        test_type = self.custom_test_type
        # Native dbt tests (no prefix)
        if test_type == "accepted_values":
            values = [v.strip() for v in self.custom_test_expression.split(",") if v.strip()]
            if not values:
                return None
            return {"accepted_values": {"values": values}}
        elif test_type == "relationships":
            if not self.custom_test_min_value:
                return None
            return {
                "relationships": {
                    "to": self.custom_test_min_value,
                    "field": self.custom_test_max_value,
                }
            }
        # dbt_utils tests (prefixed)
        key = f"dbt_utils.{test_type}"
        if test_type == "expression_is_true":
            if not self.custom_test_expression:
                return None
            return {key: {"expression": self.custom_test_expression}}
        elif test_type == "not_constant":
            return {key: {}}
        elif test_type == "not_null_proportion":
            try:
                at_least = float(self.custom_test_proportion)
            except (ValueError, TypeError):
                return None
            return {key: {"at_least": at_least}}
        elif test_type == "accepted_range":
            config: dict = {}
            if self.custom_test_min_value:
                try:
                    config["min_value"] = float(self.custom_test_min_value)
                except ValueError:
                    return None
            if self.custom_test_max_value:
                try:
                    config["max_value"] = float(self.custom_test_max_value)
                except ValueError:
                    return None
            return {key: config}
        elif test_type == "sequential_values":
            config = {}
            if self.custom_test_min_value:
                try:
                    config["interval"] = int(self.custom_test_min_value)
                except ValueError:
                    return None
            return {key: config}
        return None

    def remove_column_test(self, col_name: str, test_display: str):
        """Remove a non-standard test identified by its display string."""
        self.columns = _recompute_columns(
            [
                self._remove_test_by_display(col, test_display) if col.name == col_name else col
                for col in self.columns
            ]
        )

    @staticmethod
    def _remove_test_by_display(col: ColumnItem, test_display: str) -> ColumnItem:
        new_tests = []
        for t in col.tests:
            if isinstance(t, dict):
                key = next(iter(t), "")
                if key == test_display:
                    continue
            new_tests.append(t)
        return col.model_copy(update={"tests": new_tests})

    # -- Save --

    async def save_model_detail(self):
        # #673. `_get_org_id` is deliberately unguarded (AC5 — it renders), so
        # without this the org id is read off a session that may have ended.
        if not await self._require_live_session():
            return

        org_id = await self._get_org_id()

        try:
            dbt_config = json.loads(self.form_dbt_config) if self.form_dbt_config.strip() else {}
        except json.JSONDecodeError as e:
            self.error_message = f"Invalid dbt config JSON: {e}"
            return

        # Merge alias and tags into dbt_config
        if self.form_alias.strip():
            dbt_config["alias"] = self.form_alias.strip()
        else:
            dbt_config.pop("alias", None)
        parsed_tags = [t.strip() for t in self.form_tags.split(",") if t.strip()]
        if parsed_tags:
            dbt_config["tags"] = parsed_tags
        else:
            dbt_config.pop("tags", None)

        # Validate column tests
        for col in self.columns:
            err = _validate_column_tests(col.tests)
            if err:
                self.error_message = f"Column '{col.name}': {err}"
                return

        columns_data = [
            {
                "name": c.name,
                "data_type": c.data_type,
                "description": c.description,
                "tests": c.tests,
            }
            for c in self.columns
        ]

        catalog_svc = CatalogService()

        with get_sync_session() as session:
            entry = catalog_svc.update_entry(
                session,
                org_id,
                self.entry_id,
                description=self.form_description or None,
                columns=columns_data,
                dbt_config=dbt_config,
            )
            if entry is None:
                self.error_message = "Model not found"
                return

            # Regenerate YML
            try:
                dbt_svc = DbtProjectService(settings.dbt_projects_dir)
                dbt_svc.ensure_project(org_id)

                if entry.entry_type == CatalogEntryType.SOURCE_TABLE and entry.connection_id:
                    from collections import defaultdict

                    all_entries = catalog_svc.get_entries_by_connection(
                        session,
                        org_id,
                        entry.connection_id,
                    )
                    by_dataset: dict[str, list] = defaultdict(list)
                    for e in all_entries:
                        by_dataset[e.dataset_name].append(
                            {
                                "name": e.table_name,
                                "columns": e.columns or [],
                            }
                        )
                    sources = [
                        {
                            "name": ds,
                            "schema": ds,
                            "tables": tbls,
                        }
                        for ds, tbls in sorted(by_dataset.items())
                    ]
                    # Resolve connection name for filename
                    encryption = EncryptionService(settings.credential_encryption_key)
                    conn_svc = ConnectionService(encryption)
                    conn = conn_svc.get_connection(session, org_id, entry.connection_id)
                    conn_name_snake = to_snake_case(conn.name) if conn else "unknown"
                    dbt_svc.write_source_yml_for_connection(org_id, conn_name_snake, sources)
                elif entry.entry_type == CatalogEntryType.DBT_MODEL:
                    dbt_svc.write_model_yml(
                        org_id,
                        entry.table_name,
                        entry.schema_name,
                        columns=columns_data,
                        description=entry.description,
                        dbt_config=dbt_config,
                    )
            except Exception:
                # Best-effort by design: the catalog edit is already valid and
                # must commit. But a dropped YML rewrite means the dbt project
                # on disk silently disagrees with the catalog the user just
                # saved, and the next dbt run is what discovers it (core#723).
                _log.exception(
                    "dbt YML regeneration failed and was dropped: entry=%s type=%s",
                    getattr(entry, "table_name", "?"),
                    getattr(entry, "entry_type", "?"),
                )

            session.commit()
        self.error_message = ""
        yield await self._saved_toast("models.saved_toast", "Model saved")
