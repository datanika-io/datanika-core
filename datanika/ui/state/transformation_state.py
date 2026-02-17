"""Transformation state for Reflex UI."""

import re

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.transformation import Materialization
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.transformation_service import TransformationService
from datanika.ui.state.base_state import BaseState, get_sync_session
from datanika.ui.state.connection_state import DESTINATION_TYPES

_REF_PATTERN = re.compile(r"""\{\{\s*ref\(\s*['"]([^'"]*?)$""")


class TransformationItem(BaseModel):
    id: int = 0
    name: str = ""
    description: str = ""
    materialization: str = ""
    schema_name: str = ""
    tags: str = ""
    connection_name: str = ""


class TransformationState(BaseState):
    transformations: list[TransformationItem] = []
    form_name: str = ""
    form_sql_body: str = ""
    form_materialization: str = "view"
    form_description: str = ""
    form_schema_name: str = "staging"
    # Connection
    dest_conn_options: list[str] = []
    form_connection_option: str = ""
    # Tags
    form_tags: str = ""
    # Schema combobox
    schema_options: list[str] = []
    adding_new_schema: bool = False
    # Ref autocomplete
    all_model_names: list[str] = []
    ref_suggestions: list[str] = []
    ref_suggestion_index: int = -1
    ref_selected_name: str = ""
    show_ref_popover: bool = False
    ref_dismissed: bool = False
    # Test results
    test_result_message: str = ""
    # SQL preview
    preview_sql: str = ""
    # Incremental materialization config
    form_unique_key: str = ""
    form_strategy: str = ""
    form_updated_at: str = ""
    form_on_schema_change: str = "ignore"
    # 0 = creating new, >0 = editing existing transformation
    editing_transformation_id: int = 0

    def set_form_name(self, value: str):
        self.form_name = value

    def set_form_sql_body(self, value: str):
        self.form_sql_body = value
        self.ref_dismissed = False

    def set_form_materialization(self, value: str):
        self.form_materialization = value

    def set_form_description(self, value: str):
        self.form_description = value

    def set_form_schema_name(self, value: str):
        if value == "+ Add new...":
            self.adding_new_schema = True
            self.form_schema_name = ""
        else:
            self.adding_new_schema = False
            self.form_schema_name = value

    def set_new_schema_name(self, value: str):
        self.form_schema_name = value

    def confirm_new_schema(self):
        name = self.form_schema_name.strip()
        if name:
            existing = {o for o in self.schema_options if o != "+ Add new..."}
            existing.add(name)
            self.schema_options = sorted(existing) + ["+ Add new..."]
        self.adding_new_schema = False

    def set_form_connection_option(self, value: str):
        self.form_connection_option = value

    def set_form_tags(self, value: str):
        self.form_tags = value

    def set_form_unique_key(self, value: str):
        self.form_unique_key = value

    def set_form_strategy(self, value: str):
        self.form_strategy = value

    def set_form_updated_at(self, value: str):
        self.form_updated_at = value

    def set_form_on_schema_change(self, value: str):
        self.form_on_schema_change = value

    def _reset_form(self):
        self.editing_transformation_id = 0
        self.form_name = ""
        self.form_sql_body = ""
        self.form_materialization = "view"
        self.form_description = ""
        self.form_schema_name = "staging"
        self.form_connection_option = ""
        self.form_tags = ""
        self.form_unique_key = ""
        self.form_strategy = ""
        self.form_updated_at = ""
        self.form_on_schema_change = "ignore"
        self.adding_new_schema = False
        self.show_ref_popover = False
        self.ref_selected_name = ""
        self.ref_dismissed = False
        self.error_message = ""

    async def load_transformations(self):
        org_id = await self._get_org_id()
        svc = TransformationService()
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        with get_sync_session() as session:
            rows = svc.list_transformations(session, org_id)
            conns = conn_svc.list_connections(session, org_id)
            conn_names = {c.id: f"{c.name} ({c.connection_type.value})" for c in conns}

            self.transformations = [
                TransformationItem(
                    id=t.id,
                    name=t.name,
                    description=t.description or "",
                    materialization=t.materialization.value,
                    schema_name=t.schema_name,
                    tags=", ".join(t.tags) if t.tags else "",
                    connection_name=conn_names.get(t.destination_connection_id, "")
                    if t.destination_connection_id
                    else "",
                )
                for t in rows
            ]

            # Connection options (destinations only)
            self.dest_conn_options = [
                f"{c.id} — {c.name} ({c.connection_type.value})"
                for c in conns
                if c.connection_type.value in DESTINATION_TYPES
            ]

            # Schema options (unique existing + default + "Add new")
            schemas = {t.schema_name for t in rows}
            schemas.add("staging")
            self.schema_options = sorted(schemas) + ["+ Add new..."]

            # Catalog entries for ref autocomplete
            from datanika.services.catalog_service import CatalogService

            catalog_svc = CatalogService()
            entries = catalog_svc.list_entries(session, org_id)
            names = set()
            for e in entries:
                names.add(e.table_name)
                if e.dbt_config and e.dbt_config.get("alias"):
                    names.add(e.dbt_config["alias"])
            self.all_model_names = sorted(names)

        self.error_message = ""

    def _parse_connection_id(self) -> int | None:
        if not self.form_connection_option:
            return None
        try:
            return int(self.form_connection_option.split(" — ")[0])
        except (ValueError, IndexError):
            return None

    def _parse_tags(self) -> list[str]:
        return [t.strip() for t in self.form_tags.split(",") if t.strip()]

    def _build_incremental_config(self) -> dict | None:
        if self.form_materialization != "incremental":
            return None
        cfg: dict = {}
        if self.form_unique_key.strip():
            cfg["unique_key"] = self.form_unique_key.strip()
        if self.form_strategy.strip():
            cfg["strategy"] = self.form_strategy.strip()
        if self.form_updated_at.strip():
            cfg["updated_at"] = self.form_updated_at.strip()
        if self.form_on_schema_change and self.form_on_schema_change != "ignore":
            cfg["on_schema_change"] = self.form_on_schema_change
        return cfg or None

    def _populate_incremental_form(self, incremental_config: dict | None):
        if incremental_config:
            self.form_unique_key = incremental_config.get("unique_key", "")
            self.form_strategy = incremental_config.get("strategy", "")
            self.form_updated_at = incremental_config.get("updated_at", "")
            self.form_on_schema_change = incremental_config.get("on_schema_change", "ignore")
        else:
            self.form_unique_key = ""
            self.form_strategy = ""
            self.form_updated_at = ""
            self.form_on_schema_change = "ignore"

    async def save_transformation(self):
        org_id = await self._get_org_id()
        svc = TransformationService()
        conn_id = self._parse_connection_id()
        tags = self._parse_tags()
        inc_cfg = self._build_incremental_config()
        try:
            with get_sync_session() as session:
                if self.editing_transformation_id:
                    svc.update_transformation(
                        session,
                        org_id,
                        self.editing_transformation_id,
                        name=self.form_name,
                        sql_body=self.form_sql_body,
                        materialization=Materialization(self.form_materialization),
                        description=self.form_description or None,
                        schema_name=self.form_schema_name,
                        destination_connection_id=conn_id,
                        tags=tags,
                        incremental_config=inc_cfg,
                    )
                else:
                    svc.create_transformation(
                        session,
                        org_id,
                        self.form_name,
                        self.form_sql_body,
                        Materialization(self.form_materialization),
                        description=self.form_description or None,
                        schema_name=self.form_schema_name,
                        destination_connection_id=conn_id,
                        tags=tags,
                        incremental_config=inc_cfg,
                    )
                session.commit()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to save transformation")
            return
        self._reset_form()
        await self.load_transformations()

    def _find_conn_option(self, connection_id: int | None) -> str:
        if not connection_id:
            return ""
        prefix = f"{connection_id} — "
        return next((o for o in self.dest_conn_options if o.startswith(prefix)), "")

    async def edit_transformation(self, transformation_id: int):
        """Load a transformation into the form for editing."""
        org_id = await self._get_org_id()
        svc = TransformationService()
        with get_sync_session() as session:
            t = svc.get_transformation(session, org_id, transformation_id)
            if t is None:
                self.error_message = "Transformation not found"
                return
            self.form_name = t.name
            self.form_sql_body = t.sql_body
            self.form_materialization = t.materialization.value
            self.form_description = t.description or ""
            self.form_schema_name = t.schema_name
            self.form_connection_option = self._find_conn_option(t.destination_connection_id)
            self.form_tags = ", ".join(t.tags) if t.tags else ""
            self._populate_incremental_form(t.incremental_config)
        self.editing_transformation_id = transformation_id
        self.error_message = ""

    async def copy_transformation(self, transformation_id: int):
        """Load a transformation into the form as a new copy."""
        org_id = await self._get_org_id()
        svc = TransformationService()
        with get_sync_session() as session:
            t = svc.get_transformation(session, org_id, transformation_id)
            if t is None:
                self.error_message = "Transformation not found"
                return
            self.form_name = f"{t.name}_copy"
            self.form_sql_body = t.sql_body
            self.form_materialization = t.materialization.value
            self.form_description = t.description or ""
            self.form_schema_name = t.schema_name
            self.form_connection_option = self._find_conn_option(t.destination_connection_id)
            self.form_tags = ", ".join(t.tags) if t.tags else ""
            self._populate_incremental_form(t.incremental_config)
        self.editing_transformation_id = 0
        self.error_message = ""

    def cancel_edit(self):
        """Cancel editing and reset the form."""
        self._reset_form()

    async def delete_transformation(self, transformation_id: int):
        org_id = await self._get_org_id()
        svc = TransformationService()
        with get_sync_session() as session:
            svc.delete_transformation(session, org_id, transformation_id)
            session.commit()
        await self.load_transformations()

    async def run_tests(self, transformation_id: int):
        """Run dbt tests for a transformation."""
        from datanika.config import settings
        from datanika.services.dbt_project import DbtProjectService

        org_id = await self._get_org_id()
        svc = TransformationService()
        with get_sync_session() as session:
            t = svc.get_transformation(session, org_id, transformation_id)
            if t is None:
                self.test_result_message = "Transformation not found"
                return
            model_name = t.name
            tests_config = t.tests_config

        if not tests_config or not tests_config.get("columns"):
            self.test_result_message = "No tests configured for this transformation"
            return

        try:
            dbt_svc = DbtProjectService(settings.dbt_projects_dir)
            dbt_svc.write_tests_config(org_id, model_name, tests_config)
            result = dbt_svc.run_test(org_id, model_name)
            if result["success"]:
                self.test_result_message = f"Tests passed for {model_name}"
            else:
                self.test_result_message = f"Tests failed for {model_name}: {result['logs']}"
        except Exception as e:
            self.test_result_message = f"Error running tests: {self._safe_error(e)}"

    async def preview_compiled_sql(self, transformation_id: int):
        """Compile dbt model and show compiled SQL."""
        from datanika.config import settings
        from datanika.services.connection_service import ConnectionService
        from datanika.services.dbt_project import DbtProjectService
        from datanika.services.encryption import EncryptionService

        org_id = await self._get_org_id()
        svc = TransformationService()
        with get_sync_session() as session:
            t = svc.get_transformation(session, org_id, transformation_id)
            if t is None:
                self.preview_sql = "Transformation not found"
                return

            try:
                dbt_svc = DbtProjectService(settings.dbt_projects_dir)
                dbt_svc.ensure_project(org_id)

                # Generate profiles.yml so dbt can resolve the target
                if t.destination_connection_id:
                    encryption = EncryptionService(settings.credential_encryption_key)
                    conn_svc = ConnectionService(encryption)
                    conn = conn_svc.get_connection(
                        session, org_id, t.destination_connection_id
                    )
                    if conn:
                        decrypted = conn_svc.get_connection_config(
                            session, org_id, t.destination_connection_id
                        )
                        if decrypted:
                            dbt_svc.generate_profiles_yml(
                                org_id, conn.connection_type.value, decrypted
                            )

                # Write the model .sql so dbt can find it
                dbt_svc.write_model(
                    org_id,
                    t.name,
                    t.sql_body,
                    schema_name=t.schema_name,
                    materialization=t.materialization.value,
                    incremental_config=t.incremental_config,
                )

                result = dbt_svc.compile_model(org_id, t.name)
                if result["success"] and result["compiled_sql"]:
                    self.preview_sql = result["compiled_sql"]
                else:
                    logs = result["logs"] or "No logs available"
                    self.preview_sql = f"Compile failed: {logs}"
            except Exception as e:
                self.preview_sql = f"Error compiling: {self._safe_error(e)}"

    # -- Ref autocomplete --

    def _detect_ref(self, sql: str):
        """Check if SQL text contains an unclosed ref('... pattern and show suggestions."""
        match = _REF_PATTERN.search(sql)
        if match:
            partial = match.group(1).lower()
            self.ref_suggestions = [
                n for n in self.all_model_names if n.lower().startswith(partial)
            ][:20]
            self.ref_suggestion_index = 0 if self.ref_suggestions else -1
            self.ref_selected_name = self.ref_suggestions[0] if self.ref_suggestions else ""
            self.show_ref_popover = bool(self.ref_suggestions)
        else:
            self.show_ref_popover = False
            self.ref_suggestions = []
            self.ref_suggestion_index = -1
            self.ref_selected_name = ""

    def detect_ref_suggestions(self):
        """Called by JS after debounce delay. Detects ref pattern and shows popover."""
        if self.ref_dismissed:
            return
        self._detect_ref(self.form_sql_body)

    def ref_navigate_up(self):
        if not self.show_ref_popover or not self.ref_suggestions:
            return
        self.ref_suggestion_index = max(self.ref_suggestion_index - 1, 0)
        self.ref_selected_name = self.ref_suggestions[self.ref_suggestion_index]

    def ref_navigate_down(self):
        if not self.show_ref_popover or not self.ref_suggestions:
            return
        self.ref_suggestion_index = min(
            self.ref_suggestion_index + 1, len(self.ref_suggestions) - 1
        )
        self.ref_selected_name = self.ref_suggestions[self.ref_suggestion_index]

    def ref_select_current(self):
        if not self.show_ref_popover:
            return
        if 0 <= self.ref_suggestion_index < len(self.ref_suggestions):
            self._apply_ref_suggestion(self.ref_suggestions[self.ref_suggestion_index])

    def ref_dismiss(self):
        self.show_ref_popover = False
        self.ref_dismissed = True

    def select_ref_suggestion(self, name: str):
        """Click handler for a popover suggestion item."""
        self._apply_ref_suggestion(name)

    def _apply_ref_suggestion(self, name: str):
        """Replace the partial ref pattern with the complete ref call."""
        match = _REF_PATTERN.search(self.form_sql_body)
        if match:
            # match.start(1) is where the partial name begins (after the opening quote)
            # match.end() is end of the partial name
            before = self.form_sql_body[:match.start(1)]
            after = self.form_sql_body[match.end():]
            self.form_sql_body = before + name + "') }}" + after
        self.show_ref_popover = False
