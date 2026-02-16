"""Transformation state for Reflex UI."""

import json

from pydantic import BaseModel

from datanika.models.transformation import Materialization
from datanika.services.transformation_service import TransformationService
from datanika.ui.state.base_state import BaseState, get_sync_session


class TransformationItem(BaseModel):
    id: int = 0
    name: str = ""
    description: str = ""
    materialization: str = ""
    schema_name: str = ""


class TransformationState(BaseState):
    transformations: list[TransformationItem] = []
    form_name: str = ""
    form_sql_body: str = ""
    form_materialization: str = "view"
    form_description: str = ""
    form_schema_name: str = "staging"
    # Tests config (JSON string for column tests)
    form_tests_config: str = "{}"
    # Test results
    test_result_message: str = ""
    # SQL preview
    preview_sql: str = ""
    # 0 = creating new, >0 = editing existing transformation
    editing_transformation_id: int = 0

    def set_form_name(self, value: str):
        self.form_name = value

    def set_form_sql_body(self, value: str):
        self.form_sql_body = value

    def set_form_materialization(self, value: str):
        self.form_materialization = value

    def set_form_description(self, value: str):
        self.form_description = value

    def set_form_schema_name(self, value: str):
        self.form_schema_name = value

    def set_form_tests_config(self, value: str):
        self.form_tests_config = value

    def _reset_form(self):
        self.editing_transformation_id = 0
        self.form_name = ""
        self.form_sql_body = ""
        self.form_materialization = "view"
        self.form_description = ""
        self.form_schema_name = "staging"
        self.form_tests_config = "{}"
        self.error_message = ""

    async def load_transformations(self):
        org_id = await self._get_org_id()
        svc = TransformationService()
        with get_sync_session() as session:
            rows = svc.list_transformations(session, org_id)
            self.transformations = [
                TransformationItem(
                    id=t.id,
                    name=t.name,
                    description=t.description or "",
                    materialization=t.materialization.value,
                    schema_name=t.schema_name,
                )
                for t in rows
            ]
        self.error_message = ""

    async def save_transformation(self):
        org_id = await self._get_org_id()
        svc = TransformationService()
        try:
            tests_config = json.loads(self.form_tests_config)
        except json.JSONDecodeError:
            self.error_message = "Invalid JSON in tests config"
            return
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
                        tests_config=tests_config if tests_config else None,
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
                        tests_config=tests_config if tests_config else None,
                    )
                session.commit()
        except Exception as e:
            self.error_message = str(e)
            return
        self._reset_form()
        await self.load_transformations()

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
            self.form_tests_config = json.dumps(t.tests_config) if t.tests_config else "{}"
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
            self.form_name = f"{t.name} (copy)"
            self.form_sql_body = t.sql_body
            self.form_materialization = t.materialization.value
            self.form_description = t.description or ""
            self.form_schema_name = t.schema_name
            self.form_tests_config = json.dumps(t.tests_config) if t.tests_config else "{}"
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
            self.test_result_message = f"Error running tests: {e}"

    async def preview_compiled_sql(self, transformation_id: int):
        """Compile dbt model and show compiled SQL."""
        from datanika.config import settings
        from datanika.services.dbt_project import DbtProjectService

        org_id = await self._get_org_id()
        svc = TransformationService()
        with get_sync_session() as session:
            t = svc.get_transformation(session, org_id, transformation_id)
            if t is None:
                self.preview_sql = "Transformation not found"
                return
            model_name = t.name

        try:
            dbt_svc = DbtProjectService(settings.dbt_projects_dir)
            result = dbt_svc.compile_model(org_id, model_name)
            if result["success"] and result["compiled_sql"]:
                self.preview_sql = result["compiled_sql"]
            else:
                self.preview_sql = f"Compile failed: {result['logs']}"
        except Exception as e:
            self.preview_sql = f"Error compiling: {e}"
