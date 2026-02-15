"""Transformation state for Reflex UI."""

import json

import reflex as rx

from etlfabric.models.transformation import Materialization
from etlfabric.services.transformation_service import TransformationService
from etlfabric.ui.state.base_state import BaseState, get_sync_session


class TransformationItem(rx.Base):
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

    def load_transformations(self):
        svc = TransformationService()
        with get_sync_session() as session:
            rows = svc.list_transformations(session, self.org_id)
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

    def create_transformation(self):
        svc = TransformationService()
        try:
            tests_config = json.loads(self.form_tests_config)
        except json.JSONDecodeError:
            self.error_message = "Invalid JSON in tests config"
            return
        try:
            with get_sync_session() as session:
                svc.create_transformation(
                    session,
                    self.org_id,
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
        self.form_name = ""
        self.form_sql_body = ""
        self.form_description = ""
        self.form_schema_name = "staging"
        self.form_tests_config = "{}"
        self.error_message = ""
        self.load_transformations()

    def delete_transformation(self, transformation_id: int):
        svc = TransformationService()
        with get_sync_session() as session:
            svc.delete_transformation(session, self.org_id, transformation_id)
            session.commit()
        self.load_transformations()

    def run_tests(self, transformation_id: int):
        """Run dbt tests for a transformation."""
        from etlfabric.config import settings
        from etlfabric.services.dbt_project import DbtProjectService

        svc = TransformationService()
        with get_sync_session() as session:
            t = svc.get_transformation(session, self.org_id, transformation_id)
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
            dbt_svc.write_tests_config(self.org_id, model_name, tests_config)
            result = dbt_svc.run_test(self.org_id, model_name)
            if result["success"]:
                self.test_result_message = f"Tests passed for {model_name}"
            else:
                self.test_result_message = f"Tests failed for {model_name}: {result['logs']}"
        except Exception as e:
            self.test_result_message = f"Error running tests: {e}"

    def preview_compiled_sql(self, transformation_id: int):
        """Compile dbt model and show compiled SQL."""
        from etlfabric.config import settings
        from etlfabric.services.dbt_project import DbtProjectService

        svc = TransformationService()
        with get_sync_session() as session:
            t = svc.get_transformation(session, self.org_id, transformation_id)
            if t is None:
                self.preview_sql = "Transformation not found"
                return
            model_name = t.name

        try:
            dbt_svc = DbtProjectService(settings.dbt_projects_dir)
            result = dbt_svc.compile_model(self.org_id, model_name)
            if result["success"] and result["compiled_sql"]:
                self.preview_sql = result["compiled_sql"]
            else:
                self.preview_sql = f"Compile failed: {result['logs']}"
        except Exception as e:
            self.preview_sql = f"Error compiling: {e}"
