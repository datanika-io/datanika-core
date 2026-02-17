"""Model detail state for Reflex UI — view/edit a single catalog entry."""

import json

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
from datanika.ui.state.base_state import BaseState, get_sync_session


class ColumnItem(BaseModel):
    name: str = ""
    data_type: str = ""
    description: str = ""
    tests: list = []


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

    async def load_model_detail(self):
        raw_id = self.router.page.params.get("id", "0")
        try:
            self.entry_id = int(raw_id)
        except (ValueError, TypeError):
            self.error_message = "Invalid model ID"
            return

        org_id = await self._get_org_id()
        catalog_svc = CatalogService()
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        pipe_svc = PipelineService(conn_svc)
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
            self.form_dbt_config = json.dumps(entry.dbt_config or {}, indent=2)

            # Resolve origin name
            if entry.origin_type == NodeType.PIPELINE:
                pipelines = pipe_svc.list_pipelines(session, org_id)
                names = {p.id: p.name for p in pipelines}
                self.origin_name = names.get(entry.origin_id, f"Pipeline #{entry.origin_id}")
            else:
                transformations = transform_svc.list_transformations(session, org_id)
                names = {t.id: t.name for t in transformations}
                self.origin_name = names.get(
                    entry.origin_id, f"Transformation #{entry.origin_id}"
                )

            # Populate columns
            self.columns = [
                ColumnItem(
                    name=c.get("name", ""),
                    data_type=c.get("data_type", ""),
                    description=c.get("description", ""),
                    tests=c.get("tests", []),
                )
                for c in (entry.columns or [])
            ]
        self.error_message = ""

    def set_form_description(self, value: str):
        self.form_description = value

    def set_form_dbt_config(self, value: str):
        self.form_dbt_config = value

    def set_column_description(self, index: int, value: str):
        if 0 <= index < len(self.columns):
            updated = self.columns[index].model_copy(update={"description": value})
            self.columns[index] = updated

    async def save_model_detail(self):
        org_id = await self._get_org_id()

        try:
            dbt_config = json.loads(self.form_dbt_config) if self.form_dbt_config.strip() else {}
        except json.JSONDecodeError as e:
            self.error_message = f"Invalid dbt config JSON: {e}"
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
                session, org_id, self.entry_id,
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
                        session, org_id, entry.connection_id,
                    )
                    by_dataset: dict[str, list] = defaultdict(list)
                    for e in all_entries:
                        by_dataset[e.dataset_name].append({
                            "name": e.table_name,
                            "columns": e.columns or [],
                        })
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
                pass  # YML regeneration is best-effort

            session.commit()
        self.error_message = ""
