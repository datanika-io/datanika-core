"""Model catalog state for Reflex UI — list of all catalog entries."""

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.dependency import NodeType
from datanika.services.catalog_service import CatalogService
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.pipeline_service import PipelineService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService
from datanika.ui.state.base_state import BaseState, get_sync_session


class ModelItem(BaseModel):
    id: int = 0
    entry_type: str = ""
    origin_name: str = ""
    table_name: str = ""
    schema_name: str = ""
    last_run_status: str = ""
    last_run_datetime: str = ""
    last_run_rows: int = 0
    column_count: int = 0


class ModelState(BaseState):
    models: list[ModelItem] = []

    async def load_models(self):
        org_id = await self._get_org_id()
        catalog_svc = CatalogService()
        exec_svc = ExecutionService()
        encryption = EncryptionService(settings.credential_encryption_key)
        conn_svc = ConnectionService(encryption)
        upload_svc = UploadService(conn_svc)
        transform_svc = TransformationService()
        pipeline_svc = PipelineService()

        with get_sync_session() as session:
            entries = catalog_svc.list_entries(session, org_id)

            # Build name maps
            uploads = upload_svc.list_uploads(session, org_id)
            upload_names = {u.id: u.name for u in uploads}
            transformations = transform_svc.list_transformations(session, org_id)
            trans_names = {t.id: t.name for t in transformations}
            pipelines = pipeline_svc.list_pipelines(session, org_id)
            pipeline_names = {p.id: p.name for p in pipelines}

            items = []
            for entry in entries:
                # Resolve origin name
                if entry.origin_type == NodeType.UPLOAD:
                    origin_name = upload_names.get(
                        entry.origin_id, f"Upload #{entry.origin_id}"
                    )
                elif entry.origin_type == NodeType.PIPELINE:
                    origin_name = pipeline_names.get(
                        entry.origin_id, f"Pipeline #{entry.origin_id}"
                    )
                else:
                    origin_name = trans_names.get(
                        entry.origin_id, f"Transformation #{entry.origin_id}"
                    )

                # Get last run
                target_type = entry.origin_type
                runs = exec_svc.list_runs(
                    session, org_id,
                    target_type=target_type,
                    target_id=entry.origin_id,
                    limit=1,
                )
                last_run = runs[0] if runs else None

                items.append(
                    ModelItem(
                        id=entry.id,
                        entry_type=entry.entry_type.value,
                        origin_name=origin_name,
                        table_name=entry.table_name,
                        schema_name=entry.schema_name,
                        last_run_status=last_run.status.value if last_run else "",
                        last_run_datetime=(
                            str(last_run.finished_at)
                            if last_run and last_run.finished_at
                            else ""
                        ),
                        last_run_rows=last_run.rows_loaded or 0 if last_run else 0,
                        column_count=len(entry.columns) if entry.columns else 0,
                    )
                )
            self.models = items
        self.error_message = ""
