"""Model catalog state for Reflex UI — list of all catalog entries."""

from collections import defaultdict

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.dependency import NodeType
from datanika.models.run import RunStatus
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


def _pick_latest_run(*runs):
    """Return the most recent run from a list of candidates (ignoring None)."""
    best = None
    for r in runs:
        if r is None:
            continue
        if best is None or (
            r.finished_at and (best.finished_at is None or r.finished_at > best.finished_at)
        ):
            best = r
    return best


class ModelState(BaseState):
    models: list[ModelItem] = []

    #: False until ``load_models`` has answered once (core#872).
    #:
    #: ⚠️ This is the flag ``loaded_without_catalog`` below silently depended on.
    #: That one distinguishes two *honest* empties from each other; it cannot
    #: distinguish either from a table that has not loaded — and before this
    #: flag existed, `/models` rendered its "no models yet" callout on the very
    #: first paint, telling a user who has data, in words, that they have none.
    #: One production poll stayed empty for 30 s and that one was a **real**
    #: emptiness (core#869), which is the whole problem: until the not-yet-loaded
    #: case is separable, an honest empty cannot be read as honest.
    models_loaded: bool = False

    #: True when the catalog is empty **and** this org has already completed a
    #: load that reported rows (core#883).
    #:
    #: The generic empty-state copy — "Run an upload or transformation to
    #: populate the catalog" — is correct only for a user who has never loaded
    #: anything. After a green run with a row count it instructs the user to do
    #: the thing they just did, which is worse than saying nothing: the absent
    #: signal is replaced by a confident wrong one. The upload task now writes
    #: the diagnosis onto the run (see `tasks/upload_tasks.py`), and this flag
    #: is what points the user at it.
    loaded_without_catalog: bool = False

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
            trans_by_id = {t.id: t for t in transformations}
            pipelines = pipeline_svc.list_pipelines(session, org_id)
            pipeline_names = {p.id: p.name for p in pipelines}

            # Group pipelines by destination_connection_id for fast lookup
            pipelines_by_dest: dict[int, list] = defaultdict(list)
            for p in pipelines:
                pipelines_by_dest[p.destination_connection_id].append(p)

            # Query all pipeline runs and keep only the latest per pipeline
            all_pipeline_runs = exec_svc.list_runs(session, org_id, target_type=NodeType.PIPELINE)
            latest_pipeline_run: dict[int, object] = {}
            for r in all_pipeline_runs:
                prev = latest_pipeline_run.get(r.target_id)
                if prev is None or (
                    r.finished_at and (prev.finished_at is None or r.finished_at > prev.finished_at)
                ):
                    latest_pipeline_run[r.target_id] = r

            items = []
            for entry in entries:
                # Resolve origin name
                if entry.origin_type == NodeType.UPLOAD:
                    origin_name = upload_names.get(entry.origin_id, f"Upload #{entry.origin_id}")
                elif entry.origin_type == NodeType.PIPELINE:
                    origin_name = pipeline_names.get(
                        entry.origin_id, f"Pipeline #{entry.origin_id}"
                    )
                else:
                    origin_name = trans_names.get(
                        entry.origin_id, f"Transformation #{entry.origin_id}"
                    )

                # Get last run — for TRANSFORMATION entries, also check pipeline runs
                runs = exec_svc.list_runs(
                    session,
                    org_id,
                    target_type=entry.origin_type,
                    target_id=entry.origin_id,
                    limit=1,
                )
                last_run = runs[0] if runs else None

                if entry.origin_type == NodeType.TRANSFORMATION:
                    trans = trans_by_id.get(entry.origin_id)
                    if trans is not None:
                        dest_id = trans.destination_connection_id
                        # Find pipelines sharing the same destination (or all if
                        # the transformation inherits destination via NULL)
                        matching_pipelines = (
                            pipelines_by_dest.get(dest_id, []) if dest_id is not None else pipelines
                        )
                        for p in matching_pipelines:
                            pr = latest_pipeline_run.get(p.id)
                            last_run = _pick_latest_run(last_run, pr)

                items.append(
                    ModelItem(
                        id=entry.id,
                        entry_type=entry.entry_type.value,
                        origin_name=origin_name,
                        table_name=entry.table_name,
                        schema_name=entry.schema_name,
                        last_run_status=last_run.status.value if last_run else "",
                        last_run_datetime=(
                            str(last_run.finished_at) if last_run and last_run.finished_at else ""
                        ),
                        last_run_rows=last_run.rows_loaded or 0 if last_run else 0,
                        column_count=len(entry.columns) if entry.columns else 0,
                    )
                )
            self.models = items

            # Only asked when the catalog is empty, which is the only case that
            # consumes the answer — so the normal page load pays nothing.
            # Reset unconditionally: this state object is reused across loads,
            # and a stale True would keep the wrong copy on screen after the
            # user fixes the destination.
            self.loaded_without_catalog = False
            if not items:
                self.loaded_without_catalog = any(
                    (r.rows_loaded or 0) > 0
                    for r in exec_svc.list_runs(session, org_id, status=RunStatus.SUCCESS)
                )
        # Set AFTER both the rows and the empty-state diagnosis, so no render can
        # catch the flag True beside a half-computed answer.
        self.models_loaded = True
        self.error_message = ""
