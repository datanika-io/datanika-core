"""Model catalog state for Reflex UI — list of all catalog entries."""

from collections import defaultdict

from pydantic import BaseModel

from datanika.config import settings
from datanika.models.dependency import NodeType
from datanika.models.run import CatalogSyncVerdict, RunStatus
from datanika.services.catalog_service import CatalogService
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.pipeline_service import PipelineService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService
from datanika.ui.state.base_state import BaseState, get_sync_session


class UncataloguedUpload(BaseModel):
    """An upload whose most recent successful run did not catalogue its tables (core#1398)."""

    upload_id: int = 0
    upload_name: str = ""
    run_id: int = 0
    #: `/runs?run=<id>` opens that run's log.
    run_href: str = ""
    #: `unreadable` or `no_tables`, a `CatalogSyncVerdict` value.
    verdict: str = ""
    #: Translated and filled in on the server: a Reflex Var cannot be formatted in the browser.
    message: str = ""


class ModelItem(BaseModel):
    id: int = 0
    entry_type: str = ""
    origin_name: str = ""
    table_name: str = ""
    schema_name: str = ""
    last_run_status: str = ""
    last_run_datetime: str = ""
    last_run_rows: int | None = None
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

    #: Each upload whose most recent SUCCESSFUL run did not catalogue its tables (core#1398,
    #: `SPEC_EARNED_VERDICTS` §4.6), however many other rows the catalogue lists.
    #: `loaded_without_catalog` answers only when the whole catalogue is empty, so one catalogued
    #: upload anywhere in the org used to hide every uncatalogued one.
    uncatalogued_uploads: list[UncataloguedUpload] = []

    async def _uncatalogued_notice(self, upload_name: str, run) -> UncataloguedUpload:
        """The notice for one upload, in the reader's locale, naming which thing happened.

        Only `no_tables` carries the deleted / renamed / misconfigured advice. When the sync
        raised, the connection is correct and the catalogue could not read the destination, and
        advice to change the connection is the same defect as no notice (`PRODUCT_RULES` §15a).
        """
        if run.catalog_sync_verdict == CatalogSyncVerdict.NO_TABLES:
            text = await self._translated(
                "models.catalog_no_tables",
                "“{upload}” loaded rows, but no tables were found in schema “{schema}”, so "
                "nothing from it is listed here. The schema may have been deleted, renamed, or "
                "set incorrectly on the destination connection.",
            )
        else:
            text = await self._translated(
                "models.catalog_unreadable",
                "“{upload}” loaded its data, but the catalog could not read the destination, "
                "so its tables are not listed here. The data is in the destination, and the "
                "connection does not need to change.",
            )
        message = text.replace("{upload}", upload_name).replace(
            "{schema}", run.catalog_sync_schema or ""
        )
        return UncataloguedUpload(
            upload_id=run.target_id,
            upload_name=upload_name,
            run_id=run.run_id,
            run_href=f"/runs?run={run.run_id}",
            verdict=str(run.catalog_sync_verdict),
            message=message,
        )

    async def load_models(self):
        from datanika.ui.state.auth_state import AuthState

        auth = await self.get_state(AuthState)
        org_id = auth.current_org.id or 0
        user_id = auth.current_user.id or 0
        if org_id == 0 or user_id == 0:
            return

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
                        last_run_rows=(last_run.rows_loaded if last_run else None),
                        column_count=len(entry.columns) if entry.columns else 0,
                    )
                )
            self.models = items

            # core#1398. The most recent successful run of each upload, and what its catalogue
            # sync found. A later failure loaded nothing new, so it neither raises nor clears a
            # notice. NULL is a run from before the verdict was recorded: nothing is known.
            uncatalogued = [
                (upload_names[latest.target_id], latest)
                for latest in exec_svc.latest_upload_catalog_verdicts(session, org_id)
                if latest.target_id in upload_names
                and latest.catalog_sync_verdict
                in (CatalogSyncVerdict.UNREADABLE, CatalogSyncVerdict.NO_TABLES)
            ]

            # Only asked when the catalog is empty, which is the only case that
            # consumes the answer — so the normal page load pays nothing.
            # Reset unconditionally: this state object is reused across loads,
            # and a stale True would keep the wrong copy on screen after the
            # user fixes the destination.
            self.loaded_without_catalog = False
            if not items:
                self.loaded_without_catalog = any(
                    # `is not None` first: a run whose count we could not read is not
                    # evidence that rows arrived, and `or 0` said it was (core#1170 AC3).
                    r.rows_loaded is not None and r.rows_loaded > 0
                    for r in exec_svc.list_runs(session, org_id, status=RunStatus.SUCCESS)
                )
        self.uncatalogued_uploads = [
            await self._uncatalogued_notice(name, run) for name, run in uncatalogued
        ]
        # Set AFTER both the rows and the empty-state diagnosis, so no render can
        # catch the flag True beside a half-computed answer.
        self.models_loaded = True
        self.error_message = ""
