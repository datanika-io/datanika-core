"""Upload execution Celery tasks."""

import logging
import traceback
from collections import defaultdict
from pathlib import PurePosixPath

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.catalog_entry import CatalogEntryType
from datanika.models.connection import Connection
from datanika.models.dependency import NodeType
from datanika.models.upload import Upload, UploadMode, UploadStatus
from datanika.services.catalog_service import CatalogService
from datanika.services.connection_service import _build_sa_url, get_org_connection
from datanika.services.dbt_project import DbtProjectService
from datanika.services.dlt_runner import DltRunnerService, destination_dataset_name
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService, get_org_run
from datanika.services.naming import to_snake_case
from datanika.services.upload_service import to_dataset_name
from datanika.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)
execution_service = ExecutionService()


def _extract_bytes_from_load_info(load_info) -> int | None:
    """Extract total bytes written from dlt LoadInfo per spec §5.5.

    Walks load_info.load_packages[*].completed_jobs[*].job_file_info and
    sums file_size. This is post-normalization bytes — captures JSON
    amplification honestly.
    """
    import contextlib
    import os

    if load_info is None:
        return None
    total = 0
    try:
        for pkg in load_info.load_packages:
            for job in pkg.jobs.get("completed_jobs", []):
                file_info = getattr(job, "file_path", None)
                if file_info:
                    with contextlib.suppress(OSError):
                        total += os.path.getsize(file_info)
    except Exception:
        logger.debug("Could not extract bytes from LoadInfo", exc_info=True)
        return None
    return total if total > 0 else None


def _sync_catalog_after_upload(
    session: Session,
    org_id: int,
    upload: Upload,
    dst_conn: Connection,
    dst_config: dict,
    dataset_name: str,
) -> int:
    """Sync catalog entries and write source YML after a successful upload run."""
    catalog_svc = CatalogService()
    sa_url = _build_sa_url(dst_config, dst_conn.connection_type)

    # Introspect destination tables in the dataset schema
    tables = catalog_svc.introspect_tables(sa_url, schema_name=dataset_name)

    for tbl in tables:
        catalog_svc.upsert_entry(
            session,
            org_id,
            entry_type=CatalogEntryType.SOURCE_TABLE,
            origin_type=NodeType.UPLOAD,
            origin_id=upload.id,
            table_name=tbl["table_name"],
            schema_name=dataset_name,
            dataset_name=dataset_name,
            columns=tbl["columns"],
            connection_id=dst_conn.id,
        )

    # Build source YML for the entire connection (all datasets)
    all_entries = catalog_svc.get_entries_by_connection(session, org_id, dst_conn.id)
    by_dataset: dict[str, list] = defaultdict(list)
    for entry in all_entries:
        by_dataset[entry.dataset_name].append(
            {
                "name": entry.table_name,
                "columns": entry.columns or [],
            }
        )

    sources = [
        {
            "name": ds_name,
            "schema": ds_name,
            "description": f"Data loaded by upload into {ds_name}",
            "tables": ds_tables,
        }
        for ds_name, ds_tables in sorted(by_dataset.items())
    ]

    from datanika.config import settings

    dbt_svc = DbtProjectService(settings.dbt_projects_dir)
    dbt_svc.ensure_project(org_id)
    conn_name_snake = to_snake_case(dst_conn.name)
    dbt_svc.write_source_yml_for_connection(org_id, conn_name_snake, sources)
    return len(tables)


def run_upload(
    run_id: int,
    org_id: int,
    session: Session | None = None,
    encryption: EncryptionService | None = None,
) -> None:
    """Execute a dlt upload.

    When called from Celery, ``session`` and ``encryption`` are created
    internally.  Tests pass them directly — in that case, the caller
    manages transaction boundaries (no commit/rollback here).
    """
    own_session = session is None
    if own_session:
        from datanika.db import get_sync_session

        session = get_sync_session()

    if encryption is None:
        from datanika.config import settings

        encryption = EncryptionService(settings.credential_encryption_key)

    try:
        # Check run quota before starting (cloud plugin may block).
        # Uploads are Path A with predicted_runs=1 — one submission
        # counts as one run for both gating and metering, per
        # datanika-cloud/docs/billing_contract.md.
        from datanika.hooks import emit

        emit("run.before_execute", session=session, org_id=org_id, predicted_runs=1)

        execution_service.start_run(session, org_id, run_id)
        if own_session:
            session.commit()

        run = get_org_run(session, org_id, run_id)
        upload = session.execute(
            select(Upload).where(Upload.id == run.target_id, Upload.org_id == org_id)
        ).scalar_one()

        src_conn = get_org_connection(session, org_id, upload.source_connection_id)
        dst_conn = get_org_connection(session, org_id, upload.destination_connection_id)
        if src_conn is None or dst_conn is None:
            raise ValueError(
                f"Upload {upload.id} references a connection that is not available to org {org_id}"
            )

        src_config = encryption.decrypt(src_conn.config_encrypted)
        dst_config = encryption.decrypt(dst_conn.config_encrypted)

        bytes_processed = None  # filled by either ETL or ELT path

        if upload.mode == UploadMode.ELT:
            # V2 P3 — ELT path: stream source → Arrow → raw schema
            from datanika.services.elt_runner import stream_to_raw
            from datanika.services.ir.schema import IR

            ir = IR.from_dict(upload.dlt_config.get("ir") or upload.dlt_config)
            stats = stream_to_raw(
                ir=ir,
                run_id=run_id,
                source_config=src_config,
                destination_config=dst_config,
                source_type=src_conn.connection_type.value,
                destination_type=dst_conn.connection_type.value,
            )
            rows = stats.rows
            bytes_processed = stats.bytes_out
            logs = (
                f"ELT stream complete: {stats.rows} rows, "
                f"{stats.bytes_out} bytes out, {stats.batches} batches, "
                f"{stats.duration_ms}ms"
            )
            dataset_name = ir.target.raw_schema
        else:
            # ETL path (existing behaviour)
            # Extract uploaded file if present (for csv/json/parquet)
            uploaded_file_id = src_config.get("uploaded_file_id") or upload.dlt_config.get(
                "uploaded_file_id"
            )
            extracted_dir = None
            uploaded_file = None
            dlt_config = dict(upload.dlt_config)

            if uploaded_file_id:
                from datanika.config import settings as app_settings
                from datanika.services.file_upload_service import (
                    FileUploadService,
                    get_org_uploaded_file,
                )

                file_svc = FileUploadService(app_settings.file_uploads_dir)
                # #732: the record names an archive path this task extracts
                # and reads, so a cross-org id reads another tenant's
                # uploaded data. The bare lookup also ignored `deleted_at`,
                # whose archive `cleanup_orphaned_archives` may have removed.
                uploaded_file = get_org_uploaded_file(session, upload.org_id, uploaded_file_id)
                if uploaded_file:
                    extracted_dir = file_svc.extract_for_dlt(uploaded_file)
                    dlt_config["bucket_url"] = extracted_dir
                    # Name the table after the file the user uploaded. This is
                    # the only layer that knows the original name — the runner
                    # sees a hash-named extract dir and a `*.csv` glob, so
                    # without this the advertised onboarding run lands in a
                    # table called `csv` (core#492).
                    if not dlt_config.get("table_name"):
                        stem = PurePosixPath(uploaded_file.original_name).stem
                        if stem:
                            dlt_config["table_name"] = stem

            try:
                from datanika.config import settings as app_cfg

                runner = DltRunnerService(pipelines_dir=app_cfg.dlt_pipelines_dir)
                # Where the rows land (core#610).
                #
                # The destination connection's own `dataset` (BigQuery) /
                # `schema` (Databricks, Snowflake) wins. The form marks that
                # field **required** and the user filled it in; promoting the
                # *upload name* into a physical location instead — which is all
                # this line used to do — ignored them silently and scattered one
                # dataset per pipeline across a warehouse they meant to be one
                # tidy `raw_data`.
                #
                # The upload name stays the fallback, and for postgres / mysql /
                # duckdb it is still the only name available: those destinations
                # have no such field on the connection.
                #
                # ⚠️ ONE variable, used for the run *and* for the catalog sync
                # below. If they ever disagreed, the catalog would introspect a
                # schema the rows are not in and come back empty — the data
                # invisible in Models/Catalog while the run reported success.
                # Pinned by test_the_catalog_is_synced_against_the_dataset_the_
                # rows_went_to, because sharing a local today is one refactor
                # away from not being true, and the failure is silent.
                dataset_name = destination_dataset_name(
                    dst_conn.connection_type.value, dst_config
                ) or to_dataset_name(upload.name)
                result = runner.execute(
                    pipeline_id=run_id,
                    source_type=src_conn.connection_type.value,
                    source_config=src_config,
                    destination_type=dst_conn.connection_type.value,
                    destination_config=dst_config,
                    dlt_config=dlt_config,
                    dataset_name=dataset_name,
                    run_id=run_id,
                )
                rows = result["rows_loaded"]
                logs = str(result["load_info"])
                # ETL bytes_processed from LoadInfo file sizes (spec §5.5)
                bytes_processed = _extract_bytes_from_load_info(result.get("load_info"))
            finally:
                if extracted_dir and uploaded_file:
                    file_svc.cleanup_extracted(uploaded_file)

        execution_service.complete_run(session, org_id, run_id, rows_loaded=rows, logs=logs)

        table_count = 1  # fallback
        try:
            table_count = _sync_catalog_after_upload(
                session,
                org_id,
                upload,
                dst_conn,
                dst_config,
                dataset_name,
            )
        except Exception as exc:
            logger.exception("Catalog sync failed (non-fatal)")
            # Non-fatal to the run, but not invisible: the load succeeded and
            # the data is there, while Models/Catalog will not show it. Saying
            # so on the run is the difference between a user filing a bug and
            # a user concluding the product does not work (core#494).
            execution_service.append_logs(
                session,
                org_id,
                run_id,
                "WARNING: the data loaded successfully, but the catalog sync failed, "
                "so these tables will not appear under Models/Catalog: "
                f"{exc.__class__.__name__}: {exc}",
            )
        else:
            # A sync that *succeeds* and finds nothing is the other half of
            # core#494, and it was still silent (core#883).
            #
            # `get_table_names(schema=<missing>)` returns `[]` rather than
            # raising on every dialect we support, so a destination dataset
            # that was deleted, renamed or mistyped introspects exactly like an
            # empty one. `_sync_catalog_after_upload` then writes no entries and
            # returns 0 *as a success value*, which nothing branches on — the
            # run goes green with a row count, `/models` stays empty, and the
            # `except` warning above never fires.
            #
            # Rows loaded with no tables found is a contradiction, and it is the
            # one condition that needs no knowledge of WHICH of those causes
            # applies. `rows == 0` with no tables is a legitimately empty load
            # and stays silent, which is what keeps this from being always-on.
            #
            # Diagnostics only: status, `rows_loaded` and `table_count` are
            # untouched. The load did succeed; the catalog is what is missing.
            if rows and table_count == 0:
                execution_service.append_logs(
                    session,
                    org_id,
                    run_id,
                    f"WARNING: the data loaded successfully ({rows} rows), but no tables "
                    f"were found in destination schema '{dataset_name}', so nothing will "
                    "appear under Models/Catalog. Check that the destination connection's "
                    "dataset/schema matches where the rows were written, and that it still "
                    "exists.",
                )

        upload.status = UploadStatus.ACTIVE
        session.flush()
        if own_session:
            session.commit()

    except Exception as exc:
        if own_session:
            session.rollback()
        execution_service.fail_run(
            session,
            org_id,
            run_id,
            error_message=str(exc),
            logs=traceback.format_exc(),
        )
        run_obj = get_org_run(session, org_id, run_id)
        if run_obj:
            upload = session.execute(
                select(Upload).where(Upload.id == run_obj.target_id, Upload.org_id == org_id)
            ).scalar_one_or_none()
            if upload:
                upload.status = UploadStatus.ERROR
                session.flush()
        if own_session:
            session.commit()

    else:
        # Metering lives in `else`, not at the end of `try` (core#522).
        #
        # It used to fire before `flush`/`commit`, so a commit failure after a
        # successful load ended the run FAILED having *already been billed* —
        # and cloud's handlers meter on their own session and commit
        # independently, so the rollback did not take the ledger row with it.
        # `else` runs only when the whole `try` completed, which makes the
        # ordering structural: no future statement added inside `try` can slip
        # in front of it, and anything that raises there skips it.
        #
        # It stays ahead of `finally`, which closes the session the handlers
        # are handed.
        #
        # Trade-off, taken deliberately: if the worker dies between the commit
        # and here, the run is under-metered rather than over-metered. That is
        # the right direction to fail — and the comment below was only ever
        # strictly true at this point.
        from datanika.hooks import announce

        # `announce`, not `emit`: the run is already complete, so no subscriber
        # may veto it or starve the ones behind it (core#456). session/run_id/
        # status are what the notification handlers need to say *which* run
        # succeeded — without them the feature is alive but says nothing.
        announce(
            "run.upload_completed",
            session=session,
            org_id=org_id,
            run_id=run_id,
            status="success",
            target_type="upload",
            target_id=upload.id,
            table_count=table_count,
            bytes_processed=bytes_processed,
        )

    finally:
        # Clean up dlt working directory regardless of success/failure
        try:
            from datanika.config import settings as _cleanup_cfg

            DltRunnerService(pipelines_dir=_cleanup_cfg.dlt_pipelines_dir).cleanup_pipeline(
                pipeline_id=run_id, run_id=run_id
            )
        except Exception:
            # Cleanup failure must not turn a successful upload into a failed
            # task. It does, however, leak a working directory per run, and the
            # hourly `cleanup_orphaned_dlt_dirs` sweep then quietly absorbs the
            # symptom -- so without this line a systematic cleanup failure looks
            # like normal disk growth (core#723).
            logger.exception("dlt working directory not cleaned up: run_id=%s", run_id)
        if own_session:
            session.close()


@celery_app.task(bind=True, name="datanika.run_upload", max_retries=60)
def run_upload_task(self, run_id: int, org_id: int, scheduled: bool = False):
    """Celery entry point for upload execution."""
    if scheduled:
        from datanika.tasks.dependency_helpers import check_deps_or_retry

        check_deps_or_retry(self, run_id, org_id, NodeType.UPLOAD)

    from datanika.services.concurrency_service import acquire, release

    if not acquire(org_id):
        raise self.retry(countdown=30, max_retries=60)
    try:
        run_upload(run_id=run_id, org_id=org_id)
    finally:
        release(org_id)
