"""Upload execution Celery tasks."""

import logging
import traceback
from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.catalog_entry import CatalogEntryType
from datanika.models.connection import Connection
from datanika.models.dependency import NodeType
from datanika.models.run import Run
from datanika.models.upload import Upload, UploadStatus
from datanika.services.catalog_service import CatalogService
from datanika.services.connection_service import _build_sa_url
from datanika.services.dbt_project import DbtProjectService
from datanika.services.dlt_runner import DltRunnerService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.naming import to_snake_case
from datanika.services.upload_service import to_dataset_name
from datanika.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)
execution_service = ExecutionService()


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
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session as SyncSession

        from datanika.config import settings

        engine = create_engine(settings.database_url_sync)
        session = SyncSession(engine)

    if encryption is None:
        from datanika.config import settings

        encryption = EncryptionService(settings.credential_encryption_key)

    try:
        execution_service.start_run(session, run_id)
        if own_session:
            session.commit()

        run = session.get(Run, run_id)
        upload = session.execute(
            select(Upload).where(Upload.id == run.target_id, Upload.org_id == org_id)
        ).scalar_one()

        src_conn = session.get(Connection, upload.source_connection_id)
        dst_conn = session.get(Connection, upload.destination_connection_id)

        src_config = encryption.decrypt(src_conn.config_encrypted)
        dst_config = encryption.decrypt(dst_conn.config_encrypted)

        # Extract uploaded file if present (for csv/json/parquet with file upload)
        uploaded_file_id = src_config.get("uploaded_file_id") or upload.dlt_config.get(
            "uploaded_file_id"
        )
        extracted_dir = None
        uploaded_file = None
        dlt_config = dict(upload.dlt_config)

        if uploaded_file_id:
            from datanika.config import settings as app_settings
            from datanika.models.uploaded_file import UploadedFile
            from datanika.services.file_upload_service import FileUploadService

            file_svc = FileUploadService(app_settings.file_uploads_dir)
            uploaded_file = session.get(UploadedFile, uploaded_file_id)
            if uploaded_file:
                extracted_dir = file_svc.extract_for_dlt(uploaded_file)
                dlt_config["bucket_url"] = extracted_dir

        try:
            runner = DltRunnerService()
            dataset_name = to_dataset_name(upload.name)
            result = runner.execute(
                pipeline_id=upload.id,
                source_type=src_conn.connection_type.value,
                source_config=src_config,
                destination_type=dst_conn.connection_type.value,
                destination_config=dst_config,
                dlt_config=dlt_config,
                dataset_name=dataset_name,
            )
            rows = result["rows_loaded"]
            logs = str(result["load_info"])
        finally:
            if extracted_dir and uploaded_file:
                file_svc.cleanup_extracted(uploaded_file)

        execution_service.complete_run(session, run_id, rows_loaded=rows, logs=logs)

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
        except Exception:
            logger.exception("Catalog sync failed (non-fatal)")

        from datanika.hooks import emit

        emit("run.upload_completed", org_id=org_id, table_count=table_count)

        upload.status = UploadStatus.ACTIVE
        session.flush()
        if own_session:
            session.commit()

    except Exception as exc:
        if own_session:
            session.rollback()
        execution_service.fail_run(
            session,
            run_id,
            error_message=str(exc),
            logs=traceback.format_exc(),
        )
        run_obj = session.get(Run, run_id)
        if run_obj:
            upload = session.execute(
                select(Upload).where(Upload.id == run_obj.target_id, Upload.org_id == org_id)
            ).scalar_one_or_none()
            if upload:
                upload.status = UploadStatus.ERROR
                session.flush()
        if own_session:
            session.commit()

    finally:
        if own_session:
            session.close()


@celery_app.task(bind=True, name="datanika.run_upload")
def run_upload_task(self, run_id: int, org_id: int, scheduled: bool = False):
    """Celery entry point for upload execution."""
    if scheduled:
        from datanika.tasks.dependency_helpers import check_deps_or_retry

        check_deps_or_retry(self, run_id, org_id, NodeType.UPLOAD)
    run_upload(run_id=run_id, org_id=org_id)
