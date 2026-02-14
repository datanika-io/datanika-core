"""Pipeline execution Celery tasks."""

import traceback

import dlt
from sqlalchemy import select
from sqlalchemy.orm import Session

from etlfabric.models.connection import Connection
from etlfabric.models.pipeline import Pipeline
from etlfabric.models.run import Run
from etlfabric.services.encryption import EncryptionService
from etlfabric.services.execution_service import ExecutionService
from etlfabric.tasks.celery_app import celery_app

execution_service = ExecutionService()


def run_pipeline(
    run_id: int,
    org_id: int,
    session: Session | None = None,
    encryption: EncryptionService | None = None,
) -> None:
    """Execute a dlt pipeline.

    When called from Celery, ``session`` and ``encryption`` are created
    internally.  Tests pass them directly — in that case, the caller
    manages transaction boundaries (no commit/rollback here).
    """
    own_session = session is None
    if own_session:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session as SyncSession

        from etlfabric.config import settings

        engine = create_engine(settings.database_url_sync)
        session = SyncSession(engine)

    if encryption is None:
        from etlfabric.config import settings

        encryption = EncryptionService(settings.credential_encryption_key)

    try:
        execution_service.start_run(session, run_id)
        if own_session:
            session.commit()

        run = session.get(Run, run_id)
        pipeline = session.execute(
            select(Pipeline).where(Pipeline.id == run.target_id, Pipeline.org_id == org_id)
        ).scalar_one()

        src_conn = session.get(Connection, pipeline.source_connection_id)
        dst_conn = session.get(Connection, pipeline.destination_connection_id)

        src_config = encryption.decrypt(src_conn.config_encrypted)
        dst_config = encryption.decrypt(dst_conn.config_encrypted)

        dlt_pipeline = dlt.pipeline(
            pipeline_name=f"pipeline_{pipeline.id}",
            destination=dst_config,
        )
        result = dlt_pipeline.run(src_config, **pipeline.dlt_config)

        rows = getattr(result, "loads_count", 0) or 0
        execution_service.complete_run(session, run_id, rows_loaded=rows, logs="")
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
        if own_session:
            session.commit()

    finally:
        if own_session:
            session.close()


@celery_app.task(bind=True, name="etlfabric.run_pipeline")
def run_pipeline_task(self, run_id: int, org_id: int):
    """Celery entry point for pipeline execution."""
    run_pipeline(run_id=run_id, org_id=org_id)


@celery_app.task(bind=True, name="etlfabric.run_transformation")
def run_transformation(self, transformation_id: str, org_id: str):
    """Execute a dbt transformation. Implementation in Phase 3."""
    raise NotImplementedError("Transformation execution will be implemented in Step 10")
