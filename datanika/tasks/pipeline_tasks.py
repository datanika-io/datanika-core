"""dbt pipeline execution Celery tasks."""

import logging
import traceback

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.connection import Connection
from datanika.models.pipeline import Pipeline
from datanika.models.run import Run
from datanika.services.dbt_project import DbtProjectService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.pipeline_service import PipelineService
from datanika.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)
execution_service = ExecutionService()


def run_pipeline(
    run_id: int,
    org_id: int,
    session: Session | None = None,
    encryption: EncryptionService | None = None,
) -> None:
    """Execute a dbt pipeline.

    When called from Celery, ``session`` and ``encryption`` are created
    internally.  Tests pass them directly.
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
        pipeline = session.execute(
            select(Pipeline).where(Pipeline.id == run.target_id, Pipeline.org_id == org_id)
        ).scalar_one()

        dst_conn = session.get(Connection, pipeline.destination_connection_id)
        dst_config = encryption.decrypt(dst_conn.config_encrypted)

        from datanika.config import settings

        dbt_svc = DbtProjectService(settings.dbt_projects_dir)
        dbt_svc.ensure_project(org_id)

        # Generate profiles.yml from destination connection
        dbt_svc.generate_profiles_yml(
            org_id, dst_conn.connection_type.value, dst_config,
        )

        # Build selector
        selector = PipelineService.build_selector(pipeline.models, pipeline.custom_selector)

        # Execute dbt command
        result = dbt_svc.run_command(
            org_id,
            pipeline.command.value,
            selector=selector,
            full_refresh=pipeline.full_refresh,
        )

        if result["success"]:
            execution_service.complete_run(
                session, run_id,
                rows_loaded=result["rows_affected"],
                logs=result["logs"],
            )
        else:
            execution_service.fail_run(
                session, run_id,
                error_message="dbt command failed",
                logs=result["logs"],
            )

        if own_session:
            session.commit()

    except Exception as exc:
        if own_session:
            session.rollback()
        execution_service.fail_run(
            session, run_id,
            error_message=str(exc),
            logs=traceback.format_exc(),
        )
        if own_session:
            session.commit()

    finally:
        if own_session:
            session.close()


@celery_app.task(bind=True, name="datanika.run_pipeline")
def run_pipeline_task(self, run_id: int, org_id: int):
    """Celery entry point for dbt pipeline execution."""
    run_pipeline(run_id=run_id, org_id=org_id)
