"""Transformation execution Celery tasks."""

import traceback

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.run import Run
from datanika.models.transformation import Transformation
from datanika.services.dbt_project import DbtProjectService
from datanika.services.execution_service import ExecutionService
from datanika.tasks.celery_app import celery_app

execution_service = ExecutionService()


def run_transformation(
    run_id: int,
    org_id: int,
    session: Session | None = None,
) -> None:
    """Execute a dbt transformation.

    When called from Celery, ``session`` is created internally.
    Tests pass it directly — in that case, the caller manages transaction
    boundaries (no commit/rollback here).
    """
    own_session = session is None
    if own_session:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session as SyncSession

        from datanika.config import settings

        engine = create_engine(settings.database_url_sync)
        session = SyncSession(engine)

    try:
        execution_service.start_run(session, run_id)
        if own_session:
            session.commit()

        run = session.get(Run, run_id)
        transformation = session.execute(
            select(Transformation).where(
                Transformation.id == run.target_id,
                Transformation.org_id == org_id,
            )
        ).scalar_one_or_none()

        if transformation is None:
            raise ValueError(
                f"Transformation not found: target_id={run.target_id}, org_id={org_id}"
            )

        from datanika.config import settings

        dbt_svc = DbtProjectService(settings.dbt_projects_dir)
        dbt_svc.ensure_project(org_id)
        dbt_svc.write_model(
            org_id,
            transformation.name,
            transformation.sql_body,
            schema_name=transformation.schema_name,
            materialization=transformation.materialization.value,
        )
        result = dbt_svc.run_model(org_id, transformation.name)
        rows = result["rows_affected"]
        logs = result["logs"]

        execution_service.complete_run(session, run_id, rows_loaded=rows, logs=logs)
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


@celery_app.task(bind=True, name="datanika.run_transformation")
def run_transformation_task(self, run_id: int, org_id: int):
    """Celery entry point for transformation execution."""
    run_transformation(run_id=run_id, org_id=org_id)
