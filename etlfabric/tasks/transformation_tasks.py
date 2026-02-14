"""Transformation execution Celery tasks (mocked dbt)."""

import traceback

from sqlalchemy import select
from sqlalchemy.orm import Session

from etlfabric.models.run import Run
from etlfabric.models.transformation import Transformation
from etlfabric.services.execution_service import ExecutionService
from etlfabric.tasks.celery_app import celery_app

execution_service = ExecutionService()


def _execute_dbt(transformation: Transformation) -> int:
    """Placeholder for real dbt execution — returns mocked row count."""
    return 0


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

        from etlfabric.config import settings

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

        rows = _execute_dbt(transformation)

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


@celery_app.task(bind=True, name="etlfabric.run_transformation")
def run_transformation_task(self, run_id: int, org_id: int):
    """Celery entry point for transformation execution."""
    run_transformation(run_id=run_id, org_id=org_id)
