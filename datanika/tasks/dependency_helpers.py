"""Dependency check helpers for Celery tasks."""

import logging

from celery.exceptions import MaxRetriesExceededError

from datanika.models.dependency import NodeType
from datanika.models.run import Run
from datanika.services.dependency_check import check_upstream_dependencies
from datanika.services.execution_service import ExecutionService

logger = logging.getLogger(__name__)

DEPENDENCY_RETRY_COUNTDOWN = 60
DEPENDENCY_MAX_RETRIES = 5


def check_deps_or_retry(
    task,
    run_id: int,
    org_id: int,
    node_type: NodeType,
    session=None,
) -> None:
    """Check upstream dependencies; retry the Celery task if unsatisfied.

    When called from Celery, ``session`` is created internally.
    Tests pass it directly.

    If all dependencies are satisfied, returns normally (caller proceeds).
    If unsatisfied, calls ``task.retry()`` which raises ``Retry`` and
    re-enqueues the task.
    On ``MaxRetriesExceededError``, fails the run and re-raises.
    """
    own_session = session is None
    if own_session:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session as SyncSession

        from datanika.config import settings

        engine = create_engine(settings.database_url_sync)
        session = SyncSession(engine)

    try:
        run = session.get(Run, run_id)
        if run is None:
            return

        result = check_upstream_dependencies(
            session, org_id, node_type, run.target_id
        )

        if result.satisfied:
            return

        unsatisfied_str = ", ".join(result.unsatisfied_nodes)
        logger.info(
            "Run %d: upstream dependencies not satisfied (%s), retrying...",
            run_id,
            unsatisfied_str,
        )

        try:
            raise task.retry(
                countdown=DEPENDENCY_RETRY_COUNTDOWN,
                max_retries=DEPENDENCY_MAX_RETRIES,
            )
        except MaxRetriesExceededError:
            error_msg = (
                f"Upstream dependencies not satisfied after "
                f"{DEPENDENCY_MAX_RETRIES} retries: {unsatisfied_str}"
            )
            exec_svc = ExecutionService()
            exec_svc.fail_run(session, run_id, error_message=error_msg, logs=error_msg)
            if own_session:
                session.commit()
            raise

    finally:
        if own_session:
            session.close()
