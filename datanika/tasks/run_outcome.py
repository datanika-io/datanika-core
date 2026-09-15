"""Make a run task's Celery verdict agree with the run it executed (core#1352).

``run_upload``, ``run_pipeline`` and ``run_transformation`` catch every exception, record the
failure on the run row, commit and return normally. That contract is deliberate: tests and the
hooks integration call them directly. But their Celery wrappers then returned normally too, so
Celery recorded **SUCCESS** for a run that ended **FAILED**. QA measured it on ``master``: the
worker logged ``succeeded in 31.91s: None``, and ``celery_task_failed_total`` stayed at 0. So the
worker log, the event stream, the exporter and the ``celery-task-failures`` rule were all told the
opposite of the run's own status.
"""

from __future__ import annotations

from datanika.models.run import RunStatus
from datanika.services.execution_service import get_org_run


class RunFailedError(RuntimeError):
    """A run task's Celery wrapper executed a run that ended FAILED.

    The message names the run and its kind, never the run's error text. That text is
    user-controlled, can carry a connection string, and already lives on the run row.

    ⚠️ ``celery-exporter`` labels failures by exception class, so this name is what tells a failed
    RUN apart from a task that crashed (see core#1356, which rewrites the rule that counts them).
    Do not rename it without telling whoever owns that rule.
    """


def raise_if_run_failed(org_id: int, run_id: int, kind: str) -> None:
    """Read the run's terminal status in a fresh session, and raise if it is FAILED.

    Called by each wrapper after the inner function has committed and returned, and after the
    concurrency slot has been released, so neither the row nor the slot depends on the raise.

    No run task declares ``autoretry_for``, so this ends the task rather than re-running it
    (core#1352 AC4). A run that ended CANCELLED or SUCCESS does not raise.
    """
    from datanika.db import get_sync_session

    session = get_sync_session()
    try:
        run = get_org_run(session, org_id, run_id)
        status = run.status if run is not None else None
    finally:
        session.close()
    if status == RunStatus.FAILED:
        raise RunFailedError(f"{kind} run {run_id} ended FAILED; its error is on the run row")
