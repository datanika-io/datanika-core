"""Dedicated scheduler process — the single owner of APScheduler (core#648).

Why this file exists
--------------------
``GRANIAN_WORKERS: "4"`` runs 1 arbiter + 4 workers, and every one of them imported
``datanika.datanika``, which armed a ``BackgroundScheduler`` at module level. APScheduler
3.x claims due jobs with a plain ``SELECT`` — no ``FOR UPDATE``, no lease, no owner column —
so all five saw every due job and dispatched it. QA measured 5 of 5 processes dispatching on
29 of 29 firings, and the count scaled exactly with the instance count (1, 2, 5), which is
the signature of the lock-free read and of nothing else. ``max_instances`` and ``coalesce``
are per-process and do not cross that boundary.

So exactly one process owns the scheduler, and it is this one — its own container, like
``beat``, rather than a thread inside ``celery``. Same reasoning as core#653: a dispatch
thread dying inside a live worker leaves the container ``Up`` and ``healthy``, which is
observationally identical to the bug. As a container it has a ``container_last_seen`` series
and ``container-down`` covers it.

The consequence that had to be fixed in the same change
------------------------------------------------------
Moving the scheduler out of the web tier breaks the path that used to keep it current.
``ScheduleService`` called ``sync_schedule()`` on a scheduler in its own process; across a
process boundary that call has nowhere to land, and APScheduler offers no cross-process
notification. Measured on 3.11.2 against real Postgres: a job written by another instance,
due in 2 s, produced 0 dispatches at t+10 s, and one explicit ``wakeup()`` then ran it
immediately.

So the ``schedules`` table is the only channel, and this process polls it. The reconcile
heartbeat below does both jobs at once — it re-reads the table, and by existing in the job
set it bounds ``_process_jobs()``'s sleep, which is otherwise ``TIMEOUT_MAX`` (49.7 days).

⚠️ Never run two of these. The duplication this fixes is per *process*, not per service.
"""

from __future__ import annotations

import logging
import signal
import threading
from types import FrameType

from datanika.config import settings
from datanika.db import get_sync_session
from datanika.scheduler import scheduler_integration

logger = logging.getLogger(__name__)

#: Id of the process-local repeating reconcile. Lives in the memory jobstore, so it never
#: appears in ``apscheduler_jobs`` — that table's row count is a core#648 diagnostic.
RECONCILE_JOB_ID = "datanika_reconcile"

_stop = threading.Event()


def reconcile_once() -> dict[str, int]:
    """Re-read `schedules` and make the jobstore match it."""
    session = get_sync_session()
    try:
        result = scheduler_integration.reconcile(session)
    finally:
        session.close()
    logger.info(
        "Scheduler reconcile complete",
        extra={"synced": result["synced"], "removed": result["removed"]},
    )
    return result


def bootstrap() -> None:
    """Arm the scheduler, load the schedules, and set up the repeating reconcile."""
    scheduler_integration.start()
    session = get_sync_session()
    try:
        result = scheduler_integration.reconcile(session)
    finally:
        session.close()
    logger.info(
        "Scheduler process started",
        extra={"synced": result["synced"], "removed": result["removed"]},
    )
    scheduler_integration.schedule_internal(
        func=reconcile_once,
        seconds=settings.scheduler_reconcile_seconds,
        job_id=RECONCILE_JOB_ID,
    )


def _handle_signal(signum: int, _frame: FrameType | None) -> None:
    logger.info("Scheduler process stopping", extra={"signal": signum})
    _stop.set()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, _handle_signal)
    bootstrap()
    _stop.wait()
    scheduler_integration.shutdown()


if __name__ == "__main__":
    main()
