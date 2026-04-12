"""Async run-completion waiter for the ``?wait=true`` trigger mode.

When an agent sends ``POST /uploads/{id}/run?wait=true&timeout=120``,
the endpoint dispatches the Celery task as usual, then calls
``wait_for_run()`` which polls the run's DB status until it leaves
``PENDING`` / ``RUNNING`` or the timeout expires.

Polling, not blocking — does NOT hold a Celery worker slot.
"""

from __future__ import annotations

import asyncio
import logging

from datanika.db import get_sync_session
from datanika.models.run import Run, RunStatus

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 120
MAX_TIMEOUT = 300
POLL_INTERVAL = 2.0

_TERMINAL = frozenset({RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELLED})


async def wait_for_run(
    run_id: int,
    org_id: int,
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> Run | None:
    """Poll until the run reaches a terminal status or the timeout expires.

    Returns the final ``Run`` object, or ``None`` if the run wasn't
    found (shouldn't happen — caller just created it). Raises no
    exceptions; callers map the result to an HTTP response.
    """
    clamped = max(1, min(int(timeout), MAX_TIMEOUT))
    elapsed = 0.0

    while elapsed < clamped:
        await asyncio.sleep(POLL_INTERVAL)
        elapsed += POLL_INTERVAL

        with get_sync_session() as session:
            run = session.get(Run, run_id)
            if run is None:
                return None
            if run.org_id != org_id:
                return None
            if run.status in _TERMINAL:
                # Detach from session so the caller can serialize it
                session.expunge(run)
                return run

    # Timeout — return the run in whatever state it's in now.
    with get_sync_session() as session:
        run = session.get(Run, run_id)
        if run is not None:
            session.expunge(run)
        return run
