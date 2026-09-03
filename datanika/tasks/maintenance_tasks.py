"""Celery tasks for scheduled maintenance — cleanup orphaned files and stale artifacts.

⚠️ **This sweep does not touch run history, and must not (core#1000).** See the module
docstring of ``datanika.services.maintenance_service`` for the decision and its published
consequences.
"""

import logging

from datanika.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


class MaintenanceError(RuntimeError):
    """Raised when the maintenance task's database block does not complete.

    Carries the counters gathered *before* the failure, so the Celery result backend
    records how far the sweep got as well as that it failed.

    Before core#709 this path returned a dict instead, and Celery recorded ``SUCCESS``.
    The dict's key count was the only thing that differed — and it did not differ
    reliably: measured against the code of the day, a failure returned 3 keys, 4 keys, or,
    when ``session.commit()`` was what threw, a full **5**, indistinguishable from a clean
    run. A commit failure is the position that matters most, because every sweep below
    buffers its DELETEs until then. (Those counts are of the five sweeps that existed then;
    core#1000 removed the run purge, so a clean run reports **four**. The argument does not
    depend on the number — do not re-derive it from today's code.)
    """


@celery_app.task(name="datanika.run_maintenance")
def run_maintenance_task() -> dict:
    """Hourly maintenance: clean orphaned dlt dirs, dbt artifacts, archives, spent tokens.

    ⚠️ Deliberately does **not** purge run history — core#1000. See the module docstring.
    """
    from datanika.config import settings
    from datanika.services.maintenance_service import (
        cleanup_dbt_targets,
        cleanup_orphaned_archives,
        cleanup_orphaned_dlt_dirs,
    )
    from datanika.ui.state.base_state import get_sync_session

    results = {}

    results["dbt_targets"] = cleanup_dbt_targets(
        settings.dbt_projects_dir, settings.maintenance_dbt_max_age_hours
    )

    try:
        with get_sync_session() as session:
            # Pass session so active runs are protected from cleanup
            results["dlt_dirs"] = cleanup_orphaned_dlt_dirs(
                settings.dlt_pipelines_dir,
                settings.maintenance_dlt_max_age_hours,
                session=session,
            )
            results["orphaned_archives"] = cleanup_orphaned_archives(
                session, settings.file_uploads_dir
            )
            # Spent reset tokens (core#623). They are only hashes of dead
            # capabilities, but they land in every nightly dump forever
            # otherwise. Rides the existing sweep rather than becoming new
            # infrastructure.
            from datanika.services.password_reset_service import PasswordResetService

            results["expired_reset_tokens"] = PasswordResetService.purge_expired(session)
            session.commit()
    except Exception as exc:
        # Report a failure as a failure (core#709). This handler used to log and then fall
        # through to the same "Maintenance complete" INFO line and the same dict return as a
        # healthy no-op, so Celery recorded SUCCESS and no observer could tell the two apart.
        #
        # The two zeroed counters that used to be assigned here are deliberately gone: a
        # sweep that never ran is not a sweep that found nothing, and a `0` read as the
        # latter. `results` now holds exactly the sweeps that did complete.
        logger.exception(
            "Maintenance FAILED: database cleanup did not complete. Sweeps that finished first: %s",
            results,
        )
        raise MaintenanceError(
            f"database cleanup did not complete ({type(exc).__name__}: {exc}); "
            f"counters gathered before the failure: {results}"
        ) from exc

    logger.info("Maintenance complete: %s", results)
    return results
