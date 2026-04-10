"""Celery tasks for scheduled maintenance — cleanup orphaned files, old runs."""

import logging

from datanika.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(name="datanika.run_maintenance")
def run_maintenance_task() -> dict:
    """Hourly maintenance: clean orphaned dlt dirs, dbt artifacts, old runs, archives."""
    from datanika.config import settings
    from datanika.services.maintenance_service import (
        cleanup_dbt_targets,
        cleanup_orphaned_archives,
        cleanup_orphaned_dlt_dirs,
        purge_old_runs,
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
            results["purged_runs"] = purge_old_runs(
                session, settings.maintenance_run_retention_days
            )
            results["orphaned_archives"] = cleanup_orphaned_archives(
                session, settings.file_uploads_dir
            )
            session.commit()
    except Exception:
        logger.exception("Maintenance DB cleanup failed")
        results["purged_runs"] = 0
        results["orphaned_archives"] = 0

    logger.info("Maintenance complete: %s", results)
    return results
