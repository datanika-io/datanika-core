"""Maintenance service — cleanup orphaned files and stale artifacts.

⚠️ **There is deliberately no run-retention sweep here (core#1000).** ``purge_old_runs``
soft-deleted completed runs past 90 days, and **no reader could observe the mark**: this
codebase has no global soft-delete filter, and neither ``ExecutionService.list_runs`` /
``get_org_run`` nor ``dependency_check`` carries a ``deleted_at`` predicate. The one place
that does read ``Run.deleted_at`` is ``cleanup_orphaned_dlt_dirs`` below, and it selects
RUNNING/PENDING runs while the purge only ever marked SUCCESS/FAILED/CANCELLED — so the two
never intersected. The sweep therefore hid nothing and removed nothing, while logging a purge
count hourly from 2026-08-30 (core#653) — retention that read as enforced and was not.

Founder decision, 2026-09-03: **the published pages are right and the sweep was wrong.**
``datanika.io/privacy`` and ``/trust`` state that run history, run logs and configuration
metadata are retained *"for as long as the organization exists"*. Re-introducing a
time-based purge — here or anywhere — is a change to those pages first and to this file
second. Guarded by ``tests/test_tasks/test_maintenance_tasks.py::TestRunHistoryIsNeverPurged``.
"""

import logging
import os
import shutil
import time
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.run import Run, RunStatus
from datanika.models.uploaded_file import UploadedFile
from datanika.services.file_upload_service import resolve_archive_path

logger = logging.getLogger(__name__)


def cleanup_orphaned_dlt_dirs(
    pipelines_dir: str,
    max_age_hours: int = 24,
    session: Session | None = None,
) -> int:
    """Remove dlt pipeline working directories older than max_age_hours.

    When a DB session is provided, skips directories whose run is still
    RUNNING or PENDING — prevents deleting files mid-upload.

    Returns count of removed directories.
    """
    if not os.path.isdir(pipelines_dir):
        return 0

    # Collect active run IDs to protect their dirs
    active_run_ids: set[int] = set()
    if session is not None:
        active_runs = (
            session.execute(
                select(Run.id).where(
                    Run.status.in_([RunStatus.RUNNING, RunStatus.PENDING]),
                    Run.deleted_at.is_(None),
                )
            )
            .scalars()
            .all()
        )
        active_run_ids = set(active_runs)

    cutoff = time.time() - (max_age_hours * 3600)
    removed = 0

    for entry in os.scandir(pipelines_dir):
        if not entry.is_dir():
            continue
        if entry.stat().st_mtime >= cutoff:
            continue
        # Extract run_id from dir name: pipeline_{id}_run_{run_id}
        run_id = _extract_run_id(entry.name)
        if run_id is not None and run_id in active_run_ids:
            logger.debug("Skipping dlt dir (run still active): %s", entry.path)
            continue
        shutil.rmtree(entry.path, ignore_errors=True)
        logger.info("Removed orphaned dlt dir: %s", entry.path)
        removed += 1

    return removed


def _extract_run_id(dirname: str) -> int | None:
    """Extract run_id from a dlt pipeline directory name like 'pipeline_5_run_42'."""
    parts = dirname.rsplit("_run_", 1)
    if len(parts) == 2:
        try:
            return int(parts[1])
        except ValueError:
            pass
    return None


def cleanup_dbt_targets(projects_dir: str, max_age_hours: int = 48) -> int:
    """Remove dbt target/ directories older than max_age_hours.

    Returns count of removed directories.
    """
    if not os.path.isdir(projects_dir):
        return 0

    cutoff = time.time() - (max_age_hours * 3600)
    removed = 0

    for tenant_dir in Path(projects_dir).glob("tenant_*/target"):
        if tenant_dir.is_dir() and tenant_dir.stat().st_mtime < cutoff:
            shutil.rmtree(tenant_dir, ignore_errors=True)
            logger.info("Removed stale dbt target: %s", tenant_dir)
            removed += 1

    return removed


def cleanup_orphaned_archives(session: Session, uploads_dir: str) -> int:
    """Remove archive files for soft-deleted UploadedFile records.

    Returns count of removed files.

    ``uploads_dir`` was accepted and never read until core#712. That mattered:
    rows written with the default relative ``FILE_UPLOADS_DIR`` name a location
    relative to the *web tier's* working directory, while this sweep runs in
    ``beat``. ``os.path.isfile`` answered False for every one of them, the loop
    skipped, and the function returned ``0`` — indistinguishable from a volume
    with nothing to reclaim. A deleted upload's bytes stayed on disk for the life
    of the volume, on a box whose disk pressure has its own prune script.
    Resolving against ``uploads_dir`` fixes the existing rows in place, so no
    data migration is needed.
    """
    deleted_files = (
        session.execute(select(UploadedFile).where(UploadedFile.deleted_at.is_not(None)))
        .scalars()
        .all()
    )

    removed = 0
    for record in deleted_files:
        if not record.archive_path:
            continue
        archive_path = resolve_archive_path(record.archive_path, uploads_dir)
        if os.path.isfile(archive_path):
            os.remove(archive_path)
            logger.info("Removed orphaned archive: %s", archive_path)
            removed += 1

    return removed
