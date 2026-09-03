"""Maintenance service — cleanup orphaned files, old runs, stale artifacts."""

import logging
import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

from sqlalchemy import select, update
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


def purge_old_runs(session: Session, retention_days: int = 90) -> int:
    """Soft-delete Run records older than retention_days.

    Only deletes completed/failed runs, not running ones.
    Returns count of purged records.
    """
    # Use naive UTC datetime for SQLite compatibility in tests
    now = datetime.utcnow()
    cutoff = now - timedelta(days=retention_days)

    result = session.execute(
        update(Run)
        .where(
            Run.deleted_at.is_(None),
            Run.created_at < cutoff,
            Run.status.in_([RunStatus.SUCCESS, RunStatus.FAILED, RunStatus.CANCELLED]),
        )
        .values(deleted_at=now)
    )
    count = result.rowcount
    if count:
        logger.info("Purged %d old run records (older than %d days)", count, retention_days)
    return count


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
