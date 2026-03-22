"""Maintenance service — cleanup orphaned files, old runs, stale artifacts."""

import logging
import os
import shutil
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from datanika.models.run import Run, RunStatus
from datanika.models.uploaded_file import UploadedFile

logger = logging.getLogger(__name__)


def cleanup_orphaned_dlt_dirs(pipelines_dir: str, max_age_hours: int = 24) -> int:
    """Remove dlt pipeline working directories older than max_age_hours.

    Returns count of removed directories.
    """
    if not os.path.isdir(pipelines_dir):
        return 0

    cutoff = time.time() - (max_age_hours * 3600)
    removed = 0

    for entry in os.scandir(pipelines_dir):
        if entry.is_dir() and entry.stat().st_mtime < cutoff:
            shutil.rmtree(entry.path, ignore_errors=True)
            logger.info("Removed orphaned dlt dir: %s", entry.path)
            removed += 1

    return removed


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
    """
    deleted_files = session.execute(
        select(UploadedFile).where(UploadedFile.deleted_at.is_not(None))
    ).scalars().all()

    removed = 0
    for record in deleted_files:
        if record.archive_path and os.path.isfile(record.archive_path):
            os.remove(record.archive_path)
            logger.info("Removed orphaned archive: %s", record.archive_path)
            removed += 1

    return removed
