"""TDD tests for maintenance service — cleanup orphaned files, old runs, stale artifacts."""

import os
import time
import uuid
from datetime import datetime, timedelta

import pytest

from datanika.models.run import Run, RunStatus
from datanika.models.uploaded_file import UploadedFile
from datanika.models.user import Organization
from datanika.services.maintenance_service import (
    cleanup_dbt_targets,
    cleanup_orphaned_archives,
    cleanup_orphaned_dlt_dirs,
    purge_old_runs,
)


@pytest.fixture
def org(db_session):
    o = Organization(name="MaintOrg", slug=f"maint-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


class TestCleanupOrphanedDltDirs:
    def test_removes_old_dirs(self, tmp_path):
        old_dir = tmp_path / "pipeline_1_run_1"
        old_dir.mkdir()
        # Set mtime to 48 hours ago
        old_time = time.time() - 48 * 3600
        os.utime(old_dir, (old_time, old_time))

        removed = cleanup_orphaned_dlt_dirs(str(tmp_path), max_age_hours=24)
        assert removed == 1
        assert not old_dir.exists()

    def test_preserves_recent_dirs(self, tmp_path):
        recent_dir = tmp_path / "pipeline_2_run_2"
        recent_dir.mkdir()
        # mtime is now (recent)

        removed = cleanup_orphaned_dlt_dirs(str(tmp_path), max_age_hours=24)
        assert removed == 0
        assert recent_dir.exists()

    def test_nonexistent_dir_returns_zero(self):
        removed = cleanup_orphaned_dlt_dirs("/nonexistent/path", max_age_hours=1)
        assert removed == 0

    def test_mixed_old_and_recent(self, tmp_path):
        old_dir = tmp_path / "pipeline_old"
        old_dir.mkdir()
        os.utime(old_dir, (time.time() - 48 * 3600, time.time() - 48 * 3600))

        recent_dir = tmp_path / "pipeline_recent"
        recent_dir.mkdir()

        removed = cleanup_orphaned_dlt_dirs(str(tmp_path), max_age_hours=24)
        assert removed == 1
        assert not old_dir.exists()
        assert recent_dir.exists()


class TestCleanupProtectsActiveRuns:
    def test_skips_dir_for_running_upload(self, db_session, tmp_path, org):
        """Maintenance must NOT delete dlt dirs for still-running uploads."""
        # Create a "running" run in the DB
        running_run = Run(
            org_id=org.id,
            target_type="upload",
            target_id=1,
            status=RunStatus.RUNNING,
        )
        db_session.add(running_run)
        db_session.flush()
        run_id = running_run.id

        # Create the corresponding pipeline dir (old enough to be cleaned)
        pipeline_dir = tmp_path / f"pipeline_1_run_{run_id}"
        pipeline_dir.mkdir()
        (pipeline_dir / "state.json").write_text("{}")
        old_time = time.time() - 48 * 3600
        os.utime(pipeline_dir, (old_time, old_time))

        # Run cleanup with session — should skip this dir
        removed = cleanup_orphaned_dlt_dirs(str(tmp_path), max_age_hours=24, session=db_session)
        assert removed == 0
        assert pipeline_dir.exists()  # Protected!

    def test_removes_dir_for_completed_run(self, db_session, tmp_path, org):
        """Dirs for completed runs should be cleaned normally."""
        done_run = Run(
            org_id=org.id,
            target_type="upload",
            target_id=1,
            status=RunStatus.SUCCESS,
        )
        db_session.add(done_run)
        db_session.flush()
        run_id = done_run.id

        pipeline_dir = tmp_path / f"pipeline_1_run_{run_id}"
        pipeline_dir.mkdir()
        old_time = time.time() - 48 * 3600
        os.utime(pipeline_dir, (old_time, old_time))

        removed = cleanup_orphaned_dlt_dirs(str(tmp_path), max_age_hours=24, session=db_session)
        assert removed == 1
        assert not pipeline_dir.exists()


class TestCleanupDbtTargets:
    def test_removes_old_target(self, tmp_path):
        tenant_dir = tmp_path / "tenant_1" / "target"
        tenant_dir.mkdir(parents=True)
        (tenant_dir / "run_results.json").write_text("{}")
        old_time = time.time() - 72 * 3600
        os.utime(tenant_dir, (old_time, old_time))

        removed = cleanup_dbt_targets(str(tmp_path), max_age_hours=48)
        assert removed == 1
        assert not tenant_dir.exists()

    def test_preserves_recent_target(self, tmp_path):
        tenant_dir = tmp_path / "tenant_1" / "target"
        tenant_dir.mkdir(parents=True)

        removed = cleanup_dbt_targets(str(tmp_path), max_age_hours=48)
        assert removed == 0
        assert tenant_dir.exists()


class TestPurgeOldRuns:
    def test_purges_old_completed_runs(self, db_session, org):
        old_run = Run(
            org_id=org.id,
            target_type="upload",
            target_id=1,
            status=RunStatus.SUCCESS,
        )
        db_session.add(old_run)
        db_session.flush()
        # Manually set created_at to 100 days ago
        old_run.created_at = datetime.utcnow() - timedelta(days=100)
        db_session.flush()

        count = purge_old_runs(db_session, retention_days=90)
        assert count == 1
        db_session.refresh(old_run)
        assert old_run.deleted_at is not None

    def test_preserves_recent_runs(self, db_session, org):
        recent_run = Run(
            org_id=org.id,
            target_type="upload",
            target_id=1,
            status=RunStatus.SUCCESS,
        )
        db_session.add(recent_run)
        db_session.flush()

        count = purge_old_runs(db_session, retention_days=90)
        assert count == 0
        db_session.refresh(recent_run)
        assert recent_run.deleted_at is None

    def test_preserves_running_old_runs(self, db_session, org):
        running_run = Run(
            org_id=org.id,
            target_type="upload",
            target_id=1,
            status=RunStatus.RUNNING,
        )
        db_session.add(running_run)
        db_session.flush()
        running_run.created_at = datetime.utcnow() - timedelta(days=100)
        db_session.flush()

        count = purge_old_runs(db_session, retention_days=90)
        assert count == 0  # Running runs are never purged


class TestCleanupOrphanedArchives:
    def test_removes_archive_for_deleted_record(self, db_session, tmp_path, org):
        archive_path = tmp_path / "test.tar.gz"
        archive_path.write_bytes(b"fake archive")

        record = UploadedFile(
            org_id=org.id,
            original_name="test.csv",
            content_type="csv",
            file_size=100,
            file_hash="abc123",
            archive_path=str(archive_path),
            deleted_at=datetime.utcnow(),
        )
        db_session.add(record)
        db_session.flush()

        removed = cleanup_orphaned_archives(db_session, str(tmp_path))
        assert removed == 1
        assert not archive_path.exists()

    def test_preserves_archive_for_active_record(self, db_session, tmp_path, org):
        archive_path = tmp_path / "active.tar.gz"
        archive_path.write_bytes(b"active archive")

        record = UploadedFile(
            org_id=org.id,
            original_name="active.csv",
            content_type="csv",
            file_size=100,
            file_hash="def456",
            archive_path=str(archive_path),
        )
        db_session.add(record)
        db_session.flush()

        removed = cleanup_orphaned_archives(db_session, str(tmp_path))
        assert removed == 0
        assert archive_path.exists()


class TestDltRunnerCleanup:
    def test_cleanup_removes_pipeline_dir(self, tmp_path):
        from datanika.services.dlt_runner import DltRunnerService

        svc = DltRunnerService(pipelines_dir=str(tmp_path))
        pipeline_dir = tmp_path / "pipeline_1_run_1"
        pipeline_dir.mkdir()
        (pipeline_dir / "state.json").write_text("{}")

        svc.cleanup_pipeline(pipeline_id=1, run_id=1)
        assert not pipeline_dir.exists()

    def test_cleanup_nonexistent_is_noop(self, tmp_path):
        from datanika.services.dlt_runner import DltRunnerService

        svc = DltRunnerService(pipelines_dir=str(tmp_path))
        svc.cleanup_pipeline(pipeline_id=999, run_id=999)  # no error

    def test_cleanup_without_pipelines_dir_is_noop(self):
        from datanika.services.dlt_runner import DltRunnerService

        svc = DltRunnerService()  # no pipelines_dir
        svc.cleanup_pipeline(pipeline_id=1, run_id=1)  # no error


class TestDbtCleanTarget:
    def test_clean_target_removes_directory(self, tmp_path):
        from datanika.services.dbt_project import DbtProjectService

        svc = DbtProjectService(str(tmp_path))
        svc.ensure_project(1)
        target_dir = svc.get_project_path(1) / "target"
        target_dir.mkdir()
        (target_dir / "run_results.json").write_text("{}")

        svc.clean_target(1)
        assert not target_dir.exists()

    def test_clean_target_nonexistent_is_noop(self, tmp_path):
        from datanika.services.dbt_project import DbtProjectService

        svc = DbtProjectService(str(tmp_path))
        svc.ensure_project(1)
        svc.clean_target(1)  # no error, target doesn't exist


class TestCleanupOrphanedArchivesResolvesRelativeRows:
    """core#712 — the sweep took `uploads_dir` and never used it.

    `cleanup_orphaned_archives` guards on `os.path.isfile(record.archive_path)`
    and **skips**. Rows written with the default relative `FILE_UPLOADS_DIR`
    resolve against the *sweeping* process's CWD, not the writing one, so from
    `beat` the guard answers False for every one of them: nothing is removed,
    the function returns 0, and `run_maintenance` logs a clean sweep. A deleted
    upload's bytes stay on disk for the life of the volume.

    The existing coverage above cannot see this — it stores an absolute
    `tmp_path` and therefore only ever exercises the case that already worked.

    Live since `beat` first ran on 2026-08-30; before that the sweep had never
    executed in production at all, which is why nobody had seen it report 0.
    """

    def _relative_row(self, db_session, org, uploads, name):
        archives = uploads / "archives"
        archives.mkdir(parents=True, exist_ok=True)
        archive = archives / f"{name}.tar.gz"
        archive.write_bytes(b"fake archive")

        record = UploadedFile(
            org_id=org.id,
            original_name=f"{name}.csv",
            content_type="csv",
            file_size=12,
            file_hash=name,
            # exactly what save_file wrote before core#712
            archive_path=os.path.join(".", "uploaded_files", "archives", f"{name}.tar.gz"),
            deleted_at=datetime.utcnow(),
        )
        db_session.add(record)
        db_session.flush()
        return archive

    def test_removes_a_relative_row_after_the_cwd_changed(
        self, db_session, tmp_path, monkeypatch, org
    ):
        uploads = tmp_path / "uploaded_files"
        archive = self._relative_row(db_session, org, uploads, "deadbeef")

        # Control first: the bytes exist, so a failure below is the sweep's and
        # not a broken fixture.
        assert archive.is_file()

        elsewhere = tmp_path / "beat"
        elsewhere.mkdir()
        monkeypatch.chdir(elsewhere)

        removed = cleanup_orphaned_archives(db_session, str(uploads))

        assert not archive.exists(), (
            "the archive of a soft-deleted upload is still on disk — "
            "cleanup_orphaned_archives skipped it and reported a clean sweep"
        )
        assert removed == 1, "a skipped file must not be counted as a healthy zero"

    def test_an_active_relative_row_is_still_preserved(
        self, db_session, tmp_path, monkeypatch, org
    ):
        """The negative control: resolving paths must not start deleting live data."""
        uploads = tmp_path / "uploaded_files"
        archive = self._relative_row(db_session, org, uploads, "stillalive")
        record = db_session.query(UploadedFile).filter(UploadedFile.file_hash == "stillalive").one()
        record.deleted_at = None
        db_session.flush()

        assert archive.is_file()

        elsewhere = tmp_path / "beat"
        elsewhere.mkdir()
        monkeypatch.chdir(elsewhere)

        removed = cleanup_orphaned_archives(db_session, str(uploads))

        assert archive.is_file(), "an upload that was never deleted must keep its bytes"
        assert removed == 0
