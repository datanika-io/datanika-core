"""TDD tests for FileUploadService."""

import os
import tarfile

import pytest

from datanika.models.uploaded_file import UploadedFile
from datanika.services.file_upload_service import FileUploadService


@pytest.fixture
def uploads_dir(tmp_path):
    return str(tmp_path / "uploads")


@pytest.fixture
def svc(uploads_dir):
    return FileUploadService(uploads_dir)


@pytest.fixture
def sample_csv():
    return b"id,name\n1,Alice\n2,Bob\n"


class TestSaveFile:
    def test_creates_archive_and_record(self, svc, db_session, uploads_dir, sample_csv):
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-upload-svc")
        db_session.add(org)
        db_session.flush()

        record = svc.save_file(db_session, org.id, "data.csv", sample_csv)

        assert isinstance(record, UploadedFile)
        assert record.original_name == "data.csv"
        assert record.content_type == "csv"
        assert record.file_size == len(sample_csv)
        assert len(record.file_hash) == 64  # SHA-256 hex
        assert record.archive_path.endswith(".tar.gz")
        assert os.path.isfile(record.archive_path)

        # Verify archive contains the original file
        with tarfile.open(record.archive_path, "r:gz") as tar:
            names = tar.getnames()
            assert "data.csv" in names

    def test_rejects_oversized(self, svc, db_session):
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-upload-big")
        db_session.add(org)
        db_session.flush()

        huge = b"x" * (20 * 1024 * 1024 + 1)
        with pytest.raises(ValueError, match="exceeds maximum"):
            svc.save_file(db_session, org.id, "huge.csv", huge)

    def test_rejects_invalid_extension(self, svc, db_session):
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-upload-ext")
        db_session.add(org)
        db_session.flush()

        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(db_session, org.id, "script.py", b"print('hi')")

    def test_json_extension(self, svc, db_session):
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-upload-json")
        db_session.add(org)
        db_session.flush()

        record = svc.save_file(db_session, org.id, "data.json", b'[{"a":1}]')
        assert record.content_type == "json"

    def test_parquet_extension(self, svc, db_session):
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-upload-pq")
        db_session.add(org)
        db_session.flush()

        record = svc.save_file(db_session, org.id, "data.parquet", b"PAR1fakecontent")
        assert record.content_type == "parquet"


class TestExtractForDlt:
    def test_returns_directory(self, svc, db_session, sample_csv):
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-extract")
        db_session.add(org)
        db_session.flush()

        record = svc.save_file(db_session, org.id, "data.csv", sample_csv)
        extracted_dir = svc.extract_for_dlt(record)

        assert os.path.isdir(extracted_dir)
        assert os.path.isfile(os.path.join(extracted_dir, "data.csv"))

        # Read content matches original
        with open(os.path.join(extracted_dir, "data.csv"), "rb") as f:
            assert f.read() == sample_csv

    def test_missing_archive_names_the_cause_not_just_the_path(self, svc, db_session, sample_csv):
        """A worker that cannot see the archive must say why (core#529).

        This is the single most expensive error message in the product. When
        the web tier and the Celery worker do not share the uploads directory,
        the run dies here in under 100ms with 0 rows, and the bare
        ``[Errno 2] No such file or directory: './uploaded_files/archives/…'``
        names a path while saying nothing about the cause. It cost three CI
        rounds and two departments to work out that the two tiers had separate
        filesystems — and it is the first thing a Helm self-hoster hits on the
        advertised CSV → DuckDB onboarding path.

        So the assertion is on the diagnosis, not on the exception type.
        """
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-missing-archive")
        db_session.add(org)
        db_session.flush()

        record = svc.save_file(db_session, org.id, "data.csv", sample_csv)
        os.remove(record.archive_path)

        with pytest.raises(FileNotFoundError) as exc:
            svc.extract_for_dlt(record)

        message = str(exc.value)
        assert record.archive_path in message, "the path is still needed for support"
        assert "share" in message.lower(), "must say the tiers need a shared directory"
        assert "FILE_UPLOADS_DIR" in message, "must name the setting the operator changes"


class TestCleanupExtracted:
    def test_removes_dir(self, svc, db_session, sample_csv):
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-cleanup")
        db_session.add(org)
        db_session.flush()

        record = svc.save_file(db_session, org.id, "data.csv", sample_csv)
        extracted_dir = svc.extract_for_dlt(record)
        assert os.path.isdir(extracted_dir)

        svc.cleanup_extracted(record)
        assert not os.path.isdir(extracted_dir)


class TestDeleteFile:
    def test_soft_deletes(self, svc, db_session, sample_csv):
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug="acme-del-file")
        db_session.add(org)
        db_session.flush()

        record = svc.save_file(db_session, org.id, "data.csv", sample_csv)
        archive_path = record.archive_path
        assert os.path.isfile(archive_path)

        result = svc.delete_file(db_session, org.id, record.id)
        assert result is True
        assert record.deleted_at is not None
        assert not os.path.isfile(archive_path)


class TestArchivePathIsCwdIndependent:
    """core#712 — a relative `archive_path` in a DB row is not a location.

    `settings.file_uploads_dir` defaults to `"./uploaded_files"`, so with
    `FILE_UPLOADS_DIR` unset the value written to the database is relative to
    the *writer's* working directory. Every reader is a different process —
    `run_upload` and the hourly `beat` sweep both live in containers the web
    tier never entered — so `os.path.isfile` on that value answers False and
    each reader's guard silently skips.

    Two halves, and the second is the one that loses data:
      * `extract_for_dlt` raises, loudly, with a good diagnosis (already true);
      * `delete_file` and `cleanup_orphaned_archives` **skip and report success**.
    """

    def _org(self, db_session, slug):
        from datanika.models.user import Organization

        org = Organization(name="Acme", slug=slug)
        db_session.add(org)
        db_session.flush()
        return org

    def test_save_file_stores_an_absolute_path_under_a_relative_uploads_dir(
        self, db_session, tmp_path, monkeypatch, sample_csv
    ):
        """The class fix, not the instance fix — abspath at construction time."""
        monkeypatch.chdir(tmp_path)
        svc = FileUploadService("./uploaded_files")  # the config.py default, verbatim
        org = self._org(db_session, "acme-relpath-save")

        record = svc.save_file(db_session, org.id, "data.csv", sample_csv)

        assert os.path.isabs(record.archive_path), (
            "a relative path in a DB row is stable only while every reader shares "
            f"the writer's CWD; got {record.archive_path!r}"
        )
        assert os.path.isfile(record.archive_path)

    def test_a_saved_archive_survives_a_change_of_working_directory(
        self, db_session, tmp_path, monkeypatch, sample_csv
    ):
        """Ordered so an unfixed run fails on the CWD change, not on absence.

        The control below runs *first* and from the writer's own directory, so a
        failure after the `chdir` cannot be read as "the archive was never
        written".
        """
        writer_cwd = tmp_path / "web"
        reader_cwd = tmp_path / "worker"
        writer_cwd.mkdir()
        reader_cwd.mkdir()

        monkeypatch.chdir(writer_cwd)
        svc = FileUploadService("./uploaded_files")
        org = self._org(db_session, "acme-relpath-cwd")
        record = svc.save_file(db_session, org.id, "data.csv", sample_csv)

        # Control — the bytes really are on disk, at the writer's own location.
        on_disk = writer_cwd / "uploaded_files" / "archives" / f"{record.file_hash}.tar.gz"
        assert on_disk.is_file(), "fixture is broken: nothing was written"
        assert os.path.isfile(record.archive_path), "readable from the writer's CWD"

        # The reader: another container, another working directory, same volume.
        monkeypatch.chdir(reader_cwd)
        assert os.path.isfile(record.archive_path), (
            "the stored path stopped resolving when the CWD changed — this is the "
            "defect, and it is why the Celery worker cannot read what the web tier wrote"
        )

    def test_delete_file_removes_the_bytes_of_a_legacy_relative_row(
        self, db_session, tmp_path, monkeypatch
    ):
        """A row written before this fix still has to be deletable.

        `delete_file` guards on `os.path.isfile` and skips, so it returns True,
        soft-deletes the record, and leaves the archive on disk forever.
        """
        from datanika.models.uploaded_file import UploadedFile

        uploads = tmp_path / "uploaded_files"
        archives = uploads / "archives"
        archives.mkdir(parents=True)
        archive = archives / "cafebabe.tar.gz"
        archive.write_bytes(b"fake archive")

        org = self._org(db_session, "acme-relpath-delete")
        record = UploadedFile(
            org_id=org.id,
            original_name="legacy.csv",
            content_type="csv",
            file_size=12,
            file_hash="cafebabe",
            # exactly what save_file wrote before core#712
            archive_path=os.path.join(".", "uploaded_files", "archives", "cafebabe.tar.gz"),
        )
        db_session.add(record)
        db_session.flush()

        # Control first: the bytes exist before anything is asked to remove them.
        assert archive.is_file()

        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir()
        monkeypatch.chdir(elsewhere)

        # The operator has done what extract_for_dlt's error message asks:
        # FILE_UPLOADS_DIR is an absolute path shared by both tiers.
        svc = FileUploadService(str(uploads))
        assert svc.delete_file(db_session, org.id, record.id) is True

        assert not archive.exists(), (
            "delete_file reported success and left the archive on disk — the bytes "
            "of a deleted upload are never reclaimed"
        )

    def test_resolve_archive_path_leaves_an_absolute_path_alone(self, tmp_path):
        from datanika.services.file_upload_service import resolve_archive_path

        absolute = str(tmp_path / "archives" / "x.tar.gz")
        assert resolve_archive_path(absolute, str(tmp_path / "other")) == absolute

    def test_resolve_archive_path_reroots_a_relative_path_under_uploads_dir(self, tmp_path):
        from datanika.services.file_upload_service import resolve_archive_path

        resolved = resolve_archive_path(
            os.path.join(".", "uploaded_files", "archives", "x.tar.gz"),
            str(tmp_path / "vol"),
        )
        assert resolved == str(tmp_path / "vol" / "archives" / "x.tar.gz")
