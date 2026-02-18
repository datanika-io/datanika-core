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
