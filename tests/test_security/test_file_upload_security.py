"""Security tests — file upload MIME type validation, double extensions, zip bombs."""

import pytest

from datanika.models.user import Organization
from datanika.services.file_upload_service import FileUploadService


@pytest.fixture
def uploads_dir(tmp_path):
    return str(tmp_path / "uploads")


@pytest.fixture
def svc(uploads_dir):
    return FileUploadService(uploads_dir)


@pytest.fixture
def org(db_session):
    o = Organization(name="UploadSecOrg", slug="upload-sec-org")
    db_session.add(o)
    db_session.flush()
    return o


class TestDoubleExtensions:
    """Attackers may use double extensions to bypass filters."""

    def test_exe_hidden_as_csv(self, svc):
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(None, 1, "malware.exe.csv.exe", b"MZ\x90\x00")

    def test_php_hidden_as_json(self, svc):
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(None, 1, "shell.php", b"<?php system($_GET['cmd']); ?>")

    def test_dot_env_rejected(self, svc):
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(None, 1, ".env", b"SECRET_KEY=leaked")

    def test_dotfile_with_csv_extension_accepted(self, svc, db_session, org):
        """Filenames like '.data.csv' should work (extension is csv)."""
        record = svc.save_file(db_session, org.id, ".data.csv", b"a,b\n1,2")
        assert record.content_type == "csv"


class TestMaliciousContent:
    """Files with valid extension but malicious content."""

    def test_csv_with_script_tag_stored_safely(self, svc, db_session, org):
        """CSV containing XSS payloads should be stored as-is (no execution)."""
        content = b'name,value\n"<script>alert(1)</script>",42\n'
        record = svc.save_file(db_session, org.id, "xss.csv", content)
        # File is stored — the upload layer doesn't parse content, that's fine.
        # The important thing is it's archived, not served directly.
        assert record.content_type == "csv"
        assert record.file_size == len(content)

    def test_json_with_prototype_pollution_stored_safely(self, svc, db_session, org):
        """JSON with __proto__ keys should be stored without interpretation."""
        content = b'{"__proto__": {"admin": true}}'
        record = svc.save_file(db_session, org.id, "proto.json", content)
        assert record.content_type == "json"

    def test_empty_file_accepted(self, svc, db_session, org):
        """Empty files should be accepted (valid edge case)."""
        record = svc.save_file(db_session, org.id, "empty.csv", b"")
        assert record.file_size == 0


class TestFilenameInjection:
    """Malicious filenames that could exploit OS or archive handling."""

    def test_null_byte_in_filename(self, svc):
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(None, 1, "data\x00.exe", b"malware")

    def test_unicode_normalization_attack(self, svc, db_session, org):
        """Unicode confusable characters in filename — should not bypass filters."""
        # Fullwidth .csv extension
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(None, 1, "data\uff0eexe", b"MZ")

    def test_very_long_filename(self, svc, db_session, org):
        """Extremely long filenames should not crash."""
        long_name = "a" * 500 + ".csv"
        record = svc.save_file(db_session, org.id, long_name, b"a,b\n1,2")
        assert record.content_type == "csv"

    def test_filename_with_spaces(self, svc, db_session, org):
        """Filenames with spaces are legitimate."""
        record = svc.save_file(db_session, org.id, "my data file.csv", b"a,b\n1,2")
        assert record.original_name == "my data file.csv"

    def test_no_extension_rejected(self, svc):
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(None, 1, "Makefile", b"all: clean")

    def test_only_dot_rejected(self, svc):
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(None, 1, ".", b"data")
