"""Security tests — path traversal attacks on file paths and dbt project directories."""

import pytest

from datanika.services.dbt_project import DbtProjectError, DbtProjectService
from datanika.services.file_upload_service import FileUploadService


class TestDbtModelPathTraversal:
    def test_dotdot_in_model_name(self, tmp_path):
        svc = DbtProjectService(str(tmp_path))
        svc.ensure_project(1)
        with pytest.raises(DbtProjectError):
            svc.write_model(1, "../secret", "SELECT 1", "staging")

    def test_dotdot_in_schema_name(self, tmp_path):
        svc = DbtProjectService(str(tmp_path))
        svc.ensure_project(1)
        with pytest.raises(DbtProjectError):
            svc.write_model(1, "model", "SELECT 1", "../../../etc")

    def test_absolute_path_in_model_name(self, tmp_path):
        svc = DbtProjectService(str(tmp_path))
        svc.ensure_project(1)
        with pytest.raises(DbtProjectError):
            svc.write_model(1, "/etc/passwd", "SELECT 1", "staging")

    def test_null_byte_in_model_name(self, tmp_path):
        svc = DbtProjectService(str(tmp_path))
        svc.ensure_project(1)
        with pytest.raises(DbtProjectError):
            svc.write_model(1, "model\x00.sql", "SELECT 1", "staging")


class TestFileUploadPathTraversal:
    def test_dotdot_in_filename_rejected(self, tmp_path):
        svc = FileUploadService(str(tmp_path))
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(None, 1, "../../.env", b"secret")

    def test_path_separator_in_filename_rejected(self, tmp_path):
        svc = FileUploadService(str(tmp_path))
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(None, 1, "/etc/passwd", b"root:x:0:0")

    def test_valid_csv_accepted(self, db_session, tmp_path):
        svc = FileUploadService(str(tmp_path))
        record = svc.save_file(db_session, 1, "data.csv", b"a,b\n1,2")
        assert record.content_type == "csv"
        assert record.original_name == "data.csv"

    def test_only_allowed_extensions(self, tmp_path):
        svc = FileUploadService(str(tmp_path))
        for ext in ["py", "sh", "exe", "env", "sql", "yml"]:
            with pytest.raises(ValueError, match="Unsupported file type"):
                svc.save_file(None, 1, f"file.{ext}", b"data")
