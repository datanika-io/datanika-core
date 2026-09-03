"""Security tests — path traversal attacks on file paths and dbt project directories."""

import pathlib
import tarfile

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
    """Where the upload path's traversal defence actually is.

    🚨 This class used to assert that ``"../../.env"`` and ``"/etc/passwd"`` were
    refused, under names claiming a traversal defence. Both are refused by the
    **extension allowlist** -- ``.env`` is not an allowed type and
    ``/etc/passwd`` has no extension at all -- and ``save_file`` contains no
    path handling whatsoever. Measured: the identical traversal payload with an
    allowed extension, ``"../../evil.csv"``, is **accepted**, and its name is
    written verbatim as the tar member name.

    The real defence is ``filter="data"`` on ``extractall`` in
    ``extract_for_dlt``. Removing it left the whole security suite at 424
    passed, 0 red -- so the one mechanism that stops an escape was the one thing
    this file did not test. Those two assertions are kept below under honest
    names, as extension-allowlist tests, and the traversal coverage is real now.
    """

    def test_dotdot_filename_is_refused_for_its_extension_not_its_path(self, tmp_path):
        """``.env`` is refused as a TYPE. Named for what the error actually says."""
        svc = FileUploadService(str(tmp_path))
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(None, 1, "../../.env", b"secret")

    def test_path_separator_filename_is_refused_for_having_no_extension(self, tmp_path):
        svc = FileUploadService(str(tmp_path))
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(None, 1, "/etc/passwd", b"root:x:0:0")

    @pytest.mark.parametrize("name", ["../../evil.csv", "..\\..\\evil.csv", "/tmp/evil.csv"])
    def test_a_traversal_name_with_an_allowed_extension_reaches_the_archive(
        self, db_session, tmp_path, name
    ):
        """The discriminator the two assertions above never had.

        This is the paired acceptance control required of every refusal
        assertion: send the same shape with data the *unrelated* validator
        accepts. It passes that validator, which is what proves the refusals
        above are measuring the extension check and nothing else.

        It is deliberately an observation, not a demand: ``save_file`` accepting
        these is tracked separately. What must not change silently is the
        member name reaching the archive, because that is the input to the
        extraction guard below.
        """
        svc = FileUploadService(str(tmp_path))
        record = svc.save_file(db_session, 1, name, b"a,b\n1,2")
        assert record.content_type == "csv"
        assert record.original_name == name
        with tarfile.open(record.archive_path, "r:gz") as tar:
            assert tar.getnames() == [name]

    def test_extraction_refuses_an_archive_member_that_escapes_the_target(
        self, db_session, tmp_path
    ):
        """The actual traversal guard: ``extractall(..., filter="data")``.

        Red-first artifact: with ``filter="data"`` removed from
        ``extract_for_dlt``, ``tests/test_security`` reported 424 passed / 0
        red. This test is what makes that mutation visible.

        The archive is built through ``save_file`` rather than by hand, so the
        member name under test is one the product will actually produce.
        """
        uploads = tmp_path / "uploads"
        svc = FileUploadService(str(uploads))
        record = svc.save_file(db_session, 1, "../../escaped.csv", b"a,b\n1,2")

        with pytest.raises(tarfile.FilterError):
            svc.extract_for_dlt(record)

        # The assertion that survives an implementation change: nothing was
        # written outside the extraction directory.
        extract_root = uploads / "extracted"
        escapees = [p for p in tmp_path.rglob("escaped.csv") if extract_root not in p.parents]
        assert escapees == [], f"extraction escaped its directory: {escapees}"

    def test_extraction_of_a_well_formed_archive_still_works(self, db_session, tmp_path):
        """False-positive control for the guard above.

        A deny-everything ``extract_for_dlt`` would satisfy that test perfectly.
        """
        uploads = tmp_path / "uploads"
        svc = FileUploadService(str(uploads))
        record = svc.save_file(db_session, 1, "data.csv", b"a,b\n1,2")
        extracted = svc.extract_for_dlt(record)
        assert (pathlib.Path(extracted) / "data.csv").read_bytes() == b"a,b\n1,2"

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
