"""Security tests — path traversal attacks on file paths and dbt project directories."""

import pathlib
import tarfile
from io import BytesIO

import pytest

from datanika.services.dbt_project import DbtProjectError, DbtProjectService
from datanika.services.file_upload_service import FileUploadService


def _archive_with_member(svc, session, member_name, content=b"a,b\n1,2"):
    """Produce the archive ``save_file`` used to write for a path-bearing name.

    core#1027 made ``save_file`` refuse those names, so the extraction guard can
    no longer be reached through the product's own front door. Rather than
    hand-rolling a tar -- which would drift the moment ``save_file``'s archive
    shape changed, leaving the guard tested against a fixture nobody produces --
    this has ``save_file`` write a **real** archive under an ordinary name and
    then re-writes only the member's ``name``. Everything else (the DB record,
    the hash-named archive path, the container format) comes from the product;
    the one field under test is the only synthetic part.

    The ``len(members) == 1`` assertion is the anti-drift control: if
    ``save_file`` ever writes a different shape, this fails loudly here instead
    of silently testing something the product does not produce.
    """
    record = svc.save_file(session, 1, "placeholder.csv", content)

    with tarfile.open(record.archive_path, "r:gz") as tar:
        members = tar.getmembers()
        assert len(members) == 1, f"save_file's archive shape changed: {tar.getnames()}"
        info = members[0]
        payload = tar.extractfile(info).read()

    info.name = member_name
    with tarfile.open(record.archive_path, "w:gz") as tar:
        tar.addfile(info, BytesIO(payload))

    record.original_name = member_name
    return record


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

    The extraction-time defence is ``filter="data"`` on ``extractall`` in
    ``extract_for_dlt``. Removing it left the whole security suite at 424
    passed, 0 red -- so the one mechanism that stops an escape was the one thing
    this file did not test. Those two assertions are kept below under honest
    names, as extension-allowlist tests, and the traversal coverage is real now.

    🆕 **core#1027 closes the acceptance at the tier the user is standing on.**
    ``save_file`` now refuses a name carrying ``/`` or ``\\``. Two things about
    that are worth stating here, because both are load-bearing and neither is
    obvious:

    * **The extraction guard is not made redundant by it.** Archives written
      *before* this fix are still on disk and are still extracted by
      ``extract_for_dlt``, so ``filter="data"`` remains the thing standing
      between a stored member name and the filesystem. Its test below builds
      the archive by hand for exactly that reason: the product no longer
      produces such a member, and the guard must still be shown to work.
    * **``filter="data"`` does not refuse every path — it refuses ``..`` and
      silently *relocates* an absolute one.** Pinned below. So the two checks
      cover different inputs and neither one subsumes the other.

    🚨 **One measured correction to the issue's own severity reasoning**, kept
    here because it is the reason ``save_file`` refusing ``\\`` matters. The
    issue concluded *"there is no escape — it dies at extraction with a
    FilterError"*, which was measured on Windows. Re-measured inside
    ``python:3.12-slim``, i.e. the platform production runs on:

    * ``'../../escaped.csv'`` raises ``OutsideDestinationError`` — same verdict.
    * ``'..\\..\\escaped.csv'`` **extracts cleanly**, as one file whose name
      happens to contain backslashes, because POSIX does not split on ``\\``.
      The upload's default ``*.csv`` glob then *matches* it, so the run does not
      die at all — it succeeds, and derives a table named ``..\\..\\escaped``.

    **The conclusion survives and the mechanism does not:** there is still no
    escape, so this stays S3. But the production-reachable case is
    accept-now-**succeed**-later with a mangled table name, not
    accept-now-fail-later, and no test asserts that behaviour here because the
    two platforms genuinely disagree about it. What is asserted instead is that
    the product can no longer create such an archive.
    """

    def test_dotdot_filename_is_refused_for_its_extension_not_its_path(self, tmp_path):
        """``.env`` is refused as a TYPE. Named for what the error actually says.

        core#1027 kept this true on purpose: the extension check runs *before*
        the path check, so this name is still attributed to its type. That
        ordering is pinned by
        ``test_the_extension_check_runs_before_the_path_check`` below -- without
        it, a later refactor could swap the two, this test would still pass on
        its ``pytest.raises`` alone, and the extension allowlist would quietly
        stop being exercised for traversal-shaped names.
        """
        svc = FileUploadService(str(tmp_path))
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(None, 1, "../../.env", b"secret")

    def test_path_separator_filename_is_refused_for_having_no_extension(self, tmp_path):
        svc = FileUploadService(str(tmp_path))
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.save_file(None, 1, "/etc/passwd", b"root:x:0:0")

    @pytest.mark.parametrize(
        "name",
        [
            "../../evil.csv",
            "..\\..\\evil.csv",
            "/tmp/evil.csv",
            "./data.csv",
            "C:\\data.csv",
            "subdir/data.csv",
        ],
    )
    def test_a_traversal_name_with_an_allowed_extension_is_refused_for_its_path(
        self, db_session, tmp_path, name
    ):
        """core#1027. This test previously **recorded the acceptance**.

        It was the discriminator the two extension assertions above never had:
        the same traversal shape with data the *unrelated* validator accepts,
        proving those refusals measure the extension check and nothing else.
        It was written as an observation, deliberately, and the issue said the
        red it goes on the fix is the intended signal rather than collateral.
        This is that red, turned into the demand.

        Three assertions, because "it raised" is the weakest of them:

        1. it raises at all;
        2. the message names the **path**, not the type -- that is what
           distinguishes this fix from the extension allowlist that was already
           refusing the neighbouring cases;
        3. **nothing was written to disk.** A refusal that still leaves an
           archive behind would satisfy 1 and 2 while leaving the tar member
           this issue is about sitting in the uploads directory.
        """
        svc = FileUploadService(str(tmp_path))
        with pytest.raises(ValueError, match="must not contain a path") as excinfo:
            svc.save_file(db_session, 1, name, b"a,b\n1,2")

        assert "Unsupported file type" not in str(excinfo.value), (
            "refused for the wrong reason: the extension allowlist answered, "
            "so this name never reached the path check"
        )
        assert list(tmp_path.rglob("*.tar.gz")) == [], "an archive was written despite the refusal"

    @pytest.mark.parametrize(
        "name",
        [
            "my..data.csv",
            "..hidden.csv",
            "data...csv",
            "my data file.csv",
            ".data.csv",
            "data-2026.01.02.json",
        ],
    )
    def test_an_ordinary_name_containing_dots_is_still_accepted(self, db_session, tmp_path, name):
        """False-positive control for the refusal above.

        A path check written as ``".." in filename`` would refuse every one of
        these, and the suite would still be green on the traversal cases -- a
        fix that breaks legitimate uploads while looking correct. The separator
        is the thing that makes a name a path; ``..`` on its own is just dots.
        """
        svc = FileUploadService(str(tmp_path))
        record = svc.save_file(db_session, 1, name, b"a,b\n1,2")
        assert record.original_name == name

    def test_the_extension_check_runs_before_the_path_check(self, db_session, tmp_path):
        """Pins the ordering the two honest-name tests above depend on.

        Both orderings refuse both names, so no ``pytest.raises`` can tell them
        apart -- only the message can. Stated as one test so the reason lives in
        one place: with the checks swapped, ``../../.env`` would be attributed to
        its path and the extension allowlist would stop being exercised for any
        traversal-shaped name, silently.
        """
        svc = FileUploadService(str(tmp_path))

        # db_session, not None: without the fix the second call is *accepted*
        # and reaches session.add, and a real session makes that read as the
        # assertion failure it is rather than as an AttributeError on None.
        with pytest.raises(ValueError) as by_type:
            svc.save_file(db_session, 1, "../../.env", b"secret")
        with pytest.raises(ValueError) as by_path:
            svc.save_file(db_session, 1, "../../evil.csv", b"a,b\n1,2")

        assert "Unsupported file type" in str(by_type.value)
        assert "must not contain a path" in str(by_path.value)

    def test_extraction_refuses_an_archive_member_that_escapes_the_target(
        self, db_session, tmp_path
    ):
        """The extraction-time guard: ``extractall(..., filter="data")``.

        Red-first artifact: with ``filter="data"`` removed from
        ``extract_for_dlt``, ``tests/test_security`` reported 424 passed / 0
        red. This test is what makes that mutation visible.

        🆕 core#1027 changed how the archive is built, and the reason matters.
        It used to go through ``save_file``, so that the member name under test
        was one the product would actually produce. ``save_file`` now refuses
        that name, so the archive is written directly -- which is **not** a
        weaker test: it is the only way to reach the guard, and it is the shape
        that still exists on disk from before the fix. Deleting this test on the
        grounds that ``save_file`` now refuses would remove the only coverage of
        the layer that protects those already-stored archives.
        """
        uploads = tmp_path / "uploads"
        svc = FileUploadService(str(uploads))
        record = _archive_with_member(svc, db_session, "../../escaped.csv")

        with pytest.raises(tarfile.FilterError):
            svc.extract_for_dlt(record)

        # The assertion that survives an implementation change: nothing was
        # written outside the extraction directory.
        extract_root = uploads / "extracted"
        escapees = [p for p in tmp_path.rglob("escaped.csv") if extract_root not in p.parents]
        assert escapees == [], f"extraction escaped its directory: {escapees}"

    def test_extraction_relocates_an_absolute_member_rather_than_refusing_it(
        self, db_session, tmp_path
    ):
        """🚨 ``filter="data"`` is NOT a general path check, and this pins the gap.

        Measured (core#1027): a member named ``/tmp/escaped.csv`` does **not**
        raise. The ``data`` filter strips the leading separator and extracts it
        to ``<dest>/tmp/escaped.csv`` -- contained, but not refused, and not
        where anything looks for it. The upload's default glob is ``*.csv``,
        which is not recursive, so the run then dies in the Celery worker with
        *"No files matched '*.csv'"* -- an error naming the glob, on a file the
        user named perfectly reasonably from their own point of view.

        This test exists so that nobody deletes ``save_file``'s path check on
        the belief that the extraction filter already covers it. It covers
        ``..`` and it does not cover this.
        """
        uploads = tmp_path / "uploads"
        svc = FileUploadService(str(uploads))
        record = _archive_with_member(svc, db_session, "/tmp/escaped.csv")

        extracted = pathlib.Path(svc.extract_for_dlt(record))

        assert (extracted / "tmp" / "escaped.csv").read_bytes() == b"a,b\n1,2", (
            "the data filter's behaviour on an absolute member changed; "
            "re-derive whether save_file's path check is still the only guard"
        )
        assert not (extracted / "escaped.csv").exists(), (
            "the member landed at the top level, so the *.csv glob would find it "
            "and this test no longer describes the failure it was written about"
        )

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
