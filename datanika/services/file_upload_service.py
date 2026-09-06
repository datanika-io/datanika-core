"""File upload service — save, archive, extract, and delete uploaded files."""

import hashlib
import os
import shutil
import tarfile
from datetime import UTC, datetime
from io import BytesIO

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.errors import UserFacingError
from datanika.models.uploaded_file import UploadedFile

ALLOWED_EXTENSIONS = {"csv", "json", "parquet"}


def resolve_archive_path(archive_path: str, uploads_dir: str) -> str:
    """Resolve a stored ``archive_path`` against *this* process's uploads directory.

    Rows written before core#712 hold a path relative to the working directory of
    the process that wrote them — normally the web tier, while every reader is a
    different container: ``run_upload`` in the Celery worker, and the hourly
    archive sweep in ``beat``. ``os.path.isfile`` on such a value silently answers
    False there, and every caller in this codebase guarded on exactly that and
    skipped. For ``cleanup_orphaned_archives`` the consequence is a storage leak
    that reads as a clean sweep: a deleted upload's bytes are never reclaimed and
    the counter says ``0``.

    An absolute path is returned unchanged. A relative one is re-rooted under
    ``uploads_dir`` keeping its ``archives/<name>`` tail, so an operator who has
    set ``FILE_UPLOADS_DIR`` to the shared absolute path — which is what
    :meth:`FileUploadService.extract_for_dlt`'s error already tells them to do —
    makes every legacy row resolvable **without a data migration**.

    When ``uploads_dir`` is itself relative this is no worse than the status quo:
    it resolves against the reader's CWD exactly as the bare path did.
    """
    if os.path.isabs(archive_path):
        return archive_path
    return os.path.join(os.path.abspath(uploads_dir), "archives", os.path.basename(archive_path))


def get_org_uploaded_file(session: Session, org_id: int, file_id: int) -> UploadedFile | None:
    """Resolve an uploaded file *within* an org — the single definition of ownership.

    `run_upload` used a bare `session.get(UploadedFile, uploaded_file_id)`
    (#732), which neither scoped the org nor filtered `deleted_at`. Both matter
    here and for the same reason: the record names an **archive path on disk**
    that the task then extracts and reads. A cross-org id reads another
    tenant's uploaded data; a soft-deleted one reads an archive
    `cleanup_orphaned_archives` may already have removed.
    """
    stmt = select(UploadedFile).where(
        UploadedFile.id == file_id,
        UploadedFile.org_id == org_id,
        UploadedFile.deleted_at.is_(None),
    )
    return session.execute(stmt).scalar_one_or_none()


class FileUploadService:
    MAX_FILE_SIZE = 20 * 1024 * 1024  # 20 MB

    def __init__(self, uploads_dir: str):
        # Absolute at construction, so every path derived from it is absolute too
        # (core#712). Fixing it here rather than at the one `os.path.join` in
        # `save_file` fixes the class: `_extracted_dir` and every future caller
        # inherit it, and a row can never again be written CWD-relative.
        self._uploads_dir = os.path.abspath(uploads_dir)

    def _archives_dir(self) -> str:
        path = os.path.join(self._uploads_dir, "archives")
        os.makedirs(path, exist_ok=True)
        return path

    def _extracted_dir(self) -> str:
        path = os.path.join(self._uploads_dir, "extracted")
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def _infer_content_type(filename: str) -> str:
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        if ext not in ALLOWED_EXTENSIONS:
            raise UserFacingError(
                f"Unsupported file type '.{ext}'. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
            )
        return ext

    @staticmethod
    def _reject_path_segments(filename: str) -> None:
        """Refuse a name that is a *path* rather than a file name (core#1027).

        Before this, ``save_file`` validated size and extension and looked at
        the path not at all. ``'../../.env'`` and ``'/etc/passwd'`` were refused
        — but by the **extension allowlist**, which is why the same traversal
        with an allowed extension, ``'../../evil.csv'``, was accepted and
        written verbatim as the tar member name.

        Why this is not redundant with ``extract_for_dlt``'s ``filter="data"``:

        * that filter refuses ``..`` and **relocates** an absolute member rather
          than refusing it (``/tmp/x.csv`` extracts to ``<dest>/tmp/x.csv``), so
          the two checks cover different inputs;
        * even where it does refuse, it refuses in the **Celery worker**, long
          after the user has left the upload form. Accept-now-fail-later is the
          slower, more confusing failure, and the error it produces names the
          extraction or the file glob rather than the filename the user chose.

        🚨 **Both separators, and the backslash is the one that matters in
        production.** Reflex reduces an uploaded name to ``Path(name.lstrip("/")).name``
        (``reflex/app.py``), and ``Path`` is ``PosixPath`` in the container. So
        by the time a browser upload reaches here, a ``/``-bearing name has
        *already* been reduced to its basename — incidentally, not by design —
        while a ``\\``-bearing one arrives **whole**, because POSIX does not
        treat ``\\`` as a separator. Measured, on Linux:

        =========================  ====================  ==================
        filename                   reflex, container     reflex, dev box
        =========================  ====================  ==================
        ``../../evil.csv``         ``evil.csv``          ``evil.csv``
        ``..\\..\\evil.csv``       ``..\\..\\evil.csv``  ``evil.csv``
        =========================  ====================  ==================

        ⚠️ **So an end-to-end reproduction on a Windows dev box shows nothing
        reaching this function, and production is the platform where one does.**
        Anyone re-deriving this issue locally through the UI gets the flattering
        answer.

        And it is why the check is an explicit separator test rather than the
        more obvious ``filename != os.path.basename(filename)``:
        ``os.path.basename`` is ``posixpath.basename`` in the container, which
        returns ``'..\\..\\evil.csv'`` unchanged — blind to the single case that
        is actually reachable there. (The two also disagree on drive prefixes:
        ``'C:data.csv'`` loses its drive on Windows and survives whole on
        POSIX.) A validator that answers differently on the developer's machine
        and in production is worse than the hole it closes.

        Invariant this relies on: ``_infer_content_type`` has already run, so
        the name ends in an allowed extension and its final segment is
        non-empty. Pinned by
        ``test_the_extension_check_runs_before_the_path_check``.
        """
        if "/" not in filename and "\\" not in filename:
            return
        basename = filename.replace("\\", "/").rsplit("/", 1)[-1]
        raise UserFacingError(f"File name must not contain a path. Upload it as '{basename}'.")

    def save_file(
        self,
        session: Session,
        org_id: int,
        filename: str,
        content: bytes,
    ) -> UploadedFile:
        """Validate size, compute SHA-256, write tar.gz archive, create DB record."""
        if len(content) > self.MAX_FILE_SIZE:
            raise UserFacingError(
                f"File size ({len(content)} bytes) exceeds maximum ({self.MAX_FILE_SIZE} bytes)"
            )

        content_type = self._infer_content_type(filename)
        # core#1027. Order is load-bearing, not incidental: the extension check
        # stays first so a traversal name with a *disallowed* extension is still
        # attributed to its type, which is what the two allowlist tests in
        # tests/test_security/test_path_traversal.py measure. Swapping them
        # would leave those names refused — and the allowlist untested for them.
        self._reject_path_segments(filename)
        file_hash = hashlib.sha256(content).hexdigest()

        # Create tar.gz archive
        archive_name = f"{file_hash}.tar.gz"
        archive_path = os.path.join(self._archives_dir(), archive_name)

        with tarfile.open(archive_path, "w:gz") as tar:
            info = tarfile.TarInfo(name=filename)
            info.size = len(content)
            tar.addfile(info, BytesIO(content))

        record = UploadedFile(
            org_id=org_id,
            original_name=filename,
            content_type=content_type,
            file_size=len(content),
            file_hash=file_hash,
            archive_path=archive_path,
        )
        session.add(record)
        session.flush()
        return record

    def extract_for_dlt(self, uploaded_file: UploadedFile) -> str:
        """Extract archive to temp dir, return path to extracted directory."""
        extract_path = os.path.join(self._extracted_dir(), uploaded_file.file_hash)
        os.makedirs(extract_path, exist_ok=True)
        archive_path = resolve_archive_path(uploaded_file.archive_path, self._uploads_dir)

        try:
            with tarfile.open(archive_path, "r:gz") as tar:
                tar.extractall(extract_path, filter="data")
        except FileNotFoundError as exc:
            # This runs in the Celery worker; the archive was written by the web
            # tier. If the two do not share this directory the run dies here,
            # before any data moves, and the bare OSError names a path while
            # saying nothing about the cause — which is how core#529 cost three
            # CI rounds. Deployment is the overwhelmingly likely explanation, so
            # say so; the alternative (a genuinely deleted archive) is still
            # identifiable from the path.
            raise FileNotFoundError(
                f"Uploaded file archive not found: {archive_path}\n"
                "The web tier stored this file and this worker cannot see it, which "
                "normally means the two do not share the uploads directory. Mount one "
                "volume read-write on both the app and the Celery worker, and set "
                f"FILE_UPLOADS_DIR (currently {self._uploads_dir}) to the same absolute "
                "path in both — the default is relative to the working directory, so "
                "sharing the volume alone is not enough."
            ) from exc

        return extract_path

    def cleanup_extracted(self, uploaded_file: UploadedFile) -> None:
        """Remove extracted files after DLT run completes."""
        extract_path = os.path.join(self._extracted_dir(), uploaded_file.file_hash)
        if os.path.isdir(extract_path):
            shutil.rmtree(extract_path)

    def delete_file(self, session: Session, org_id: int, file_id: int) -> bool:
        """Soft-delete record + remove archive from disk."""
        stmt = select(UploadedFile).where(
            UploadedFile.id == file_id,
            UploadedFile.org_id == org_id,
            UploadedFile.deleted_at.is_(None),
        )
        record = session.execute(stmt).scalar_one_or_none()
        if record is None:
            return False

        record.deleted_at = datetime.now(UTC)
        session.flush()

        archive_path = resolve_archive_path(record.archive_path, self._uploads_dir)
        if os.path.isfile(archive_path):
            os.remove(archive_path)

        return True
