"""File upload service — save, archive, extract, and delete uploaded files."""

import hashlib
import os
import shutil
import tarfile
from datetime import UTC, datetime
from io import BytesIO

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.uploaded_file import UploadedFile

ALLOWED_EXTENSIONS = {"csv", "json", "parquet"}


class FileUploadService:
    MAX_FILE_SIZE = 20 * 1024 * 1024  # 20 MB

    def __init__(self, uploads_dir: str):
        self._uploads_dir = uploads_dir

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
            raise ValueError(
                f"Unsupported file type '.{ext}'. Allowed: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
            )
        return ext

    def save_file(
        self,
        session: Session,
        org_id: int,
        filename: str,
        content: bytes,
    ) -> UploadedFile:
        """Validate size, compute SHA-256, write tar.gz archive, create DB record."""
        if len(content) > self.MAX_FILE_SIZE:
            raise ValueError(
                f"File size ({len(content)} bytes) exceeds maximum ({self.MAX_FILE_SIZE} bytes)"
            )

        content_type = self._infer_content_type(filename)
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

        with tarfile.open(uploaded_file.archive_path, "r:gz") as tar:
            tar.extractall(extract_path, filter="data")

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

        if os.path.isfile(record.archive_path):
            os.remove(record.archive_path)

        return True
