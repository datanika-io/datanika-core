from sqlalchemy import BigInteger, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from datanika.models.base import Base, TenantMixin, TimestampMixin


class UploadedFile(Base, TenantMixin, TimestampMixin):
    __tablename__ = "uploaded_files"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    original_name: Mapped[str] = mapped_column(String(500), nullable=False)
    content_type: Mapped[str] = mapped_column(String(100), nullable=False)
    file_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    file_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    archive_path: Mapped[str] = mapped_column(Text, nullable=False)
