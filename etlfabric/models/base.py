import uuid
from datetime import datetime

from sqlalchemy import DateTime, String, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import CHAR, TypeDecorator


class UUIDType(TypeDecorator):
    """Platform-independent UUID type. Uses CHAR(32) for SQLite compatibility,
    stores as hex string internally but exposes uuid.UUID to Python."""

    impl = CHAR(32)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None:
            if isinstance(value, uuid.UUID):
                return value.hex
            return uuid.UUID(value).hex
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            return uuid.UUID(value)
        return value


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class TenantMixin:
    """Mixin for models that belong to an organization (tenant)."""

    org_id: Mapped[uuid.UUID] = mapped_column(UUIDType(), nullable=False, index=True)
