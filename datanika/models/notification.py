"""In-app notification model for the Notification Center (#68)."""

import enum

from sqlalchemy import BigInteger, DateTime, Enum, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from datanika.models.base import Base, TenantMixin, TimestampMixin


class NotificationType(enum.StrEnum):
    RUN_FAILED = "run_failed"
    RUN_SUCCEEDED = "run_succeeded"
    QUOTA_WARNING = "quota_warning"
    QUOTA_EXCEEDED = "quota_exceeded"
    # V2 P5 Option B — cycle-boundary overage billing (core#249)
    CHARGE_INCOMING = "charge_incoming"
    CHARGE_ISSUED = "charge_issued"
    CHARGE_FAILED = "charge_failed"


class Notification(Base, TenantMixin, TimestampMixin):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    type: Mapped[NotificationType] = mapped_column(
        Enum(
            NotificationType,
            native_enum=False,
            length=30,
            values_callable=lambda e: [i.value for i in e],
        ),
        nullable=False,
    )
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    resource_type: Mapped[str] = mapped_column(String(30), nullable=False)
    resource_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    read_at: Mapped[None] = mapped_column(DateTime(timezone=True), nullable=True)
