"""NotificationChannel model."""

import enum

from sqlalchemy import Boolean, Enum, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from datanika.models.base import Base, TenantMixin, TimestampMixin


class ChannelType(enum.StrEnum):
    EMAIL = "email"
    SLACK = "slack"
    TELEGRAM = "telegram"
    WEBHOOK = "webhook"


class NotificationChannel(Base, TenantMixin, TimestampMixin):
    __tablename__ = "notification_channels"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    channel_type: Mapped[ChannelType] = mapped_column(
        Enum(
            ChannelType,
            native_enum=False,
            length=20,
            values_callable=lambda e: [i.value for i in e],
        ),
        nullable=False,
    )
    config: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    events: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
