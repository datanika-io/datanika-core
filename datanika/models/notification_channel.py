"""NotificationChannel model."""

import enum
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Enum, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import JSON

from datanika.models.base import Base, TenantMixin, TimestampMixin

#: Longest error text we will keep. An unbounded error string is a second way to
#: store a payload, and the column is rendered in the UI.
MAX_LAST_ERROR = 500


class ChannelType(enum.StrEnum):
    EMAIL = "email"
    SLACK = "slack"
    TELEGRAM = "telegram"
    WEBHOOK = "webhook"


class DeliveryStatus(enum.StrEnum):
    """Outcome of the most recent delivery attempt (core#652).

    Stored as a plain string rather than a DB enum: this is an observability
    column, and a new outcome must never require a migration on a table whose
    rows an operator is reading during an incident.
    """

    SUCCESS = "success"
    FAILED = "failed"
    #: Not attempted, and correctly so — e.g. no SMTP relay is configured.
    #: Deliberately distinct from FAILED: an unconfigured relay is a normal
    #: self-hosted deployment, not a fault, and conflating the two is what sent
    #: every reader of the old log line to go and check a working relay.
    SKIPPED = "skipped"


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

    # --- delivery record (core#652) -------------------------------------
    # `is_active` answers "is this channel switched on?". The user is asking
    # "is this channel working?". Those are different questions and until these
    # columns existed the UI answered the wrong one with a green badge.
    #
    # All three are nullable with no default and no backfill, which is what
    # makes this an expand-only migration: the previously deployed code neither
    # reads nor writes them, and NULL is the honest value for a channel that has
    # not been tried yet (rendered as "never delivered", not as healthy).
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_status: Mapped[str | None] = mapped_column(String(16), nullable=True)
    #: ⚠️ Never a value from ``config`` and never the payload. Written only
    #: through ``NotificationService._record_delivery``, which redacts and
    #: truncates; assigning to this column directly bypasses both.
    last_error: Mapped[str | None] = mapped_column(String(MAX_LAST_ERROR), nullable=True)
