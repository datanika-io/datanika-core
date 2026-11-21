"""Invitation model — pending org invitations sent by email."""

import enum
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, Enum, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from datanika.models.base import Base, TenantMixin, TimestampMixin
from datanika.models.user import MemberRole


class InvitationStatus(enum.StrEnum):
    PENDING = "pending"
    ACCEPTED = "accepted"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class Invitation(Base, TenantMixin, TimestampMixin):
    __tablename__ = "invitations"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    role: Mapped[MemberRole] = mapped_column(
        Enum(MemberRole, native_enum=False, length=20), nullable=False
    )
    invited_by_user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id"), nullable=False
    )

    #: SHA-256 hex of the value in the emailed link (D3). What lookups match on.
    token_hash: Mapped[str | None] = mapped_column(
        String(64), unique=True, index=True, nullable=True
    )
    status: Mapped[InvitationStatus] = mapped_column(
        Enum(InvitationStatus, native_enum=False, length=20),
        nullable=False,
        default=InvitationStatus.PENDING,
        # The member NAME (core#1393). `Enum(..., native_enum=False)` without `values_callable`
        # stores and loads names, so the database default must be the name too, or a row that
        # takes it cannot be read back. Migration `i9j0k1l2m3n4` sets the column to match, and
        # `test_the_column_default_is_the_models_server_default` fails if the two drift.
        server_default=InvitationStatus.PENDING.name,
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
