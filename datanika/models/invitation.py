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
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    role: Mapped[MemberRole] = mapped_column(
        Enum(MemberRole, native_enum=False, length=20), nullable=False
    )
    invited_by_user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id"), nullable=False
    )
    token: Mapped[str] = mapped_column(String(500), unique=True, nullable=False)
    status: Mapped[InvitationStatus] = mapped_column(
        Enum(InvitationStatus, native_enum=False, length=20),
        nullable=False,
        default=InvitationStatus.PENDING,
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
