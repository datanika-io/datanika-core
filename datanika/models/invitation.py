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

    # ⚠️ LEGACY, release N. The invitee's address now lives in `invitation_pii.email`;
    # this column is dual-written so the previously deployed code keeps working through
    # the blue/green swap, and is DROPPED in N+2. Nullable since N — which is both what
    # lets N+1 stop writing it and what lets `erase_user` NULL it during the dual-write
    # window (SPEC_PII_SEPARATION §0.2).
    email: Mapped[str | None] = mapped_column(String(320), nullable=True)

    role: Mapped[MemberRole] = mapped_column(
        Enum(MemberRole, native_enum=False, length=20), nullable=False
    )
    invited_by_user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id"), nullable=False
    )

    # ⚠️ LEGACY, release N — same lifecycle as `email` above, and worse while it lasts:
    # this is the **plaintext JWT**, and its payload contains `{"email": <invitee>}`, so
    # `base64 -d` reads the address out of any pg_dump. `models/password_reset.py`
    # documents why not to store a token this way and names this column as what not to
    # copy. Superseded by `token_hash`.
    token: Mapped[str | None] = mapped_column(String(500), unique=True, nullable=True)

    #: SHA-256 hex of the value in the emailed link (D3). What lookups match on.
    token_hash: Mapped[str | None] = mapped_column(
        String(64), unique=True, index=True, nullable=True
    )
    status: Mapped[InvitationStatus] = mapped_column(
        Enum(InvitationStatus, native_enum=False, length=20),
        nullable=False,
        default=InvitationStatus.PENDING,
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
