"""Password-reset tokens (core#623).

A reset token is a **full account-takeover primitive**, which decides every
column here.

*Stored, not signed.* The two existing email flows disagree with each other, and
only one of them is right for this. ``verify_email`` mints a bare JWT with no DB
row, so it is replayable until ``exp`` and cannot be revoked; ``Invitation``
keeps a row with a status, so it is single-use and revocable. Single-use is a
property of stored state — a signed string cannot have it — so this is the
second kind.

*Hashed, unlike* ``Invitation.token``. That table stores its JWT verbatim and
looks it up by equality. Do not copy it: the nightly ``pg_dump`` ships off-box
and is retained 30 days, so a plaintext reset token in a dump is a live key to
an account sitting on a second server. This mirrors ``OAuthGrant.code_hash``
instead — SHA-256 hex, ``String(64)``, unique — so a database read yields
nothing replayable. SHA-256 rather than bcrypt because the token already carries
256 bits of entropy (there is nothing to stretch) and the lookup has to be an
indexed equality on the hash.

*No* ``TenantMixin``. This belongs to a person, not an organization, exactly
like ``users`` itself.
"""

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from datanika.models.base import Base, TimestampMixin


class PasswordResetToken(Base, TimestampMixin):
    __tablename__ = "password_reset_tokens"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"), nullable=False)

    # SHA-256 hex of the value that was emailed. The raw token exists in the
    # message and in the Celery argument list, and nowhere else.
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)

    # 60 minutes. The two existing flows use 24 hours and 7 days; both are TTLs
    # chosen for much weaker capabilities. 15 minutes is too short — relay
    # latency plus a phone means frequent failure, and every failure sends the
    # user back to the request form, which *increases* the number of live
    # tokens.
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # The single-use marker, set in the same transaction as the password write.
    # Also how a superseded token is retired, so "expired", "already used" and
    # "superseded" all present as one indistinguishable failure.
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
