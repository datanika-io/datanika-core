"""Change the address on an account (SPEC_EMAIL_CHANGE, core#655 AC4).

Two rules are easy to undo at the call site, and both are why this module exists.

**``users.email`` stays where it is until the link is followed.** Resend
verification re-reads its destination from the ``User`` row. Writing a
user-supplied address into that column would aim the existing resend control at
whatever the form submitted, without any change to the resend code.

**A taken address is refused out loud.** Opacity is the right default for a
public form and the wrong choice here: the actor is already signed in, the
request is rate-limited, and a silent "check your inbox" is the defect a typo'd
signup was filed about. Do not "correct" this into the password-reset shape.
"""

import hashlib
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from datanika.models.pii import EmailChangeRequest, UserPII
from datanika.models.user import User
from datanika.services.user_service import UserService

READY = "ready"
ADDRESS_TAKEN = "address_taken"
INVALID = "invalid"

# Same span as the verification resend, and a coarser per-user ceiling above
# the per-target one so rotating the address is not an unbounded mailer.
PAIR_LIMIT = 3
USER_LIMIT = 10
WINDOW_SECONDS = 3600

# 24 hours, matching ``create_email_verification_token``, not the 60-minute
# password-reset TTL. Named so a later edit cannot drift the mail copy from
# the row.
TOKEN_TTL_HOURS = 24
TOKEN_BYTES = 32


def _hash(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _aware(value: datetime | None) -> datetime | None:
    """SQLite hands back naive datetimes; Postgres does not."""
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _normalise(raw: str | None) -> str:
    return (raw or "").strip().lower()


@dataclass(frozen=True)
class EmailChangePreparation:
    status: str
    token: str | None = None


class EmailChangeService:
    PAIR_LIMIT = PAIR_LIMIT
    USER_LIMIT = USER_LIMIT
    WINDOW_SECONDS = WINDOW_SECONDS

    def __init__(self, user_service: UserService):
        self._users = user_service

    @staticmethod
    def pair_bucket(user_id: int, email: str) -> str:
        """Actor plus target. The address is hashed, same as password reset.

        A plaintext address in the Redis key would be a list of every account
        someone tried to move to, readable by anyone who can read Redis.
        """
        digest = hashlib.sha256(_normalise(email).encode()).hexdigest()
        return f"email-change:pair:{user_id}:{digest}"

    @staticmethod
    def user_bucket(user_id: int) -> str:
        return f"email-change:user:{user_id}"

    def prepare_change(
        self, session: Session, user_id: int, raw_email: str
    ) -> EmailChangePreparation:
        """Record ``pending_email`` and a hashed token. Do not touch ``users.email``."""
        user = self._users.get_user(session, user_id)
        if user is None or not user.is_active or user.deleted_at is not None:
            return EmailChangePreparation(INVALID)

        normalised = _normalise(raw_email)
        pii = self._users.get_user_pii(session, user.id)
        # The live address is the sidecar. ``users.email`` is left at the
        # previous value once a confirmation stops writing it, so treating it
        # as current would refuse a change back to that address.
        if pii is not None and pii.email:
            current = {_normalise(pii.email)}
        elif user.email:
            current = {_normalise(user.email)}
        else:
            current = set()
        current.discard("")

        # Visible on purpose. See the module docstring.
        if normalised in current or not normalised:
            return EmailChangePreparation(ADDRESS_TAKEN)

        owner = self._users.get_user_by_email(session, normalised)
        if owner is not None:
            return EmailChangePreparation(ADDRESS_TAKEN)

        held = session.execute(
            select(UserPII.user_id)
            .join(User, User.id == UserPII.user_id)
            .where(
                func.lower(UserPII.pending_email) == normalised,
                UserPII.user_id != user.id,
                User.deleted_at.is_(None),
            )
        ).first()
        if held is not None:
            return EmailChangePreparation(ADDRESS_TAKEN)

        if pii is None:
            return EmailChangePreparation(INVALID)

        now = datetime.now(UTC)
        session.execute(
            update(EmailChangeRequest)
            .where(
                EmailChangeRequest.user_id == user.id,
                EmailChangeRequest.used_at.is_(None),
            )
            .values(used_at=now)
        )

        raw = secrets.token_urlsafe(TOKEN_BYTES)
        # The address lives on ``user_pii.pending_email`` only. The token row
        # stays a hash so the nightly dump cannot replay the link.
        pii.pending_email = normalised
        session.add(
            EmailChangeRequest(
                user_id=user.id,
                token_hash=_hash(raw),
                expires_at=now + timedelta(hours=TOKEN_TTL_HOURS),
            )
        )
        session.flush()
        return EmailChangePreparation(READY, raw)

    def validate_token(self, session: Session, raw_token: str) -> EmailChangeRequest | None:
        """Return the row if the link is live. **Does not consume it.**

        Mail scanners fetch every URL in an inbound message. A validating GET
        that burned the token would make the recipient's own click fail.
        """
        if not raw_token:
            return None
        row = session.execute(
            select(EmailChangeRequest).where(
                EmailChangeRequest.token_hash == _hash(raw_token),
                EmailChangeRequest.used_at.is_(None),
            )
        ).scalar_one_or_none()
        if row is None:
            return None
        expires_at = _aware(row.expires_at)
        if expires_at is None or expires_at <= datetime.now(UTC):
            return None
        return row

    def confirm_change(self, session: Session, raw_token: str) -> User | None:
        """Apply the pending address, once, and mark it verified.

        ``None`` means the link is dead, or the address was claimed by someone
        else between the request and the click. The live address is not written
        on that path.
        """
        row = self.validate_token(session, raw_token)
        if row is None:
            return None
        user = self._users.get_user(session, row.user_id)
        if user is None or not user.is_active or user.deleted_at is not None:
            return None
        pii = self._users.get_user_pii(session, user.id)
        if pii is None or not pii.pending_email:
            return None

        normalised = _normalise(pii.pending_email)
        owner = self._users.get_user_by_email(session, normalised)
        if owner is not None and owner.id != user.id:
            if self._claim(session, row):
                pii.pending_email = None
                session.flush()
            return None

        if not self._claim(session, row):
            return None

        # The account address is the sidecar. ``users.email`` stays as it was.
        pii.email = normalised
        pii.pending_email = None
        # The click is the proof. Asking them to verify the same address again
        # is a second copy of this flow.
        user.email_verified = True
        session.flush()
        return user

    def cancel_change(self, session: Session, user_id: int) -> bool:
        """Drop the pending address and burn every outstanding link."""
        now = datetime.now(UTC)
        session.execute(
            update(EmailChangeRequest)
            .where(
                EmailChangeRequest.user_id == user_id,
                EmailChangeRequest.used_at.is_(None),
            )
            .values(used_at=now)
        )
        pii = self._users.get_user_pii(session, user_id)
        if pii is not None:
            pii.pending_email = None
        session.flush()
        return True

    @staticmethod
    def _claim(session: Session, row: EmailChangeRequest) -> bool:
        claimed = session.execute(
            update(EmailChangeRequest)
            .where(
                EmailChangeRequest.id == row.id,
                EmailChangeRequest.used_at.is_(None),
            )
            .values(used_at=datetime.now(UTC))
        )
        return claimed.rowcount == 1
