"""PasswordResetService — issue, validate and consume reset tokens (core#623).

See ``datanika/models/password_reset.py`` for why the token is stored and
hashed rather than signed. This module holds the two rules that are easy to get
wrong at the call site:

* **validation never consumes.** ``validate_token`` is what the page load calls,
  and it only reports whether the form should render. Corporate mail security
  (Defender for Office 365 Safe Links, Proofpoint, Barracuda) fetches every URL
  in an inbound message before the recipient sees it, so a GET that consumed the
  token would mean the scanner burns it and the user's own click always lands on
  "already used" — a bug that reproduces only for users at companies with mail
  scanning, i.e. exactly our target customer, and never for us.

* **consumption is atomic.** ``used_at`` is claimed with a conditional UPDATE
  and the row count is checked, so two simultaneous submits cannot both win.
"""

import hashlib
import secrets
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from datanika.models.password_reset import PasswordResetToken
from datanika.models.user import User
from datanika.services.user_service import UserService

TOKEN_TTL_MINUTES = 60

# 32 bytes of urlsafe base64 → 43 characters, ~256 bits. Enough that the token
# needs no stretching at rest, which is why SHA-256 is the right hash here.
TOKEN_BYTES = 32


def _hash(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _aware(value: datetime | None) -> datetime | None:
    """SQLite hands back naive datetimes; Postgres does not."""
    if value is None:
        return None
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


class PasswordResetService:
    def __init__(self, user_service: UserService):
        self._users = user_service

    # -- Bucket naming -------------------------------------------------

    @staticmethod
    def email_bucket(email: str) -> str:
        """Rate-limit bucket for one address, with the address hashed.

        Keying on the plaintext address would turn the Redis keyspace into a
        readable list of accounts — an enumeration oracle reachable by anyone
        who can read Redis, which is a much lower bar than reading the database.
        """
        normalised = (email or "").strip().lower()
        return f"pwreset:email:{hashlib.sha256(normalised.encode()).hexdigest()}"

    @staticmethod
    def ip_bucket(client_ip: str) -> str:
        return f"pwreset:ip:{client_ip}"

    @staticmethod
    def consume_bucket(client_ip: str) -> str:
        return f"pwreset:consume:{client_ip}"

    # -- Issue ---------------------------------------------------------

    def request_reset(self, session: Session, email: str) -> str | None:
        """Mint a token for ``email``. Returns the raw value, or ``None``.

        ``None`` means there is no account to reset — the caller must render
        exactly the same thing either way, or the response becomes an
        enumeration oracle.
        """
        user = self._users.get_user_by_email(session, email or "")
        if user is None or not user.is_active:
            return None

        # A new request supersedes every outstanding one. Two live tokens for
        # one account doubles the window an intercepted mail is useful for, and
        # users request again precisely when they think the first went astray.
        session.execute(
            update(PasswordResetToken)
            .where(
                PasswordResetToken.user_id == user.id,
                PasswordResetToken.used_at.is_(None),
            )
            .values(used_at=datetime.now(UTC))
        )

        raw = secrets.token_urlsafe(TOKEN_BYTES)
        session.add(
            PasswordResetToken(
                user_id=user.id,
                token_hash=_hash(raw),
                expires_at=datetime.now(UTC) + timedelta(minutes=TOKEN_TTL_MINUTES),
            )
        )
        session.flush()
        return raw

    # -- Validate (never consumes) -------------------------------------

    def validate_token(self, session: Session, raw_token: str) -> PasswordResetToken | None:
        """Return the row if the token is live. **Does not consume it.**"""
        if not raw_token:
            return None
        row = session.execute(
            select(PasswordResetToken).where(
                PasswordResetToken.token_hash == _hash(raw_token),
                PasswordResetToken.used_at.is_(None),
            )
        ).scalar_one_or_none()
        if row is None:
            return None
        expires_at = _aware(row.expires_at)
        if expires_at is None or expires_at <= datetime.now(UTC):
            return None
        return row

    # -- Consume -------------------------------------------------------

    def consume_token(self, session: Session, raw_token: str, new_password: str) -> User | None:
        """Set the password and burn the token. ``None`` if the token is dead.

        Password-rule failures raise ``UserServiceError`` **without** burning
        the token: a typo must not cost the user another round trip through
        their mailbox.
        """
        row = self.validate_token(session, raw_token)
        if row is None:
            return None

        user = self._users.get_user(session, row.user_id)
        if user is None or not user.is_active:
            return None

        # Validate before claiming, so a rejected password leaves the token live.
        self._users._validate_password(new_password)

        # Claim the token conditionally. Two submits racing on one token both
        # pass validate_token; only the one whose UPDATE matches a row wins.
        claimed = session.execute(
            update(PasswordResetToken)
            .where(
                PasswordResetToken.id == row.id,
                PasswordResetToken.used_at.is_(None),
            )
            .values(used_at=datetime.now(UTC))
        )
        if claimed.rowcount != 1:
            return None

        user.password_hash = self._users._auth.hash_password(new_password)
        user.password_changed_at = datetime.now(UTC)
        session.flush()
        return user

    # -- Retention -----------------------------------------------------

    @staticmethod
    def purge_expired(session: Session, retention_days: int = 30) -> int:
        """Delete tokens that expired more than ``retention_days`` ago.

        Rows stay around after expiry only so a user clicking a stale link gets
        the invalid-link page rather than a silent nothing; past that they are
        just hashes of dead capabilities accumulating in every nightly dump.
        """
        cutoff = datetime.now(UTC) - timedelta(days=retention_days)
        result = session.execute(
            delete(PasswordResetToken).where(PasswordResetToken.expires_at < cutoff)
        )
        session.flush()
        return result.rowcount or 0
