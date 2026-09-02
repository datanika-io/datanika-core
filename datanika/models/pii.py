"""Personal-data sidecar tables — one per parent, named ``<parent>_pii``.

``docs/specs/SPEC_PII_SEPARATION.md`` D1. Founder architecture decision, 2026-08-30:
*"Separate PII from internal data. GDPR-sensitive info goes into separate tables with a
FK to our internal IDs. Soft-delete users, hard-delete the sensitive info."*

Three properties of these tables are load-bearing, and each one is a decision:

**No ``TimestampMixin``.** Every other model here has ``created_at``/``updated_at``/
``deleted_at``. These deliberately do not, because ``deleted_at`` on a table whose whole
purpose is *hard* deletion is a trap: a soft-deleted row is still a row in Postgres, and
``deleted_at`` hides it from the application and from nobody else — not from ``pg_dump``,
not from a backup, not from a regulator. §0 states the split once so it is findable: *a
row that identifies a **person** is hard-deleted; a row that identifies a **record** is
soft-deleted.* Withholding the column means an erasure cannot accidentally be written as
a soft delete here.

**No ``TenantMixin``.** These are person-scoped, like ``users`` itself. An address belongs
to a person, not to an organization, and org-scoping it would make erasure depend on
which orgs someone happened to join.

**Shared PK/FK.** The foreign key *is* the primary key, so the relationship is 1:1 by
construction and there is no way to end up with two PII rows for one user — no surrogate
id, nothing to reconcile.

**Why per-parent rather than one polymorphic ``personal_data`` table.** A polymorphic
table cannot express ``email UNIQUE`` (which is the login constraint), cannot keep
per-column types, and turns every lookup into a key-name string match. The naming
convention earns its keep in exchange: ``*_pii`` is **greppable**, and two mechanisms
already derive from it rather than from a hand-maintained list —
``audit_service.PII_PAYLOAD_KEYS`` (D12.2) and the backup-export guard (§6). One
convention, two consumers.

🚨 **Adding a table here changes ``PII_PAYLOAD_KEYS``**, and a test pins that set's exact
contents. That is intentional: a new PII column should start being redacted out of audit
payloads on the day it exists, and the failing test is how the author finds out.
"""

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from datanika.models.base import Base, TimestampMixin


class UserPII(Base):
    """The personal data of a ``users`` row. Hard-deleted on erasure."""

    __tablename__ = "user_pii"

    user_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("users.id"), primary_key=True, autoincrement=False
    )

    #: The login identifier. UNIQUE moves here with the value (D2). Deleting this row
    #: therefore FREES the address for re-registration, which is the point rather than a
    #: side effect: keeping a tombstone — even a hash — to block re-registration would
    #: mean retaining a pseudonymous identifier that re-identifies the person on lookup,
    #: i.e. the exact thing they asked us to stop doing.
    email: Mapped[str] = mapped_column(String(320), nullable=False, unique=True)

    full_name: Mapped[str] = mapped_column(String(255), nullable=False)

    #: For OAuth this is the provider's subject identifier — pseudonymous, still Art. 4(1)
    #: personal data. For SAML/OIDC SSO it is **the email address, verbatim**
    #: (``services/sso_routes.py`` passes ``oauth_provider_id=email``), so it is a second
    #: copy of the column above under a name nobody greps for. That is why it is here.
    oauth_provider_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    #: D8: an email change in flight. The live ``email`` does not move until the link sent
    #: to the NEW address is followed, so an attacker with a live session cannot silently
    #: take the account over.
    pending_email: Mapped[str | None] = mapped_column(String(320), nullable=True)


class InvitationPII(Base):
    """The invitee's address — *their* personal data, on a row somebody else authored."""

    __tablename__ = "invitation_pii"

    invitation_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("invitations.id"), primary_key=True, autoincrement=False
    )
    email: Mapped[str] = mapped_column(String(320), nullable=False)


class NotificationChannelPII(Base):
    """The address or chat id an EMAIL/Telegram channel delivers to.

    It used to live inside ``notification_channels.config``, **in the same JSON column as
    the Slack webhook URL and the Telegram bot token** — one column mixing personal data
    with secrets, which makes both harder to reason about. Secrets stay in ``config``
    (they are an org property, not personal data, and folding them in would imply erasure
    must decrypt secrets); the recipient moves here.
    """

    __tablename__ = "notification_channel_pii"

    channel_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("notification_channels.id"), primary_key=True, autoincrement=False
    )
    recipient: Mapped[str] = mapped_column(String(320), nullable=False)


class EmailChangeRequest(Base, TimestampMixin):
    """A pending email change (D8), deliberately **not** a ``*_pii`` table.

    It is PII-free by construction, and that is why the name does not end in ``_pii``: the
    address this token refers to lives in ``user_pii.pending_email``, never in the token
    or in this row. So it contributes no key to ``PII_PAYLOAD_KEYS`` — ``pending_email``
    reaches that set through ``user_pii``, where the value actually is.

    Same shape as ``PasswordResetToken``, for the same reason: the nightly ``pg_dump``
    ships off-box and is retained 30 days, so what is stored must not be replayable.
    SHA-256 hex of the value in the emailed link, single-use via ``used_at``.
    """

    __tablename__ = "email_change_requests"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"), nullable=False)
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
