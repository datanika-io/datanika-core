"""AuditLog model for tracking user actions."""

import enum
from datetime import datetime

from sqlalchemy import JSON, BigInteger, DateTime, Enum, ForeignKey, String, func
from sqlalchemy.orm import Mapped, mapped_column

from datanika.models.base import Base, TenantMixin


class AuditAction(enum.StrEnum):
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    LOGIN = "login"
    LOGOUT = "logout"
    RUN = "run"


class AuditResourceType(enum.StrEnum):
    """The declared vocabulary of ``audit_logs.resource_type`` (core#1128).

    ⚠️ **This is NOT a database enum and must not become one.** The column stays
    ``String(100)``: rows written before this class existed carry values it does not
    govern, and ``Enum(..., native_enum=False)`` would make the *previously deployed*
    container raise ``LookupError`` when it reads one — on a page that lists rows for a
    whole org, so a single unknown value breaks the page for every reader mid-swap. That
    is the same t1 hazard that made core#1127 a one-string fix rather than a new member.

    What it exists for is the **reader**. ``/audit-logs`` filters by ``resource_type``
    against a list of options, and that list was hand-maintained beside a growing set of
    writers — so it drifted in both directions at once, which is the worst available
    outcome:

    * 7 of the 13 written types were **not filterable**, ``password`` and ``session``
      among them;
    * the filter offered ``membership``, which **nothing has ever written**, while
      ``member`` carried 7 of the writes.

    An admin asking *"who removed this person?"* picked the option that looked right and
    got an empty table. The record was there; the only instrument for reading it reported
    zero — and an empty audit table does not read as a broken filter, it reads as *"nobody
    did it"*. That is the second of the two failure modes SPEC_AUDIT_TRAIL §1 names, and
    the worse one: an absent log is not consulted, a lying one is believed.

    🔑 **Call sites still pass plain strings, deliberately.** Binding 36 literals to this
    enum would be a wider diff for the same guarantee: what actually stops the drift is
    ``tests/test_services/test_audit_call_site_vocabulary.py``, which fails when the
    written set and this class disagree in *either* direction. Rewriting the call sites
    would not add a check — it would only change where the same check reads from — and a
    typo blessed into this enum is equally possible under both shapes. So the property to
    preserve is the test, not the spelling at the call site.

    **Adding a type:** add the member here in the same PR as the writer. The filter picks
    it up with no further edit, which is the whole point; the guard fails if you add one
    without a writer, or a writer without one.
    """

    API_KEY = "api_key"
    CONNECTION = "connection"
    DEPENDENCY = "dependency"
    IMPORT = "import"
    MEMBER = "member"
    NOTIFICATION_CHANNEL = "notification_channel"
    ORG = "org"
    # noqa reason: S105 flags the *name* PASSWORD bound to a string literal. This is the
    # resource type of a password-change audit row, not a credential — the value is the
    # word itself and is written to `audit_logs.resource_type` verbatim.
    PASSWORD = "password"  # noqa: S105
    PIPELINE = "pipeline"
    SCHEDULE = "schedule"
    SESSION = "session"
    TRANSFORMATION = "transformation"
    UPLOAD = "upload"
    USER = "user"


class AuditLog(Base, TenantMixin):
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int | None] = mapped_column(BigInteger, ForeignKey("users.id"), nullable=True)
    action: Mapped[AuditAction] = mapped_column(
        Enum(AuditAction, native_enum=False, length=20), nullable=False
    )
    resource_type: Mapped[str] = mapped_column(String(100), nullable=False)
    resource_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    old_values: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    new_values: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    ip_address: Mapped[str | None] = mapped_column(String(45), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
