"""Widen notifications.type column from VARCHAR(14) to VARCHAR(50).

V2 P5 re-dry-run (2026-04-20) surfaced a silent-failure bug: the
original notifications-table migration (``x3t0u1v2w4q5``) declared
``type`` as ``sa.Enum(..., native_enum=False)`` without an explicit
``length``. SQLAlchemy sized the column to the longest enum value at
migration time — ``"quota_exceeded"`` (14 chars). When core#254 added
``charge_incoming`` (15 chars) the new value exceeded the column
width. Postgres raised ``DataError: value too long for type character
varying(14)``, but the notification subscriber's ``_with_session``
decorator caught and logged the exception (by design — notifier
failures must not break the emitting task). Net effect: cloud's
charge_cycle_overages reported success, ``usage_ledger.charge_incoming_sent``
flipped to True, but the user never saw the in-app banner or email.

Widening to VARCHAR(50) (not 32 — the model was updated to 50 for
headroom so we don't need another migration next time a longer type
name lands).

SQLite maps VARCHAR to TEXT regardless of length, so tests never saw
this. core#276 tracks a realistic-Postgres INSERT probe that would
have caught this class of bug.

Revision ID: c9d4e5f6g7h8
Revises: b8c3d4e5f6g7
Create Date: 2026-04-20 22:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c9d4e5f6g7h8"
down_revision: str | None = "b8c3d4e5f6g7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Metadata-only ALTER on Postgres — widening a VARCHAR's max length
    # does not rewrite existing rows. No lock beyond the brief ACCESS
    # EXCLUSIVE for the DDL itself. Safe to run live.
    op.alter_column(
        "notifications",
        "type",
        existing_type=sa.String(length=14),
        type_=sa.String(length=50),
        existing_nullable=False,
    )
    connection = op.get_bind()
    connection.commit()


def downgrade() -> None:
    # Narrowing back would fail if any row has a value longer than 14
    # chars — which is why this migration exists. Downgrade is only
    # safe after purging or rewriting those rows manually. Test suite
    # round-trips on an empty DB.
    op.alter_column(
        "notifications",
        "type",
        existing_type=sa.String(length=50),
        type_=sa.String(length=14),
        existing_nullable=False,
    )
    connection = op.get_bind()
    connection.commit()
