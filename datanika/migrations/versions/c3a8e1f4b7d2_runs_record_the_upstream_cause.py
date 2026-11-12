"""``runs`` records which upstream success started an unscheduled downstream.

SPEC_DBT_PROJECT_AND_DOWNSTREAM. The follow-up must ignore the success that
caused the in-flight run. A timestamp is not enough: the cause can finish in
the same second the downstream row is inserted, and SQLite's ``now()`` is
often that coarse. ``caused_by_run_id`` is that memory.

**Expand/contract (SPEC_EXPAND_CONTRACT_MIGRATIONS).** One nullable
``ADD COLUMN``, no default, no foreign key, no uniqueness, no backfill. The
previously deployed worker inserts runs without naming the column, and NULL
is the honest value for a cron run, a manual run, and every run from before
this migration.

Revision ID: c3a8e1f4b7d2
Revises: b6c1d7e3f9a4
Create Date: 2026-11-12 18:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c3a8e1f4b7d2"
down_revision: str | None = "b6c1d7e3f9a4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("caused_by_run_id", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    op.drop_column("runs", "caused_by_run_id")
