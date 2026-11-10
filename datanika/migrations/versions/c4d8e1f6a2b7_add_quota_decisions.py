"""quota_decisions — a durable bytes-quota decision.

Expand-only ``CREATE TABLE``. The previously deployed code never names this
table, so at ``t1`` it keeps reading and writing the tables it already has.
Nothing in this revision alters an existing table.

The row is written by the cloud plugin when a bytes-quota check would reject
a run, including the dry-run arm. ``limit`` is the plan allowance. The name
is reserved in PostgreSQL; the column is created under that name and the
dialect quotes it. No unique constraint: the record is append-only, and a
new UNIQUE is not expand-safe.

https://github.com/datanika-io/datanika-cloud/issues/146

Revision ID: c4d8e1f6a2b7
Revises: j0k1l2m3n4o5
Create Date: 2026-11-10 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c4d8e1f6a2b7"
down_revision: str | None = "j0k1l2m3n4o5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "quota_decisions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True, nullable=False),
        sa.Column(
            "org_id",
            sa.BigInteger(),
            sa.ForeignKey("organizations.id"),
            nullable=False,
        ),
        sa.Column("metric", sa.String(length=50), nullable=False),
        sa.Column("decision", sa.String(length=16), nullable=False),
        sa.Column("reason", sa.String(length=32), nullable=False),
        sa.Column("predicted", sa.BigInteger(), nullable=True),
        sa.Column("usage", sa.BigInteger(), nullable=False),
        sa.Column("limit", sa.BigInteger(), nullable=False),
        sa.Column("plan_name", sa.String(length=100), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_quota_decisions_org_id", "quota_decisions", ["org_id"])


def downgrade() -> None:
    op.drop_index("ix_quota_decisions_org_id", table_name="quota_decisions")
    op.drop_table("quota_decisions")
