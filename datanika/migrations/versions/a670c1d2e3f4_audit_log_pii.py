"""Create audit_log_pii, the sign-in address sidecar (core#670).

Expand: one new table. ``audit_logs`` is not altered. The legacy ``ip_address``
column stays. Nothing here is a drop, a rename, or a new NOT NULL on a column
that already exists.

Revision ID: a670c1d2e3f4
Revises: c3a8e1f4b7d2
Create Date: 2026-11-20
"""

import sqlalchemy as sa
from alembic import op

revision: str = "a670c1d2e3f4"
down_revision: str | None = "c3a8e1f4b7d2"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.create_table(
        "audit_log_pii",
        sa.Column(
            "audit_log_id",
            sa.BigInteger(),
            sa.ForeignKey("audit_logs.id"),
            nullable=False,
        ),
        sa.Column("ip_address", sa.String(length=45), nullable=False),
        sa.PrimaryKeyConstraint("audit_log_id"),
    )


def downgrade() -> None:
    op.drop_table("audit_log_pii")
