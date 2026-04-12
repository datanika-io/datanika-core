"""Add notifications table for in-app notification center

Revision ID: x3t0u1v2w4q5
Revises: w2s9t0u1v3p4
Create Date: 2026-04-12 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "x3t0u1v2w4q5"
down_revision: str | None = "w2s9t0u1v3p4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "notifications",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("org_id", sa.BigInteger(), sa.ForeignKey("organizations.id"), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "type",
            sa.Enum(
                "run_failed",
                "run_succeeded",
                "quota_warning",
                "quota_exceeded",
                name="notificationtype",
                native_enum=False,
            ),
            nullable=False,
        ),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("resource_type", sa.String(30), nullable=False),
        sa.Column("resource_id", sa.BigInteger(), nullable=False),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_notifications_org_id", "notifications", ["org_id"])

    # Force commit — Alembic env.py with SQLAlchemy 2.0 autobegin
    connection = op.get_bind()
    connection.commit()


def downgrade() -> None:
    op.drop_index("ix_notifications_org_id")
    op.drop_table("notifications")
