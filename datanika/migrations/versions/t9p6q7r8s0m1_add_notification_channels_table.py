"""Add notification_channels table

Revision ID: t9p6q7r8s0m1
Revises: s8o5p6q7r9l0
Create Date: 2026-04-10 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "t9p6q7r8s0m1"
down_revision: Union[str, None] = "s8o5p6q7r9l0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "notification_channels",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column(
            "channel_type",
            sa.Enum("email", "slack", "telegram", "webhook", native_enum=False, length=20),
            nullable=False,
        ),
        sa.Column("config", sa.JSON(), nullable=False),
        sa.Column("events", sa.JSON(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("org_id", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.Column("deleted_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    connection = op.get_bind()
    connection.commit()


def downgrade() -> None:
    op.drop_table("notification_channels")
    connection = op.get_bind()
    connection.commit()
