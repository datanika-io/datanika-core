"""Add rate_limit_rpm column to plans table

Revision ID: r7n4o5p6q8k9
Revises: q6m3n4o5p7j8
Create Date: 2026-04-09 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "r7n4o5p6q8k9"
down_revision: Union[str, None] = "q6m3n4o5p7j8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "plans",
        sa.Column("rate_limit_rpm", sa.Integer(), nullable=False, server_default=sa.text("60")),
    )
    # Set plan-specific rate limits
    op.execute("UPDATE plans SET rate_limit_rpm = 30 WHERE slug = 'free'")
    op.execute("UPDATE plans SET rate_limit_rpm = 120 WHERE slug = 'pro-monthly'")
    op.execute("UPDATE plans SET rate_limit_rpm = 300 WHERE slug = 'enterprise-monthly'")


def downgrade() -> None:
    op.drop_column("plans", "rate_limit_rpm")
