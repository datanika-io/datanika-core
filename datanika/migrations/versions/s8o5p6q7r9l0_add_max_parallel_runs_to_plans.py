"""Add max_parallel_runs column to plans table

Revision ID: s8o5p6q7r9l0
Revises: r7n4o5p6q8k9
Create Date: 2026-04-10 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "s8o5p6q7r9l0"
down_revision: Union[str, None] = "r7n4o5p6q8k9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "plans",
        sa.Column("max_parallel_runs", sa.Integer(), nullable=False, server_default=sa.text("5")),
    )
    op.execute("UPDATE plans SET max_parallel_runs = 2 WHERE slug = 'free'")
    op.execute("UPDATE plans SET max_parallel_runs = 5 WHERE slug = 'pro-monthly'")
    op.execute("UPDATE plans SET max_parallel_runs = 20 WHERE slug = 'enterprise-monthly'")


def downgrade() -> None:
    op.drop_column("plans", "max_parallel_runs")
