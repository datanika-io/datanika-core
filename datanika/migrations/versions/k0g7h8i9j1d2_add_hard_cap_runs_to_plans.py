"""Add hard_cap_runs column to plans table

Revision ID: k0g7h8i9j1d2
Revises: j9f6g7h8i0c1
Create Date: 2026-03-22 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "k0g7h8i9j1d2"
down_revision: str | None = "j9f6g7h8i0c1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _table_exists(table_name: str) -> bool:
    conn = op.get_bind()
    insp = inspect(conn)
    return table_name in insp.get_table_names()


def upgrade() -> None:
    if _table_exists("plans"):
        op.add_column(
            "plans",
            sa.Column("hard_cap_runs", sa.Boolean(), nullable=False, server_default="false"),
        )


def downgrade() -> None:
    if _table_exists("plans"):
        op.drop_column("plans", "hard_cap_runs")
