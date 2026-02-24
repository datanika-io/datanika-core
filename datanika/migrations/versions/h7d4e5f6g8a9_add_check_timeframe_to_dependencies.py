"""add check_timeframe to dependencies

Revision ID: h7d4e5f6g8a9
Revises: g6c3d4e5f7a8
Create Date: 2026-02-24 18:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "h7d4e5f6g8a9"
down_revision: str | None = "g6c3d4e5f7a8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "dependencies",
        sa.Column("check_timeframe_value", sa.Integer(), nullable=True),
    )
    op.add_column(
        "dependencies",
        sa.Column("check_timeframe_unit", sa.String(10), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("dependencies", "check_timeframe_unit")
    op.drop_column("dependencies", "check_timeframe_value")
