"""Add sso_enabled column to plans table

Revision ID: p5l2m3n4o6i7
Revises: o4k1l2m3n5h6
Create Date: 2026-03-24 20:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "p5l2m3n4o6i7"
down_revision: str | None = "o4k1l2m3n5h6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "plans",
        sa.Column("sso_enabled", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    op.execute("UPDATE plans SET sso_enabled = true WHERE slug = 'enterprise-monthly'")


def downgrade() -> None:
    op.drop_column("plans", "sso_enabled")
