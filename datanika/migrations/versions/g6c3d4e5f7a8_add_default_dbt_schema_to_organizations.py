"""add default_dbt_schema to organizations

Revision ID: g6c3d4e5f7a8
Revises: f5b1c2d3e4f6
Create Date: 2026-02-24 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "g6c3d4e5f7a8"
down_revision: str | None = "f5b1c2d3e4f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "organizations",
        sa.Column(
            "default_dbt_schema", sa.String(255), nullable=False, server_default="datanika"
        ),
    )


def downgrade() -> None:
    op.drop_column("organizations", "default_dbt_schema")
