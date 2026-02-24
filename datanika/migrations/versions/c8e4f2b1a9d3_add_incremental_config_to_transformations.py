"""add incremental_config to transformations

Revision ID: c8e4f2b1a9d3
Revises: b7e2f1a3c4d6
Create Date: 2026-02-18 14:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c8e4f2b1a9d3"
down_revision: str | None = "b7e2f1a3c4d6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "transformations",
        sa.Column("incremental_config", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("transformations", "incremental_config")
