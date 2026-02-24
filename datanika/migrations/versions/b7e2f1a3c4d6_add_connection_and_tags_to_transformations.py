"""add connection and tags to transformations

Revision ID: b7e2f1a3c4d6
Revises: d5d30abce35c
Create Date: 2026-02-18 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b7e2f1a3c4d6"
down_revision: str | None = "d5d30abce35c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "transformations",
        sa.Column("destination_connection_id", sa.BigInteger(), nullable=True),
    )
    op.create_foreign_key(
        "fk_transformations_connection",
        "transformations",
        "connections",
        ["destination_connection_id"],
        ["id"],
    )
    op.add_column(
        "transformations",
        sa.Column("tags", sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("transformations", "tags")
    op.drop_constraint("fk_transformations_connection", "transformations", type_="foreignkey")
    op.drop_column("transformations", "destination_connection_id")
