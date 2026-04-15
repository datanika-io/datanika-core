"""Add mode column to uploads and pipelines.

V2 P1 IR scaffolding (SPEC_ELT_IR_ARCHITECTURE.md §5.6 / §11).
Source → IR → {dlt ETL, dbt ELT} dispatch flag. Defaults to 'etl' for
existing rows so nothing changes at runtime until the ELT builders land
in P3.

Revision ID: z6a1b2c3d4e5
Revises: z5v2w3x4y6a7
Create Date: 2026-04-16 00:05:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "z6a1b2c3d4e5"
down_revision: str | None = "z5v2w3x4y6a7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "uploads",
        sa.Column(
            "mode",
            sa.String(length=20),
            nullable=False,
            server_default="etl",
        ),
    )
    op.add_column(
        "pipelines",
        sa.Column(
            "mode",
            sa.String(length=20),
            nullable=False,
            server_default="etl",
        ),
    )
    connection = op.get_bind()
    connection.commit()


def downgrade() -> None:
    op.drop_column("pipelines", "mode")
    op.drop_column("uploads", "mode")
    connection = op.get_bind()
    connection.commit()
