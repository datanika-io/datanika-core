"""Add source_template_slug and template_first_run_fired_at to connections

Revision ID: y4u1v2w3x5z6
Revises: x3t0u1v2w4q5
Create Date: 2026-04-14 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "y4u1v2w3x5z6"
down_revision: str | None = "x3t0u1v2w4q5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "connections",
        sa.Column("source_template_slug", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "connections",
        sa.Column(
            "template_first_run_fired_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("connections", "template_first_run_fired_at")
    op.drop_column("connections", "source_template_slug")
