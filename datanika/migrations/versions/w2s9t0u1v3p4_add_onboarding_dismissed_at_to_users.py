"""Add onboarding_checklist_dismissed_at column to users

Revision ID: w2s9t0u1v3p4
Revises: v1r8s9t0u2o3
Create Date: 2026-04-10 20:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "w2s9t0u1v3p4"
down_revision: str | None = "v1r8s9t0u2o3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column(
            "onboarding_checklist_dismissed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    connection = op.get_bind()
    connection.commit()


def downgrade() -> None:
    op.drop_column("users", "onboarding_checklist_dismissed_at")
    connection = op.get_bind()
    connection.commit()
