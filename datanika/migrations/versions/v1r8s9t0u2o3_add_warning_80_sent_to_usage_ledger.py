"""Add warning_80_sent column to usage_ledger

Revision ID: v1r8s9t0u2o3
Revises: u0q7r8s9t1n2
Create Date: 2026-04-10 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "v1r8s9t0u2o3"
down_revision: Union[str, None] = "u0q7r8s9t1n2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "usage_ledger",
        sa.Column("warning_80_sent", sa.Boolean(), nullable=False, server_default=sa.text("false")),
    )
    connection = op.get_bind()
    connection.commit()


def downgrade() -> None:
    op.drop_column("usage_ledger", "warning_80_sent")
    connection = op.get_bind()
    connection.commit()
