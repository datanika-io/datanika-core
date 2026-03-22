"""Add email_verified column to users table

Revision ID: l1h8i9j0k2e3
Revises: k0g7h8i9j1d2
Create Date: 2026-03-22 14:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "l1h8i9j0k2e3"
down_revision: Union[str, None] = "k0g7h8i9j1d2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "users",
        sa.Column("email_verified", sa.Boolean(), nullable=False, server_default="false"),
    )


def downgrade() -> None:
    op.drop_column("users", "email_verified")
