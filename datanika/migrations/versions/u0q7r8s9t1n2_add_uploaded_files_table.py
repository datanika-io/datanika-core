"""Add uploaded_files table

Revision ID: u0q7r8s9t1n2
Revises: t9p6q7r8s0m1
Create Date: 2026-04-10 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "u0q7r8s9t1n2"
down_revision: str | None = "t9p6q7r8s0m1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "uploaded_files",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("original_name", sa.String(length=500), nullable=False),
        sa.Column("content_type", sa.String(length=100), nullable=False),
        sa.Column("file_size", sa.BigInteger(), nullable=False),
        sa.Column("file_hash", sa.String(length=64), nullable=False),
        sa.Column("archive_path", sa.Text(), nullable=False),
        sa.Column("org_id", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.Column("deleted_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    connection = op.get_bind()
    connection.commit()


def downgrade() -> None:
    op.drop_table("uploaded_files")
    connection = op.get_bind()
    connection.commit()
