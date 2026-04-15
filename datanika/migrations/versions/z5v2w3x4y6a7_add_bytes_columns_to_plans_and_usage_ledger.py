"""Add bytes_included, overage_bytes_price_cents_per_gb, hard_cap_bytes to plans;
source_run_id to usage_ledger.

V2 P1 plumbing for SPEC_VOLUME_METERING.md §7 P1 (GB-processed billing dimension).
All columns are nullable or defaulted — existing rows untouched.

Revision ID: z5v2w3x4y6a7
Revises: y4u1v2w3x5z6
Create Date: 2026-04-15 20:30:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "z5v2w3x4y6a7"
down_revision: str | None = "y4u1v2w3x5z6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "plans",
        sa.Column("bytes_included", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "plans",
        sa.Column("overage_bytes_price_cents_per_gb", sa.Integer(), nullable=True),
    )
    op.add_column(
        "plans",
        sa.Column(
            "hard_cap_bytes",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
    )
    op.add_column(
        "usage_ledger",
        sa.Column("source_run_id", sa.BigInteger(), nullable=True),
    )
    op.create_index(
        "ix_usage_ledger_source_run_id",
        "usage_ledger",
        ["source_run_id"],
    )
    connection = op.get_bind()
    connection.commit()


def downgrade() -> None:
    op.drop_index("ix_usage_ledger_source_run_id", table_name="usage_ledger")
    op.drop_column("usage_ledger", "source_run_id")
    op.drop_column("plans", "hard_cap_bytes")
    op.drop_column("plans", "overage_bytes_price_cents_per_gb")
    op.drop_column("plans", "bytes_included")
    connection = op.get_bind()
    connection.commit()
