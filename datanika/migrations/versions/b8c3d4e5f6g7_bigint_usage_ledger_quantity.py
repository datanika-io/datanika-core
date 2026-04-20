"""Widen usage_ledger.quantity from int32 → int64.

V2 P5 Option B dry-run (2026-04-20) surfaced an overflow in staging:
Pro's ``bytes_included`` is 100 GB (≈ 107,374,182,400 bytes), well above
the int32 max of 2,147,483,647. Any real customer row overflowed on
insert with ``NumericValueOutOfRange``.

Other byte columns were already correctly sized:
- ``plans.bytes_included`` is ``bigint`` from z5v2w3x4y6a7 ✓
- ``charges.overage_quantity`` is ``bigint`` from a7b1c2d3e4f5 ✓

This migration only needs to widen the one column.

Revision ID: b8c3d4e5f6g7
Revises: a7b1c2d3e4f5
Create Date: 2026-04-20 16:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b8c3d4e5f6g7"
down_revision: str | None = "a7b1c2d3e4f5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Metadata-only change on Postgres when the column has no indexes that
    # depend on its type — a full table rewrite is not required. Safe to run
    # live (no lock beyond a brief ACCESS EXCLUSIVE for the DDL).
    op.alter_column(
        "usage_ledger",
        "quantity",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=False,
        existing_server_default=sa.text("0"),
    )
    connection = op.get_bind()
    connection.commit()


def downgrade() -> None:
    # The reverse narrowing would fail on rows where quantity > 2^31 - 1
    # (which is exactly the scenario this migration exists to unblock).
    # If a rollback is ever needed, callers must first purge or zero out
    # offending rows manually.
    op.alter_column(
        "usage_ledger",
        "quantity",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=False,
        existing_server_default=sa.text("0"),
    )
    connection = op.get_bind()
    connection.commit()
