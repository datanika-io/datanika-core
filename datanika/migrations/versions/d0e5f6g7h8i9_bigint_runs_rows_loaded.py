"""Widen runs.rows_loaded from int32 → int64.

Third of the V2 P5 int32-overflow fixes (core#283). Same shape as
b8c3d4e5f6g7 (usage_ledger.quantity, core#272) and c9d4e5f6g7h8
(notifications.type width, core#279). Surfaced by core#282's
realistic-size INSERT probe which had been landed as an xfail
against 3 billion rows — Enterprise clickstream / event / log
backfills routinely exceed 2^31 rows in a single pipeline run.

Chain of custody:
- ``dlt_runner._extract_rows_loaded(pipeline)`` returns the row count
- ``upload_tasks.run_upload_task`` passes it to
  ``execution_service.complete_run(..., rows_loaded=...)``
- ``complete_run`` writes ``run.rows_loaded = rows_loaded``
- INSERT → ``NumericValueOutOfRange: integer out of range`` on any
  count ≥ 2^31.

Audit of other ``runs`` columns (same pass):
- ``id``: Integer autoincrement PK — ~10 yrs headroom at current
  trajectory; widening deferred.
- ``target_id``: BigInteger ✓
- ``org_id``: BigInteger ✓ (via TenantMixin)

Metadata-only ``ALTER COLUMN`` on Postgres; no table rewrite.

Revision ID: d0e5f6g7h8i9
Revises: c9d4e5f6g7h8
Create Date: 2026-04-20 23:45:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d0e5f6g7h8i9"
down_revision: str | None = "c9d4e5f6g7h8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column(
        "runs",
        "rows_loaded",
        existing_type=sa.Integer(),
        type_=sa.BigInteger(),
        existing_nullable=True,
    )
    connection = op.get_bind()
    connection.commit()


def downgrade() -> None:
    # Narrowing back would fail on any row whose rows_loaded > 2^31 - 1
    # (the scenario that prompted the widening). Safe only on an empty
    # or purged DB — which is what ``test_roundtrip.py`` exercises.
    op.alter_column(
        "runs",
        "rows_loaded",
        existing_type=sa.BigInteger(),
        type_=sa.Integer(),
        existing_nullable=True,
    )
    connection = op.get_bind()
    connection.commit()
