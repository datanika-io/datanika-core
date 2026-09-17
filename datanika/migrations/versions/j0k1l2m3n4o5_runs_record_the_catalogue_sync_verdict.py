"""``runs`` records what an upload's catalogue sync found -- core#1398.

`/models` names an upload whose most recent successful run did not catalogue its tables, and says
which of two things happened (``SPEC_EARNED_VERDICTS`` §4.6). The run's log already told them
apart, in prose. These two columns carry it as data:

* ``catalog_sync_verdict``: ``catalogued``, ``empty``, ``no_tables`` or ``unreadable``
  (``datanika.models.run.CatalogSyncVerdict``);
* ``catalog_sync_schema``: the schema the sync asked, recorded for ``no_tables``.

A plain ``VARCHAR`` holding the value, not an SQLAlchemy ``Enum``: a non-native ``Enum`` without
``values_callable`` stores member NAMES, which is core#1391's class.

**Expand/contract (SPEC_EXPAND_CONTRACT_MIGRATIONS).** Two nullable ``ADD COLUMN`` statements, with
no default, no backfill and no rewrite. The previously deployed code never names these columns, so
at ``t1`` it inserts and reads runs exactly as before. NULL means "not recorded": a run from before
this migration, or any run that is not an upload. The page reads NULL as "nothing known" and shows
no notice.

Revision ID: j0k1l2m3n4o5
Revises: i9j0k1l2m3n4
Create Date: 2026-09-17 10:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "j0k1l2m3n4o5"
down_revision: str | None = "i9j0k1l2m3n4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("catalog_sync_verdict", sa.String(length=32), nullable=True))
    op.add_column("runs", sa.Column("catalog_sync_schema", sa.String(length=255), nullable=True))


def downgrade() -> None:
    op.drop_column("runs", "catalog_sync_schema")
    op.drop_column("runs", "catalog_sync_verdict")
