"""Add runs.bytes_processed and usage_ledger.mode — the two metering dimensions.

Revision ID: b4c7d1e8f2a6
Revises: e7f2a9c4b1d8
Create Date: 2026-09-02 17:00:00.000000

core#910 and core#912, in one migration because both columns are expand-only,
neither is read by the currently deployed code, and **cloud has no migrations of
its own**: `usage_ledger` is a cloud model over a core-migrated table, so the
column has to be created here or it does not exist. Splitting them would put the
cloud model change one promotion ahead of the column it reads, which is the one
ordering this pair must never take.

``runs.bytes_processed`` (core#912 option (a)) — core computes a byte count on
every upload run today and then throws it away, handing it to a hook and keeping
no record. That is why ``datanika_bytes_processed_by_run`` has zero call sites
and why core#907's fix for the cloud counter does not carry over: core must never
import cloud, and ``usage_ledger`` is cloud's. A durable per-run byte count is
worth having on its own merits regardless of Prometheus — the run detail page can
show it, and support can answer "how big was that load" without the billing
plugin installed. Open-source core has no volume visibility at all today.

``BigInteger`` deliberately, for the reason core#283 gave for ``rows_loaded``
directly above it: an Enterprise backfill exceeds 2^31 comfortably, and bytes
exceed it four thousand times sooner than rows do.

``usage_ledger.mode`` (core#910) — ``SPEC_GB_THROUGHPUT_METRICS.md`` §3.1/§3.3
and ``SPEC_VOLUME_METERING.md`` §8 all specify ``{org_id, mode}`` on the bytes
counter, and §6's panel 5 is the one Prometheus-backed panel on the Volume
Metrics dashboard *because* — the spec's own words — "``mode`` is not on the
ledger row". core#907 made that counter a collector over ``usage_ledger``, so the
ledger is now the only source and the label cannot exist until the column does.
It is also worth having durably in its own right: "which ingestion mode produced
these bytes" is a question a billing dispute can ask.

**Expand-only.** Two nullable ``ADD COLUMN``s, no backfill, no ``SET NOT NULL``,
no index. Under blue/green the previously deployed code runs against this schema
and neither column is referenced by it, so the window is inert. Existing rows
keep ``NULL``, which the readers must treat as a real distinction — "written
before this column existed" — and not silently drop (core#910 acceptance 2).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b4c7d1e8f2a6"
down_revision: str | None = "e7f2a9c4b1d8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("bytes_processed", sa.BigInteger(), nullable=True))
    op.add_column("usage_ledger", sa.Column("mode", sa.String(length=16), nullable=True))


def downgrade() -> None:
    op.drop_column("usage_ledger", "mode")
    op.drop_column("runs", "bytes_processed")
