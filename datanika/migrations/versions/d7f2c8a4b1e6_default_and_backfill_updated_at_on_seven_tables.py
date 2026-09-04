"""Give ``updated_at`` a default and backfill the NULLs — core#1069, release N (expand).

``TimestampMixin`` declares ``created_at``/``updated_at`` as ``DateTime(timezone=True)``,
``server_default=func.now()``, ``nullable=False``. Seven ``create_table`` calls disagree
with it on **three independent properties at once**, and each fails in a different
direction: a ``None`` where the type says ``datetime``, a naive datetime where the type
says aware, and an absent default where the model implies one.

🔴 **The fix is NOT a mixin change**, and getting that backwards would have made things
worse. Of the 22 core tables using the mixin, **17 already agree** with it. Loosening the
mixin to ``Mapped[datetime | None]`` would make those 17 wrong to accommodate the drifted
ones. ``charges`` (cloud) and ``users`` (core) get all three properties right, which is
what proves the declaration is achievable and *is* the intended shape.

This release closes disagreement 3 only
=======================================
``updated_at`` on these seven is nullable **with no default**, so any INSERT omitting it
stores NULL. Reproducible today: a raw ``INSERT INTO plans (...)`` omitting ``updated_at``
stores NULL against a real Postgres, and ``Plan.updated_at`` says that cannot happen.
``datanika/services/api_v1_routes.py`` already reads all seven of its ``updated_at``
values as ``x.updated_at.isoformat() if x.updated_at else None`` — the application has
been defending against this for as long as it has existed.

**Disagreements 1 and 2 are release N+1** and are deliberately absent here:

* ``SET NOT NULL`` on the 14 columns
* ``ALTER ... TYPE timestamptz USING <col> AT TIME ZONE 'UTC'`` on the same 14

Both are on ``SPEC_EXPAND_CONTRACT_MIGRATIONS``'s *never in the same release as the code
needing it* list. Under blue/green the **previously deployed** code runs against this
schema while the old container is still serving, and CI cannot catch a break there
because it only ever runs one version against one schema.

⚠️ **Before N+1 ships, Infra measures the remaining NULLs on production.** ``SET NOT NULL``
against a table holding a NULL fails the migration, which under the container start
command fails the deploy and leaves the old colour serving. That is the safe failure, but
it should be a measurement rather than a surprise. This migration is what makes that
measurement come back zero.

⚠️ **The timezone half is the one to think hardest about.** ``AT TIME ZONE 'UTC'``
*reinterprets* stored naive values as UTC. If anything ever wrote local time into these
columns the conversion shifts them silently — and core#726 is the standing proof that a
timestamp moved by a migration fails **open** rather than loudly.

Expand/contract compliance
==========================
Both operations here are on the safe-now list.

* ``SET DEFAULT`` governs **future INSERTs only**. It rewrites no existing row and no
  type. The previously deployed code either supplies a value (unchanged) or omits one and
  now receives ``now()`` instead of ``NULL`` — which its own ``if x.updated_at else None``
  reads already handle, and which the ORM model was expecting all along.
* The backfill writes ``datetime`` where there was ``NULL``, into a column the deployed
  models already type as ``Mapped[datetime]``. Nothing narrows, nothing is dropped, no
  constraint is added.

``created_at``, not ``now()``
============================
🔑 A NULL ``updated_at`` is backfilled from the row's own ``created_at``. ``now()`` would
assert that every untouched row was modified at migration time — false, and exactly the
class of quiet rewrite core#726 is the standing example of (that one re-backfilled
``password_changed_at`` from ``created_at`` and moved a session-revocation baseline
*backwards*). The difference is what the column means: ``password_changed_at`` is a
security baseline and moving it opens a window, while ``updated_at`` is a modification
timestamp and a row never updated since creation is honestly described by its
``created_at``. Nothing in the tree reads ``updated_at`` for an authorization decision —
the seven readers are all API serialization.

``created_at`` is nullable on these seven too, but unlike ``updated_at`` it *does* carry a
default, so it is only NULL where something wrote one explicitly. It is repaired in the
same statement because release N+1 puts ``SET NOT NULL`` on **both** columns, and a NULL
left here is a failed deploy there.

Batching
========
⚠️ The loop bounds each **statement**, not the transaction. ``autocommit_block()`` is
unavailable to every migration in this tree — ``env.py`` opens the transaction before
alembic does (**core#933**) — so there is no commit between batches yet. Bounding the
statement is still worth having, and the loop becomes a genuinely incremental backfill for
free once #933 is fixed.

The batch predicate uses ``ctid`` rather than a primary key: it needs no assumption about
what each of the seven tables calls its PK, and it is universal in Postgres. These
migrations only ever run against Postgres — model tests build their schema with
``Base.metadata.create_all`` and never invoke alembic.

Revision ID: d7f2c8a4b1e6
Revises: c5e9a3b7d2f4
Create Date: 2026-09-04

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d7f2c8a4b1e6"
down_revision: str | None = "c5e9a3b7d2f4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The seven tables core#1069 measured — four in core, three in cloud. Cloud has no
#: migration tree of its own (its three tables are created by core migrations), which is
#: why one core migration reaches all seven.
#:
#: ⚠️ Do not trust this list on its own. ``tests/test_migrations/
#: test_d7f2_timestamp_defaults_expand.py`` re-derives the drifted set from
#: ``information_schema`` at the parent revision and fails if it is not exactly these —
#: a retyped list is how this defect happened, and core#1060 is the same shape one tier
#: over.
TABLES: tuple[str, ...] = (
    "invitations",
    "notification_channels",
    "sso_configs",
    "uploaded_files",
    "plans",
    "subscriptions",
    "usage_ledger",
)

#: Rows per statement. Small enough to bound the work, large enough that these tables are
#: one pass each in practice.
_BATCH = 5_000


def _backfill(table: str) -> None:
    """Repair NULL timestamps, one bounded statement at a time.

    Both assignments read the row's **pre-update** values, so ``updated_at`` takes the
    original ``created_at`` even though ``created_at`` is repaired in the same statement.
    Every row the predicate matches is left non-NULL by it (``now()`` cannot be NULL), so
    the loop always terminates.
    """
    conn = op.get_bind()
    statement = sa.text(
        f"""
        UPDATE {table}
           SET updated_at = COALESCE(updated_at, created_at, now()),
               created_at = COALESCE(created_at, now())
         WHERE ctid IN (
                 SELECT ctid FROM {table}
                  WHERE updated_at IS NULL OR created_at IS NULL
                  LIMIT {_BATCH}
               )
        """
    )
    while True:
        affected = conn.execute(statement).rowcount
        if not affected:
            break


def upgrade() -> None:
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} ALTER COLUMN updated_at SET DEFAULT now()")
        _backfill(table)


def downgrade() -> None:
    """Drop the defaults again.

    ⚠️ **The backfill is not undone, and could not be.** Which rows held NULL is not
    recoverable once they hold a value, and inventing a rule to re-NULL them would delete
    real data on any row the application wrote in between. Dropping the default restores
    the schema exactly; the rows stay repaired, which is the direction that cannot hurt.

    This is not marked ``one_way``: the data-preservation round trip seeds every row with
    an explicit ``updated_at``, so nothing is NULL when it snapshots and the re-upgrade's
    backfill is a no-op. Marking it would switch **both** round-trip guards off for the
    whole release, which is a far bigger loss than this asymmetry.
    """
    for table in TABLES:
        op.execute(f"ALTER TABLE {table} ALTER COLUMN updated_at DROP DEFAULT")
