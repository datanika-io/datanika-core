"""Seven tables' timestamps become NOT NULL and tz-aware (core#1069 release N+1).

⚠️ The line above is deliberately **ASCII**: alembic echoes a revision's first docstring
line into its own log, and this machine's console codec is cp1251 — an em-dash there comes
back as ``вЂ”`` in captured output. Cosmetic here, and the same trap that has truncated real
probe output mid-run (WORKFLOW_RULES §13 trap 4).

``TimestampMixin`` declares ``DateTime(timezone=True)``, ``server_default=func.now()`` and
``nullable=False``. Seven ``create_table`` calls disagree on all three, so the model has
been lying about **14 columns**: a ``None`` where the type says ``datetime``, a naive
datetime where the type says aware, and — until release N — an absent default.

⚠️ **The fix is not a mixin change.** Measured over the whole tree: **26 tables use the
mixin, 19 agree with it, 7 disagree.** Relaxing the mixin would make 19 correct
declarations wrong to accommodate 7 drifted ``create_table``s, and it addresses one of the
three disagreements. ``charges`` and ``users`` get all three right, which is what proves
the declaration achievable. This is drift in seven tables, not a design to rethink.

Release N — ``d7f2c8a4b1e6``, on `master` and verified on the serving colour — added the
missing ``updated_at`` default and backfilled the NULLs. **This is the contract half.**

expand-contract: contract phase for core#1069. The model declared
``nullable=False`` + ``timezone=True`` on these columns from the day it was written, so the
*code* has never needed a release to stop using the old shape — it is the schema catching
up. At ``t1`` the previously deployed container runs the same model against the tightened
schema: it cannot write NULL (nothing writes these columns at all, see below), and it reads
aware values through a column it already declares as ``DateTime(timezone=True)``. Release N
(``d7f2c8a4b1e6``) is the expand half and is already on `master`.

Why this is safe, in the order the evidence actually carries
------------------------------------------------------------
1. 🔑 **By construction, not by row count.** A source census across ``datanika/``,
   ``datanika_cloud/`` and ``scripts/`` finds **zero** writers of ``created_at`` or
   ``updated_at`` on these seven tables — no attribute assignment, no constructor kwarg on
   a mapped class, no raw SQL. (The four constructor kwargs that do exist are on pydantic
   *display* DTOs — ``InvitationItem``, ``ApiKeyItem`` and friends — whose ``created_at``
   is a ``str``.) Every value therefore comes from ``server_default=func.now()`` /
   ``onupdate=func.now()``, which **Postgres** evaluates, under ``TimeZone = UTC``. NULL is
   not producible. A local-time value is not producible. Guarded by
   ``tests/test_migrations/test_timestamp_writers_census.py``.
2. **The production gate is green** — Infra, 2026-09-05, read on the serving colour
   (``datanika-app-b``, ``alembic_version = d7f2c8a4b1e6``): all 14 columns, 0 NULLs. Armed
   by re-running the same census without the ``is_nullable = 'YES'`` filter, which returns
   54 columns (14 YES / 40 NO) — so an all-zero reading was not a broken query.
3. ⚠️ **Say the denominator.** That gate examined **11 rows across 3 tables** — ``plans``
   5, ``uploaded_files`` 3, ``usage_ledger`` 3. **Four of the seven hold zero rows** and
   pass *vacuously*. A ``SET NOT NULL`` on an empty table is safe and proves nothing; those
   four are genuinely tested by the first row anyone inserts. *"We verified seven tables"*
   would overstate it, and it would overstate it in both places it could be said — release
   N's own in-suite census examined **one** row (#1092).

Reader-side impact, because "nothing writes them" is only half the question
---------------------------------------------------------------------------
Censused: **no production code compares these columns to a datetime**, so nothing starts
raising ``can't compare offset-naive and offset-aware``. The one comparison in the tree is
``Notification.created_at >= since`` — ``notifications``, not ``notification_channels``.

Rendering: ``invitations`` goes out through ``strftime("%Y-%m-%d %H:%M")``, unchanged by
awareness. ``/api/v1`` serialises ``notification_channels`` with ``.isoformat()``, which
gains a ``+00:00`` suffix — and that makes the endpoint **consistent rather than
inconsistent**: ``connections``, ``uploads``, ``pipelines`` and ``runs`` are already
``timestamptz`` and already return the offset. Today the same API answers two ways.

The timezone half, and what each check can and cannot establish
----------------------------------------------------------------
``AT TIME ZONE 'UTC'`` **reinterprets** a stored naive value as UTC. If anything had ever
written local time (this box is EEST, UTC+3) the conversion would shift it silently, and
core#726 is the standing proof that a timestamp moved by a migration fails **open**.

Point 1 is the argument; the two pre-flight checks below are corroboration, and each is
honest about its reach. 🔴 Infra's nearest-neighbour probe was discarded **on its own
control** — fed a deliberate +3 h shift it returned **+1577 s, not +10800**, because the
join re-selects a different partner, so a *uniform* shift is invisible to it and its three
tidy near-zero deltas said nothing. The surviving FK-paired reading is **n = 1**.

🔴 **A third, same-row check was built and REMOVED** — see the block above ``upgrade()``.
It rested on an observation rather than an invariant and refused a correct database on its
first run outside this file's own tests. Recorded as a failed control rather than deleted,
because the rule it earned is the one worth keeping: *a pre-flight that can abort a
production deploy must rest on an invariant, or it will be deleted under pressure and take
the checks beside it with it.*
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f1a4c8e2d6b3"
down_revision: str | None = "e8b3d5c7f2a9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: The seven ``create_table`` calls that disagree with ``TimestampMixin``. Named here and
#: imported by the tests, so the DDL and its assertions cannot drift apart about scope.
TABLES: tuple[str, ...] = (
    "invitations",
    "notification_channels",
    "sso_configs",
    "uploaded_files",
    "plans",
    "subscriptions",
    "usage_ledger",
)

#: ``deleted_at`` is deliberately absent: it is nullable **by design** on every table
#: including the ones that get the mixin right, so it is not part of the disagreement.
COLUMNS: tuple[str, ...] = ("created_at", "updated_at")

#: How far ahead of ``now()`` a stored value may sit before it is treated as evidence of a
#: local-time write. Clock skew between the app and the database is the only legitimate
#: source of a positive delta here; the failure mode being detected is **three hours**.
_FUTURE_TOLERANCE = "1 minute"


def _preflight_census(conn) -> None:
    """Print rows beside NULLs, per table, before asserting anything about either.

    🔑 A count is not a measurement unless something says the rows were there to count. A
    table reading ``0 NULLs`` because it is empty and one reading ``0 NULLs`` because the
    backfill worked are the **same number** and warrant different confidence — and this is
    a ``SET NOT NULL`` on 14 columns, so the difference is the whole question. Release N's
    own census read as seven tables' worth of evidence while examining one row (#1092), and
    the restore drill printed ``PASS (plans=5)`` beside ``users=0``.
    """
    populated = 0
    parts = []
    for table in TABLES:
        rows = conn.execute(sa.text(f"SELECT count(*) FROM {table}")).scalar_one()  # noqa: S608
        populated += 1 if rows else 0
        parts.append(f"{table}={rows}")
    total = sum(int(p.split("=")[1]) for p in parts)
    print(
        f"[core#1069 pre-flight] rows={total} populated={populated}/{len(TABLES)} "
        f"columns={len(TABLES) * len(COLUMNS)} :: " + " ".join(parts)
    )


def _assert_no_nulls(conn) -> None:
    """Re-run Infra's step-2 gate at migration time — which is as *immediately before this
    ships* as it is possible to be.

    ``SET NOT NULL`` against a table holding a NULL fails anyway; what this adds is the
    **name of the offending column**. Postgres' own error names a constraint, and under the
    container start command a failure here is a failed deploy — the safe direction, and the
    one moment the message is read under pressure.
    """
    offenders = []
    for table in TABLES:
        for column in COLUMNS:
            nulls = conn.execute(
                sa.text(f"SELECT count(*) FROM {table} WHERE {column} IS NULL")  # noqa: S608
            ).scalar_one()
            if nulls:
                offenders.append(f"{table}.{column} ({nulls} row(s))")
    if offenders:
        raise RuntimeError(
            "core#1069: refusing to SET NOT NULL while these columns hold NULLs: "
            + ", ".join(offenders)
            + ". Release N (d7f2c8a4b1e6) backfills them; a row inserted after that "
            "backfill and before this migration is the case to look for."
        )


def _assert_nothing_lands_in_the_future(conn) -> None:
    """A value written in local time is *ahead* of the true instant, so a recently written
    row becomes future-dated the moment it is read as UTC.

    ⚠️ **This cannot see a uniform shift on an old row**, and it is not claimed to. The
    argument that no local time was ever written is the source census (no writers at all);
    this is the cheap control on it, and it costs one query per column.
    """
    offenders = []
    for table in TABLES:
        for column in COLUMNS:
            ahead = conn.execute(
                sa.text(  # noqa: S608
                    f"SELECT count(*) FROM {table} "
                    f"WHERE {column} AT TIME ZONE 'UTC' > now() + interval '{_FUTURE_TOLERANCE}'"
                )
            ).scalar_one()
            if ahead:
                offenders.append(f"{table}.{column} ({ahead} row(s))")
    if offenders:
        raise RuntimeError(
            "core#1069: refusing to convert to timestamptz — these values are in the "
            "FUTURE once read as UTC, which is what a local-time write looks like on a "
            f"box ahead of UTC: {', '.join(offenders)}. Do NOT relax this check; establish "
            "what wrote them first (core#726: a timestamp moved by a migration fails open)."
        )


# 🔴 A THIRD CHECK WAS BUILT HERE AND REMOVED. Kept as a named failure rather than
# deleted silently, because the reason generalises.
#
# `usage_ledger` carries `period_start`/`period_end` as `timestamptz` on the SAME ROW, so
# asserting `created_at` falls inside its own billing period needs no join — which is
# exactly what makes it attractive after Infra's nearest-neighbour probe failed its own
# control (a shifted value simply re-selects a different partner).
#
# It refused a CORRECT database on its first run outside this file's own tests:
# `test_data_preservation_roundtrip.py` seeds a `usage_ledger` row with an arbitrary
# `created_at` and an arbitrary period, and the check aborted the migration.
#
# 🔑 The real defect is not the harness. **"A ledger row is written inside its own billing
# period" is an OBSERVATION, not an invariant** — nothing in the schema or the code
# enforces it, `record_usage` merely computes the period from `now()`, and
# `billing/e2e_admin.py` writes ledger rows directly. A pre-flight that aborts a production
# deploy has to rest on an invariant. One that can refuse a correct database gets deleted
# under deploy pressure, and it takes the checks beside it with it.
#
# What remains is honest about its reach: `_assert_no_nulls` and
# `_assert_nothing_lands_in_the_future` are both invariants of `server_default=now()`, and
# the actual argument is the writers census — nothing writes these columns at all.
# Pinned by `test_a_ledger_row_outside_its_period_does_not_refuse_the_migration`.


def upgrade() -> None:
    conn = op.get_bind()
    _preflight_census(conn)
    _assert_no_nulls(conn)
    _assert_nothing_lands_in_the_future(conn)

    for table in TABLES:
        for column in COLUMNS:
            # `postgresql_using` is required: without it Postgres converts through the
            # SESSION time zone, so the same migration would produce different instants on
            # a box that is not on UTC. `AT TIME ZONE 'UTC'` RELABELS a naive value.
            op.alter_column(
                table,
                column,
                existing_type=sa.DateTime(),
                type_=sa.DateTime(timezone=True),
                postgresql_using=f"{column} AT TIME ZONE 'UTC'",
                nullable=False,
                existing_nullable=True,
                existing_server_default=sa.text("now()"),
            )


def downgrade() -> None:
    # Exactly inverse, and asserted as such by comparing VALUES rather than the schema:
    # core#726 round-tripped schema-identically and data-lossily, and only comparing values
    # caught it. `AT TIME ZONE 'UTC'` on a timestamptz yields the UTC wall clock, which is
    # what the naive column held before.
    for table in TABLES:
        for column in COLUMNS:
            op.alter_column(
                table,
                column,
                existing_type=sa.DateTime(timezone=True),
                type_=sa.DateTime(),
                postgresql_using=f"{column} AT TIME ZONE 'UTC'",
                nullable=True,
                existing_nullable=False,
                existing_server_default=sa.text("now()"),
            )
