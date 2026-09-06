"""Release N+1 of core#1069: the seven tables' timestamps become NOT NULL and tz-aware.

``TimestampMixin`` declares ``DateTime(timezone=True)``, ``server_default=func.now()`` and
``nullable=False``. Seven ``create_table`` calls disagree on all three, so the model has
been lying about **14 columns** for months: a ``None`` where the type says ``datetime``, a
naive datetime where the type says aware, and (until release N) an absent default.

Release N (``d7f2c8a4b1e6``, on `master` and on the box) added the missing ``updated_at``
default and backfilled. This is the **contract** half.

Why this is safe, in the order the evidence actually carries
------------------------------------------------------------
1. 🔑 **By construction, not by row count.** A source census across ``datanika/``,
   ``datanika_cloud/`` and ``scripts/`` finds **zero** writers of ``created_at`` or
   ``updated_at`` on these seven tables — no attribute assignment, no constructor kwarg,
   no raw SQL. Every value therefore comes from ``server_default=func.now()`` /
   ``onupdate=func.now()``, which **Postgres** evaluates, under ``TimeZone = UTC``. So
   NULL is not producible, and a local-time value is not producible.
   Pinned by ``tests/test_migrations/test_timestamp_writers_census.py``.
2. **The production gate is green** — Infra, 2026-09-05, on the serving colour
   (``alembic_version = d7f2c8a4b1e6``): all 14 columns report 0 NULLs.
3. ⚠️ **Say the denominator.** That gate examined **11 rows across 3 tables** — ``plans``
   5, ``uploaded_files`` 3, ``usage_ledger`` 3. The other **four of the seven hold zero
   rows**, so they pass *vacuously*: a green there is a statement about the schema, not
   about data. Four tables will be genuinely tested by the first row anyone inserts.
   ``SET NOT NULL`` on an empty table is safe and proves nothing.

The timezone half, and what the checks can and cannot establish
----------------------------------------------------------------
``AT TIME ZONE 'UTC'`` **reinterprets** stored naive values as UTC. If anything had ever
written local time (this box is EEST, UTC+3) the conversion would shift them silently, and
core#726 is the standing proof that a timestamp moved by a migration fails **open**.

Point 1 is the argument. The migration's own pre-flight checks are corroboration, and each
is honest about its reach:

* **no NULLs** — re-runs the gate at migration time, which is as "immediately before it
  ships" as it is possible to be. Names the offending ``table.column``.
* **nothing lands in the future** — a value written in local time is 3 hours *ahead* of the
  true instant, so for any recently-written row it becomes future-dated. ⚠️ It cannot see a
  uniform shift on an old row; that is what point 1 is for.
🔴 A third, same-row check on ``usage_ledger``'s billing period was built and
**removed** — it rested on an observation rather than an invariant and refused a correct
database. See ``test_a_ledger_row_outside_its_period_does_not_refuse_the_migration``, which
pins the removal so it cannot come back by instinct.

🔴 Infra's nearest-neighbour probe was discarded **on its own control**: fed a deliberate
+3 h shift it returned **+1577 s, not +10800**, because the join re-selects a different
partner. Its three tidy near-zero deltas therefore said nothing. The FK-paired
``usage_ledger → runs`` reading that survived is **n = 1**.

So the empirical half is thin on purpose, and it is not what carries this. **Point 1 is the
argument**; the future check is a cheap invariant-backed control on it, and the removed
ledger check is recorded as a failed one. A control that has to be kept honest about its
reach is worth more than one that reads as proof.
"""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text

from tests.test_migrations.conftest import _run_alembic

PARENT_REVISION = "e8b3d5c7f2a9"
THIS_REVISION = "f1a4c8e2d6b3"

#: The seven tables and 14 columns this migration tightens. Imported from the migration so
#: the test and the DDL cannot disagree about the scope — a restated list is what let seven
#: `create_table`s drift apart in the first place.
from datanika.migrations.versions.f1a4c8e2d6b3_timestamp_contract_on_seven_tables import (  # noqa: E402
    COLUMNS,
    TABLES,
)


@pytest.fixture
def at_parent(roundtrip_db_url):
    engine = create_engine(roundtrip_db_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    result = _run_alembic(["upgrade", PARENT_REVISION], roundtrip_db_url)
    assert result.returncode == 0, f"could not reach {PARENT_REVISION}: {result.stderr}"
    return engine


def _describe(engine, table: str, column: str) -> tuple[str, str, str | None]:
    with engine.begin() as conn:
        return conn.execute(
            text(
                "SELECT data_type, is_nullable, column_default "
                "FROM information_schema.columns "
                "WHERE table_name = :t AND column_name = :c"
            ),
            {"t": table, "c": column},
        ).one()


def _seed_org(conn) -> int:
    """`usage_ledger.org_id` has a real FK, so the ledger probes need a parent row."""
    return conn.execute(
        text("INSERT INTO organizations (name, slug) VALUES ('Probe', 'probe-org') RETURNING id")
    ).scalar_one()


def _insert_ledger(conn, created: str, period: tuple[str, str]) -> None:
    org_id = _seed_org(conn)
    conn.execute(
        text(
            "INSERT INTO usage_ledger (org_id, metric, quantity, period_start, period_end, "
            "created_at, updated_at) VALUES (:org, 'model_runs', 1, :start, :end, :ts, :ts)"
        ),
        {"org": org_id, "start": period[0], "end": period[1], "ts": created},
    )


def _insert_plan(conn, slug: str, **columns) -> None:
    """A ``plans`` row. ``overage_run_price_cents`` is NOT NULL with no default since
    ``c5e9a3b7d2f4``, so omitting it fails for an unrelated reason."""
    extra = list(columns)
    conn.execute(
        text(
            "INSERT INTO plans (name, slug, paddle_price_id, paddle_product_id, price_cents, "
            "interval, overage_run_price_cents"
            + "".join(f", {c}" for c in extra)
            + ") VALUES ('Probe', :slug, :slug, 'pro_probe', 100, 'monthly', 0"
            + "".join(f", :{c}" for c in extra)
            + ")"
        ),
        {"slug": slug, **columns},
    )


# ---------------------------------------------------------------------------
# Controls — without these, "the columns are correct" and "this file never
# reached the schema" are the same observation.
# ---------------------------------------------------------------------------


def test_control_every_column_is_naive_and_nullable_at_the_parent(at_parent):
    wrong = []
    for table in TABLES:
        for column in COLUMNS:
            data_type, is_nullable, _default = _describe(at_parent, table, column)
            if data_type != "timestamp without time zone" or is_nullable != "YES":
                wrong.append(f"{table}.{column}={data_type}/{is_nullable}")
    assert not wrong, (
        "these columns were already tightened at the parent revision, so every assertion "
        f"below would pass without this migration doing anything: {wrong}"
    )


def test_control_release_n_supplies_a_default_on_both_columns(at_parent):
    """Release N is the precondition. If its defaults are gone, the ``SET NOT NULL``
    below is resting on nothing and this file needs re-deriving rather than re-running."""
    for table in TABLES:
        for column in COLUMNS:
            _t, _n, default = _describe(at_parent, table, column)
            assert default is not None and "now()" in default, (
                f"{table}.{column} has no now() default at {PARENT_REVISION} — release N "
                "(d7f2c8a4b1e6) is not in effect"
            )


# ---------------------------------------------------------------------------
# The contract itself.
# ---------------------------------------------------------------------------


def test_all_fourteen_columns_become_not_null_and_tz_aware(at_parent, roundtrip_db_url):
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    wrong = []
    for table in TABLES:
        for column in COLUMNS:
            data_type, is_nullable, _default = _describe(at_parent, table, column)
            if data_type != "timestamp with time zone" or is_nullable != "NO":
                wrong.append(f"{table}.{column}={data_type}/{is_nullable}")
    assert not wrong, f"the model still disagrees with the database on: {wrong}"
    assert len(TABLES) * len(COLUMNS) == 14, "the scope moved; re-derive the issue's figures"


def test_the_defaults_survive_the_type_change(at_parent, roundtrip_db_url):
    """A type change rewrites the column, and a dropped default would silently restore
    the NULL-able behaviour release N added the default to prevent."""
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    for table in TABLES:
        for column in COLUMNS:
            _t, _n, default = _describe(at_parent, table, column)
            assert default is not None and "now()" in default, (
                f"{table}.{column} lost its server default across the type change"
            )


def test_a_stored_value_is_reinterpreted_as_utc_and_not_shifted(at_parent, roundtrip_db_url):
    """🚨 core#726's lesson: counts and NOT NULL both survive a shift. Only comparing
    **values** catches one.

    ``2026-08-31 21:42:51`` naive must come back as ``21:42:51+00:00`` — the same
    wall-clock reading, relabelled. If the conversion used the session's time zone rather
    than an explicit ``AT TIME ZONE 'UTC'``, a box on EEST would return ``18:42:51+00``.
    """
    stored = "2026-08-31 21:42:51"
    with at_parent.begin() as conn:
        _insert_plan(conn, "probe-utc", created_at=stored, updated_at=stored)

    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    with at_parent.begin() as conn:
        conn.execute(text("SET TIME ZONE 'UTC'"))
        value = conn.execute(
            text("SELECT created_at FROM plans WHERE slug = 'probe-utc'")
        ).scalar_one()

    assert value == datetime(2026, 8, 31, 21, 42, 51, tzinfo=UTC), (
        f"the stored instant moved: {value!r}. AT TIME ZONE 'UTC' must RELABEL a naive "
        "value, never convert it through the session time zone."
    )


def test_the_migration_states_how_many_rows_it_examined(at_parent, roundtrip_db_url):
    """Condition (b): state the denominator rather than letting "seven tables" imply more.

    A count is not a measurement unless something says the rows were there to count — the
    same defect release N's own census carried (#1092), and the shape the restore drill
    printed as ``PASS (plans=5)`` beside ``users=0``.
    """
    with at_parent.begin() as conn:
        _insert_plan(conn, "probe-census", created_at="2020-01-02 03:04:05")
        # Derived, not typed: `plans` already holds the `free` row `q6m3n4o5p7j8` inserts,
        # and hard-coding "1" here would be the same defect the census exists to prevent.
        expected = conn.execute(text("SELECT count(*) FROM plans")).scalar_one()
    assert expected >= 2, "the probe row did not land; the assertion below would be vacuous"

    result = _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url)
    assert result.returncode == 0
    output = result.stdout + result.stderr

    assert "core#1069 pre-flight" in output, (
        "the migration printed no census line at all, so its checks cannot be told apart "
        f"from checks that never ran:\n{output[-2000:]}"
    )
    assert f"rows={expected}" in output and "populated=1/7" in output, (
        f"the census did not report the rows it was given (expected rows={expected}, "
        f"one populated table of seven):\n{output[-2000:]}"
    )
    assert f"plans={expected}" in output, (
        f"the per-table breakdown disagrees with the total:\n{output[-2000:]}"
    )


# ---------------------------------------------------------------------------
# The pre-flight checks, armed against the states they exist to refuse. Each is
# vacuous on an empty database, which is exactly what these rows fix.
# ---------------------------------------------------------------------------


def test_a_null_refuses_the_migration_and_names_the_column(at_parent, roundtrip_db_url):
    """The failure `SET NOT NULL` would produce anyway, made legible.

    Postgres' own error names a constraint, not the row that caused it. Under the
    container start command this is a **failed deploy**, so it is the one moment the
    message is read under pressure.
    """
    with at_parent.begin() as conn:
        _insert_plan(conn, "probe-null", updated_at=None)

    result = _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url)
    assert result.returncode != 0, "a NULL did not stop the contract migration"
    assert "plans.updated_at" in result.stdout + result.stderr, (
        "the refusal did not name the offending column:\n" + (result.stdout + result.stderr)[-2000:]
    )

    _t, is_nullable, _d = _describe(at_parent, "plans", "created_at")
    assert is_nullable == "YES", "the migration tightened some columns before refusing"


def test_a_future_dated_value_refuses_the_migration(at_parent, roundtrip_db_url):
    """The timezone check, armed on the shape it exists for.

    A value written in this box's local time (EEST, UTC+3) is 3 hours ahead of the true
    instant, so a recently-written row becomes future-dated the moment it is read as UTC.
    """
    ahead = (datetime.now(UTC) + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
    with at_parent.begin() as conn:
        _insert_plan(conn, "probe-future", created_at=ahead, updated_at=ahead)

    result = _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url)
    assert result.returncode != 0, "a future-dated timestamp did not stop the migration"
    assert "plans.created_at" in result.stdout + result.stderr


def test_a_present_dated_value_does_not_refuse_the_migration(at_parent, roundtrip_db_url):
    """The other half of the future check, and the one that makes it a control.

    A pattern narrowed until it matches nothing also stops matching real failures. A row
    written *now* must pass — the check's tolerance is a minute, not a guess.
    """
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    with at_parent.begin() as conn:
        _insert_plan(conn, "probe-now", created_at=now, updated_at=now)

    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0


def test_a_ledger_row_outside_its_period_does_not_refuse_the_migration(at_parent, roundtrip_db_url):
    """🔴 Pins a check that was BUILT AND REMOVED, so it cannot come back by instinct.

    ``usage_ledger`` carries ``period_start``/``period_end`` as ``timestamptz`` on the
    **same row**, so asserting ``created_at`` falls inside its own billing period needs no
    join — attractive precisely because Infra's nearest-neighbour probe failed its own
    control when a shifted value re-selected a different partner.

    It refused a **correct** database on its first run outside this file:
    ``test_data_preservation_roundtrip.py`` seeds a ledger row with an arbitrary
    ``created_at`` and an arbitrary period, and the migration aborted.

    🔑 The harness is not the defect. *"A ledger row is written inside its own
    billing period"* is an **observation, not an invariant** — nothing enforces it,
    ``record_usage`` merely computes the period from ``now()``, and
    ``billing/e2e_admin.py`` writes ledger rows directly. **A pre-flight that can abort a
    production deploy must rest on an invariant**, or it gets deleted under pressure and
    takes the checks beside it with it.

    So this row must pass. If it ever starts failing, someone has re-added the check.
    """
    with at_parent.begin() as conn:
        _insert_ledger(
            conn,
            created="2026-09-01 02:00:00",
            period=("2026-08-01 00:00:00+00", "2026-09-01 00:00:00+00"),
        )

    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0, (
        "a usage_ledger row outside its own billing period aborted the migration. That "
        "relationship is an observation, not an invariant - see the block above upgrade() "
        "in f1a4c8e2d6b3."
    )


# ---------------------------------------------------------------------------
# Rollback.
# ---------------------------------------------------------------------------


def test_the_downgrade_restores_naive_nullable_columns_without_moving_a_value(
    at_parent, roundtrip_db_url
):
    """A rollback must be exactly inverse. core#726 round-tripped **schema-identically and
    data-lossily**, and only comparing values caught it."""
    stored = "2026-08-31 21:42:51"
    with at_parent.begin() as conn:
        _insert_plan(conn, "probe-round", created_at=stored, updated_at=stored)

    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    down = _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url)
    assert down.returncode == 0, f"downgrade failed: {down.stderr}"

    for table in TABLES:
        for column in COLUMNS:
            data_type, is_nullable, default = _describe(at_parent, table, column)
            assert data_type == "timestamp without time zone", f"{table}.{column} stayed aware"
            assert is_nullable == "YES", f"{table}.{column} stayed NOT NULL"
            assert default is not None and "now()" in default, f"{table}.{column} lost its default"

    with at_parent.begin() as conn:
        value = conn.execute(
            text("SELECT created_at FROM plans WHERE slug = 'probe-round'")
        ).scalar_one()
    assert value == datetime(2026, 8, 31, 21, 42, 51), (
        f"the round trip moved the stored instant: {value!r}"
    )

    # And back up, because a rollback is rarely the last thing that happens.
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
