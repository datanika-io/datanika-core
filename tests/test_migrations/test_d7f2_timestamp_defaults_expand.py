"""``updated_at`` must stop being nullable-with-no-default on seven tables — core#1069.

This is **release N (expand)** of the chain scoped on core#1069. It does the two things
[`SPEC_EXPAND_CONTRACT_MIGRATIONS`](../../docs/specs/SPEC_EXPAND_CONTRACT_MIGRATIONS.md)
lists as safe-now — add a default, backfill in batches — and deliberately does **none** of
the three it lists as never-in-the-same-release: no ``SET NOT NULL``, no type change, no
new constraint. Those are release N+1, after Infra has measured the remaining NULLs on
production.

🔴 **The framing this corrects.** The inherited handoff said the durable fix was making
``created_at``/``updated_at`` nullable *in core's ``TimestampMixin``*. Measured against
the shipped tree: **17 of 22** tables using the mixin already agree with it (NOT NULL on
both sides). Loosening the mixin would make 17 correct tables wrong to accommodate the
drifted ones, and would address only one of **three** independent disagreements.
``charges`` and ``users`` get all three right, which is what proves the mixin's
declaration is achievable and is the intended shape. **The mixin is right; seven
``create_table`` calls are wrong.**

What this release fixes, and what it does not
---------------------------------------------
Of core#1069's three disagreements, this closes **only the third** — ``updated_at``
nullable *with no default*, so any INSERT that omits it stores NULL into a column the
model types ``Mapped[datetime]``. That one is reproducible today: a raw
``INSERT INTO plans (...)`` omitting ``updated_at`` stores NULL against a real Postgres.

Disagreement 1 (``timestamp`` vs ``timestamptz``) and disagreement 2 (``SET NOT NULL``)
are the contract half and stay open. ⚠️ The timezone change is the one to think hardest
about: ``AT TIME ZONE 'UTC'`` reinterprets stored naive values, and core#726 is the
standing proof that a timestamp moved by a migration fails **open** rather than loudly.

Why the seven are derived here and not retyped
----------------------------------------------
``_tables_missing_updated_at_default`` reads the property out of ``information_schema``
rather than trusting the list in the migration. A retyped list is what produced this
defect in the first place — and core#1060 taught the sharper version: *a tier nobody ever
wrote an UPDATE for is more wrong and less visible than one somebody tried to correct*.
So the assertion is a **positive statement of what the catalogue must hold**, in both
directions: exactly these seven before, and **none at all** after. An eighth table that
drifts in later fails the "before" assertion, which is the point.

Why a real Postgres
-------------------
Model-level tests build their schema from ``Base.metadata.create_all``, so they go green
the moment the *model* is right, whatever the database holds — and the whole of core#1069
is the model and the database disagreeing. Only alembic against a real Postgres can say
what the **column** does.
"""

import pytest
from sqlalchemy import create_engine, text

from tests.test_migrations.conftest import _run_alembic

PARENT_REVISION = "c5e9a3b7d2f4"
THIS_REVISION = "d7f2c8a4b1e6"

#: The seven tables core#1069 measured, four in core and three in cloud. Cloud has no
#: migration tree of its own — its three tables are created by core migrations — which is
#: why one core migration reaches all seven.
EXPECTED_TABLES = {
    "invitations",
    "notification_channels",
    "sso_configs",
    "uploaded_files",
    "plans",
    "subscriptions",
    "usage_ledger",
}

#: Tables that get all three properties right. They are the controls: this migration must
#: leave them exactly as it found them, and their existence is what makes "seven drifted
#: `create_table`s" the diagnosis rather than "the mixin is unachievable".
CONTROL_TABLES = ("users", "charges")

_TIMESTAMPED_WITHOUT_DEFAULT = text(
    """
    SELECT u.table_name
      FROM information_schema.columns u
      JOIN information_schema.columns c
        ON c.table_schema = u.table_schema
       AND c.table_name = u.table_name
       AND c.column_name = 'created_at'
     WHERE u.table_schema = 'public'
       AND u.column_name = 'updated_at'
       AND u.column_default IS NULL
    """
)


def _tables_missing_updated_at_default(engine) -> set[str]:
    """Tables carrying both timestamps where ``updated_at`` has no column default.

    Derived from the catalogue rather than from the models, deliberately: three of the
    seven are cloud tables, and ``datanika_cloud`` is not importable from core's venv. A
    model-side derivation would silently cover four of seven and report clean.
    """
    with engine.begin() as conn:
        return {row[0] for row in conn.execute(_TIMESTAMPED_WITHOUT_DEFAULT)}


def _updated_at(engine, table, where_sql, params=None):
    with engine.begin() as conn:
        return conn.execute(
            text(f"SELECT updated_at FROM {table} WHERE {where_sql}"), params or {}
        ).scalar_one()


#: A probe row in ``plans``. ``plans`` is used because it has no FK parents, so a row can
#: be made in one statement — the timestamp property under test is identical on all seven.
#:
#: ⚠️ ``overage_run_price_cents`` is stated on purpose and its absence is not an option:
#: ``c5e9a3b7d2f4`` — this migration's own parent — dropped that column's default so an
#: INSERT omitting a price is **refused** rather than silently priced at a cent a run
#: (cloud#177). The first version of this file omitted it and four tests died on a
#: ``NotNullViolation``, which is that guard working on its author.
_INSERT_PROBE = (
    "INSERT INTO plans (name, slug, paddle_price_id, paddle_product_id, price_cents, "
    "interval, overage_run_price_cents{extra_cols}) VALUES "
    "('Probe', :slug, 'pri_probe', 'pro_probe', 100, 'monthly', 0{extra_vals})"
)


def _insert_probe(conn, slug, **columns):
    """Insert a probe row, naming any extra columns the caller wants to control."""
    extra = list(columns)
    conn.execute(
        text(
            _INSERT_PROBE.format(
                extra_cols="".join(f", {c}" for c in extra),
                extra_vals="".join(f", :{c}" for c in extra),
            )
        ),
        {"slug": slug, **columns},
    )


@pytest.fixture
def at_parent(roundtrip_db_url):
    """A database migrated to the revision immediately before this one."""
    engine = create_engine(roundtrip_db_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    result = _run_alembic(["upgrade", PARENT_REVISION], roundtrip_db_url)
    assert result.returncode == 0, f"could not reach {PARENT_REVISION}: {result.stderr}"
    return engine


def _upgrade(engine, url):
    result = _run_alembic(["upgrade", THIS_REVISION], url)
    assert result.returncode == 0, f"upgrade to {THIS_REVISION} failed: {result.stderr}"
    return engine


# ---------------------------------------------------------------------------
# 1. The catalogue, in both directions
# ---------------------------------------------------------------------------


def test_control_exactly_these_seven_lack_a_default_at_the_parent(at_parent):
    """🔑 Arming **and** census. If this ever reads a different set, the migration's own
    table list is stale and the "after" assertion below would pass while doing less."""
    measured = _tables_missing_updated_at_default(at_parent)
    assert measured == EXPECTED_TABLES, (
        f"the drifted set is {sorted(measured)}, not the seven core#1069 measured. "
        "Update the migration's TABLES and this constant together — an eighth table "
        "that drifted in is exactly what this assertion exists to surface."
    )


def test_after_the_migration_no_timestamped_table_lacks_the_default(at_parent, roundtrip_db_url):
    """The criterion. Not 'the seven are fixed' — **none are left**, which is a statement
    the next drifted table also fails."""
    _upgrade(at_parent, roundtrip_db_url)
    assert _tables_missing_updated_at_default(at_parent) == set()


def test_the_controls_are_untouched(at_parent, roundtrip_db_url):
    """``users`` and ``charges`` already had the default; this must be a no-op for them."""
    with at_parent.begin() as conn:
        before = {
            t: conn.execute(
                text(
                    "SELECT column_default FROM information_schema.columns WHERE "
                    "table_schema='public' AND table_name=:t AND column_name='updated_at'"
                ),
                {"t": t},
            ).scalar_one()
            for t in CONTROL_TABLES
        }
    assert all(v is not None for v in before.values()), (
        f"a control table had no default at the parent revision: {before}. These two are "
        "the evidence that the mixin's declaration is achievable — if they have drifted, "
        "core#1069's diagnosis needs re-deriving before this migration is trusted."
    )

    _upgrade(at_parent, roundtrip_db_url)

    with at_parent.begin() as conn:
        after = {
            t: conn.execute(
                text(
                    "SELECT column_default FROM information_schema.columns WHERE "
                    "table_schema='public' AND table_name=:t AND column_name='updated_at'"
                ),
                {"t": t},
            ).scalar_one()
            for t in CONTROL_TABLES
        }
    assert after == before, f"the migration rewrote a control table's default: {before} -> {after}"


# ---------------------------------------------------------------------------
# 2. Behaviour — what an INSERT that omits the column actually stores
# ---------------------------------------------------------------------------


def test_control_an_insert_omitting_updated_at_stores_null_at_the_parent(at_parent):
    """🚨 The defect, reproduced rather than described. Without this the test below
    cannot distinguish "the default works" from "nothing ever wrote NULL here"."""
    with at_parent.begin() as conn:
        _insert_probe(conn, "probe-null")
    assert _updated_at(at_parent, "plans", "slug = 'probe-null'") is None


def test_an_insert_omitting_updated_at_now_stores_a_timestamp(at_parent, roundtrip_db_url):
    _upgrade(at_parent, roundtrip_db_url)
    with at_parent.begin() as conn:
        _insert_probe(conn, "probe-default")
    assert _updated_at(at_parent, "plans", "slug = 'probe-default'") is not None


# ---------------------------------------------------------------------------
# 3. The backfill
# ---------------------------------------------------------------------------


def test_an_existing_null_is_backfilled_from_created_at_not_from_now(at_parent, roundtrip_db_url):
    """🔑 ``created_at``, not ``now()``, and the difference is a claim about the row.

    ``now()`` would assert that every untouched row was modified at migration time —
    false, and the kind of quiet rewrite core#726 is the standing example of. A row that
    has never been updated since it was created is honestly described by its own
    ``created_at``.
    """
    with at_parent.begin() as conn:
        _insert_probe(conn, "probe-backfill", created_at="2020-01-02 03:04:05")
        created = conn.execute(
            text("SELECT created_at FROM plans WHERE slug = 'probe-backfill'")
        ).scalar_one()
    assert _updated_at(at_parent, "plans", "slug = 'probe-backfill'") is None

    _upgrade(at_parent, roundtrip_db_url)

    assert _updated_at(at_parent, "plans", "slug = 'probe-backfill'") == created


def test_a_row_that_already_has_updated_at_is_left_alone(at_parent, roundtrip_db_url):
    """The backfill must be a repair, not a rewrite — and this is also what makes the
    data-preservation round-trip safe: on the re-upgrade every row is already non-NULL,
    so the second pass changes nothing."""
    with at_parent.begin() as conn:
        _insert_probe(
            conn,
            "probe-keep",
            created_at="2020-01-02 03:04:05",
            updated_at="2021-06-07 08:09:10",
        )
    before = _updated_at(at_parent, "plans", "slug = 'probe-keep'")

    _upgrade(at_parent, roundtrip_db_url)

    assert _updated_at(at_parent, "plans", "slug = 'probe-keep'") == before


def test_a_null_created_at_is_backfilled_too(at_parent, roundtrip_db_url):
    """Release N+1 puts ``SET NOT NULL`` on **both** columns. A NULL ``created_at``
    left behind here makes that migration fail on the box — which is the safe failure
    (the old colour keeps serving), but it should be a measurement, not a surprise."""
    with at_parent.begin() as conn:
        _insert_probe(conn, "probe-nocreated", created_at=None)
    with at_parent.begin() as conn:
        seeded = conn.execute(
            text("SELECT created_at FROM plans WHERE slug = 'probe-nocreated'")
        ).scalar_one()
    assert seeded is None, (
        "the probe row did not actually get a NULL created_at, so the assertion below "
        "would pass against a row that never had the defect"
    )

    _upgrade(at_parent, roundtrip_db_url)

    with at_parent.begin() as conn:
        row = conn.execute(
            text("SELECT created_at, updated_at FROM plans WHERE slug = 'probe-nocreated'")
        ).one()
    assert row.created_at is not None, "a NULL created_at survives into release N+1"
    assert row.updated_at is not None


def test_no_null_timestamps_remain_on_any_of_the_seven(at_parent, roundtrip_db_url):
    """The measurement release N owes release N+1 — and **what it examined, stated**.

    🚨 **This docstring used to read "asserted on every one of the seven rather than on
    the one table it was convenient to seed". That was an overclaim, and it was exactly
    backwards.** ``at_parent`` drops and recreates ``public`` before every test, so the
    schema is migrated up from empty: measured, the seven tables hold **1 row between
    them** — ``plans``, from the free-plan seed migration — and the other six hold
    **zero**. ``count(*) WHERE … IS NULL == 0`` is arithmetically true of an empty table,
    so the loop below cannot fail for six of the seven. Iterating all seven made it read
    like seven tables' worth of evidence.

    That is the ``plans >= 5`` restore-drill failure in miniature: a count is not a
    measurement unless something says the rows were there to count. So the populated set
    is **pinned**, and the loop's real coverage is a stated fact rather than an
    impression.

    🔑 **The backfill itself IS armed** —
    ``test_an_existing_null_is_backfilled_from_created_at_not_from_now`` and
    ``test_a_null_created_at_is_backfilled_too`` insert probe rows with NULL timestamps
    and assert they come back non-NULL, the second with an explicit control that the
    probe really did start NULL. This test is the *census*, not the behaviour, and only
    the census was overclaiming.

    ⚠️ **Release N+1's basis is the PRODUCTION count, and it is Infra's** (core#1069
    step 2). When it arrives it must carry **rows beside NULLs, per table** — a table
    reading ``0 NULLs`` because it is empty and one reading ``0 NULLs`` because the
    backfill worked are the same number and call for different confidence.
    """
    _upgrade(at_parent, roundtrip_db_url)

    census: dict[str, tuple[int, int]] = {}
    with at_parent.begin() as conn:
        for table in sorted(EXPECTED_TABLES):
            rows = conn.execute(text(f"SELECT count(*) FROM {table}")).scalar_one()
            remaining = conn.execute(
                text(f"SELECT count(*) FROM {table} WHERE created_at IS NULL OR updated_at IS NULL")
            ).scalar_one()
            census[table] = (rows, remaining)
            assert remaining == 0, f"{table} still holds {remaining} NULL timestamp row(s)"

    populated = {table for table, (rows, _) in census.items() if rows}
    assert populated == {"plans"}, (
        f"the tables this assertion actually examined are {sorted(populated)}, expected "
        f"['plans'] (full census, table -> (rows, nulls): {census}).\n\n"
        "If this GREW, the harness now seeds more of the seven and the loop above is "
        "real evidence for them — say so and widen this set. If it SHRANK, even the one "
        "row is gone and the loop is wholly vacuous. Either way the number release N+1 "
        "rests on is the PRODUCTION count (core#1069 step 2, Infra's), not this one."
    )


# ---------------------------------------------------------------------------
# 4. Expand/contract — what this migration must NOT do
# ---------------------------------------------------------------------------


def test_nullability_and_type_are_deliberately_unchanged(at_parent, roundtrip_db_url):
    """🚨 The expand/contract boundary, asserted rather than promised.

    ``SET NOT NULL`` and ``timestamp -> timestamptz`` are both on the spec's
    never-in-the-same-release list. Under blue/green the previously deployed code runs
    against this schema, and both would break it. If a later edit sneaks either into this
    migration, this goes red — which is the only mechanical thing standing between a
    correct chain and a two-releases-in-one deploy.
    """
    _upgrade(at_parent, roundtrip_db_url)
    with at_parent.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT table_name, column_name, is_nullable, data_type "
                "FROM information_schema.columns WHERE table_schema='public' "
                "AND column_name IN ('created_at','updated_at') "
                "AND table_name = ANY(:tables)"
            ),
            {"tables": sorted(EXPECTED_TABLES)},
        ).all()

    assert len(rows) == 2 * len(EXPECTED_TABLES), (
        f"expected 14 timestamp columns across the seven, read {len(rows)} — the query "
        "is not seeing what this test claims to check"
    )
    for row in rows:
        assert row.is_nullable == "YES", (
            f"{row.table_name}.{row.column_name} is NOT NULL — that is release N+1, and "
            "shipping it here breaks the previously deployed code under blue/green"
        )
        assert row.data_type == "timestamp without time zone", (
            f"{row.table_name}.{row.column_name} is {row.data_type} — the timezone "
            "conversion is release N+1 (core#1069 disagreement 1), and "
            "AT TIME ZONE 'UTC' silently shifts any value that was not stored as UTC"
        )
