"""The four paid slugs must actually hold NULL — cloud#151 step 4.

Steps 1-3 made "unlimited" *representable* and made every reader tolerate it.
This is the one that makes it **true**: `/pricing`, the homepage,
`/features/volume-pricing/`, `/docs/scheduling-guide`, two blog posts and the
``SoftwareApplication`` JSON-LD all sell **"Unlimited schedules"** on Pro and
Enterprise while production enforces ``max_schedules = 9999``.

Why these assertions run against a real Postgres through alembic
----------------------------------------------------------------
Every other test in this repo builds its schema with ``Base.metadata.create_all``,
which reads the **model**. A model-level test of this would be green the moment
someone changed a Python constant, whatever the database holds — and the whole
of core#928 is about migrations whose intent never reached a row. The claim here
is *"the row holds NULL"*, so only the database can answer it.

⚠️ **The from-scratch case is asserted too, and it asserts the unhappy answer.**
core#928: no migration has ever created the paid rows, so on a from-scratch build
this UPDATE matches nothing and the rows are created afterwards taking the
column's ``server_default`` of 10. That is pre-existing — a fresh build gets 10
today, before and after this migration — so it is recorded here rather than
fixed here (**core#1047**), and the test pins the *shape* so nobody later reads a
green suite as evidence that fresh builds are covered. The class guard that
exists for exactly this does not catch it either: **core#1048**.

🔑 That state is **not an empty table**: at this revision ``plans`` holds exactly
``free``, because it is the only slug any migration INSERTs. The first draft of
this file tried to seed ``free`` itself and died on ``plans_slug_key`` — which is
the asymmetry core#928 is made of, arriving as a test failure. It is also why
``free`` is the one row in production that the chain's paid-slug UPDATEs reached.
"""

import pytest
from sqlalchemy import create_engine, text

from datanika.migrations.versions.a9c4e2b7d5f3_null_paid_schedule_limits import (
    UNLIMITED_SCHEDULE_SLUGS,
)
from tests.test_migrations.conftest import _run_alembic

PARENT_REVISION = "e3a5c7b9d1f4"
THIS_REVISION = "a9c4e2b7d5f3"

#: What production measures on the four paid rows today (cloud#151).
PROD_CEILING = 9999
#: What `q6m3n4o5p7j8` INSERTs for `free`, and what `/pricing` publishes for it.
FREE_CEILING = 2

_INSERT = text(
    "INSERT INTO plans (name, slug, paddle_price_id, paddle_product_id, price_cents, "
    "interval, max_schedules) VALUES (:name, :slug, :price, 'probe', 7900, "
    "'monthly', :max_schedules)"
)


def _seed_paid(engine):
    """Create the four paid rows at the ceiling production actually holds.

    🔑 ``free`` is deliberately NOT created here, and the reason is the whole of
    core#928: ``q6m3n4o5p7j8`` **INSERTs** it, so by this revision it already
    exists — while the four paid slugs are created by nothing in either repo and
    have to be supplied by hand, exactly as production had to. The first version
    of this helper inserted ``free`` too and died on ``plans_slug_key``; that
    failure is the asymmetry, and it is why ``free`` is the one row in
    production that every paid-slug UPDATE in the chain actually reached.
    """
    with engine.begin() as conn:
        for slug in UNLIMITED_SCHEDULE_SLUGS:
            conn.execute(
                _INSERT,
                {
                    "name": slug,
                    "slug": slug,
                    "price": f"pri_{slug}",
                    "max_schedules": PROD_CEILING,
                },
            )


def _ceilings(engine) -> dict[str, int | None]:
    with engine.begin() as conn:
        return dict(conn.execute(text("SELECT slug, max_schedules FROM plans")).all())


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


# ---------------------------------------------------------------------------
# Controls — they attribute the red below. Without them, "the rows are NULL"
# and "the rows were never there" produce the same green.
# ---------------------------------------------------------------------------


def test_control_the_paid_rows_hold_a_finite_ceiling_at_the_parent_revision(at_parent):
    _seed_paid(at_parent)
    before = _ceilings(at_parent)
    assert [before[s] for s in UNLIMITED_SCHEDULE_SLUGS] == [PROD_CEILING] * 4, (
        "the paid rows were not finite before this migration, so the assertions "
        "below would pass without it doing anything"
    )


def test_control_the_slug_list_is_the_four_paid_slugs(at_parent):
    """Pins the set, so a fifth slug or a dropped one is a decision.

    `free` must never be in it — its cap of 2 is published and enforced, and it
    is the one plan row a migration creates, so it is the one row a widened
    UPDATE would actually reach.
    """
    assert set(UNLIMITED_SCHEDULE_SLUGS) == {
        "pro-monthly",
        "pro-annual",
        "enterprise-monthly",
        "enterprise-annual",
    }
    assert "free" not in UNLIMITED_SCHEDULE_SLUGS


# ---------------------------------------------------------------------------
# Regressions — red without the migration's UPDATE.
# ---------------------------------------------------------------------------


def test_the_four_paid_rows_read_null_after_the_migration(at_parent, roundtrip_db_url):
    _seed_paid(at_parent)
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    after = _ceilings(at_parent)
    still_capped = {s: after[s] for s in UNLIMITED_SCHEDULE_SLUGS if after[s] is not None}
    assert not still_capped, f"paid slugs still carry a ceiling: {still_capped}"


def test_free_keeps_its_published_cap(at_parent, roundtrip_db_url):
    """Anti-widening control.

    ``UPDATE plans SET max_schedules = NULL`` with no WHERE would satisfy the
    test above perfectly and silently uncap the one tier the page caps.
    """
    _seed_paid(at_parent)
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    assert _ceilings(at_parent)["free"] == FREE_CEILING


def test_a_row_the_migration_does_not_name_is_untouched(at_parent, roundtrip_db_url):
    """Second anti-widening control, on a slug that is in neither list."""
    _seed_paid(at_parent)
    with at_parent.begin() as conn:
        conn.execute(
            _INSERT,
            {"name": "Legacy", "slug": "legacy-tier", "price": "pri_legacy", "max_schedules": 7},
        )
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    assert _ceilings(at_parent)["legacy-tier"] == 7


# ---------------------------------------------------------------------------
# The from-scratch path — recorded, not fixed. See the module docstring.
# ---------------------------------------------------------------------------


def test_the_migration_matches_no_rows_on_a_from_scratch_build(at_parent, roundtrip_db_url):
    """core#928's shape, pinned rather than papered over.

    🔑 **This is what a from-scratch build actually looks like, and it is not an
    empty table.** At this revision ``plans`` holds exactly one row — ``free`` —
    because that is the only slug any migration INSERTs. The four paid slugs do
    not exist yet, so this UPDATE matches nothing and the rows are created out of
    band afterwards, taking the column's ``server_default``.

    A migration that raised on zero matched rows would break every new
    deployment, so this one cannot and must not. What this test records is that a
    green run here is **not** evidence a fresh build ends up correct.
    """
    before = _ceilings(at_parent)
    assert set(before) == {"free"}, (
        f"a migration now creates more than `free` ({sorted(before)}). If it creates "
        "the paid rows, that is the real fix for core#928 and this test should say so"
    )

    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    assert _ceilings(at_parent) == before, "the migration touched a row it does not name"

    # And the reason it is not enough: the default a later out-of-band INSERT
    # would take is still 10, not NULL.
    with at_parent.begin() as conn:
        default = conn.execute(
            text(
                "SELECT column_default FROM information_schema.columns "
                "WHERE table_name = 'plans' AND column_name = 'max_schedules'"
            )
        ).scalar_one()
    assert default is not None and default.startswith("10"), (
        "the server_default changed. If it is now NULL the from-scratch path is "
        "fixed and this test should say so instead of warning about it"
    )


# ---------------------------------------------------------------------------
# Rollback.
# ---------------------------------------------------------------------------


def test_the_downgrade_restores_the_fair_use_figure(at_parent, roundtrip_db_url):
    _seed_paid(at_parent)
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    down = _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url)
    assert down.returncode == 0, f"downgrade failed: {down.stderr}"

    after = _ceilings(at_parent)
    assert [after[s] for s in UNLIMITED_SCHEDULE_SLUGS] == [PROD_CEILING] * 4
    assert after["free"] == FREE_CEILING

    # And back up again — a rollback is rarely the last thing that happens.
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    assert all(_ceilings(at_parent)[s] is None for s in UNLIMITED_SCHEDULE_SLUGS)


def test_the_downgrade_leaves_a_deliberate_finite_ceiling_alone(at_parent, roundtrip_db_url):
    """The ``AND max_schedules IS NULL`` predicate, and why it is there.

    The downgrade undoes rows *this* migration NULLed. A paid row someone has
    since given a real ceiling was not one of them, and a rollback is the worst
    moment to overwrite a deliberate value.
    """
    _seed_paid(at_parent)
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    with at_parent.begin() as conn:
        conn.execute(text("UPDATE plans SET max_schedules = 42 WHERE slug = 'pro-monthly'"))

    assert _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url).returncode == 0

    after = _ceilings(at_parent)
    assert after["pro-monthly"] == 42, "the downgrade clobbered a deliberate ceiling"
    assert after["enterprise-monthly"] == PROD_CEILING
