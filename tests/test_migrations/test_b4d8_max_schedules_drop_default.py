"""``plans.max_schedules`` must stop carrying a ``server_default`` (core#1047).

A from-scratch build capped Pro and Enterprise at **10 schedules** on tiers `/pricing`
sells as **"Unlimited schedules"**, and **no migration could repair it**, because the paid
rows do not exist when migrations run. That is core#928's asymmetry, one column over:

1. ``j9f6g7h8i0c1`` declares ``max_schedules`` with ``server_default="10"``;
2. no migration creates a paid slug — the only slug any migration ``INSERT``s is ``free``;
3. so ``a9c4e2b7d5f3``'s ``UPDATE … WHERE slug IN (four paid slugs)`` matches **zero rows**,
   and the paid rows are created afterwards, out of band, taking the default.

Production reads 9999 only because whatever created its rows supplied that value. A new
cloud deployment has no such luck.

🔴 **This REVERSES a decision made one day earlier in cloud#151**, which chose
``server_default`` precisely so that omission would keep meaning "capped": *"omission must
not mean unlimited: for a quota, that is the wrong direction to fail in."* Sound in general,
wrong about this column, because of who actually creates plan rows — see the cloud PR and
``datanika_cloud/billing/models.py``. **This makes the column fail OPEN**, and the one thing
bounding that is ``free``'s INSERT being explicit, which
``test_the_free_plan_keeps_its_published_cap_of_two`` below pins.

**Expand/contract.** Dropping a default is a *loosening*: it rewrites no existing row and
the previously deployed code reads the same values at ``t1``. The direction to be careful
about is the downgrade, which re-adds it — also harmless, since re-adding a default does not
touch existing rows, asserted below.

Why this file rather than a model assertion
--------------------------------------------
Cloud's own tests build the schema with ``Base.metadata.create_all``, which reads the
**model** — so a cloud-side assertion is green the moment the model changes, whatever the
database holds. Only alembic against a real Postgres can say what the *column* does, and the
column is what a row created by a script outside both repositories will meet.
"""

import pytest
from sqlalchemy import create_engine, text

from tests.test_migrations.conftest import _run_alembic

PARENT_REVISION = "a9c4e2b7d5f3"
THIS_REVISION = "b4d8f1a2c6e9"

#: An INSERT that deliberately OMITS ``max_schedules`` — the shape every out-of-band
#: creator uses, and the one the column default was answering.
_INSERT_OMITTING = text(
    "INSERT INTO plans (name, slug, paddle_price_id, paddle_product_id, price_cents, "
    "interval) VALUES ('Pro', :slug, :price, 'pro_probe', 7900, 'monthly')"
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


def _column_default(engine):
    with engine.begin() as conn:
        return conn.execute(
            text(
                "SELECT column_default FROM information_schema.columns "
                "WHERE table_name = 'plans' AND column_name = 'max_schedules'"
            )
        ).scalar_one()


def _stored(engine, slug):
    with engine.begin() as conn:
        return conn.execute(
            text("SELECT max_schedules FROM plans WHERE slug = :slug"), {"slug": slug}
        ).scalar_one()


# ---------------------------------------------------------------------------
# Controls — these attribute the reds below. Without them, "the default is
# absent" and "this file never reached the schema" are the same observation.
# ---------------------------------------------------------------------------


def test_control_the_default_is_present_at_the_parent_revision(at_parent):
    default = _column_default(at_parent)
    assert default is not None and "10" in default, (
        f"max_schedules already had no default at {PARENT_REVISION} (got {default!r}), so "
        "every assertion below would pass without this migration doing anything"
    )


def test_control_an_omitting_insert_takes_the_default_at_the_parent_revision(at_parent):
    """The defect itself, reproduced. This is what a from-scratch build does."""
    with at_parent.begin() as conn:
        conn.execute(_INSERT_OMITTING, {"slug": "pro-parent", "price": "pri_parent"})

    assert _stored(at_parent, "pro-parent") == 10, (
        "a plan row created without max_schedules did NOT come out as 10 at the parent "
        "revision — the premise of core#1047 no longer holds and this file needs re-deriving"
    )


# ---------------------------------------------------------------------------
# Regressions — red before this migration exists.
# ---------------------------------------------------------------------------


def test_the_column_has_no_default_after_the_migration(at_parent, roundtrip_db_url):
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    assert _column_default(at_parent) is None


def test_an_omitting_insert_now_lands_as_null(at_parent, roundtrip_db_url):
    """The criterion, asserted by writing a row rather than by reading a catalogue.

    NULL means **uncapped** to all three readers (``check_schedule_quota``, and the billing
    page's ``PlanInfo``/``SubscriptionInfo``), so this is the line where "Unlimited
    schedules" stops being false on a fresh deployment.
    """
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    with at_parent.begin() as conn:
        conn.execute(_INSERT_OMITTING, {"slug": "pro-fresh", "price": "pri_fresh"})

    assert _stored(at_parent, "pro-fresh") is None, (
        "a plan row created without max_schedules still took a finite cap, so a "
        "from-scratch build still enforces a limit on tiers sold as unlimited"
    )


def test_the_free_plan_keeps_its_published_cap_of_two(at_parent, roundtrip_db_url):
    """🚨 The bound on the fail-open, and the one row a wrong answer here would uncap.

    ``free`` is the only slug a migration creates and the only schedule cap actually
    enforced. Its INSERT (``q6m3n4o5p7j8``) supplies ``max_schedules`` explicitly, so it
    never depended on the default — but that is exactly the sort of claim that is true when
    written and false later. Asserted against the real migrated database, before and after,
    so a change to that INSERT reds here rather than silently uncapping the free tier.
    """
    assert _stored(at_parent, "free") == 2, "the free plan's cap was not 2 before the migration"

    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    assert _stored(at_parent, "free") == 2, (
        "dropping the column default changed the free plan's stored cap. It must not: "
        "the migration alters the column, never a row."
    )


def test_an_explicit_value_still_stores_as_itself(at_parent, roundtrip_db_url):
    """Anti-widening control: removing a default must not disturb explicit writes."""
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    with at_parent.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO plans (name, slug, paddle_price_id, paddle_product_id, "
                "price_cents, interval, max_schedules) VALUES ('Cap', 'capped', 'pri_cap', "
                "'pro_cap', 100, 'monthly', 7)"
            )
        )

    assert _stored(at_parent, "capped") == 7


def test_the_downgrade_restores_the_default_without_rewriting_rows(at_parent, roundtrip_db_url):
    """A rollback must re-arm the default for NEW rows and leave existing NULLs alone.

    Re-adding a default is not a backfill, and asserting that here is the point: a
    downgrade that "helpfully" filled the NULLs would silently re-cap every paid tier on
    the way back — the exact damage this migration exists to undo.
    """
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    with at_parent.begin() as conn:
        conn.execute(_INSERT_OMITTING, {"slug": "pro-unl", "price": "pri_unl"})
    assert _stored(at_parent, "pro-unl") is None

    down = _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url)
    assert down.returncode == 0, f"downgrade failed: {down.stderr}"

    default = _column_default(at_parent)
    assert default is not None and "10" in default, f"the default was not restored: {default!r}"
    assert _stored(at_parent, "pro-unl") is None, (
        "the downgrade backfilled an existing NULL, re-capping a tier that had been "
        "correctly uncapped"
    )
    with at_parent.begin() as conn:
        conn.execute(_INSERT_OMITTING, {"slug": "pro-after-down", "price": "pri_ad"})
    assert _stored(at_parent, "pro-after-down") == 10

    # And back up again, because a rollback is rarely the last thing that happens.
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    assert _column_default(at_parent) is None
