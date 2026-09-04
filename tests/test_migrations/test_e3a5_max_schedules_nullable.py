"""``plans.max_schedules`` must accept NULL before anything can set one (cloud#151 step 3).

`/pricing`, the homepage, `/features/volume-pricing/`, `/docs/scheduling-guide`, two blog
posts and the ``SoftwareApplication`` JSON-LD all advertise **"Unlimited schedules"** on Pro
and Enterprise. Production enforces ``max_schedules = 9999``. Under the founder's standing
option-(c) decision the page stays and the code moves.

The convention that carries "unlimited" in this codebase is **NULL, enforced by readers** —
``plans.bytes_included``, ``plans.max_api_keys``, ``subscriptions.seats_purchased`` and now
``plans.max_schedules`` all mean *uncapped* when NULL. Three readers already implement it for
this column:

1. ``BillingService.check_schedule_quota`` — cloud#165, shipped;
2. ``BillingState``'s ``PlanInfo`` / ``SubscriptionInfo`` — cloud#164, shipped;
3. this migration, which is what makes the value **representable at all**.

⚠️ **Until this migration, cloud#151's own written plan could not run.** ``max_schedules`` is
``nullable=False`` in core migration ``j9f6g7h8i0c1`` *and* in the cloud model, so
``UPDATE plans SET max_schedules = NULL`` would have failed outright — the issue named a
data migration with no expand step in front of it.

**Expand-only, per SPEC_EXPAND_CONTRACT_MIGRATIONS.** Dropping ``NOT NULL`` is a *loosening*:
at ``t1`` the previously deployed code runs against this schema and every existing row still
carries an integer, so nothing it reads changes. The forbidden direction is the other one —
``SET NOT NULL`` — and the downgrade below is exactly that, which is why it backfills first.

🚨 **This does NOT set any row to NULL, deliberately.** That is cloud#151 step 4, and it must
wait until all three readers are in the **running image** — not merely on a branch. Cloud
ships inside the core image at a pinned ``ref: master``, so a cloud promotion with no core
promotion behind it is a change that has not shipped, and the billing-page reader (cloud#164)
is on cloud ``dev`` only. Arming the NULL early takes the billing page down for exactly the
paying customer the feature is for.

Why this file exists rather than a model assertion
--------------------------------------------------
Every other test in this repo builds its schema with ``Base.metadata.create_all``, which
reads the **model** — so a cloud-side test of ``Mapped[int | None]`` is green the moment the
model changes, whatever the database says. Only alembic against a real Postgres can answer
whether the *column* accepts NULL, which is what production will do.
"""

import pytest
from sqlalchemy import create_engine, text

from tests.test_migrations.conftest import _run_alembic

PARENT_REVISION = "d5e1f3a7b9c2"
THIS_REVISION = "e3a5c7b9d1f4"

_INSERT = text(
    "INSERT INTO plans (name, slug, paddle_price_id, paddle_product_id, price_cents, "
    "interval, max_schedules) VALUES ('Pro', :slug, :price, 'pro_probe', 7900, "
    "'monthly', :max_schedules)"
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


def _is_nullable(engine) -> str:
    with engine.begin() as conn:
        return conn.execute(
            text(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_name = 'plans' AND column_name = 'max_schedules'"
            )
        ).scalar_one()


# ---------------------------------------------------------------------------
# Control — green before and after. It attributes the red below.
# ---------------------------------------------------------------------------


def test_control_the_column_is_not_null_at_the_parent_revision(at_parent):
    """Without this, *"the insert succeeded"* and *"this file never reached the schema"*
    are the same observation."""
    assert _is_nullable(at_parent) == "NO", (
        "max_schedules was already nullable at the parent revision, so the assertions "
        "below would pass without this migration doing anything"
    )


# ---------------------------------------------------------------------------
# Regressions — red before this migration exists.
# ---------------------------------------------------------------------------


def test_the_column_accepts_null_after_the_migration(at_parent, roundtrip_db_url):
    """The criterion, asserted by writing a row rather than by reading a catalogue.

    ``information_schema`` is checked separately below; this one proves the database
    actually accepts the value, which is the thing cloud#151 step 4 will do.
    """
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    with at_parent.begin() as conn:
        conn.execute(_INSERT, {"slug": "pro-null", "price": "pri_null", "max_schedules": None})
        stored = conn.execute(
            text("SELECT max_schedules FROM plans WHERE slug = 'pro-null'")
        ).scalar_one()

    assert stored is None, f"the NULL was coerced on the way in: {stored!r}"


def test_the_column_reports_nullable_in_the_catalogue(at_parent, roundtrip_db_url):
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    assert _is_nullable(at_parent) == "YES"


def test_a_finite_limit_still_stores_as_itself(at_parent, roundtrip_db_url):
    """Anti-widening control: dropping NOT NULL must not disturb the ordinary case."""
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    with at_parent.begin() as conn:
        conn.execute(_INSERT, {"slug": "free-2", "price": "pri_free2", "max_schedules": 2})
        stored = conn.execute(
            text("SELECT max_schedules FROM plans WHERE slug = 'free-2'")
        ).scalar_one()

    assert stored == 2


def test_the_downgrade_backfills_before_it_re_tightens(at_parent, roundtrip_db_url):
    """A downgrade that only re-adds NOT NULL fails on any row the release set NULL.

    That is not hypothetical: cloud#151 step 4 exists to create exactly those rows, and a
    rollback is when you least want a migration to abort halfway. The backfill runs first,
    and it restores the fair-use figure (9999) the paid slugs carried before — which is
    faithful for every row step 4 will NULL, and stated rather than implied because NULL
    has no exact integer inverse.
    """
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    with at_parent.begin() as conn:
        conn.execute(_INSERT, {"slug": "pro-unl", "price": "pri_unl", "max_schedules": None})

    down = _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url)
    assert down.returncode == 0, f"downgrade aborted on a NULL row: {down.stderr}"

    assert _is_nullable(at_parent) == "NO"
    with at_parent.begin() as conn:
        restored = conn.execute(
            text("SELECT max_schedules FROM plans WHERE slug = 'pro-unl'")
        ).scalar_one()
    assert restored == 9999, f"the backfill left {restored!r}"

    # And back up again, because a rollback is rarely the last thing that happens.
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    assert _is_nullable(at_parent) == "YES"
