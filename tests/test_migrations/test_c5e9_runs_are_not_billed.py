"""``plans.overage_run_price_cents`` must stop defaulting to 1 cent — cloud#177.

**Founder decision, 2026-09-04**: *"If the page describes this run as free, it must be
free."* `/pricing` publishes `15,000 model runs / month` and **no runs overage rate at
all**; `pricing-tiers.ts` calls model runs *"a secondary, non-billed fair-use quota"*;
`SPEC_PRICING_V2` §2.2 calls the same thing a *"fair-use orchestration limit"*. The
schema disagreed with all three: ``server_default="1"`` on a ``NOT NULL`` column, and
``hard_cap_runs = false`` on every paid slug since core#713 — so the runs dimension did
not block, it **billed**, at a rate nothing we publish states.

Two assertions here, and they are different claims
--------------------------------------------------
1. **Existing rows are zeroed.** Every plan, not a slug list — see
   ``test_a_slug_no_list_could_have_named_is_zeroed_too``, which is the discriminating
   control between this migration and the one somebody will be tempted to write instead.
2. **Absence stops meaning "charge".** The column keeps ``NOT NULL`` and loses its
   default, so an INSERT that omits a price is **refused** rather than silently priced.

🔑 **The pairing this closes, and it is the finding worth more than either fix.**
``max_schedules`` defaulted to ``10``: *absence gave product away* (core#1047).
``overage_run_price_cents`` defaulted to ``1``: *absence takes money*. Same
``create_table``, opposite directions, neither default deliberate. The rule the pair
yields — **a column that only gates may fail open; a column that prices must not have a
default at all** — is why this migration does not simply mirror ``b4d8f1a2c6e9``:
``max_schedules`` became nullable so absence means *uncapped*, while this column stays
``NOT NULL`` so absence means *refused*. NULL would be worse than either, because
``_overage_price_cents`` does ``plan.overage_run_price_cents * overage_quantity`` with no
None handling.

**Expand/contract (SPEC_EXPAND_CONTRACT_MIGRATIONS).** Dropping a default is a loosening
and rewrites no row's *type*; the ``UPDATE`` writes a value the previously deployed code
already reads correctly (``amount_cents <= 0`` → skip, no crash). At ``t1`` the old cloud
model still carries a Python-side ``default=1``, so ORM inserts from the old container
keep supplying a value and cannot hit the ``NOT NULL``. Nothing here narrows a type, adds
a constraint or drops a column.

Why this file rather than a cloud-side assertion
------------------------------------------------
Cloud's tests build their schema from the **model** via ``Base.metadata.create_all``, so a
cloud assertion goes green the moment the model changes, whatever the database holds. Only
alembic against a real Postgres can say what the *column* does — and the column is what a
row created by the out-of-band creator (in neither repository) actually meets.
"""

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

from tests.test_migrations.conftest import _run_alembic

PARENT_REVISION = "b4d8f1a2c6e9"
THIS_REVISION = "c5e9a3b7d2f4"

#: The four paid slugs. Created here by hand **because no migration creates them** —
#: that asymmetry (core#928) is the whole reason a slug-keyed correction cannot work on a
#: fresh build, and reproducing it is what makes these tests describe reality.
PAID_SLUGS = ("pro-monthly", "pro-annual", "enterprise-monthly", "enterprise-annual")

#: An INSERT that omits the price — the shape every out-of-band creator uses, and the one
#: the column default was silently answering with "charge a cent a run".
_INSERT_OMITTING = text(
    "INSERT INTO plans (name, slug, paddle_price_id, paddle_product_id, price_cents, "
    "interval) VALUES ('Probe', :slug, :price, 'pro_probe', 7900, 'monthly')"
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
                "WHERE table_name = 'plans' AND column_name = 'overage_run_price_cents'"
            )
        ).scalar_one()


def _is_nullable(engine):
    with engine.begin() as conn:
        return conn.execute(
            text(
                "SELECT is_nullable FROM information_schema.columns "
                "WHERE table_name = 'plans' AND column_name = 'overage_run_price_cents'"
            )
        ).scalar_one()


def _stored(engine, slug):
    with engine.begin() as conn:
        return conn.execute(
            text("SELECT overage_run_price_cents FROM plans WHERE slug = :slug"),
            {"slug": slug},
        ).scalar_one()


def _create_paid_rows_the_way_production_was(engine):
    """Create the four paid slugs **omitting the price**, as a fresh build does.

    This is not a convenience fixture — it is the defect. No migration creates these
    slugs, so on a from-scratch database they are made afterwards by something outside
    both repositories, and every column it forgets comes from the ``server_default``.
    """
    with engine.begin() as conn:
        for i, slug in enumerate(PAID_SLUGS):
            conn.execute(_INSERT_OMITTING, {"slug": slug, "price": f"pri_{i}"})


# ---------------------------------------------------------------------------
# Controls. Without these, "the price is zero" and "this file never reached the
# schema" are the same observation.
# ---------------------------------------------------------------------------


def test_control_the_default_is_present_at_the_parent_revision(at_parent):
    default = _column_default(at_parent)
    assert default is not None and "1" in default, (
        f"overage_run_price_cents already had no default at {PARENT_REVISION} "
        f"(got {default!r}), so every assertion below would pass without this "
        "migration doing anything"
    )


def test_control_an_omitting_insert_charges_a_cent_at_the_parent_revision(at_parent):
    """The defect itself, reproduced. This is what a from-scratch build bills."""
    _create_paid_rows_the_way_production_was(at_parent)

    charging = {slug: _stored(at_parent, slug) for slug in PAID_SLUGS}
    assert set(charging.values()) == {1}, (
        f"paid rows created without a price did NOT come out at 1 cent/run: {charging}. "
        "The premise of cloud#177 no longer holds and this file needs re-deriving"
    )


def test_control_the_column_is_not_null_at_the_parent_revision(at_parent):
    """Pins the half this migration deliberately does **not** change.

    ``b4d8f1a2c6e9`` made ``max_schedules`` nullable because NULL there means *uncapped*
    and three readers honour it. Here NULL has no reader — ``_overage_price_cents``
    multiplies the value — so ``NOT NULL`` is what turns an omitted price into a refusal
    instead of a crash at charge time.
    """
    assert _is_nullable(at_parent) == "NO"


# ---------------------------------------------------------------------------
# Regressions — every one of these is red until c5e9a3b7d2f4 exists.
# ---------------------------------------------------------------------------


def test_the_column_has_no_default_after_the_migration(at_parent, roundtrip_db_url):
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    assert _column_default(at_parent) is None


def test_the_column_stays_not_null(at_parent, roundtrip_db_url):
    """Dropping a default must not quietly relax the constraint that makes it safe."""
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    assert _is_nullable(at_parent) == "NO"


def test_an_insert_that_omits_the_price_is_now_refused(at_parent, roundtrip_db_url):
    """🔑 The founder's rule, made mechanical: **absence must not take money.**

    Asserted by attempting the write, not by reading a catalogue — a default can be
    absent from ``information_schema`` while something else supplies one.
    """
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    with pytest.raises(IntegrityError) as exc, at_parent.begin() as conn:
        conn.execute(_INSERT_OMITTING, {"slug": "pro-silent", "price": "pri_silent"})

    assert "overage_run_price_cents" in str(exc.value), (
        "the INSERT was refused for some other reason, so this test is not measuring "
        f"what it claims: {exc.value}"
    )


def test_existing_paid_rows_are_zeroed(at_parent, roundtrip_db_url):
    """The repair half, for every environment whose rows already exist — production
    included, and every future staging or DR rebuild seeded before this shipped."""
    _create_paid_rows_the_way_production_was(at_parent)
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    priced = {slug: _stored(at_parent, slug) for slug in PAID_SLUGS}
    assert set(priced.values()) == {0}, (
        f"paid plans still charge for model runs after the migration: {priced}. "
        "/pricing publishes no runs overage rate, so any non-zero here bills a customer "
        "for something no surface we own discloses"
    )


def test_a_slug_no_list_could_have_named_is_zeroed_too(at_parent, roundtrip_db_url):
    """🚨 The discriminating control, and the reason the UPDATE carries no WHERE.

    A slug-keyed correction is the obvious shape — ``f6a7b8c9d0e1`` uses it and
    core#1060's first suggested fix proposed it. It is **wrong for this decision**, twice
    over: it matches zero rows on a fresh build (core#928's asymmetry, the defect these
    issues are about), and it silently misses any slug the list's author did not know —
    the ``e2e-*`` plans ``billing/e2e_admin.py`` creates, and whatever a future
    environment names its rows.

    "Model runs are not billed" is a statement about the product, not about four strings.
    A slug-keyed migration passes every other test in this file and fails this one.
    """
    with at_parent.begin() as conn:
        conn.execute(_INSERT_OMITTING, {"slug": "e2e-bytes-probe", "price": "pri_e2e"})
    assert _stored(at_parent, "e2e-bytes-probe") == 1  # took the default, as designed

    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    assert _stored(at_parent, "e2e-bytes-probe") == 0, (
        "a plan whose slug is in no migration's list still charges for model runs. "
        "The UPDATE must not be keyed on slug — see this test's docstring"
    )


def test_the_free_plan_was_already_zero_and_stays_zero(at_parent, roundtrip_db_url):
    """``free`` is the only slug a migration creates, and ``q6m3n4o5p7j8`` already writes
    ``0`` for it explicitly. Asserted before **and** after, so the migration is shown to
    be idempotent on the one row that was already correct."""
    assert _stored(at_parent, "free") == 0, "the free plan's runs price was not 0 already"

    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    assert _stored(at_parent, "free") == 0


def test_an_explicit_price_still_stores_as_itself(at_parent, roundtrip_db_url):
    """Anti-widening control. The charging *mechanism* is untouched — cloud#177 changed
    what our plans charge, not whether a plan can charge. A migration that added a CHECK
    or rewrote the column type would pass every test above and fail this one."""
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

    with at_parent.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO plans (name, slug, paddle_price_id, paddle_product_id, "
                "price_cents, interval, overage_run_price_cents) VALUES ('Priced', "
                "'priced', 'pri_p', 'pro_p', 100, 'monthly', 7)"
            )
        )

    assert _stored(at_parent, "priced") == 7


def test_the_downgrade_restores_the_default_without_re_pricing(at_parent, roundtrip_db_url):
    """A rollback re-arms the default for NEW rows and leaves the zeroed rows at zero.

    🚨 **Deliberate asymmetry, and it is the one judgement in this migration.** A true
    inverse would restore ``1`` on every row it zeroed. It must not: re-pricing on the way
    down would start billing customers, on a dimension we publish as fair-use, at the
    exact moment an operator is rolling back for an unrelated reason. Under-billing is
    recoverable; a surprise charge on a page that promised none is not. The downgrade
    therefore restores **DDL, not prices** — the same call ``b4d8f1a2c6e9`` made, for the
    same reason.
    """
    _create_paid_rows_the_way_production_was(at_parent)
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    assert _stored(at_parent, "pro-monthly") == 0

    down = _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url)
    assert down.returncode == 0, f"downgrade failed: {down.stderr}"

    default = _column_default(at_parent)
    assert default is not None and "1" in default, f"the default was not restored: {default!r}"
    assert _stored(at_parent, "pro-monthly") == 0, (
        "the downgrade re-priced a zeroed plan. A rollback must not start charging for "
        "something /pricing publishes as included"
    )

    with at_parent.begin() as conn:
        conn.execute(_INSERT_OMITTING, {"slug": "pro-after-down", "price": "pri_ad"})
    assert _stored(at_parent, "pro-after-down") == 1

    # And back up again — a rollback is rarely the last thing that happens.
    assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
    assert _column_default(at_parent) is None
    assert _stored(at_parent, "pro-after-down") == 0
