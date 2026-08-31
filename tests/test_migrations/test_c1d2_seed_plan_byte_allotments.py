"""The V2 plan-catalogue seed, against a real Postgres with real rows in it.

[core#713]. V2 P1 added ``bytes_included`` / ``overage_bytes_price_cents_per_gb``
/ ``hard_cap_bytes`` to ``plans`` in ``z5v2w3x4y6a7``. **No migration has ever
put a value in them**, and ``check_bytes_quota`` reads NULL as "this plan has no
volume dimension — skip". So enforcement skipped every row, and a cutover whose
success signal is "no quota errors" could not tell that apart from enforcement
working.

Two things this file checks that a schema round-trip cannot:

1. **Values, not shape.** ``test_roundtrip.py`` runs every migration on an
   *empty* database, so an ``UPDATE plans SET …`` there touches zero rows and
   proves nothing. These tests seed the catalogue at the parent revision first.
2. **The rollback window does not discard a hand-set value.** This is the
   [core#726] lesson applied before it costs anything: that migration's
   ``downgrade()`` dropped a column and its ``upgrade()`` re-derived it from a
   default, so a round-trip came back schema-identical and data-lossily. The
   same trap is live here — a naive ``downgrade()`` that NULLs the columns
   hands the next ``upgrade()`` a NULL, which it would then overwrite with the
   catalogue value, silently discarding whatever an operator had set.

The published numbers on datanika.io/pricing are the acceptance criteria
(founder decision 2026-08-31, option (c) on landing#396), so they are written
here as literals rather than imported from anything the migration could also be
wrong about.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from tests.test_migrations.conftest import _run_alembic

PARENT_REVISION = "a7b8c9d0e1f2"
THIS_REVISION = "c1d2e3f4a5b6"

GIB = 1024**3

# datanika.io/pricing, verbatim. Free 10 GB hard-capped; Pro 100 GB + $0.50/GB;
# Enterprise 1 TB + $0.25/GB.
EXPECTED = {
    "free": (10 * GIB, None, True),
    "pro-monthly": (100 * GIB, 50, False),
    "pro-annual": (100 * GIB, 50, False),
    "enterprise-monthly": (1024**4, 25, False),
    "enterprise-annual": (1024**4, 25, False),
}

# The plan rows the catalogue is seeded from. Only `free`, `pro-monthly` and
# `enterprise-monthly` are created by migrations; the two annual rows come from
# `datanika-cloud/scripts/seed_annual_plans.py`, so they are seeded explicitly
# here — a migration that only handles the three it can see would leave the
# annual tiers unenforced, which is the same defect one level down.
_SEED_SLUGS = [
    ("free", "Free", 0, "monthly", 500, True),
    ("pro-monthly", "Pro", 7900, "monthly", 15000, True),
    ("pro-annual", "Pro", 79000, "yearly", 15000, True),
    ("enterprise-monthly", "Enterprise", 39900, "monthly", 50000, False),
    ("enterprise-annual", "Enterprise", 399000, "yearly", 50000, False),
]


def _seed_catalogue(conn) -> None:
    conn.execute(text("DELETE FROM plans"))
    for slug, name, price, interval, runs, hard_cap_runs in _SEED_SLUGS:
        conn.execute(
            text(
                """
                INSERT INTO plans (
                    name, slug, paddle_price_id, paddle_product_id, price_cents,
                    interval, runs_included, hard_cap_runs, is_active
                ) VALUES (
                    :name, :slug, :price_id, :product_id, :price,
                    :interval, :runs, :hard_cap_runs, true
                )
                """
            ),
            {
                "name": name,
                "slug": slug,
                "price_id": f"pri_{slug}",
                "product_id": f"pro_{slug}",
                "price": price,
                "interval": interval,
                "runs": runs,
                "hard_cap_runs": hard_cap_runs,
            },
        )


@pytest.fixture
def seeded_at_parent(roundtrip_db_url):
    """A database at the parent revision holding the five catalogue rows."""
    engine = create_engine(roundtrip_db_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))

    result = _run_alembic(["upgrade", PARENT_REVISION], roundtrip_db_url)
    assert result.returncode == 0, result.stderr

    with engine.begin() as conn:
        _seed_catalogue(conn)
    return engine


def _read_plans(engine) -> dict[str, tuple]:
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT slug, bytes_included, overage_bytes_price_cents_per_gb, "
                "hard_cap_bytes, hard_cap_runs FROM plans"
            )
        ).all()
    return {r[0]: (r[1], r[2], r[3], r[4]) for r in rows}


class TestSeedsTheCatalogue:
    def test_every_plan_gets_a_byte_allotment(self, seeded_at_parent, roundtrip_db_url):
        """The headline: no plan row may come out of this migration with NULL.

        This is the assertion that would have caught the present state, and it
        is the one that catches the *next* plan added without an allotment.
        """
        before = _read_plans(seeded_at_parent)
        assert all(v[0] is None for v in before.values()), "fixture must start unseeded"

        result = _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url)
        assert result.returncode == 0, result.stderr

        after = _read_plans(seeded_at_parent)
        unseeded = [slug for slug, v in after.items() if v[0] is None]
        assert unseeded == [], f"plans still reading NULL bytes_included: {unseeded}"

    @pytest.mark.parametrize("slug", sorted(EXPECTED))
    def test_values_match_the_published_page(self, seeded_at_parent, roundtrip_db_url, slug):
        result = _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url)
        assert result.returncode == 0, result.stderr

        included, price, hard_cap = EXPECTED[slug]
        row = _read_plans(seeded_at_parent)[slug]
        assert row[0] == included, f"{slug} bytes_included"
        assert row[1] == price, f"{slug} overage_bytes_price_cents_per_gb"
        assert row[2] is hard_cap, f"{slug} hard_cap_bytes"

    def test_pro_stops_hard_capping_runs(self, seeded_at_parent, roundtrip_db_url):
        """*"No surprise mid-cycle blocks"* — datanika.io/pricing FAQ.

        Pro shipped as ``hard_cap_runs = true`` at 15,000 runs, so it blocked
        mid-cycle on the secondary dimension while the page promised it would
        not. Overage bills; it does not block.
        """
        assert _read_plans(seeded_at_parent)["pro-monthly"][3] is True

        result = _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url)
        assert result.returncode == 0, result.stderr

        after = _read_plans(seeded_at_parent)
        assert after["pro-monthly"][3] is False
        assert after["pro-annual"][3] is False
        assert after["enterprise-monthly"][3] is False

    def test_free_still_hard_caps_runs(self, seeded_at_parent, roundtrip_db_url):
        """Negative control for the test above.

        A migration that simply cleared ``hard_cap_runs`` everywhere would
        satisfy it. Free's cap is published (*"500 model runs / month"*) and
        stays enforced — the page promises no mid-cycle blocks for Pro and
        Enterprise only.
        """
        result = _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url)
        assert result.returncode == 0, result.stderr

        assert _read_plans(seeded_at_parent)["free"][3] is True


class TestRollbackDoesNotDiscardAHandSetValue:
    """The [core#726] trap, checked before it costs anything.

    ``billing/e2e_admin.py`` writes ``plan.bytes_included`` directly to seed
    overage tenants, and an operator can set it by hand. A ``downgrade()`` that
    NULLs the column unconditionally hands the next ``upgrade()`` a NULL, which
    it fills with the catalogue value — so the round-trip is schema-identical
    and silently discards the operator's number. Counts and NOT-NULL both
    survive that; only comparing **values** catches it.
    """

    def test_a_custom_allotment_survives_downgrade_then_upgrade(
        self, seeded_at_parent, roundtrip_db_url
    ):
        result = _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url)
        assert result.returncode == 0, result.stderr

        custom = 250 * GIB
        with seeded_at_parent.begin() as conn:
            conn.execute(
                text("UPDATE plans SET bytes_included = :v WHERE slug = 'pro-monthly'"),
                {"v": custom},
            )

        assert _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url).returncode == 0
        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

        assert _read_plans(seeded_at_parent)["pro-monthly"][0] == custom, (
            "the operator's 250 GB was replaced by the catalogue's 100 GB across the "
            "rollback window — this is core#726 in a new place"
        )

    def test_an_untouched_row_round_trips_to_the_catalogue_value(
        self, seeded_at_parent, roundtrip_db_url
    ):
        """Negative control: a downgrade that did nothing at all would pass the
        test above. A row nobody customised must still come back seeded."""
        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
        assert _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url).returncode == 0

        assert _read_plans(seeded_at_parent)["free"][0] is None, "downgrade did not revert"

        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
        assert _read_plans(seeded_at_parent)["free"][0] == 10 * GIB

    def test_upgrade_does_not_overwrite_a_preset_value(self, seeded_at_parent, roundtrip_db_url):
        """core#713's explicit requirement: set the column only where it is
        currently NULL, or the migration resets a staging fixture mid-suite."""
        preset = 7 * GIB
        with seeded_at_parent.begin() as conn:
            conn.execute(
                text("UPDATE plans SET bytes_included = :v WHERE slug = 'free'"),
                {"v": preset},
            )

        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
        assert _read_plans(seeded_at_parent)["free"][0] == preset


class TestIdempotent:
    def test_running_the_migration_twice_is_stable(self, seeded_at_parent, roundtrip_db_url):
        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0
        first = _read_plans(seeded_at_parent)

        assert _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url).returncode == 0
        assert _run_alembic(["upgrade", THIS_REVISION], roundtrip_db_url).returncode == 0

        assert _read_plans(seeded_at_parent) == first
