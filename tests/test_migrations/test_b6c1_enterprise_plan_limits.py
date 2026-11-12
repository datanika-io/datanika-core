"""core#1060. An enterprise row created after the correction used to take Free defaults.

``b6c1d7e3f9a4`` updates ``enterprise-monthly`` and ``enterprise-annual`` when those
slugs already exist. On a fresh database the slugs are absent and the UPDATE matches
zero rows. A row inserted afterwards still has to end on seats 10, connections 50,
runs 50000 and extra seat 2500. Free's server defaults stay 2, 5, 500 and 1200.
"""

from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, text

from datanika.migrations.versions.b6c1d7e3f9a4_correct_enterprise_plan_limits import (
    downgrade,
    upgrade,
)

_FREE_DEFAULTS = (2, 5, 500, 1200)
_PUBLISHED = (10, 50, 50000, 2500)


def _table(conn) -> None:
    conn.execute(
        text(
            """
            CREATE TABLE plans (
                id INTEGER PRIMARY KEY,
                slug VARCHAR(50) NOT NULL UNIQUE,
                price_cents INTEGER NOT NULL DEFAULT 0,
                seats_included INTEGER NOT NULL DEFAULT 2,
                max_connections INTEGER NOT NULL DEFAULT 5,
                runs_included INTEGER NOT NULL DEFAULT 500,
                extra_seat_price_cents INTEGER NOT NULL DEFAULT 1200
            )
            """
        )
    )


def _run(conn, fn) -> None:
    ctx = MigrationContext.configure(conn)
    with Operations.context(ctx):
        fn()


def _upgrade(conn) -> None:
    _run(conn, upgrade)


def _downgrade(conn) -> None:
    _run(conn, downgrade)


def _limits(conn, slug: str) -> tuple:
    row = conn.execute(
        text(
            "SELECT seats_included, max_connections, runs_included, "
            "extra_seat_price_cents FROM plans WHERE slug = :s"
        ),
        {"s": slug},
    ).one()
    return tuple(row)


def test_an_absent_enterprise_slug_inserted_after_upgrade_gets_the_published_limits():
    """The fresh-database case. The slug does not exist when the migration runs."""
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        _table(conn)
        _upgrade(conn)
        conn.execute(
            text("INSERT INTO plans (slug) VALUES ('enterprise-monthly'), ('enterprise-annual')")
        )
        assert _limits(conn, "enterprise-monthly") == _PUBLISHED
        assert _limits(conn, "enterprise-annual") == _PUBLISHED
        conn.execute(text("INSERT INTO plans (slug) VALUES ('free')"))
        assert _limits(conn, "free") == _FREE_DEFAULTS


def test_an_existing_enterprise_row_is_corrected_and_keeps_its_price():
    """A database that already has the slugs. Price is not one of the four columns."""
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        _table(conn)
        conn.execute(
            text(
                "INSERT INTO plans (slug, price_cents, seats_included, max_connections, "
                "runs_included, extra_seat_price_cents) VALUES "
                "('enterprise-monthly', 39900, 10, 50, 50000, 2500), "
                "('enterprise-annual', 399000, 2, 5, 500, 1200)"
            )
        )
        _upgrade(conn)
        assert _limits(conn, "enterprise-monthly") == _PUBLISHED
        assert _limits(conn, "enterprise-annual") == _PUBLISHED
        prices = dict(conn.execute(text("SELECT slug, price_cents FROM plans")).all())
        assert prices == {"enterprise-monthly": 39900, "enterprise-annual": 399000}


def test_downgrade_stops_correcting_a_later_insert():
    """Downgrade removes the insert correction and restores a cell it wrote."""
    engine = create_engine("sqlite://")
    with engine.begin() as conn:
        _table(conn)
        _upgrade(conn)
        conn.execute(
            text("INSERT INTO plans (slug, price_cents) VALUES ('enterprise-monthly', 39900)")
        )
        assert _limits(conn, "enterprise-monthly") == _PUBLISHED
        _downgrade(conn)
        assert _limits(conn, "enterprise-monthly") == _FREE_DEFAULTS
        price = conn.execute(
            text("SELECT price_cents FROM plans WHERE slug = 'enterprise-monthly'")
        ).scalar_one()
        assert price == 39900
        conn.execute(text("DELETE FROM plans WHERE slug = 'enterprise-monthly'"))
        conn.execute(text("INSERT INTO plans (slug) VALUES ('enterprise-monthly')"))
        assert _limits(conn, "enterprise-monthly") == _FREE_DEFAULTS
