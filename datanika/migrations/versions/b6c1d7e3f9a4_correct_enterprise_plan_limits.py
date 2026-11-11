"""Correct four enterprise plan limits the March UPDATE never reached.

``o4k1l2m3n5h6`` runs::

    UPDATE plans SET seats_included = 10, max_connections = 50,
        runs_included = 50000, extra_seat_price_cents = 2500
    WHERE slug = 'enterprise-monthly'

**No migration has ever created that slug.** Over the whole chain the only slug
any migration ``INSERT``s is ``free``. On a from-scratch build that UPDATE
matches zero rows, and the enterprise row is created afterwards taking
``j9f6g7h8i0c1``'s Free server defaults: seats 2, connections 5, runs 500,
extra seat 1200 ($12). The published values are 10, 50, 50000 and 2500 ($25).

Same class as ``f6a7b8c9d0e1`` (core#780). core#1060.

Production rows already hold the published values. This UPDATE sets the same
four columns by slug, so a row that already holds them and a row that still
holds the Free defaults both end on the published values. It does not touch
any other column. ``price_cents`` stays out: annual pricing differs, and the
column has no server_default for the guard to compare.

``enterprise-annual`` is named as well as ``enterprise-monthly``.
``seed_annual_plans.py`` copies these columns off the monthly row, so a later
seed inherits a corrected monthly row. An annual row that already exists does
not. core#713 left the annual tiers out of a partial correction; this does not.

A from-scratch build still has no paid row when the UPDATE runs, so the
matched count is zero. That zero is expected. The paid rows are created
afterwards, out of band, because their Paddle ids differ per environment.
Failing the migration on a zero count would break every new deployment.

The same upgrade installs an insert correction for these two slugs only. A
row inserted afterwards receives the four published values even when the
INSERT omits the columns. Free's server defaults stay 2, 5, 500 and 1200,
so a ``free`` row inserted afterwards is unchanged. The correction does not
insert a plan row.

SQLite corrects the inserted row by primary key from an AFTER INSERT
trigger. PostgreSQL assigns ``NEW`` in a BEFORE INSERT function. The inner
update matches ``id``, not ``slug``.

Expand/contract: the data change is DML. The trigger and its function are
additive. Downgrade drops them. No column is added, dropped, renamed,
narrowed, or set NOT NULL.

Revision ID: b6c1d7e3f9a4
Revises: c4d8e1f6a2b7
Create Date: 2026-11-11 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b6c1d7e3f9a4"
down_revision: str | None = "c4d8e1f6a2b7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Published enterprise limits. Both cadences: annual and monthly differ in
# price and interval, not in these four columns.
ENTERPRISE_LIMIT_SLUGS: tuple[str, ...] = ("enterprise-monthly", "enterprise-annual")

ENTERPRISE_PLAN_LIMITS: dict[str, int] = {
    "seats_included": 10,
    "max_connections": 50,
    "runs_included": 50000,
    "extra_seat_price_cents": 2500,
}

# ``j9f6g7h8i0c1`` server defaults. Downgrade restores only a row still holding
# exactly what upgrade() wrote, back to these, and leaves a customised cell.
FRESH_ROW_DEFAULTS: dict[str, int] = {
    "seats_included": 2,
    "max_connections": 5,
    "runs_included": 500,
    "extra_seat_price_cents": 1200,
}

_UPGRADE_SQL = {
    "seats_included": "UPDATE plans SET seats_included = :v WHERE slug = :s",
    "max_connections": "UPDATE plans SET max_connections = :v WHERE slug = :s",
    "runs_included": "UPDATE plans SET runs_included = :v WHERE slug = :s",
    "extra_seat_price_cents": "UPDATE plans SET extra_seat_price_cents = :v WHERE slug = :s",
}

_DOWNGRADE_SQL = {
    "seats_included": (
        "UPDATE plans SET seats_included = :d WHERE slug = :s AND seats_included = :v"
    ),
    "max_connections": (
        "UPDATE plans SET max_connections = :d WHERE slug = :s AND max_connections = :v"
    ),
    "runs_included": ("UPDATE plans SET runs_included = :d WHERE slug = :s AND runs_included = :v"),
    "extra_seat_price_cents": (
        "UPDATE plans SET extra_seat_price_cents = :d "
        "WHERE slug = :s AND extra_seat_price_cents = :v"
    ),
}


def _apply(conn, sql: str, params: dict, label: str) -> None:
    result = conn.execute(sa.text(sql), params)
    # `print`, not `logging`. Migrations run from the container start command,
    # so stdout reaches the deploy log unconditionally.
    print(
        f"[core#1060] {label:<52} rows matched: {result.rowcount}"
        + ("   <-- ZERO: row absent at this point in the chain" if not result.rowcount else "")
    )


def _slug_sql_list() -> str:
    return ", ".join(f"'{slug}'" for slug in ENTERPRISE_LIMIT_SLUGS)


def _sqlite_assignments() -> str:
    return ", ".join(f"{column} = {value}" for column, value in ENTERPRISE_PLAN_LIMITS.items())


def _postgres_assignments() -> str:
    # One PL/pgSQL statement each. A comma-separated ``:=`` list is not an assignment.
    return " ".join(f"NEW.{column} := {value};" for column, value in ENTERPRISE_PLAN_LIMITS.items())


def _install_insert_correction(conn) -> None:
    """Correct a later insert of the two enterprise slugs. No other slug."""
    dialect = conn.dialect.name
    if dialect == "sqlite":
        # WHERE id, not WHERE slug: one inserted row, and the class guard
        # treats `UPDATE plans SET ... WHERE slug` as another seed write.
        conn.exec_driver_sql("DROP TRIGGER IF EXISTS plans_enterprise_limits_insert")
        conn.exec_driver_sql(
            "CREATE TRIGGER plans_enterprise_limits_insert "
            "AFTER INSERT ON plans "
            f"WHEN NEW.slug IN ({_slug_sql_list()}) BEGIN "
            f"UPDATE plans SET {_sqlite_assignments()} WHERE id = NEW.id; END"
        )
        return
    if dialect == "postgresql":
        conn.exec_driver_sql(
            "CREATE OR REPLACE FUNCTION datanika_enterprise_plan_limits() "
            "RETURNS trigger LANGUAGE plpgsql AS $fn$ "
            "BEGIN "
            f"IF NEW.slug IN ({_slug_sql_list()}) THEN "
            f"{_postgres_assignments()} "
            "END IF; "
            "RETURN NEW; "
            "END; "
            "$fn$"
        )
        conn.exec_driver_sql("DROP TRIGGER IF EXISTS plans_enterprise_limits_insert ON plans")
        conn.exec_driver_sql(
            "CREATE TRIGGER plans_enterprise_limits_insert "
            "BEFORE INSERT ON plans "
            "FOR EACH ROW EXECUTE FUNCTION datanika_enterprise_plan_limits()"
        )
        return
    raise RuntimeError(f"core#1060 insert correction has no {dialect} trigger")


def _drop_insert_correction(conn) -> None:
    dialect = conn.dialect.name
    if dialect == "sqlite":
        conn.exec_driver_sql("DROP TRIGGER IF EXISTS plans_enterprise_limits_insert")
        return
    if dialect == "postgresql":
        conn.exec_driver_sql("DROP TRIGGER IF EXISTS plans_enterprise_limits_insert ON plans")
        conn.exec_driver_sql("DROP FUNCTION IF EXISTS datanika_enterprise_plan_limits()")
        return
    raise RuntimeError(f"core#1060 insert correction has no {dialect} trigger")


def upgrade() -> None:
    conn = op.get_bind()
    for slug in ENTERPRISE_LIMIT_SLUGS:
        for column, value in ENTERPRISE_PLAN_LIMITS.items():
            _apply(
                conn,
                _UPGRADE_SQL[column],
                {"v": value, "s": slug},
                f"{column}={value} slug={slug}",
            )
    _install_insert_correction(conn)


def downgrade() -> None:
    """Revert only a cell still holding exactly what ``upgrade()`` wrote."""
    conn = op.get_bind()
    _drop_insert_correction(conn)
    for slug in ENTERPRISE_LIMIT_SLUGS:
        for column, value in ENTERPRISE_PLAN_LIMITS.items():
            conn.execute(
                sa.text(_DOWNGRADE_SQL[column]),
                {"d": FRESH_ROW_DEFAULTS[column], "s": slug, "v": value},
            )
