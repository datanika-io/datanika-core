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

A from-scratch build still has no paid row when this runs, so the matched
count is zero and a row created afterwards still takes the server_default.
That is the limit ``g7h8i9j0k1l2`` records too. The rows-matched line below is
what tells a present row from an absent one. Failing the migration when the
count is zero would break every new deployment.

Expand/contract: **DML only, no DDL.** No column is added, dropped, renamed,
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


def downgrade() -> None:
    """Revert only a cell still holding exactly what ``upgrade()`` wrote."""
    conn = op.get_bind()
    for slug in ENTERPRISE_LIMIT_SLUGS:
        for column, value in ENTERPRISE_PLAN_LIMITS.items():
            conn.execute(
                sa.text(_DOWNGRADE_SQL[column]),
                {"d": FRESH_ROW_DEFAULTS[column], "s": slug, "v": value},
            )
