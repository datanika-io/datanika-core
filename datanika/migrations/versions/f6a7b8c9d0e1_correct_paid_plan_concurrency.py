"""Correct max_parallel_runs on the paid plan rows the April UPDATE never reached.

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-09-02 13:40:00.000000

core#780, second half. ``s8o5p6q7r9l0`` added the column in April and ran

    UPDATE plans SET max_parallel_runs = 20 WHERE slug = 'enterprise-monthly'

**No migration has ever created that row.** Over the whole chain the only slug
any migration ``INSERT``s is ``free`` (``q6m3n4o5p7j8``); ``pro-monthly``,
``enterprise-monthly``, ``pro-annual`` and ``enterprise-annual`` are created out
of band — annual by ``datanika-cloud/scripts/seed_annual_plans.py``, monthly by
something in neither repo. So on the from-scratch **2026-07-16** rebuild alembic
ran to head against an empty ``plans``, every paid-slug UPDATE matched **zero
rows**, and the rows were created afterwards taking the column's
``server_default`` of 5.

Measured on production 2026-09-02T09:31Z (Growth + Infra): four of five rows
hold exactly 5, and ``free`` holds 2. That asymmetry is the signature — ``free``
is the one row a migration creates, so it is the one row the April UPDATE could
reach.

**Why this matters more now than it did in April.** Until cloud PR #147 the
column was read by nothing, so a wrong value was inert. It is now read on every
run dispatch, which means Enterprise is *sold* 20 and *served* 5, and the only
live effect of wiring the column up was Free going 5 → 2. A correct reader over
wrong data is indistinguishable from a correct system.

Expand/contract: **DML only, no DDL.** No column is added, dropped, narrowed or
constrained, and the currently-deployed code already reads this column, so the
previously-deployed version tolerates it by construction.

⚠️ This migration cannot *raise* when a slug matches zero rows, and that is the
trap rather than an oversight: on a genuinely fresh database the paid rows do
not exist yet, and failing there would break every new deployment. What it can
do — and what the April one did not — is **say so**. The class is caught at PR
time instead, by
``tests/test_migrations/test_plan_seed_updates_reach_real_rows.py``.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f6a7b8c9d0e1"
down_revision: str | None = "e5f6a7b8c9d0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# slug -> published concurrent-run ceiling.
#
# Free 2 and Enterprise 20 are the April values; the two annual slugs carry
# their monthly tier's entitlement, which is what `seed_annual_plans.py`'s
# COPIED_QUOTA_COLUMNS asserts ("annual and monthly differ in price and cadence,
# never in quota"). `pro-monthly` at 5 is listed even though it already equals
# the server_default — leaving it out would make this dict a partial statement
# of the entitlements and invite the next reader to treat the default as the
# answer for Pro, which is the mistake this whole issue is made of.
#
# ⚠️ Kept in step with `datanika/services/concurrency_service.py` by
# `tests/test_migrations/test_plan_seed_updates_reach_real_rows.py`, NOT by
# discipline. The dict that used to state these values lived in that module,
# was read by nobody, and drifted (core#915).
PUBLISHED_MAX_PARALLEL_RUNS: dict[str, int] = {
    "free": 2,
    "pro-monthly": 5,
    "pro-annual": 5,
    "enterprise-monthly": 20,
    "enterprise-annual": 20,
}

# Enterprise is sold SSO on the pricing page — `pricing-tiers.ts:88`
# ("SSO (SAML/OIDC)") and `pricing-faq.ts:74` — and cloud enforces it at
# `billing/service.py:818`: `if not plan.sso_enabled` -> refuse, on
# `sso_config.before_create`.
#
# `p5l2m3n4o6i7` intended `true` for `enterprise-monthly` and ran during the
# rebuild against an empty table, so the row was created afterwards with the
# column's `server_default` of **false**. `seed_annual_plans.py` then copied
# that false onto `enterprise-annual`.
#
# ⚠️ Included here even though the prod value is a PREDICTION rather than a
# measurement (core#928), because the direction is safe either way: the
# entitlement is unambiguous — the page sells it, the FAQ sells it, and the
# April migration intended it — so an idempotent UPDATE to `true` is a no-op if
# the row is already correct and a fix if it is not. `rate_limit_rpm` is
# deliberately NOT here: its published burst claim was deleted rather than
# implemented (core#703), so the migration's intent has no page behind it and
# "restore it" is a Product decision, not a repair.
SSO_ENABLED_SLUGS: tuple[str, ...] = ("enterprise-monthly", "enterprise-annual")


def _apply(conn, sql: str, params: dict, label: str) -> None:
    result = conn.execute(sa.text(sql), params)
    # `print`, not `logging`. Migrations run from the container start command,
    # so stdout reaches the deploy log unconditionally; a logger can be silenced
    # by configuration, and a report that can be silenced is the exact failure
    # this migration exists to correct.
    print(
        f"[core#780] {label:<48} rows matched: {result.rowcount}"
        + ("   <-- ZERO: row absent at this point in the chain" if not result.rowcount else "")
    )


def upgrade() -> None:
    conn = op.get_bind()
    for slug, value in PUBLISHED_MAX_PARALLEL_RUNS.items():
        _apply(
            conn,
            "UPDATE plans SET max_parallel_runs = :v WHERE slug = :s",
            {"v": value, "s": slug},
            f"max_parallel_runs={value} slug={slug}",
        )
    for slug in SSO_ENABLED_SLUGS:
        _apply(
            conn,
            "UPDATE plans SET sso_enabled = true WHERE slug = :s",
            {"s": slug},
            f"sso_enabled=true slug={slug}",
        )


def downgrade() -> None:
    """Revert only a row still holding exactly what ``upgrade()`` wrote.

    core#726's lesson, applied before it costs anything: that migration's
    downgrade dropped a column whose upgrade re-derived the value from a
    default, so the round trip came back schema-identical and **data-lossily**.
    A blanket ``SET max_parallel_runs = 5`` here would discard an operator's
    deliberately customised ceiling and hand the next ``upgrade()`` a value it
    would overwrite anyway. Matching on the current value leaves a customised
    row alone in both directions.
    """
    conn = op.get_bind()
    for slug, value in PUBLISHED_MAX_PARALLEL_RUNS.items():
        conn.execute(
            sa.text(
                "UPDATE plans SET max_parallel_runs = 5 WHERE slug = :s AND max_parallel_runs = :v"
            ),
            {"s": slug, "v": value},
        )
    for slug in SSO_ENABLED_SLUGS:
        conn.execute(
            sa.text("UPDATE plans SET sso_enabled = false WHERE slug = :s AND sso_enabled = true"),
            {"s": slug},
        )
