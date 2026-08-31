"""Seed the V2 byte allotments on plans; stop Pro hard-capping runs.

Revision ID: c1d2e3f4a5b6
Revises: a7b8c9d0e1f2
Create Date: 2026-08-31 12:00:00.000000

core#713. ``z5v2w3x4y6a7`` added ``bytes_included``,
``overage_bytes_price_cents_per_gb`` and ``hard_cap_bytes`` to ``plans`` for
V2 P1. **No migration has ever written a value into them.** Cloud's
``check_bytes_quota`` reads ``bytes_included IS NULL`` as *"this plan has no
volume dimension — skip"*, so volume enforcement skipped every row while
``DATANIKA_BYTES_QUOTA_ENFORCE=true`` was already set in production — a
cutover whose success signal is "no quota errors" cannot distinguish
enforcement working from enforcement skipping everything.

Values are the ones published on datanika.io/pricing, which the founder made
the acceptance criteria on 2026-08-31 (option (c) on landing#396): Free 10 GB
hard-capped, Pro 100 GB + $0.50/GB, Enterprise 1 TB + $0.25/GB. A GB here is
1024³ bytes, matching ``_overage_price_cents``'s ``1024**3`` divisor and the
``$0.39/GB`` effective Enterprise rate the comparison pages publish
(399 ÷ 1024, not 399 ÷ 1000).

**Also flips Pro to ``hard_cap_runs = false``.** The pricing FAQ says *"Pro and
Enterprise bill overage … No surprise mid-cycle blocks"*, and
``SPEC_PRICING_V2.md`` §4.2 heads the same point *"Runs never block mid-flight
on Pro and Enterprise"* — while Pro shipped hard-capped at 15,000 runs and
``check_run_quota`` is not flag-gated. The published 15,000 stays as a
fair-use figure with an 80% warning behind it; it no longer blocks. Free keeps
its cap: the page publishes *"500 model runs / month"* for Free and promises
no mid-cycle blocks for the paid tiers only.

Expand/contract: DML only, no DDL. Both columns already exist and the
currently-deployed cloud code already reads them, so the previously-deployed
version tolerates this by construction. What it does change is *behaviour* —
see the deploy note on the PR.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "c1d2e3f4a5b6"
down_revision: str | None = "a7b8c9d0e1f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_GIB = 1024**3

# slug -> (bytes_included, overage_bytes_price_cents_per_gb, hard_cap_bytes)
#
# The two annual slugs are created by `datanika-cloud/scripts/seed_annual_plans.py`
# rather than by a migration, so they may or may not exist here. Updating by
# slug means a missing row is a no-op rather than an error — and a migration
# that only handled the three slugs it can see would leave the annual tiers
# unenforced, which is this same defect one level down.
_CATALOGUE: dict[str, tuple[int, int | None, bool]] = {
    "free": (10 * _GIB, None, True),
    "pro-monthly": (100 * _GIB, 50, False),
    "pro-annual": (100 * _GIB, 50, False),
    "enterprise-monthly": (1024**4, 25, False),
    "enterprise-annual": (1024**4, 25, False),
}

# Tiers the pricing page promises will never block mid-cycle.
_NO_MID_CYCLE_BLOCK = ("pro-monthly", "pro-annual", "enterprise-monthly", "enterprise-annual")


def upgrade() -> None:
    for slug, (included, price, hard_cap) in _CATALOGUE.items():
        # `WHERE bytes_included IS NULL` is required, not tidiness:
        # `billing/e2e_admin.py` writes `plan.bytes_included` directly to seed
        # overage tenants, so an unconditional UPDATE would reset a staging
        # fixture out from under a running suite.
        op.execute(
            f"""
            UPDATE plans
            SET bytes_included = {included},
                overage_bytes_price_cents_per_gb = {"NULL" if price is None else price},
                hard_cap_bytes = {"true" if hard_cap else "false"}
            WHERE slug = '{slug}' AND bytes_included IS NULL
            """
        )

    # "No surprise mid-cycle blocks" (pricing FAQ). Overage bills; it does not
    # block. Enterprise was already false; Pro was not.
    slugs = ", ".join(f"'{s}'" for s in _NO_MID_CYCLE_BLOCK)
    op.execute(f"UPDATE plans SET hard_cap_runs = false WHERE slug IN ({slugs})")


def downgrade() -> None:
    for slug, (included, price, hard_cap) in _CATALOGUE.items():
        # Revert ONLY a row still holding exactly what upgrade() wrote.
        #
        # This is core#726's lesson applied before it costs anything. That
        # migration's downgrade dropped a column and its upgrade re-derived the
        # value from a default, so the round-trip came back schema-identical
        # and data-lossily. The same trap is live here: a blanket
        # `SET bytes_included = NULL` would hand the next upgrade() a NULL,
        # which it then fills with the catalogue value — silently discarding
        # an operator's or a fixture's number. Matching on the current value
        # means a customised row is left alone in both directions.
        price_match = (
            "overage_bytes_price_cents_per_gb IS NULL"
            if price is None
            else f"overage_bytes_price_cents_per_gb = {price}"
        )
        op.execute(
            f"""
            UPDATE plans
            SET bytes_included = NULL,
                overage_bytes_price_cents_per_gb = NULL,
                hard_cap_bytes = true
            WHERE slug = '{slug}'
              AND bytes_included = {included}
              AND {price_match}
              AND hard_cap_bytes IS {"true" if hard_cap else "false"}
            """
        )

    # Pro's pre-migration state. Enterprise was already false before this
    # migration (o4k1l2m3n5h6), so restoring it to true would be wrong.
    op.execute("UPDATE plans SET hard_cap_runs = true WHERE slug IN ('pro-monthly', 'pro-annual')")
