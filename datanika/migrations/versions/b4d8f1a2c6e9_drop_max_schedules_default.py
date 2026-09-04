"""Drop ``plans.max_schedules``'s server default — core#1047.

`/pricing` sells **"Unlimited schedules"** on Pro and Enterprise. A from-scratch build
enforced **10**, and no migration could repair it:

1. ``j9f6g7h8i0c1`` declares the column ``server_default="10"``;
2. **no migration creates a paid slug.** Over the whole chain the only slug any migration
   ``INSERT``s is ``free`` (``q6m3n4o5p7j8``), which supplies ``max_schedules`` explicitly;
3. so ``a9c4e2b7d5f3``'s ``UPDATE … WHERE slug IN (four paid slugs)`` matches **zero rows**
   on a fresh database, and the paid rows are created afterwards — out of band, by
   ``datanika-cloud/scripts/seed_annual_plans.py`` for annual and by something in neither
   repository for monthly — taking the default.

Production reads 9999 only because whatever created its rows supplied that value. That is
core#928's asymmetry, and this column is one of its instances.

🔴 **A REVERSAL, and it should be visible as one.** cloud#151 deliberately gave this column
a ``server_default`` one day before this migration, reasoning that *"omission must not mean
unlimited: for a quota, that is the wrong direction to fail in."* That is sound in general.
It is wrong about this column, because of **who actually creates plan rows**: for the rows
that matter the default was never a safety net, it was the mechanism that capped them. The
only cap actually enforced — ``free``'s 2 — is written explicitly by its INSERT and never
depended on the default.

⚠️ **This makes the column fail OPEN.** A plan row created by someone who intended a cap and
forgot now gets none. That is the cost, it is accepted deliberately, and what bounds it is
the ``free`` INSERT above — pinned by
``tests/test_migrations/test_b4d8_max_schedules_drop_default.py::test_the_free_plan_keeps_its_published_cap_of_two``,
which fails if that INSERT ever stops naming the column.

Every other uncapped-means-NULL column in this schema carries **no** default —
``plans.bytes_included``, ``plans.max_api_keys``, ``subscriptions.seats_purchased``.
``max_schedules`` joined that convention in cloud#151 (three readers treat NULL as uncapped)
while keeping a finite default, so the column's *absence* behaviour still said "capped at
10" while its *convention* said "NULL is unlimited". This closes that.

**Expand/contract (SPEC_EXPAND_CONTRACT_MIGRATIONS).** Dropping a default is a *loosening*
and rewrites no existing row, so at ``t1`` the previously deployed code reads exactly the
values it read before. The cloud model's own ``server_default`` is used for DDL, not for
INSERT, so a container still running the pre-change model is unaffected either way. The
downgrade re-adds the default and deliberately does **not** backfill: filling the NULLs on
the way down would re-cap every paid tier, which is the damage this migration undoes.

Pairs with dropping ``server_default="10"`` from ``datanika_cloud/billing/models.py``.
⚠️ ``datanika-cloud/tests/test_migration_coverage.py`` compares column **names** only, so
that pair has no red window in either direction (cloud#171) — neither half is enforced by a
test, only by shipping them together.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b4d8f1a2c6e9"
down_revision: str | None = "a9c4e2b7d5f3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

#: What the column carried before this migration, restored on the way down. Named rather
#: than inlined so the downgrade cannot drift from what ``j9f6g7h8i0c1`` declared.
_PREVIOUS_DEFAULT = "10"


def upgrade() -> None:
    # `server_default=None` is alembic's DROP DEFAULT. It is not the same as omitting the
    # argument, which means "leave the default alone" — the two are distinguished by a
    # sentinel, so a refactor that drops this line changes behaviour silently.
    op.alter_column("plans", "max_schedules", server_default=None)


def downgrade() -> None:
    # No backfill, on purpose. Re-adding a default governs future INSERTs only; the paid
    # rows a release NULLed stay NULL, which is what a rollback should preserve.
    op.alter_column("plans", "max_schedules", server_default=sa.text(_PREVIOUS_DEFAULT))
