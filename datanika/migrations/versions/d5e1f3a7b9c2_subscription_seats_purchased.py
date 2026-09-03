"""Give a subscription its own seat allowance (cloud#150).

``seats_included`` lives on the **``plans``** row, and a plan row is shared by
every org on that slug. So the only lever an operator has for *"contact us to add
seats"* — which `/pricing` and the homepage both advertise on Enterprise — is
``UPDATE plans SET seats_included = n WHERE slug = 'enterprise-monthly'``, **which
grants a seat to every Enterprise customer at once.** There is no supported way to
give one org an extra seat, and no code path behind the published sentence.

``subscriptions.seats_purchased`` is that per-org number. NULL means *"this
subscription has bought no extra seats"* and the plan's figure applies — the same
one meaning for NULL that ``plans.bytes_included``, ``plans.max_api_keys`` and now
``plans.max_schedules`` carry, each enforced by a reader rather than by a
convention nothing implements.

⚠️ **Not ``subscriptions.seat_count``, which already exists and is not this.**
That column is the Paddle *item quantity*, written only from the webhook and read
by nothing that enforces anything; it defaults to **1**, and on an Enterprise
subscription whose price is not per-seat it stays 1. Reading it as an allowance
would cut Enterprise from 10 seats to 1. It answers "how many units of the
subscription", which coincides with "seats" only when the price is per-seat, and
ours is not.

**Expand-only, per SPEC_EXPAND_CONTRACT_MIGRATIONS.** One nullable ``ADD COLUMN``
with no default, no backfill and no constraint. Under blue/green the previously
deployed code runs against this schema while the old container is still serving;
it neither reads nor writes this column, so it cannot be broken by it. NULL is
also the *correct* value for every existing row — none of them has bought extra
seats — so no backfill is wanted rather than merely not required.

Nothing here is destructive, so there is no contract step to schedule in N+1.

⚠️ ``subscriptions`` is a **cloud** table under core's Alembic: the column is
core's, the model is cloud's (``datanika_cloud/billing/models.py``). Cloud's
``tests/test_migration_coverage.py`` asserts the two agree in **both**
directions, so this migration and the cloud model declaration are a pair —
between merging this and merging the cloud half, cloud CI reads red on
``test_cloud_columns_have_migration``'s mirror. That red is this sequencing, not
a defect.

Revision ID: d5e1f3a7b9c2
Revises: c9d3e5f7a1b2
Create Date: 2026-09-03
"""

import sqlalchemy as sa
from alembic import op

revision = "d5e1f3a7b9c2"
down_revision = "c9d3e5f7a1b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "subscriptions",
        sa.Column("seats_purchased", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("subscriptions", "seats_purchased")
