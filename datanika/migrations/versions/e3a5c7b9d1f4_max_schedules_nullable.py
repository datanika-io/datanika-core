"""Let ``plans.max_schedules`` hold NULL, so "unlimited" is representable (cloud#151).

`/pricing`, the homepage, `/features/volume-pricing/`, `/docs/scheduling-guide`, two
blog posts and the ``SoftwareApplication`` JSON-LD all advertise **"Unlimited
schedules"** on Pro and Enterprise. Production enforces ``max_schedules = 9999``.
Under the founder's standing option-(c) decision the published page stays and the
code moves.

"Unlimited" is carried in this codebase by **NULL, enforced by readers** — the same
one meaning ``plans.bytes_included``, ``plans.max_api_keys`` and
``subscriptions.seats_purchased`` already have. Two readers for *this* column are
already shipped: ``BillingService.check_schedule_quota`` (cloud#165) and the billing
page's ``PlanInfo`` / ``SubscriptionInfo`` (cloud#164). This migration is what makes
the value representable at all.

⚠️ **cloud#151's written plan could not run without this.** It proposed
``UPDATE plans SET max_schedules = NULL`` directly, and the column is ``nullable=False``
in ``j9f6g7h8i0c1`` *and* in the cloud model — so that statement would have failed
outright. The expand step was missing from the plan, not merely from the code.

**Expand-only, per ``docs/specs/SPEC_EXPAND_CONTRACT_MIGRATIONS.md``.** Dropping
``NOT NULL`` is a *loosening*. At ``t1`` the previously deployed container serves
against this schema while every existing row still carries an integer, so nothing the
old code reads changes; the forbidden direction is ``SET NOT NULL``, which is what the
**downgrade** does, and that is why the downgrade backfills first.

🚨 **This sets no row to NULL.** That is cloud#151 step 4, and it waits until all
readers are in the **running image** rather than merely on a branch. Cloud ships
*inside* the core image at a pinned ``ref: master``, so a cloud promotion with no core
promotion behind it is a change that has not shipped — and arming the NULL before the
billing-page reader is deployed takes that page down for precisely the paying customer
the feature exists for.

⚠️ ``plans`` is a **cloud** table under core's Alembic: the column is core's, the model
is ``datanika_cloud/billing/models.py``. Cloud's ``tests/test_migration_coverage.py``
compares column **names** only, so this migration and the cloud model's
``Mapped[int | None]`` produce **no red window in either direction** — which also means
nothing mechanical will notice if only one of the two ships. Measured while writing
this: widening that guard to compare nullability would report **six pre-existing
disagreements**, all ``created_at``/``updated_at`` from ``TimestampMixin``, so it is its
own piece of work and is filed separately rather than bundled here.

Revision ID: e3a5c7b9d1f4
Revises: d5e1f3a7b9c2
Create Date: 2026-09-04
"""

import sqlalchemy as sa
from alembic import op

revision = "e3a5c7b9d1f4"
down_revision = "d5e1f3a7b9c2"
branch_labels = None
depends_on = None

#: What a NULL row is restored to on the way down. 9999 is the fair-use figure the four
#: paid slugs carry today, so it is faithful for every row step 4 will NULL. NULL has no
#: exact integer inverse, so this is a choice and is stated rather than implied.
_DOWNGRADE_FILL = 9999


def upgrade() -> None:
    op.alter_column(
        "plans",
        "max_schedules",
        existing_type=sa.Integer(),
        existing_server_default=sa.text("10"),
        nullable=True,
    )


def downgrade() -> None:
    # Backfill BEFORE re-tightening. A downgrade that only re-adds NOT NULL aborts on
    # any row a later release set to NULL, and a rollback is when a half-applied
    # migration costs the most.
    op.execute(
        sa.text("UPDATE plans SET max_schedules = :fill WHERE max_schedules IS NULL").bindparams(
            fill=_DOWNGRADE_FILL
        )
    )
    op.alter_column(
        "plans",
        "max_schedules",
        existing_type=sa.Integer(),
        existing_server_default=sa.text("10"),
        nullable=False,
    )
