"""Add plans.max_api_keys — the seam for an API-key cap, with no cap chosen.

Revision ID: e5f6a7b8c9d0
Revises: c1d2e3f4a5b6
Create Date: 2026-08-31 14:30:00.000000

core#706. API keys were the only priced dimension with no ``*.before_create``
quota hook, and the reason was structural rather than an oversight in the
limiter: ``api_middleware`` resolves ``rate_limit_rpm`` per **org** and buckets
per **key** (``bucket=f"{api_key.id}"``), while nothing capped how many keys an
org may create. An org's real API allowance was therefore
``rate_limit_rpm × (however many keys it makes)``, with the second factor
unbounded and free — QA measured that **a Free org with ten keys sustains
300 rpm, exactly what Enterprise is sold** (``plans/qa/notes/probe-705/``,
finding 4). Documenting "per key" on the pricing page made that behaviour
honest; it did not make it priced.

🚨 **This migration writes no value.** The column is nullable and every existing
row keeps NULL, which cloud's ``check_api_key_quota`` reads as *uncapped*, so
production behaviour is unchanged by this deploy. That is deliberate: the cap is
a **pricing** decision, not an engineering one. Any cap ``n`` on a tier sets that
tier's real ceiling at ``n × rate_limit_rpm``, i.e. choosing ``n`` chooses how
far each tier may exceed the next tier's published rate. Recorded on core#706
for the founder.

⚠️ Contrast with ``c1d2e3f4a5b6``, deliberately. That migration existed *because*
NULL byte allotments meant enforcement silently skipped every row while the
enforce flag was on. Here NULL is the intended, tested state and the gate is
inert by design — ``tests/test_api_key_quota.py`` (cloud) asserts both
directions so "uncapped" is a decision rather than an accident.

Expand-only: a nullable ``ADD COLUMN`` is tolerated by the currently deployed
code, which does not know the column exists
(``docs/specs/SPEC_EXPAND_CONTRACT_MIGRATIONS.md``). No backfill, no
``SET NOT NULL``, nothing destructive — and no ``server_default``, which would
choose a cap nobody decided.
"""

import sqlalchemy as sa
from alembic import op

revision = "e5f6a7b8c9d0"
down_revision = "c1d2e3f4a5b6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("plans", sa.Column("max_api_keys", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("plans", "max_api_keys")
