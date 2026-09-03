"""Record the outcome of every notification delivery attempt (core#652).

Adds three nullable columns to ``notification_channels`` so a channel can say
whether it is *working*, not merely whether it is switched on. Until now the
Settings page rendered a green "On" badge from ``is_active`` alone, beside an
email channel type that had never dispatched anything on any org.

**Expand-only, per SPEC_EXPAND_CONTRACT_MIGRATIONS.** Three nullable
``ADD COLUMN``s with no default, no backfill and no constraint. Under blue/green
the previously deployed code runs against this schema while the old container is
still serving; it neither reads nor writes these columns, so it cannot be broken
by them. NULL is also the *correct* value for a channel that has not been tried
yet — it renders as "never delivered", which is exactly what it is, so no
backfill is wanted rather than merely not required.

Nothing here is destructive, so there is no contract step to schedule in N+1.

Revision ID: c9d3e5f7a1b2
Revises: b4c7d1e8f2a6
Create Date: 2026-09-03
"""

import sqlalchemy as sa
from alembic import op

revision = "c9d3e5f7a1b2"
down_revision = "b4c7d1e8f2a6"
branch_labels = None
depends_on = None

#: Must match ``models.notification_channel.MAX_LAST_ERROR``. The bound is not
#: cosmetic: an unbounded error column is a second place for a payload to land,
#: and this one is rendered in the UI.
MAX_LAST_ERROR = 500


def upgrade() -> None:
    op.add_column(
        "notification_channels",
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "notification_channels",
        sa.Column("last_status", sa.String(length=16), nullable=True),
    )
    op.add_column(
        "notification_channels",
        sa.Column("last_error", sa.String(length=MAX_LAST_ERROR), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("notification_channels", "last_error")
    op.drop_column("notification_channels", "last_status")
    op.drop_column("notification_channels", "last_attempt_at")
