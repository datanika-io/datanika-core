"""Add Free plan and fix Enterprise overage price to 1 cent

Revision ID: q6m3n4o5p7j8
Revises: p5l2m3n4o6i7
Create Date: 2026-03-24 21:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "q6m3n4o5p7j8"
down_revision: Union[str, None] = "p5l2m3n4o6i7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        INSERT INTO plans (
            name, slug, paddle_price_id, paddle_product_id, price_cents,
            interval, seats_included, extra_seat_price_cents, runs_included,
            overage_run_price_cents, max_connections, max_schedules,
            hard_cap_runs, sso_enabled, is_active
        ) VALUES (
            'Free', 'free', 'free', 'free', 0,
            'monthly', 1, 0, 500,
            0, 5, 2,
            true, false, true
        ) ON CONFLICT (slug) DO NOTHING
        """
    )
    op.execute("UPDATE plans SET overage_run_price_cents = 1 WHERE slug = 'enterprise-monthly'")


def downgrade() -> None:
    op.execute("DELETE FROM plans WHERE slug = 'free'")
    op.execute("UPDATE plans SET overage_run_price_cents = 0 WHERE slug = 'enterprise-monthly'")
