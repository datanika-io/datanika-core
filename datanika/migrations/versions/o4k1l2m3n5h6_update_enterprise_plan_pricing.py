"""Update enterprise plan pricing

Revision ID: o4k1l2m3n5h6
Revises: n3j0k1l2m4g5
Create Date: 2026-03-24 18:00:00.000000
"""

from typing import Sequence, Union

from alembic import op

revision: str = "o4k1l2m3n5h6"
down_revision: Union[str, None] = "n3j0k1l2m4g5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE plans
        SET price_cents = 39900,
            seats_included = 10,
            extra_seat_price_cents = 2500,
            max_connections = 50,
            runs_included = 50000,
            hard_cap_runs = false
        WHERE slug = 'enterprise'
        """
    )


def downgrade() -> None:
    op.execute(
        """
        UPDATE plans
        SET price_cents = 29900,
            seats_included = 999999,
            extra_seat_price_cents = 1200,
            max_connections = 999999,
            runs_included = 999999,
            hard_cap_runs = false
        WHERE slug = 'enterprise'
        """
    )
