"""V2 P5 Option B — charges table + usage_ledger.charge_incoming_sent

Adds the ``charges`` table used by the cloud plugin's cycle-boundary
overage-billing task (V2 P5 Option B — core#249). Idempotency key on
``(subscription_id, period_start, metric)`` is enforced via a unique
index on ``charges.idempotency_key`` so a concurrent task run or retry
hits ``IntegrityError`` rather than double-charging the customer.

Also adds ``usage_ledger.charge_incoming_sent`` — the pre-charge
notification idempotency latch (~24h before cycle close, fires once
per cycle per org).

Revision ID: a7b1c2d3e4f5
Revises: z6a1b2c3d4e5
Create Date: 2026-04-18 13:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a7b1c2d3e4f5"
down_revision: str | None = "z6a1b2c3d4e5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "usage_ledger",
        sa.Column(
            "charge_incoming_sent",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
    )

    op.create_table(
        "charges",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("org_id", sa.BigInteger(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=120), nullable=False),
        sa.Column(
            "subscription_id",
            sa.BigInteger(),
            sa.ForeignKey("subscriptions.id"),
            nullable=False,
        ),
        sa.Column("period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metric", sa.String(length=50), nullable=False),
        sa.Column("overage_quantity", sa.BigInteger(), nullable=False),
        sa.Column("amount_cents", sa.Integer(), nullable=False),
        sa.Column(
            "currency",
            sa.String(length=3),
            nullable=False,
            server_default=sa.text("'USD'"),
        ),
        sa.Column(
            "status",
            sa.String(length=20),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column("paddle_transaction_id", sa.String(length=50), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_error", sa.String(length=500), nullable=True),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("paid_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
    )

    # Hard idempotency barrier — a second insert on the same cycle raises
    # IntegrityError and the task aborts before calling Paddle.
    op.create_index(
        "ix_charges_idempotency_key",
        "charges",
        ["idempotency_key"],
        unique=True,
    )
    op.create_index("ix_charges_subscription_id", "charges", ["subscription_id"])
    op.create_index("ix_charges_org_id", "charges", ["org_id"])

    connection = op.get_bind()
    connection.commit()


def downgrade() -> None:
    op.drop_index("ix_charges_org_id", table_name="charges")
    op.drop_index("ix_charges_subscription_id", table_name="charges")
    op.drop_index("ix_charges_idempotency_key", table_name="charges")
    op.drop_table("charges")
    op.drop_column("usage_ledger", "charge_incoming_sent")
    connection = op.get_bind()
    connection.commit()
