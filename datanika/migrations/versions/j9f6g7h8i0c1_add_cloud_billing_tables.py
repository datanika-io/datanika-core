"""Add cloud billing tables (plans, subscriptions, usage_ledger)

Revision ID: j9f6g7h8i0c1
Revises: i8e5f6g7h9b0
Create Date: 2026-03-21 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy import inspect

revision: str = "j9f6g7h8i0c1"
down_revision: str | None = "i8e5f6g7h9b0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _table_exists(table_name: str) -> bool:
    conn = op.get_bind()
    insp = inspect(conn)
    return table_name in insp.get_table_names()


def upgrade() -> None:
    if not _table_exists("plans"):
        op.create_table(
            "plans",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("name", sa.String(100), nullable=False),
            sa.Column("slug", sa.String(50), nullable=False),
            sa.Column("paddle_price_id", sa.String(50), nullable=False),
            sa.Column("paddle_product_id", sa.String(50), nullable=False),
            sa.Column("price_cents", sa.Integer(), nullable=False),
            sa.Column("interval", sa.String(50), nullable=False),
            sa.Column("seats_included", sa.Integer(), nullable=False, server_default="2"),
            sa.Column(
                "extra_seat_price_cents", sa.Integer(), nullable=False, server_default="1200"
            ),
            sa.Column("runs_included", sa.Integer(), nullable=False, server_default="500"),
            sa.Column("overage_run_price_cents", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("max_connections", sa.Integer(), nullable=False, server_default="5"),
            sa.Column("max_schedules", sa.Integer(), nullable=False, server_default="10"),
            sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
            sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
            sa.Column("deleted_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("slug"),
            sa.UniqueConstraint("paddle_price_id"),
        )

    if not _table_exists("subscriptions"):
        op.create_table(
            "subscriptions",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("org_id", sa.BigInteger(), sa.ForeignKey("organizations.id"), nullable=False),
            sa.Column("plan_id", sa.BigInteger(), sa.ForeignKey("plans.id"), nullable=False),
            sa.Column("paddle_customer_id", sa.String(50), nullable=False),
            sa.Column("paddle_subscription_id", sa.String(50), nullable=False),
            sa.Column("paddle_subscription_item_id", sa.String(50), nullable=True),
            sa.Column("paddle_usage_item_id", sa.String(50), nullable=True),
            sa.Column("status", sa.String(50), nullable=False, server_default="active"),
            sa.Column("seat_count", sa.Integer(), nullable=False, server_default="1"),
            sa.Column("trial_ends_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("renews_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("ends_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("card_brand", sa.String(20), nullable=True),
            sa.Column("card_last_four", sa.String(4), nullable=True),
            sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
            sa.Column("deleted_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
            sa.UniqueConstraint("paddle_subscription_id"),
        )
        op.create_index("ix_subscriptions_org_id", "subscriptions", ["org_id"])
        op.create_index("ix_subscriptions_plan_id", "subscriptions", ["plan_id"])

    if not _table_exists("usage_ledger"):
        op.create_table(
            "usage_ledger",
            sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
            sa.Column("org_id", sa.BigInteger(), sa.ForeignKey("organizations.id"), nullable=False),
            sa.Column("period_start", sa.DateTime(timezone=True), nullable=False),
            sa.Column("period_end", sa.DateTime(timezone=True), nullable=False),
            sa.Column("metric", sa.String(50), nullable=False),
            sa.Column("quantity", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("reported_to_paddle", sa.Boolean(), nullable=False, server_default="false"),
            sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=True),
            sa.Column("deleted_at", sa.DateTime(), nullable=True),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index("ix_usage_ledger_org_id", "usage_ledger", ["org_id"])


def downgrade() -> None:
    op.drop_table("usage_ledger")
    op.drop_table("subscriptions")
    op.drop_table("plans")
