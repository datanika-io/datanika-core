"""Rename LemonSqueezy columns to Paddle

Revision ID: i8e5f6g7h9b0
Revises: h7d4e5f6g8a9
Create Date: 2026-03-17 19:23:44.709276
"""

from typing import Sequence, Union

from alembic import op

revision: str = "i8e5f6g7h9b0"
down_revision: Union[str, None] = "h7d4e5f6g8a9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column("plans", "ls_variant_id", new_column_name="paddle_price_id")
    op.alter_column("plans", "ls_product_id", new_column_name="paddle_product_id")

    op.alter_column("subscriptions", "ls_customer_id", new_column_name="paddle_customer_id")
    op.alter_column(
        "subscriptions", "ls_subscription_id", new_column_name="paddle_subscription_id"
    )
    op.alter_column(
        "subscriptions",
        "ls_subscription_item_id",
        new_column_name="paddle_subscription_item_id",
    )
    op.alter_column("subscriptions", "ls_usage_item_id", new_column_name="paddle_usage_item_id")

    op.alter_column("usage_ledger", "reported_to_ls", new_column_name="reported_to_paddle")


def downgrade() -> None:
    op.alter_column("plans", "paddle_price_id", new_column_name="ls_variant_id")
    op.alter_column("plans", "paddle_product_id", new_column_name="ls_product_id")

    op.alter_column("subscriptions", "paddle_customer_id", new_column_name="ls_customer_id")
    op.alter_column(
        "subscriptions", "paddle_subscription_id", new_column_name="ls_subscription_id"
    )
    op.alter_column(
        "subscriptions",
        "paddle_subscription_item_id",
        new_column_name="ls_subscription_item_id",
    )
    op.alter_column("subscriptions", "paddle_usage_item_id", new_column_name="ls_usage_item_id")

    op.alter_column("usage_ledger", "reported_to_paddle", new_column_name="reported_to_ls")
