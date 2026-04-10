"""Rename LemonSqueezy columns to Paddle

Revision ID: i8e5f6g7h9b0
Revises: h7d4e5f6g8a9
Create Date: 2026-03-17 19:23:44.709276
"""

from collections.abc import Sequence

from alembic import op
from sqlalchemy import inspect

revision: str = "i8e5f6g7h9b0"
down_revision: str | None = "h7d4e5f6g8a9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _table_exists(table_name: str) -> bool:
    conn = op.get_bind()
    insp = inspect(conn)
    return table_name in insp.get_table_names()


def _column_exists(table_name: str, column_name: str) -> bool:
    conn = op.get_bind()
    insp = inspect(conn)
    columns = [c["name"] for c in insp.get_columns(table_name)]
    return column_name in columns


def upgrade() -> None:
    # Cloud tables may not exist on core-only installs — skip safely
    if _table_exists("plans") and _column_exists("plans", "ls_variant_id"):
        op.alter_column("plans", "ls_variant_id", new_column_name="paddle_price_id")
        op.alter_column("plans", "ls_product_id", new_column_name="paddle_product_id")

    if _table_exists("subscriptions") and _column_exists("subscriptions", "ls_customer_id"):
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

    if _table_exists("usage_ledger") and _column_exists("usage_ledger", "reported_to_ls"):
        op.alter_column("usage_ledger", "reported_to_ls", new_column_name="reported_to_paddle")


def downgrade() -> None:
    if _table_exists("plans") and _column_exists("plans", "paddle_price_id"):
        op.alter_column("plans", "paddle_price_id", new_column_name="ls_variant_id")
        op.alter_column("plans", "paddle_product_id", new_column_name="ls_product_id")

    if _table_exists("subscriptions") and _column_exists("subscriptions", "paddle_customer_id"):
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

    if _table_exists("usage_ledger") and _column_exists("usage_ledger", "reported_to_paddle"):
        op.alter_column("usage_ledger", "reported_to_paddle", new_column_name="reported_to_ls")
