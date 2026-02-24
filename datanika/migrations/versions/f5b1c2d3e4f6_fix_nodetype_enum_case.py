"""fix nodetype enum values to uppercase names

The e1f2a3b4c5d6 migration used lowercase enum *values* in UPDATE
statements, but SQLAlchemy Enum(StrEnum, native_enum=False) stores
member *names* (uppercase).  This migration fixes all affected columns.

Revision ID: f5b1c2d3e4f6
Revises: e1f2a3b4c5d6
Create Date: 2026-02-19 15:00:00.000000
"""

from collections.abc import Sequence

from alembic import op

revision: str = "f5b1c2d3e4f6"
down_revision: str | None = "e1f2a3b4c5d6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # catalog_entries: lowercase 'upload' → uppercase 'UPLOAD'
    op.execute("UPDATE catalog_entries SET origin_type = 'UPLOAD' WHERE origin_type = 'upload'")
    op.execute(
        "UPDATE catalog_entries SET origin_type = 'TRANSFORMATION'"
        " WHERE origin_type = 'transformation'"
    )
    op.execute("UPDATE catalog_entries SET origin_type = 'PIPELINE' WHERE origin_type = 'pipeline'")

    # schedules/runs: all pre-existing 'PIPELINE' entries are actually uploads
    # (the new dbt pipelines table was just created, so no new PIPELINE rows exist yet)
    op.execute("UPDATE schedules SET target_type = 'UPLOAD' WHERE target_type = 'PIPELINE'")
    op.execute("UPDATE runs SET target_type = 'UPLOAD' WHERE target_type = 'PIPELINE'")

    # dependencies: fix any lowercase values (may not exist yet)
    for col in ("upstream_type", "downstream_type"):
        op.execute(f"UPDATE dependencies SET {col} = 'UPLOAD' WHERE {col} = 'upload'")
        op.execute(
            f"UPDATE dependencies SET {col} = 'TRANSFORMATION' WHERE {col} = 'transformation'"
        )
        op.execute(f"UPDATE dependencies SET {col} = 'PIPELINE' WHERE {col} = 'pipeline'")


def downgrade() -> None:
    # Revert to lowercase values
    op.execute("UPDATE catalog_entries SET origin_type = 'upload' WHERE origin_type = 'UPLOAD'")
    op.execute("UPDATE schedules SET target_type = 'PIPELINE' WHERE target_type = 'UPLOAD'")
    op.execute("UPDATE runs SET target_type = 'PIPELINE' WHERE target_type = 'UPLOAD'")
