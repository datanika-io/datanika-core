"""add all tables to public schema

Revision ID: a1b2c3d4e5f6
Revises: f4a0a7a98a12
Create Date: 2026-02-15 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a1b2c3d4e5f6"
down_revision: str | None = "f4a0a7a98a12"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # --- New columns on existing public tables ---
    op.add_column("users", sa.Column("oauth_provider", sa.String(50), nullable=True))
    op.add_column("users", sa.Column("oauth_provider_id", sa.String(255), nullable=True))

    # --- Connections ---
    op.create_table(
        "connections",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column(
            "connection_type",
            sa.Enum(
                "postgres",
                "mysql",
                "mssql",
                "sqlite",
                "rest_api",
                "bigquery",
                "snowflake",
                "redshift",
                "s3",
                "csv",
                "json",
                "parquet",
                name="connectiontype",
                native_enum=False,
                length=30,
            ),
            nullable=False,
        ),
        sa.Column(
            "direction",
            sa.Enum(
                "source",
                "destination",
                "both",
                name="connectiondirection",
                native_enum=False,
                length=20,
            ),
            nullable=False,
        ),
        sa.Column("config_encrypted", sa.Text(), nullable=False),
        sa.Column("freshness_config", sa.JSON(), nullable=True),
        sa.Column("org_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
    )
    op.create_index("ix_connections_org_id", "connections", ["org_id"])

    # --- Pipelines ---
    op.create_table(
        "pipelines",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("source_connection_id", sa.BigInteger(), nullable=False),
        sa.Column("destination_connection_id", sa.BigInteger(), nullable=False),
        sa.Column("dlt_config", sa.JSON(), nullable=True),
        sa.Column(
            "status",
            sa.Enum(
                "draft",
                "active",
                "paused",
                "error",
                name="pipelinestatus",
                native_enum=False,
                length=20,
            ),
            nullable=False,
        ),
        sa.Column("org_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
        sa.ForeignKeyConstraint(["source_connection_id"], ["connections.id"]),
        sa.ForeignKeyConstraint(["destination_connection_id"], ["connections.id"]),
    )
    op.create_index("ix_pipelines_org_id", "pipelines", ["org_id"])

    # --- Transformations ---
    op.create_table(
        "transformations",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("sql_body", sa.Text(), nullable=False),
        sa.Column(
            "materialization",
            sa.Enum(
                "view",
                "table",
                "incremental",
                "ephemeral",
                "snapshot",
                name="materialization",
                native_enum=False,
                length=20,
            ),
            nullable=False,
        ),
        sa.Column("schema_name", sa.String(255), nullable=False),
        sa.Column("tests_config", sa.JSON(), nullable=True),
        sa.Column("org_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
    )
    op.create_index("ix_transformations_org_id", "transformations", ["org_id"])

    # --- Dependencies ---
    op.create_table(
        "dependencies",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "upstream_type",
            sa.Enum(
                "pipeline",
                "transformation",
                name="nodetype",
                native_enum=False,
                length=20,
            ),
            nullable=False,
        ),
        sa.Column("upstream_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "downstream_type",
            sa.Enum(
                "pipeline",
                "transformation",
                name="nodetype_1",
                native_enum=False,
                length=20,
            ),
            nullable=False,
        ),
        sa.Column("downstream_id", sa.BigInteger(), nullable=False),
        sa.Column("org_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
    )
    op.create_index("ix_dependencies_org_id", "dependencies", ["org_id"])

    # --- Runs ---
    op.create_table(
        "runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "target_type",
            sa.Enum(
                "pipeline",
                "transformation",
                name="nodetype_run",
                native_enum=False,
                length=20,
            ),
            nullable=False,
        ),
        sa.Column("target_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "pending",
                "running",
                "success",
                "failed",
                "cancelled",
                name="runstatus",
                native_enum=False,
                length=20,
            ),
            nullable=False,
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("logs", sa.Text(), nullable=True),
        sa.Column("rows_loaded", sa.Integer(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("org_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
    )
    op.create_index("ix_runs_org_id", "runs", ["org_id"])

    # --- Schedules ---
    op.create_table(
        "schedules",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column(
            "target_type",
            sa.Enum(
                "pipeline",
                "transformation",
                name="nodetype_sched",
                native_enum=False,
                length=20,
            ),
            nullable=False,
        ),
        sa.Column("target_id", sa.BigInteger(), nullable=False),
        sa.Column("cron_expression", sa.String(100), nullable=False),
        sa.Column("timezone", sa.String(50), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("org_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
    )
    op.create_index("ix_schedules_org_id", "schedules", ["org_id"])

    # --- API Keys ---
    op.create_table(
        "api_keys",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("key_hash", sa.String(255), nullable=False),
        sa.Column("scopes", sa.JSON(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("org_id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
    )
    op.create_index("ix_api_keys_org_id", "api_keys", ["org_id"])

    # --- Audit Logs ---
    op.create_table(
        "audit_logs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "action",
            sa.Enum(
                "create",
                "update",
                "delete",
                "login",
                "logout",
                "run",
                name="auditaction",
                native_enum=False,
                length=20,
            ),
            nullable=False,
        ),
        sa.Column("resource_type", sa.String(100), nullable=False),
        sa.Column("resource_id", sa.BigInteger(), nullable=True),
        sa.Column("old_values", sa.JSON(), nullable=True),
        sa.Column("new_values", sa.JSON(), nullable=True),
        sa.Column("ip_address", sa.String(45), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("org_id", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.ForeignKeyConstraint(["org_id"], ["organizations.id"]),
    )
    op.create_index("ix_audit_logs_org_id", "audit_logs", ["org_id"])


def downgrade() -> None:
    op.drop_table("audit_logs")
    op.drop_table("api_keys")
    op.drop_table("schedules")
    op.drop_table("runs")
    op.drop_table("dependencies")
    op.drop_table("transformations")
    op.drop_table("pipelines")
    op.drop_table("connections")
    op.drop_column("users", "oauth_provider_id")
    op.drop_column("users", "oauth_provider")
