"""Multi-tenant migration helpers.

Classifies tables as public (shared) or tenant-scoped to support
two-phase Alembic migrations: public schema first, then each tenant schema.
"""

from sqlalchemy import text
from sqlalchemy.engine import Connection

PUBLIC_TABLES: set[str] = {
    "organizations",
    "users",
    "memberships",
    "connections",
    "pipelines",
    "transformations",
    "dependencies",
    "runs",
    "schedules",
    "api_keys",
    "audit_logs",
}

# Tables managed by Alembic but not belonging to either category
_INTERNAL_TABLES: set[str] = {"alembic_version"}


def is_public_table(table_name: str) -> bool:
    return table_name in PUBLIC_TABLES


def is_tenant_table(table_name: str) -> bool:
    return table_name not in PUBLIC_TABLES and table_name not in _INTERNAL_TABLES


def get_tenant_schemas(conn: Connection) -> list[str]:
    """Return all tenant schema names (``tenant_*``) from the database."""
    result = conn.execute(
        text(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name LIKE 'tenant_%' ORDER BY schema_name"
        )
    )
    return [row[0] for row in result]
