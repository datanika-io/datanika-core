import uuid

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


class TenantService:
    def _schema_names(self, org_id: uuid.UUID) -> dict[str, str]:
        """Generate deterministic schema names for a tenant.
        Uses first 12 hex chars of org UUID for readability."""
        short_id = org_id.hex[:12]
        return {
            "config": f"tenant_{short_id}",
            "raw": f"tenant_{short_id}_raw",
            "staging": f"tenant_{short_id}_staging",
            "marts": f"tenant_{short_id}_marts",
        }

    async def provision_tenant(self, session: AsyncSession, org_id: uuid.UUID) -> dict[str, str]:
        """Create all schemas for a new tenant. Returns the schema name mapping."""
        schemas = self._schema_names(org_id)
        for schema_name in schemas.values():
            await session.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
        await session.commit()
        return schemas

    async def drop_tenant(self, session: AsyncSession, org_id: uuid.UUID) -> None:
        """Drop all schemas for a tenant. Use with extreme caution."""
        schemas = self._schema_names(org_id)
        for schema_name in schemas.values():
            await session.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))
        await session.commit()
