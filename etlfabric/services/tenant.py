from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


class TenantService:
    def config_schema_name(self, org_id: int) -> str:
        """Generate the config schema name for a tenant."""
        return f"tenant_{org_id}"

    async def provision_tenant(self, session: AsyncSession, org_id: int) -> str:
        """Create the config schema for a new tenant. Returns the schema name."""
        schema = self.config_schema_name(org_id)
        await session.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        await session.commit()
        return schema

    async def drop_tenant(self, session: AsyncSession, org_id: int) -> None:
        """Drop the config schema for a tenant. Use with extreme caution."""
        schema = self.config_schema_name(org_id)
        await session.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await session.commit()
