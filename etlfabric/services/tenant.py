from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from etlfabric.migrations.helpers import is_tenant_table
from etlfabric.models.base import Base


class TenantService:
    def config_schema_name(self, org_id: int) -> str:
        """Generate the config schema name for a tenant."""
        return f"tenant_{org_id}"

    async def provision_tenant(self, session: AsyncSession, org_id: int) -> str:
        """Create the config schema and tenant tables for a new tenant."""
        schema = self.config_schema_name(org_id)
        await session.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

        # Create tenant-scoped tables in the new schema
        conn = await session.connection()
        await conn.get_raw_connection()
        await conn.run_sync(lambda sync_conn: self._create_tenant_tables(sync_conn, schema))

        await session.commit()
        return schema

    @staticmethod
    def _create_tenant_tables(sync_conn, schema: str) -> None:
        """Create tenant-scoped tables in the given schema using metadata."""
        tenant_tables = [
            table for table in Base.metadata.sorted_tables if is_tenant_table(table.name)
        ]
        Base.metadata.create_all(sync_conn.engine, tables=tenant_tables, checkfirst=True)

    async def drop_tenant(self, session: AsyncSession, org_id: int) -> None:
        """Drop the config schema for a tenant. Use with extreme caution."""
        schema = self.config_schema_name(org_id)
        await session.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await session.commit()
