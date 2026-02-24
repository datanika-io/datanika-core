"""Catalog service — introspect tables, manage catalog entries."""

from datetime import UTC, datetime

from sqlalchemy import create_engine, inspect, select
from sqlalchemy.orm import Session

from datanika.models.catalog_entry import CatalogEntry, CatalogEntryType
from datanika.models.dependency import NodeType


class CatalogService:
    @staticmethod
    def introspect_tables(
        sa_url: str,
        schema_name: str,
        table_names: list[str] | None = None,
    ) -> list[dict]:
        """Query destination DB for table metadata.

        Returns [{"table_name": str, "columns": [{"name": str, "data_type": str}]}].
        Filters out ``_dlt_*`` system tables.
        """
        engine = create_engine(sa_url)
        try:
            insp = inspect(engine)
            if table_names is None:
                table_names = insp.get_table_names(schema=schema_name)

            results = []
            for tbl in table_names:
                if tbl.startswith("_dlt_"):
                    continue
                try:
                    cols = insp.get_columns(tbl, schema=schema_name)
                except Exception:
                    continue
                results.append(
                    {
                        "table_name": tbl,
                        "columns": [{"name": c["name"], "data_type": str(c["type"])} for c in cols],
                    }
                )
            return results
        finally:
            engine.dispose()

    @staticmethod
    def upsert_entry(
        session: Session,
        org_id: int,
        entry_type: CatalogEntryType,
        origin_type: NodeType,
        origin_id: int,
        table_name: str,
        schema_name: str,
        dataset_name: str,
        columns: list | None = None,
        connection_id: int | None = None,
        description: str | None = None,
        dbt_config: dict | None = None,
    ) -> CatalogEntry:
        """Create or update a catalog entry.

        Matched on org_id + table_name + schema_name + dataset_name.
        """
        stmt = select(CatalogEntry).where(
            CatalogEntry.org_id == org_id,
            CatalogEntry.table_name == table_name,
            CatalogEntry.schema_name == schema_name,
            CatalogEntry.dataset_name == dataset_name,
            CatalogEntry.deleted_at.is_(None),
        )
        entry = session.execute(stmt).scalar_one_or_none()

        if entry is None:
            entry = CatalogEntry(
                org_id=org_id,
                entry_type=entry_type,
                origin_type=origin_type,
                origin_id=origin_id,
                table_name=table_name,
                schema_name=schema_name,
                dataset_name=dataset_name,
                columns=columns or [],
                connection_id=connection_id,
                description=description,
                dbt_config=dbt_config or {},
            )
            session.add(entry)
        else:
            entry.entry_type = entry_type
            entry.origin_type = origin_type
            entry.origin_id = origin_id
            entry.columns = columns if columns is not None else entry.columns
            if connection_id is not None:
                entry.connection_id = connection_id
            if description is not None:
                entry.description = description
            if dbt_config is not None:
                entry.dbt_config = dbt_config

        session.flush()
        return entry

    @staticmethod
    def get_entry(session: Session, org_id: int, entry_id: int) -> CatalogEntry | None:
        stmt = select(CatalogEntry).where(
            CatalogEntry.id == entry_id,
            CatalogEntry.org_id == org_id,
            CatalogEntry.deleted_at.is_(None),
        )
        return session.execute(stmt).scalar_one_or_none()

    @staticmethod
    def list_entries(
        session: Session,
        org_id: int,
        entry_type: CatalogEntryType | None = None,
    ) -> list[CatalogEntry]:
        stmt = select(CatalogEntry).where(
            CatalogEntry.org_id == org_id,
            CatalogEntry.deleted_at.is_(None),
        )
        if entry_type is not None:
            stmt = stmt.where(CatalogEntry.entry_type == entry_type)
        stmt = stmt.order_by(CatalogEntry.created_at.desc())
        return list(session.execute(stmt).scalars().all())

    @staticmethod
    def update_entry(
        session: Session,
        org_id: int,
        entry_id: int,
        **kwargs,
    ) -> CatalogEntry | None:
        entry = CatalogService.get_entry(session, org_id, entry_id)
        if entry is None:
            return None

        if "description" in kwargs:
            entry.description = kwargs["description"]
        if "columns" in kwargs:
            entry.columns = kwargs["columns"]
        if "dbt_config" in kwargs:
            entry.dbt_config = kwargs["dbt_config"]

        session.flush()
        return entry

    @staticmethod
    def delete_entry(session: Session, org_id: int, entry_id: int) -> bool:
        entry = CatalogService.get_entry(session, org_id, entry_id)
        if entry is None:
            return False
        entry.deleted_at = datetime.now(UTC)
        session.flush()
        return True

    @staticmethod
    def get_entries_by_connection(
        session: Session,
        org_id: int,
        connection_id: int,
    ) -> list[CatalogEntry]:
        """Return SOURCE_TABLE entries for a given connection."""
        stmt = (
            select(CatalogEntry)
            .where(
                CatalogEntry.org_id == org_id,
                CatalogEntry.connection_id == connection_id,
                CatalogEntry.entry_type == CatalogEntryType.SOURCE_TABLE,
                CatalogEntry.deleted_at.is_(None),
            )
            .order_by(CatalogEntry.table_name)
        )
        return list(session.execute(stmt).scalars().all())
