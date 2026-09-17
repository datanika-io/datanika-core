"""Catalog service — introspect tables, manage catalog entries."""

import logging
from collections import defaultdict
from datetime import UTC, datetime

from dlt.destinations.impl.clickhouse.configuration import ClickHouseClientConfiguration
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.orm import Session

from datanika.models.catalog_entry import CatalogEntry, CatalogEntryType
from datanika.models.dependency import NodeType

logger = logging.getLogger(__name__)

#: Portable column metadata. Every destination we support implements
#: `information_schema.columns`; not every SQLAlchemy dialect implements
#: `get_columns` against it (core#494).
_INFORMATION_SCHEMA_COLUMNS = text(
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_schema = :schema AND table_name = :table "
    "ORDER BY ordinal_position"
)


#: dlt's ClickHouse destination has no schema level (core#1397). A dataset is a table-name PREFIX
#: inside the connection's database: ``<dataset>___<table>``, next to ``<dataset>____dlt_loads``
#: and ``<dataset>___dlt_sentinel_table``. Read from dlt's own configuration, which Datanika does
#: not override, so a changed default in dlt moves this with it.
CLICKHOUSE_DATASET_TABLE_SEPARATOR: str = ClickHouseClientConfiguration.dataset_table_separator
CLICKHOUSE_DATASET_SENTINEL_TABLE: str = ClickHouseClientConfiguration.dataset_sentinel_table_name

#: ClickHouse's own catalogue. Queried directly, because clickhouse-connect 0.11's SQLAlchemy
#: dialect lists tables by passing a plain string to ``connection.execute``, which SQLAlchemy 2
#: refuses before anything reaches the server (``ObjectNotExecutableError``, for every schema).
_CLICKHOUSE_TABLES = text(
    "SELECT database, name FROM system.tables WHERE database = :database ORDER BY name"
)
_CLICKHOUSE_TABLES_EVERYWHERE = text(
    "SELECT database, name FROM system.tables "
    "WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') "
    "ORDER BY database, name"
)
_CLICKHOUSE_COLUMNS = text(
    "SELECT name, type FROM system.columns WHERE database = :database AND table = :table "
    "ORDER BY position"
)


def clickhouse_table_names(sa_url: str, database: str | None) -> list[tuple[str, str]]:
    """``(database, table)`` for every table in ``database``, or in every user database."""
    engine = create_engine(sa_url)
    try:
        with engine.connect() as conn:
            if database:
                rows = conn.execute(_CLICKHOUSE_TABLES, {"database": database}).fetchall()
            else:
                rows = conn.execute(_CLICKHOUSE_TABLES_EVERYWHERE).fetchall()
        return [(str(db), str(name)) for db, name in rows]
    finally:
        engine.dispose()


def dbt_sources_for_entries(entries) -> list[dict]:
    """The dbt source definitions for a connection's catalogued upload tables.

    One source per dataset. Its ``schema`` is where the tables **are**, read from the entries:
    the dataset itself on every destination that has a schema level, and the connection's
    database on ClickHouse, whose entries carry the prefixed table name (core#1397). So
    ``source('<dataset>', '<dataset>___<table>')`` resolves to ``<database>.<dataset>___<table>``,
    which is the table the load wrote, and the name the catalogue and the editor's autocomplete
    already show.
    """
    by_dataset: dict[str, list] = defaultdict(list)
    for entry in entries:
        by_dataset[entry.dataset_name].append(entry)
    return [
        {
            "name": dataset,
            "schema": group[0].schema_name or dataset,
            "description": f"Data loaded by upload into {dataset}",
            "tables": [{"name": e.table_name, "columns": e.columns or []} for e in group],
        }
        for dataset, group in sorted(by_dataset.items())
    ]


class CatalogService:
    @staticmethod
    def introspect_clickhouse_dataset(sa_url: str, database: str, dataset_name: str) -> list[dict]:
        """The tables one dlt dataset loaded into a ClickHouse database (core#1397).

        Returns the same shape as :meth:`introspect_tables`, with ``table_name`` the table's real
        name in ClickHouse, prefix included. dlt's bookkeeping tables share the prefix rather
        than starting with ``_dlt_``, so they are dropped here by what follows the prefix.
        """
        prefix = f"{dataset_name}{CLICKHOUSE_DATASET_TABLE_SEPARATOR}"
        results = []
        engine = create_engine(sa_url)
        try:
            with engine.connect() as conn:
                rows = conn.execute(_CLICKHOUSE_TABLES, {"database": database}).fetchall()
                for name in (str(r[1]) for r in rows):
                    if not name.startswith(prefix):
                        continue
                    remainder = name[len(prefix) :]
                    if (
                        remainder.startswith("_dlt_")
                        or remainder == CLICKHOUSE_DATASET_SENTINEL_TABLE
                    ):
                        continue
                    columns = conn.execute(
                        _CLICKHOUSE_COLUMNS, {"database": database, "table": name}
                    ).fetchall()
                    results.append(
                        {
                            "table_name": name,
                            "columns": [{"name": str(c), "data_type": str(t)} for c, t in columns],
                        }
                    )
            return results
        finally:
            engine.dispose()

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
                columns = CatalogService._columns_for(engine, insp, tbl, schema_name)
                if columns is None:
                    continue
                results.append({"table_name": tbl, "columns": columns})
            return results
        finally:
            engine.dispose()

    @staticmethod
    def _columns_for(engine, insp, table: str, schema: str) -> list[dict] | None:
        """Column metadata for one table, with a portable fallback.

        `get_columns` used to be wrapped in a bare `except: continue`, so a
        dialect that could not answer produced an **empty catalog** rather than
        an error — and the user was told to verify their first run by browsing
        Catalog. That is exactly what happened on DuckDB (core#494):
        `duckdb_engine` derives from the PostgreSQL dialect and its
        `get_columns` queries `pg_collation`, which DuckDB does not provide::

            ProgrammingError: Catalog Error: Table with name pg_collation does not exist!

        Installing the dialect alone did **not** fix it — that only got as far
        as `get_table_names`. So a failure here now falls back to
        `information_schema`, which every destination we support implements,
        and anything still unreadable is logged instead of vanishing.
        """
        try:
            cols = insp.get_columns(table, schema=schema)
            return [{"name": c["name"], "data_type": str(c["type"])} for c in cols]
        except Exception as exc:
            logger.warning(
                "get_columns failed for %s.%s (%s) — falling back to information_schema",
                schema,
                table,
                exc.__class__.__name__,
            )

        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    _INFORMATION_SCHEMA_COLUMNS, {"schema": schema, "table": table}
                ).fetchall()
        except Exception:
            logger.exception("information_schema fallback failed for %s.%s", schema, table)
            return None

        if not rows:
            logger.warning("No column metadata found for %s.%s — skipping", schema, table)
            return None

        return [{"name": name, "data_type": str(data_type)} for name, data_type in rows]

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
