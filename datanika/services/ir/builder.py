"""IR builder — dispatches by connection kind to build an IR document.

Per SPEC_ELT_IR_ARCHITECTURE.md §5.2:
- sql_table: introspects live, constructs columns from information schema
- sql_database: same, but for all tables (or filtered subset)
- saas_rest / file: deferred to P4 (raises NotImplementedError)
"""

from __future__ import annotations

import logging

from datanika.services.ir import IR_VERSION
from datanika.services.ir.introspect import introspect_columns
from datanika.services.ir.schema import IR, IRIncremental, IRSource, IRTarget

logger = logging.getLogger(__name__)

# Connection types that support SQL introspection
SQL_TYPES = frozenset(
    {
        "postgres",
        "mysql",
        "mssql",
        "sqlite",
        "snowflake",
        "bigquery",
        "clickhouse",
        "duckdb",
        "databricks",
        "synapse",
        "redshift",
    }
)


class IRBuildError(ValueError):
    """Raised when build_ir cannot construct an IR for the given source."""


def build_ir(
    source_type: str,
    source_config: dict,
    destination_connection_id: int,
    destination_raw_schema: str = "raw",
    table: str | None = None,
    schema: str | None = None,
    incremental_config: dict | None = None,
    primary_key_columns: list[str] | None = None,
) -> IR:
    """Build an IR document for a source → destination mapping.

    For SQL sources, introspects the source table to discover columns.
    For non-SQL sources (SaaS, files), deferred to P4.
    """
    if source_type not in SQL_TYPES:
        raise IRBuildError(
            f"IR builder for source type {source_type!r} lands in V2 P4. "
            f"Supported SQL types: {sorted(SQL_TYPES)}"
        )

    if not table:
        raise IRBuildError(
            "build_ir requires a table name for sql_table mode. "
            "sql_database multi-table IR lands in a follow-up."
        )

    # Introspect columns from the live source
    columns = introspect_columns(
        config=source_config,
        connection_type=source_type,
        table=table,
        schema=schema,
    )

    if not columns:
        raise IRBuildError(f"No columns found for {source_type}://{schema or ''}.{table}")

    # Derive primary key — use provided or fall back to first column
    pk = primary_key_columns or [columns[0].target_name]

    # Build incremental if configured
    incremental = None
    if incremental_config:
        incremental = IRIncremental(
            mode=incremental_config.get("mode", "append"),
            cursor=incremental_config["cursor_path"],
        )

    ir = IR(
        ir_version=IR_VERSION,
        source=IRSource(
            kind="sql_table",
            connection_id=0,  # filled by caller with the actual connection_id
            schema=schema,
            table=table,
        ),
        columns=columns,
        primary_key=pk,
        target=IRTarget(
            connection_id=destination_connection_id,
            raw_schema=destination_raw_schema,
            table=table,
        ),
        incremental=incremental,
    )

    logger.info(
        "Built IR v%d for %s.%s: %d columns, pk=%s",
        IR_VERSION,
        schema or "(default)",
        table,
        len(columns),
        pk,
    )

    return ir
