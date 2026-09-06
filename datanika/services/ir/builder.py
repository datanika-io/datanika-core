"""IR builder — dispatches by connection kind to build an IR document.

Per SPEC_ELT_IR_ARCHITECTURE.md §5.2:
- sql_table / sql_database: introspects live via information schema
- saas_rest (P4): derived from dlt source's declared schema
- file (csv/json/parquet/s3): deferred to a future phase
"""

from __future__ import annotations

import logging

from datanika.errors import UserFacingError
from datanika.services.ir import IR_VERSION
from datanika.services.ir.introspect import introspect_columns
from datanika.services.ir.schema import IR, IRColumn, IRIncremental, IRSource, IRTarget

logger = logging.getLogger(__name__)

# Connection types that support SQL introspection (P3)
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
        "oracle",
    }
)

# SaaS connection types that use dlt verified sources (P4)
SAAS_TYPES = frozenset(
    {
        "stripe",
        "github",
        "hubspot",
        "salesforce",
        "shopify",
        "jira",
        "slack",
        "google_analytics",
        "google_ads",
        "facebook_ads",
        "zendesk",
        "airtable",
        "notion",
        "mongodb",
        "kafka",
        "rest_api",
        "google_sheets",
        "pipedrive",
        "freshdesk",
        "asana",
    }
)

# File types — not yet supported
FILE_TYPES = frozenset({"csv", "json", "parquet", "s3"})


class IRBuildError(UserFacingError):
    """Raised when build_ir cannot construct an IR for the given source."""


def _build_dlt_source(source_type: str, source_config: dict, dlt_config: dict | None = None):
    """Instantiate a dlt source for schema discovery (no data extraction)."""
    from datanika.services.dlt_runner import DltRunnerService

    runner = DltRunnerService()
    return runner.build_source(
        source_type,
        source_config,
        dlt_config or {},
        batch_size=1,  # minimal — we only need schema, not data
    )


def _build_ir_saas(
    source_type: str,
    source_config: dict,
    destination_connection_id: int,
    destination_raw_schema: str,
    table: str,
    dlt_config: dict | None = None,
    incremental_config: dict | None = None,
    primary_key_columns: list[str] | None = None,
) -> IR:
    """Build IR for a SaaS source by extracting schema from dlt resource hints.

    Per spec §5.2: "For saas_rest: derived from the connector's declared
    schema (dlt's @dlt.source already exports this)."
    """
    source = _build_dlt_source(source_type, source_config, dlt_config)

    # Find the requested resource (table name = resource name in dlt)
    if table not in source.resources:
        available = sorted(source.resources.keys())
        raise IRBuildError(
            f"Resource {table!r} not found in {source_type} source. Available: {available}"
        )

    resource = source.resources[table]
    table_schema = resource.compute_table_schema()
    raw_columns = table_schema.get("columns", {})

    if not raw_columns:
        raise IRBuildError(
            f"No columns discovered for {source_type}::{table}. "
            f"The dlt source may not declare schema hints for this resource."
        )

    # Convert dlt column hints to IR columns
    columns = [
        IRColumn(
            source_ref=col_name,
            target_name=col_name,
            type=col_meta.get("data_type", "text"),
            nullable=col_meta.get("nullable", True),
        )
        for col_name, col_meta in raw_columns.items()
        if not col_name.startswith("_dlt_")  # skip dlt internal columns
    ]

    if not columns:
        raise IRBuildError(
            f"No user columns found for {source_type}::{table} (all were _dlt_ internal)"
        )

    pk = primary_key_columns or [columns[0].target_name]

    incremental = None
    if incremental_config:
        incremental = IRIncremental(
            mode=incremental_config.get("mode", "append"),
            cursor=incremental_config["cursor_path"],
        )

    ir = IR(
        ir_version=IR_VERSION,
        source=IRSource(
            kind="saas_rest",
            connection_id=0,  # filled by caller
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
        "Built IR v%d for SaaS %s::%s: %d columns, pk=%s",
        IR_VERSION,
        source_type,
        table,
        len(columns),
        pk,
    )

    return ir


def _build_ir_openapi(
    source_config: dict,
    destination_connection_id: int,
    destination_raw_schema: str,
    table: str,
    incremental_config: dict | None = None,
    primary_key_columns: list[str] | None = None,
) -> IR:
    """Build IR for an ``openapi`` connection from its stored spec-derived catalog.

    No live call — columns come from the response schema captured at parse time
    (``openapi_import``). Falls back to the live dlt-hints path only if the
    selected resource has no declared columns.
    """
    catalog = source_config.get("resources") or []
    resource = next((r for r in catalog if r.get("name") == table), None)
    if resource is None:
        available = sorted(r.get("name", "") for r in catalog)
        raise IRBuildError(
            f"Resource {table!r} not found in openapi source. Available: {available}"
        )

    raw_columns = resource.get("columns") or []
    if not raw_columns:
        return _build_ir_saas(
            source_type="openapi",
            source_config=source_config,
            destination_connection_id=destination_connection_id,
            destination_raw_schema=destination_raw_schema,
            table=table,
            incremental_config=incremental_config,
            primary_key_columns=primary_key_columns,
        )

    columns = [
        IRColumn(
            source_ref=c["name"],
            target_name=c["name"],
            type=c.get("type", "text"),
            nullable=c.get("nullable", True),
        )
        for c in raw_columns
    ]
    pk = primary_key_columns or (
        [resource["primary_key"]] if resource.get("primary_key") else [columns[0].target_name]
    )

    incremental = None
    if incremental_config:
        incremental = IRIncremental(
            mode=incremental_config.get("mode", "append"),
            cursor=incremental_config["cursor_path"],
        )

    return IR(
        ir_version=IR_VERSION,
        source=IRSource(kind="saas_rest", connection_id=0, table=table),
        columns=columns,
        primary_key=pk,
        target=IRTarget(
            connection_id=destination_connection_id,
            raw_schema=destination_raw_schema,
            table=table,
        ),
        incremental=incremental,
    )


def build_ir(
    source_type: str,
    source_config: dict,
    destination_connection_id: int,
    destination_raw_schema: str = "raw",
    table: str | None = None,
    schema: str | None = None,
    incremental_config: dict | None = None,
    primary_key_columns: list[str] | None = None,
    dlt_config: dict | None = None,
) -> IR:
    """Build an IR document for a source → destination mapping.

    Dispatches by source type:
    - SQL types: introspect live via information_schema
    - SaaS types (P4): extract schema from dlt source hints
    - File types: not yet supported
    """
    if source_type in FILE_TYPES:
        raise IRBuildError(
            f"IR builder for file source type {source_type!r} is not supported yet. "
            f"File source IR (csv/json/parquet/s3) lands in a future phase."
        )

    if not table:
        raise IRBuildError(
            "build_ir requires a table/resource name. "
            "For SQL: the table name. For SaaS: the dlt resource name."
        )

    if source_type == "openapi":
        return _build_ir_openapi(
            source_config=source_config,
            destination_connection_id=destination_connection_id,
            destination_raw_schema=destination_raw_schema,
            table=table,
            incremental_config=incremental_config,
            primary_key_columns=primary_key_columns,
        )

    if source_type in SAAS_TYPES:
        return _build_ir_saas(
            source_type=source_type,
            source_config=source_config,
            destination_connection_id=destination_connection_id,
            destination_raw_schema=destination_raw_schema,
            table=table,
            dlt_config=dlt_config,
            incremental_config=incremental_config,
            primary_key_columns=primary_key_columns,
        )

    if source_type not in SQL_TYPES:
        raise IRBuildError(
            f"Unknown source type {source_type!r}. "
            f"Supported: SQL={sorted(SQL_TYPES)}, SaaS={sorted(SAAS_TYPES)}"
        )

    # SQL path (P3)
    columns = introspect_columns(
        config=source_config,
        connection_type=source_type,
        table=table,
        schema=schema,
    )

    if not columns:
        raise IRBuildError(f"No columns found for {source_type}://{schema or ''}.{table}")

    pk = primary_key_columns or [columns[0].target_name]

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
            connection_id=0,  # filled by caller
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
