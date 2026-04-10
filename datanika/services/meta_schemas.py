"""JSON Schemas for upload dlt_config, dbt tests, and dbt materializations.

These mirror the validation logic in upload_service.py and
transformation_service.py so AI agents (and humans) can discover
the full set of valid options without reading source code.
"""

from datanika.models.transformation import Materialization
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import (
    VALID_FILTER_OPS,
    VALID_MODES,
    VALID_ROW_ORDERS,
    VALID_SCHEMA_CONTRACT_ENTITIES,
    VALID_SCHEMA_CONTRACT_VALUES,
    VALID_WRITE_DISPOSITIONS,
)

# ---------------------------------------------------------------------------
# dlt_config schema (for Upload.dlt_config)
# ---------------------------------------------------------------------------

DLT_CONFIG_SCHEMA: dict = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "title": "Upload dlt_config",
    "description": (
        "Configuration for an extract+load operation. The schema "
        "varies by `mode`: single_table loads one table with optional "
        "incremental, full_database loads multiple tables in one run."
    ),
    "type": "object",
    "properties": {
        "mode": {
            "type": "string",
            "enum": sorted(VALID_MODES),
            "default": "full_database",
            "description": (
                "single_table: load one specific table (supports incremental). "
                "full_database: load multiple tables, optionally filtered."
            ),
        },
        "write_disposition": {
            "type": "string",
            "enum": sorted(VALID_WRITE_DISPOSITIONS),
            "description": (
                "append: insert rows. replace: drop and recreate table. "
                "merge: upsert by primary_key (requires merge_config in full_database mode)."
            ),
        },
        "table": {
            "type": "string",
            "description": "Source table name (single_table mode only).",
        },
        "table_names": {
            "type": "array",
            "items": {"type": "string"},
            "description": "List of source table names (full_database mode only).",
        },
        "primary_key": {
            "oneOf": [
                {"type": "string"},
                {"type": "array", "items": {"type": "string"}},
            ],
            "description": ("Primary key column(s) for merge disposition (single_table mode)."),
        },
        "source_schema": {
            "type": "string",
            "description": "Source schema/dataset name (default: public).",
        },
        "incremental": {
            "type": "object",
            "description": "Incremental load config (single_table mode only).",
            "properties": {
                "cursor_path": {
                    "type": "string",
                    "description": "Column name to use as the cursor (e.g. updated_at).",
                },
                "initial_value": {
                    "description": ("Starting cursor value for the first run."),
                },
                "row_order": {
                    "type": "string",
                    "enum": sorted(VALID_ROW_ORDERS),
                    "description": ("Order rows are returned by the cursor column."),
                },
            },
            "required": ["cursor_path"],
        },
        "merge_config": {
            "type": "object",
            "description": (
                "Per-table merge config (full_database mode + merge disposition). "
                "Keys are table names, values are {primary_key: str | [str]}."
            ),
            "additionalProperties": {
                "type": "object",
                "properties": {
                    "primary_key": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "array", "items": {"type": "string"}},
                        ],
                    },
                },
                "required": ["primary_key"],
            },
        },
        "batch_size": {
            "type": "integer",
            "minimum": 1,
            "description": "Number of rows per batch sent to the destination.",
        },
        "schema_contract": {
            "type": "object",
            "description": (
                "How to handle schema drift. Keys are entities, values are "
                "behaviors when an unexpected change is detected."
            ),
            "properties": {
                entity: {
                    "type": "string",
                    "enum": sorted(VALID_SCHEMA_CONTRACT_VALUES),
                }
                for entity in sorted(VALID_SCHEMA_CONTRACT_ENTITIES)
            },
        },
        "filters": {
            "type": "array",
            "description": "Row-level filters to apply during extraction.",
            "items": {
                "type": "object",
                "properties": {
                    "column": {"type": "string"},
                    "op": {
                        "type": "string",
                        "enum": sorted(VALID_FILTER_OPS),
                    },
                    "value": {"description": "Comparison value (type depends on op)."},
                },
                "required": ["column", "op", "value"],
            },
        },
    },
    "additionalProperties": False,
    "examples": [
        {
            "mode": "single_table",
            "table": "orders",
            "write_disposition": "merge",
            "primary_key": "id",
            "incremental": {
                "cursor_path": "updated_at",
                "row_order": "asc",
            },
        },
        {
            "mode": "full_database",
            "table_names": ["customers", "orders", "products"],
            "write_disposition": "merge",
            "merge_config": {
                "customers": {"primary_key": "id"},
                "orders": {"primary_key": "id"},
                "products": {"primary_key": "sku"},
            },
        },
    ],
}


# ---------------------------------------------------------------------------
# dbt test types
# ---------------------------------------------------------------------------

DBT_TESTS: list[dict] = [
    {
        "name": "not_null",
        "description": "Asserts the column has no NULL values.",
        "params_schema": {"type": "null"},
        "example": {"columns": {"id": ["not_null"]}},
    },
    {
        "name": "unique",
        "description": "Asserts every value in the column is unique.",
        "params_schema": {"type": "null"},
        "example": {"columns": {"id": ["not_null", "unique"]}},
    },
    {
        "name": "accepted_values",
        "description": ("Asserts every value in the column is one of an allowed list."),
        "params_schema": {
            "type": "array",
            "items": {"type": ["string", "number", "boolean"]},
        },
        "example": {
            "columns": {"status": {"accepted_values": ["pending", "shipped", "delivered"]}}
        },
    },
    {
        "name": "relationships",
        "description": ("Asserts every value in the column exists in another model's column."),
        "params_schema": {
            "type": "object",
            "properties": {
                "to": {
                    "type": "string",
                    "description": "Target model reference, e.g. ref('customers').",
                },
                "field": {
                    "type": "string",
                    "description": "Target column name.",
                },
            },
            "required": ["to", "field"],
        },
        "example": {
            "columns": {"customer_id": {"relationships": {"to": "ref('customers')", "field": "id"}}}
        },
    },
]


def list_dbt_tests() -> list[dict]:
    return DBT_TESTS


# ---------------------------------------------------------------------------
# dbt materializations
# ---------------------------------------------------------------------------

MATERIALIZATIONS: list[dict] = [
    {
        "name": "view",
        "description": (
            "A SQL view in the destination warehouse. Re-evaluated every "
            "query. Cheapest to build, slowest to query."
        ),
    },
    {
        "name": "table",
        "description": (
            "A physical table rebuilt on every run. Fast to query, expensive to build."
        ),
    },
    {
        "name": "incremental",
        "description": (
            "A table updated with new rows on each run. Requires "
            "incremental_config to define the merge strategy and unique key."
        ),
    },
    {
        "name": "ephemeral",
        "description": (
            "Inlined as a CTE into downstream models. No physical object in the warehouse."
        ),
    },
    {
        "name": "snapshot",
        "description": (
            "Type 2 SCD: tracks changes over time with valid_from / valid_to "
            "columns. Use for slowly-changing dimensions."
        ),
    },
]


def list_materializations() -> list[dict]:
    """Verify all enum values are documented and return the list."""
    documented = {m["name"] for m in MATERIALIZATIONS}
    enum_values = {m.value for m in Materialization}
    missing = enum_values - documented
    if missing:
        raise RuntimeError(f"Materialization enum values not documented: {missing}")
    return MATERIALIZATIONS


# Re-export for convenience
__all__ = [
    "DLT_CONFIG_SCHEMA",
    "DBT_TESTS",
    "MATERIALIZATIONS",
    "list_dbt_tests",
    "list_materializations",
    "TransformationService",  # for tests that want VALID_GENERIC_TESTS
]
