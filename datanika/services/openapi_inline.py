"""Build inlined OpenAPI schemas with discriminator-based ``oneOf`` for
``ConnectionCreate.config`` and ``UploadCreate.dlt_config`` (#57).

The single source of truth lives in:
- ``datanika.services.connection_schemas.CONFIG_SCHEMAS`` (per connection
  type field shapes)
- ``datanika.services.meta_schemas.DLT_CONFIG_SCHEMA`` (full dlt_config
  schema covering both modes)

This module walks those dicts and produces OpenAPI 3.0.3-compatible
component schemas plus a ``oneOf`` + ``discriminator`` parent so AI agents
reading the spec can see exactly what fields each connection type or
upload mode requires — no more ``{type: object}`` placeholders.

The resulting schemas are merged into the openapi.py ``SCHEMAS`` dict at
import time. Adding a new connection type to ``CONFIG_SCHEMAS`` makes it
appear in the spec automatically.
"""

from __future__ import annotations

import copy
import re
from typing import Any

from datanika.models.connection import ConnectionType
from datanika.services.connection_schemas import CONFIG_SCHEMAS
from datanika.services.meta_schemas import DLT_CONFIG_SCHEMA

CONNECTION_TYPE_VALUES: list[str] = [ct.value for ct in ConnectionType]


def _to_pascal(slug: str) -> str:
    """Convert ``google_sheets`` → ``GoogleSheets``, ``rest_api`` → ``RestApi``."""
    return "".join(part.capitalize() for part in re.split(r"[_\-]", slug) if part)


def _example_for(slug: str, schema: dict) -> dict:
    """Build a request-body example for a connection type.

    Pulls plausible values from the schema's ``properties`` so agents
    have a working template they can edit. Sensitive fields use
    ``CHANGE_ME`` so the example never accidentally leaks credentials.
    """
    props: dict[str, Any] = schema.get("properties", {})
    config: dict[str, Any] = {}
    for field, prop in props.items():
        if prop.get("format") == "password":
            config[field] = "CHANGE_ME"
        elif prop.get("type") == "integer":
            config[field] = prop.get("default", 0)
        elif prop.get("type") == "boolean":
            config[field] = prop.get("default", False)
        else:
            # String — produce a placeholder hinting at the field name
            config[field] = f"<{field}>"
    return {
        "name": f"My {_to_pascal(slug)}",
        "connection_type": slug,
        "config": config,
    }


def build_connection_inlined_schemas() -> dict[str, dict]:
    """Return component schemas for inlined connection request bodies.

    Produces three families per connection type ``<slug>``:
    - ``<Pascal>ConnectionConfig`` — the config object only
    - ``Create<Pascal>Connection`` — the full request body with
      ``connection_type`` const-fixed to ``slug``
    - All branches are gathered into ``ConnectionCreate`` (the parent
      with discriminator on ``connection_type``)

    The connection slugs come from ``CONFIG_SCHEMAS`` which is the
    single source of truth (also used by
    ``GET /api/v1/meta/connection-types``).
    """
    components: dict[str, dict] = {}
    branches: list[dict] = []
    mapping: dict[str, str] = {}

    for slug, raw_config in CONFIG_SCHEMAS.items():
        pascal = _to_pascal(slug)
        config_name = f"{pascal}ConnectionConfig"
        create_name = f"Create{pascal}Connection"

        # 1. Standalone config schema (deep-copied so we don't mutate
        #    the source-of-truth dict).
        config_schema = copy.deepcopy(raw_config)
        config_schema["title"] = f"{pascal} connection config"
        components[config_name] = config_schema

        # 2. Full create body with `connection_type` pinned via enum-of-one.
        components[create_name] = {
            "type": "object",
            "title": f"Create {pascal} connection",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Human-readable connection name",
                },
                "connection_type": {
                    "type": "string",
                    "enum": [slug],
                    "description": (
                        f"Discriminator value: must be `{slug}` for this connection type."
                    ),
                },
                "config": {
                    "$ref": f"#/components/schemas/{config_name}",
                    "description": (
                        f"Per-type configuration. See `{config_name}` for field details."
                    ),
                },
            },
            "required": ["name", "connection_type", "config"],
            "additionalProperties": False,
            "example": _example_for(slug, raw_config),
        }

        branches.append({"$ref": f"#/components/schemas/{create_name}"})
        mapping[slug] = f"#/components/schemas/{create_name}"

    # Parent: oneOf with discriminator on `connection_type`.
    components["ConnectionCreate"] = {
        "title": "Create connection request",
        "description": (
            "Request body for `POST /api/v1/connections`. The discriminator "
            "is `connection_type` — pick the variant matching your source "
            "or destination type. Each variant has a fully-typed `config` "
            "object so AI agents and code generators can produce a valid "
            "payload from the spec alone."
        ),
        "oneOf": branches,
        "discriminator": {
            "propertyName": "connection_type",
            "mapping": mapping,
        },
    }

    return components


def build_upload_inlined_schemas() -> dict[str, dict]:
    """Return component schemas for inlined upload request bodies.

    Splits the dlt_config into two variants based on ``mode``
    (``single_table`` vs ``full_database``) and exposes them via a
    discriminator on the ``dlt_config.mode`` field.

    The dlt_config field shapes come from ``DLT_CONFIG_SCHEMA`` in
    ``meta_schemas.py`` (also served by
    ``GET /api/v1/meta/dlt-config-schema``).
    """
    base_props: dict[str, Any] = DLT_CONFIG_SCHEMA.get("properties", {})

    def _pick(keys: list[str]) -> dict[str, Any]:
        return {k: copy.deepcopy(base_props[k]) for k in keys if k in base_props}

    # Fields shared by both modes
    shared = ["write_disposition", "source_schema", "batch_size", "schema_contract", "filters"]

    single_table_props: dict[str, Any] = {
        "mode": {
            "type": "string",
            "enum": ["single_table"],
            "description": "Discriminator: load a single source table.",
        },
        "table": copy.deepcopy(base_props.get("table", {"type": "string"})),
        "primary_key": copy.deepcopy(base_props.get("primary_key", {})),
        "incremental": copy.deepcopy(base_props.get("incremental", {})),
        **_pick(shared),
    }

    full_database_props: dict[str, Any] = {
        "mode": {
            "type": "string",
            "enum": ["full_database"],
            "description": "Discriminator: load multiple tables from one source.",
        },
        "table_names": copy.deepcopy(base_props.get("table_names", {})),
        "merge_config": copy.deepcopy(base_props.get("merge_config", {})),
        **_pick(shared),
    }

    components: dict[str, dict] = {
        "SingleTableDltConfig": {
            "type": "object",
            "title": "Single-table dlt_config",
            "description": (
                "Load one specific table from the source. Supports "
                "incremental cursor-based extraction and per-row filters."
            ),
            "properties": single_table_props,
            "required": ["mode", "table"],
            "additionalProperties": False,
        },
        "FullDatabaseDltConfig": {
            "type": "object",
            "title": "Full-database dlt_config",
            "description": (
                "Load multiple tables in a single run. Use `table_names` "
                "to whitelist or omit it to discover all tables. For "
                "merge disposition, supply `merge_config` keyed by table."
            ),
            "properties": full_database_props,
            "required": ["mode"],
            "additionalProperties": False,
        },
    }

    components["DltConfig"] = {
        "title": "Upload dlt_config",
        "description": (
            "Configuration for an extract+load operation. Discriminator is "
            "the nested `mode` field — `single_table` for one-table loads, "
            "`full_database` for multi-table loads."
        ),
        "oneOf": [
            {"$ref": "#/components/schemas/SingleTableDltConfig"},
            {"$ref": "#/components/schemas/FullDatabaseDltConfig"},
        ],
        "discriminator": {
            "propertyName": "mode",
            "mapping": {
                "single_table": "#/components/schemas/SingleTableDltConfig",
                "full_database": "#/components/schemas/FullDatabaseDltConfig",
            },
        },
    }

    components["UploadCreate"] = {
        "type": "object",
        "title": "Create upload request",
        "description": (
            "Request body for `POST /api/v1/uploads`. The `dlt_config` "
            "field is a discriminated union — see `DltConfig`."
        ),
        "properties": {
            "name": {"type": "string"},
            "source_connection_id": {"type": "integer"},
            "destination_connection_id": {"type": "integer"},
            "description": {"type": "string"},
            "dlt_config": {"$ref": "#/components/schemas/DltConfig"},
        },
        "required": [
            "name",
            "source_connection_id",
            "destination_connection_id",
        ],
        "example": {
            "name": "Stripe Charges",
            "source_connection_id": 1,
            "destination_connection_id": 2,
            "dlt_config": {
                "mode": "single_table",
                "table": "charges",
                "write_disposition": "merge",
                "primary_key": "id",
                "incremental": {
                    "cursor_path": "created",
                    "row_order": "asc",
                },
            },
        },
    }

    return components


def apply_inlined_schemas(schemas: dict) -> None:
    """Mutate the openapi.py SCHEMAS dict to add the inlined components
    and replace ``ConnectionCreate`` / ``UploadCreate`` with their
    discriminated variants.

    Also pins ``Connection.connection_type`` to the ConnectionType enum
    instead of a free-form string.
    """
    schemas.update(build_connection_inlined_schemas())
    schemas.update(build_upload_inlined_schemas())

    # Pin the read-side `Connection.connection_type` to the enum.
    if "Connection" in schemas:
        conn = schemas["Connection"]
        if "properties" in conn and "connection_type" in conn["properties"]:
            conn["properties"]["connection_type"] = {
                "type": "string",
                "enum": CONNECTION_TYPE_VALUES,
                "description": "Slug of the connection type.",
            }
