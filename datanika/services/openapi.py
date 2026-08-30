"""OpenAPI 3.0.3 spec and Swagger UI for the REST API v1."""

from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse
from starlette.routing import Route

API_VERSION = "1.0.0"
API_TITLE = "Datanika API"
API_DESCRIPTION = (
    "Programmatic access to Datanika resources. "
    "Authenticate with a Bearer API key (Settings > API Keys)."
)

# ---------------------------------------------------------------------------
# Reusable schema fragments
# ---------------------------------------------------------------------------

_ERR_REF = "#/components/schemas/Error"

_ERROR = {
    "type": "object",
    "properties": {
        "error": {
            "type": "object",
            "properties": {
                "code": {"type": "integer"},
                "message": {"type": "string"},
            },
        },
    },
}

_DELETED = {
    "type": "object",
    "properties": {"deleted": {"type": "boolean"}},
}

_TRIGGER = {
    "type": "object",
    "properties": {
        "run_id": {"type": "integer"},
        "status": {"type": "string", "example": "pending"},
    },
}

_TS = {"type": "string", "format": "date-time", "nullable": True}

_DBT_CMDS = ["build", "run", "test", "seed", "snapshot", "compile"]
_MAT_TYPES = ["view", "table", "incremental", "ephemeral", "snapshot"]
_RUN_STATUSES = ["pending", "running", "success", "failed", "cancelled"]
_NODE_TYPES = ["upload", "transformation", "pipeline"]
_CHANNEL_TYPES = ["email", "slack", "telegram", "webhook"]
_DIRECTIONS = ["source", "destination", "both"]


def _obj(props: dict, required: list | None = None) -> dict:
    s = {"type": "object", "properties": props}
    if required:
        s["required"] = required
    return s


def _list_of(ref: str) -> dict:
    item_ref = f"#/components/schemas/{ref}"
    return {
        "type": "object",
        "properties": {
            "items": {
                "type": "array",
                "items": {"$ref": item_ref},
            },
        },
    }


def _json_content(schema: dict) -> dict:
    return {"application/json": {"schema": schema}}


def _ref(name: str) -> dict:
    return {"$ref": f"#/components/schemas/{name}"}


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

SCHEMAS = {
    "Error": _ERROR,
    "Deleted": _DELETED,
    "TriggerResult": _TRIGGER,
    "Connection": _obj(
        {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "connection_type": {"type": "string"},
            "direction": {"type": "string", "enum": _DIRECTIONS},
            "created_at": _TS,
            "updated_at": _TS,
        }
    ),
    "ConnectionCreate": _obj(
        {
            "name": {"type": "string"},
            "connection_type": {"type": "string"},
            "config": {"type": "object"},
        },
        required=["name", "connection_type", "config"],
    ),
    "Upload": _obj(
        {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "description": {"type": "string", "nullable": True},
            "source_connection_id": {"type": "integer"},
            "destination_connection_id": {"type": "integer"},
            "dlt_config": {"type": "object"},
            "status": {"type": "string"},
            "created_at": _TS,
            "updated_at": _TS,
        }
    ),
    "UploadCreate": _obj(
        {
            "name": {"type": "string"},
            "source_connection_id": {"type": "integer"},
            "destination_connection_id": {"type": "integer"},
            "description": {"type": "string"},
            "dlt_config": {"type": "object"},
        },
        required=[
            "name",
            "source_connection_id",
            "destination_connection_id",
        ],
    ),
    "Pipeline": _obj(
        {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "description": {"type": "string", "nullable": True},
            "destination_connection_id": {"type": "integer"},
            "command": {"type": "string", "enum": _DBT_CMDS},
            "full_refresh": {"type": "boolean"},
            "models": {"type": "array", "items": {"type": "object"}},
            "custom_selector": {"type": "string", "nullable": True},
            "status": {"type": "string"},
            "created_at": _TS,
            "updated_at": _TS,
        }
    ),
    "PipelineCreate": _obj(
        {
            "name": {"type": "string"},
            "destination_connection_id": {"type": "integer"},
            "command": {"type": "string", "default": "run"},
            "description": {"type": "string"},
            "full_refresh": {"type": "boolean", "default": False},
            "models": {"type": "array", "items": {"type": "object"}},
            "custom_selector": {"type": "string"},
        },
        required=["name", "destination_connection_id"],
    ),
    "Transformation": _obj(
        {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "description": {"type": "string", "nullable": True},
            "sql_body": {"type": "string"},
            "materialization": {"type": "string", "enum": _MAT_TYPES},
            "schema_name": {"type": "string"},
            "tests_config": {"type": "object"},
            "destination_connection_id": {
                "type": "integer",
                "nullable": True,
            },
            "tags": {
                "type": "array",
                "items": {"type": "string"},
                "nullable": True,
            },
            "created_at": _TS,
            "updated_at": _TS,
        }
    ),
    "TransformationCreate": _obj(
        {
            "name": {"type": "string"},
            "sql_body": {"type": "string"},
            "materialization": {"type": "string", "default": "view"},
            "description": {"type": "string"},
            "schema_name": {"type": "string", "default": "staging"},
        },
        required=["name", "sql_body"],
    ),
    "Schedule": _obj(
        {
            "id": {"type": "integer"},
            "target_type": {"type": "string", "enum": _NODE_TYPES},
            "target_id": {"type": "integer"},
            "cron_expression": {"type": "string"},
            "timezone": {"type": "string"},
            "is_active": {"type": "boolean"},
            "created_at": _TS,
            "updated_at": _TS,
        }
    ),
    "ScheduleCreate": _obj(
        {
            "target_type": {
                "type": "string",
                "enum": _NODE_TYPES,
            },
            "target_id": {"type": "integer"},
            "cron_expression": {"type": "string"},
            "timezone": {"type": "string", "default": "UTC"},
            "is_active": {"type": "boolean", "default": True},
        },
        required=["target_type", "target_id", "cron_expression"],
    ),
    "Run": _obj(
        {
            "id": {"type": "integer"},
            "target_type": {"type": "string"},
            "target_id": {"type": "integer"},
            "status": {"type": "string", "enum": _RUN_STATUSES},
            "started_at": _TS,
            "finished_at": _TS,
            "rows_loaded": {"type": "integer", "nullable": True},
            "error_message": {"type": "string", "nullable": True},
            "created_at": _TS,
        }
    ),
    "RunLogs": _obj(
        {
            "run_id": {"type": "integer"},
            "logs": {"type": "string"},
        }
    ),
    "NotificationChannel": _obj(
        {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "channel_type": {"type": "string", "enum": _CHANNEL_TYPES},
            "config": {"type": "object"},
            "events": {"type": "array", "items": {"type": "string"}},
            "is_active": {"type": "boolean"},
            "created_at": _TS,
            "updated_at": _TS,
        }
    ),
    "NotificationChannelCreate": _obj(
        {
            "name": {"type": "string"},
            "channel_type": {
                "type": "string",
                "enum": _CHANNEL_TYPES,
            },
            "config": {"type": "object"},
            "events": {"type": "array", "items": {"type": "string"}},
        },
        required=["name", "channel_type"],
    ),
    # --- Catalog ---
    "CatalogEntry": _obj(
        {
            "id": {"type": "integer"},
            "entry_type": {
                "type": "string",
                "enum": ["source_table", "dbt_model"],
            },
            "origin_type": {
                "type": "string",
                "enum": ["upload", "transformation", "pipeline"],
            },
            "origin_id": {"type": "integer"},
            "table_name": {"type": "string"},
            "schema_name": {"type": "string"},
            "dataset_name": {"type": "string"},
            "connection_id": {"type": "integer", "nullable": True},
            "description": {"type": "string", "nullable": True},
            "columns": {
                "type": "array",
                "items": {"type": "object"},
                "nullable": True,
            },
            "dbt_config": {"type": "object", "nullable": True},
            "created_at": _TS,
            "updated_at": _TS,
        }
    ),
    # --- Tier 2 Agent Compatibility: compile + preview ---
    "CompileResult": _obj(
        {
            "compiled_sql": {
                "type": "string",
                "description": ("Fully rendered SQL after Jinja + ref/source resolution."),
            },
            "node": {
                "type": "string",
                "description": ("dbt fully-qualified node name, e.g. `model.tenant_1.stg_orders`."),
            },
        },
        required=["compiled_sql", "node"],
    ),
    "CompileError": _obj(
        {
            "error": _obj(
                {
                    "code": {
                        "type": "string",
                        "enum": ["compilation_error"],
                    },
                    "message": {"type": "string"},
                    "line": {"type": "integer", "nullable": True},
                    "column": {"type": "integer", "nullable": True},
                },
                required=["code", "message"],
            ),
        },
        required=["error"],
    ),
    "PreviewRequest": _obj(
        {
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 1000,
                "default": 100,
                "description": ("Maximum rows to return. Clamped to [1, 1000]."),
            },
        },
    ),
    "PreviewColumn": _obj(
        {
            "name": {"type": "string"},
            "type": {"type": "string"},
        },
        required=["name"],
    ),
    "PreviewResult": _obj(
        {
            "columns": {
                "type": "array",
                "items": _ref("PreviewColumn"),
            },
            "rows": {
                "type": "array",
                "items": {"type": "array"},
                "description": (
                    "Row-major result set. Each inner array matches the "
                    "`columns` order. Values are JSON-serialized scalars "
                    "or strings for unknown types."
                ),
            },
            "row_count": {"type": "integer"},
            "truncated": {
                "type": "boolean",
                "description": (
                    "True when `row_count` equals the requested limit "
                    "(the underlying result may have had more rows)."
                ),
            },
        },
        required=["columns", "rows", "row_count", "truncated"],
    ),
    "PreviewError": _obj(
        {
            "error": _obj(
                {
                    "code": {
                        "type": "string",
                        "enum": [
                            "compilation_error",
                            "execution_error",
                            "missing_destination",
                            "unsafe_sql",
                            "invalid_request",
                        ],
                    },
                    "message": {"type": "string"},
                },
                required=["code", "message"],
            ),
        },
        required=["error"],
    ),
    # --- Bulk import ---
    "ImportRequest": _obj(
        {
            "version": {"type": "integer", "enum": [2]},
            "connections": {
                "type": "array",
                "items": _ref("ConnectionCreate"),
                "description": "Connections to create (optional).",
            },
            "uploads": {
                "type": "array",
                "items": {"type": "object"},
                "description": (
                    "Uploads to create. Use connection_name references, not connection_id."
                ),
            },
            "pipelines": {
                "type": "array",
                "items": {"type": "object"},
                "description": (
                    "Pipelines to create. Use destination_connection_name, "
                    "not destination_connection_id."
                ),
            },
            "transformations": {
                "type": "array",
                "items": {"type": "object"},
                "description": "Transformations to create.",
            },
        },
        required=["version"],
    ),
    "ImportResult": _obj(
        {
            "created": _obj(
                {
                    "connections": {
                        "type": "array",
                        "items": {"type": "integer"},
                    },
                    "uploads": {
                        "type": "array",
                        "items": {"type": "integer"},
                    },
                    "pipelines": {
                        "type": "array",
                        "items": {"type": "integer"},
                    },
                    "transformations": {
                        "type": "array",
                        "items": {"type": "integer"},
                    },
                }
            ),
        },
        required=["created"],
    ),
}

# Inline per-type Connection.config and per-mode Upload.dlt_config schemas
# from the single source of truth. This replaces the placeholder
# `{type: object}` entries with discriminated `oneOf` branches.
from datanika.services.openapi_inline import apply_inlined_schemas  # noqa: E402

apply_inlined_schemas(SCHEMAS)

# ---------------------------------------------------------------------------
# Response shortcuts
# ---------------------------------------------------------------------------

_err_content = _json_content(_ref("Error"))
_401 = {"description": "Unauthorized", "content": _err_content}
_404 = {"description": "Not found", "content": _err_content}
_429 = {"description": "Rate limit exceeded", "content": _err_content}

_ID_PARAM = {
    "name": "id",
    "in": "path",
    "required": True,
    "schema": {"type": "integer"},
}


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def _list_op(tag, schema, summary, params=None):
    op = {
        "tags": [tag],
        "summary": summary,
        "responses": {
            "200": {
                "description": "OK",
                "content": _json_content(_list_of(schema)),
            },
            "401": _401,
            "429": _429,
        },
    }
    if params:
        op["parameters"] = params
    return op


def _get_op(tag, schema, summary):
    return {
        "tags": [tag],
        "summary": summary,
        "parameters": [_ID_PARAM],
        "responses": {
            "200": {
                "description": "OK",
                "content": _json_content(_ref(schema)),
            },
            "401": _401,
            "404": _404,
            "429": _429,
        },
    }


def _create_op(tag, req_schema, resp_schema, summary):
    return {
        "tags": [tag],
        "summary": summary,
        "requestBody": {
            "required": True,
            "content": _json_content(_ref(req_schema)),
        },
        "responses": {
            "201": {
                "description": "Created",
                "content": _json_content(_ref(resp_schema)),
            },
            "400": {
                "description": "Validation error",
                "content": _err_content,
            },
            "401": _401,
            "429": _429,
        },
    }


def _update_op(tag, req_schema, resp_schema, summary):
    return {
        "tags": [tag],
        "summary": summary,
        "parameters": [_ID_PARAM],
        "requestBody": {
            "required": True,
            "content": _json_content(_ref(req_schema)),
        },
        "responses": {
            "200": {
                "description": "Updated",
                "content": _json_content(_ref(resp_schema)),
            },
            "400": {
                "description": "Validation error",
                "content": _err_content,
            },
            "401": _401,
            "404": _404,
            "429": _429,
        },
    }


def _delete_op(tag, summary):
    return {
        "tags": [tag],
        "summary": summary,
        "parameters": [_ID_PARAM],
        "responses": {
            "200": {
                "description": "Deleted",
                "content": _json_content(_ref("Deleted")),
            },
            "401": _401,
            "404": _404,
            "429": _429,
        },
    }


_WAIT_PARAM = {
    "name": "wait",
    "in": "query",
    "required": False,
    "description": (
        "Block until the run reaches a terminal status. The status code then "
        "carries the run's **outcome**, not just the transport's: 200 success, "
        "408 still running at the timeout, 422 terminal but not successful. "
        "`curl --fail` and `raise_for_status()` are therefore correct idioms "
        "for 'did my pipeline work?'."
    ),
    "schema": {"type": "boolean", "default": False},
}

_TIMEOUT_PARAM = {
    "name": "timeout",
    "in": "query",
    "required": False,
    "description": "Seconds to wait when `wait=true`. Clamped to 1..300.",
    "schema": {"type": "integer", "default": 120, "minimum": 1, "maximum": 300},
}


def _trigger_op(tag, summary, *, waitable=False):
    """Build a trigger operation.

    ``waitable`` documents the ``?wait=true`` outcome-on-the-status-line
    contract (#663). It is **off by default** because
    ``/connections/{id}/test`` shares this helper and has no run to wait for —
    documenting 408/422 there would describe responses it cannot produce.
    """
    responses = {
        "202": {
            "description": "Accepted — dispatched, outcome not yet known",
            "content": _json_content(_ref("TriggerResult")),
        },
        "401": _401,
        "404": _404,
        "429": _429,
    }
    parameters = [_ID_PARAM]
    if waitable:
        parameters = [_ID_PARAM, _WAIT_PARAM, _TIMEOUT_PARAM]
        responses["200"] = {
            "description": "`wait=true` — the run finished successfully",
            "content": _json_content(_ref("Run")),
        }
        responses["408"] = {
            "description": (
                "`wait=true` — still pending/running when the wait expired. The "
                "body is the run with `timed_out: true`; it is still going. Not "
                "a failure — poll `GET /api/v1/runs/{id}` or wait again."
            ),
            "content": _json_content(_ref("Run")),
        }
        responses["422"] = {
            "description": (
                "`wait=true` — the run reached a terminal status that is not "
                "success (`failed`, `cancelled`). The body is the run, so "
                "`status` and `error_message` remain the source of truth. Not a "
                "5xx: the failure is in your pipeline, not in the server."
            ),
            "content": _json_content(_ref("Run")),
        }
    return {
        "tags": [tag],
        "summary": summary,
        "parameters": parameters,
        "responses": responses,
    }


def _crud(tag, schema, create_schema, name):
    """Generate list+get+create+update+delete paths for a resource."""
    plural = name + "s" if not name.endswith("s") else name
    base = f"/api/v1/{plural}"
    by_id = f"{base}/{{id}}"
    cap = name.capitalize()
    return {
        base: {
            "get": _list_op(tag, schema, f"List {plural}"),
            "post": _create_op(
                tag,
                create_schema,
                schema,
                f"Create {cap}",
            ),
        },
        by_id: {
            "get": _get_op(tag, schema, f"Get {cap}"),
            "put": _update_op(
                tag,
                create_schema,
                schema,
                f"Update {cap}",
            ),
            "delete": _delete_op(tag, f"Delete {cap}"),
        },
    }


# ---------------------------------------------------------------------------
# Runs query parameters
# ---------------------------------------------------------------------------

_RUNS_PARAMS = [
    {
        "name": "target_type",
        "in": "query",
        "schema": {"type": "string", "enum": _NODE_TYPES},
    },
    {
        "name": "target_id",
        "in": "query",
        "schema": {"type": "integer"},
    },
    {
        "name": "status",
        "in": "query",
        "schema": {"type": "string", "enum": _RUN_STATUSES},
    },
    {
        "name": "limit",
        "in": "query",
        "schema": {"type": "integer", "default": 50, "maximum": 200},
    },
]


# ---------------------------------------------------------------------------
# Stability annotations — docs/api_versioning.md, #137
# ---------------------------------------------------------------------------


def _annotate_stability(paths: dict) -> None:
    """Tag every operation with ``x-stability`` (stable | beta | experimental).

    Catalog endpoints are beta; everything else is stable. The tags show
    up in Swagger UI and are consumed by enterprise procurement checklists.
    """
    for path_key, methods in paths.items():
        stability = "beta" if path_key.startswith("/api/v1/catalog") else "stable"
        for op in methods.values():
            if isinstance(op, dict) and "tags" in op:
                op["x-stability"] = stability


# ---------------------------------------------------------------------------
# Full spec
# ---------------------------------------------------------------------------


def build_openapi_spec() -> dict:
    paths = {}

    # Standard CRUD resources
    for tag, schema, create, name in [
        ("Connections", "Connection", "ConnectionCreate", "connection"),
        ("Uploads", "Upload", "UploadCreate", "upload"),
        ("Pipelines", "Pipeline", "PipelineCreate", "pipeline"),
        (
            "Transformations",
            "Transformation",
            "TransformationCreate",
            "transformation",
        ),
        ("Schedules", "Schedule", "ScheduleCreate", "schedule"),
    ]:
        paths.update(_crud(tag, schema, create, name))

    # Trigger endpoints
    for resource, tag in [
        ("connections", "Connections"),
        ("uploads", "Uploads"),
        ("pipelines", "Pipelines"),
        ("transformations", "Transformations"),
    ]:
        action = "Test" if resource == "connections" else "Trigger"
        suffix = "test" if resource == "connections" else "run"
        paths[f"/api/v1/{resource}/{{id}}/{suffix}"] = {
            "post": _trigger_op(tag, f"{action} {resource[:-1]}", waitable=suffix == "run"),
        }

    # Tier 2 Agent Compatibility: transformation compile + preview
    paths["/api/v1/transformations/{id}/compile"] = {
        "post": {
            "tags": ["Transformations"],
            "summary": "Compile a transformation via dbt",
            "description": (
                "Wraps `dbt compile` for the tenant dbt project. "
                "Resolves Jinja and `{{ ref() }}` / `{{ source() }}` "
                "references. No warehouse execution."
            ),
            "parameters": [_ID_PARAM],
            "responses": {
                "200": {
                    "description": "Compilation succeeded",
                    "content": _json_content(_ref("CompileResult")),
                },
                "400": {
                    "description": "Compilation failed",
                    "content": _json_content(_ref("CompileError")),
                },
                "401": _401,
                "404": _404,
                "429": _429,
            },
        },
    }
    paths["/api/v1/transformations/{id}/preview"] = {
        "post": {
            "tags": ["Transformations"],
            "summary": "Preview a transformation's output",
            "description": (
                "Compiles the transformation and runs the resulting SQL "
                "wrapped in `SELECT * FROM (...) LIMIT N` against the "
                "destination warehouse. Defense-in-depth: the read-only "
                "SQL validator re-checks the compiled output even after "
                "dbt compile."
            ),
            "parameters": [_ID_PARAM],
            "requestBody": {
                "required": False,
                "content": _json_content(_ref("PreviewRequest")),
            },
            "responses": {
                "200": {
                    "description": "Preview rows returned",
                    "content": _json_content(_ref("PreviewResult")),
                },
                "400": {
                    "description": ("Compilation, execution, or configuration error"),
                    "content": _json_content(_ref("PreviewError")),
                },
                "401": _401,
                "404": _404,
                "429": _429,
            },
        },
    }

    # Runs (read-only)
    paths["/api/v1/runs"] = {
        "get": _list_op("Runs", "Run", "List runs", _RUNS_PARAMS),
    }
    paths["/api/v1/runs/{id}"] = {
        "get": _get_op("Runs", "Run", "Get run"),
    }
    paths["/api/v1/runs/{id}/logs"] = {
        "get": _get_op("Runs", "RunLogs", "Get run logs"),
    }

    # Notification channels
    nc = "NotificationChannel"
    ncc = "NotificationChannelCreate"
    nt = "Notifications"
    paths["/api/v1/notifications/channels"] = {
        "get": _list_op(nt, nc, "List notification channels"),
        "post": _create_op(
            nt,
            ncc,
            nc,
            "Create notification channel",
        ),
    }
    paths["/api/v1/notifications/channels/{id}"] = {
        "get": _get_op(nt, nc, "Get notification channel"),
        "put": _update_op(
            nt,
            ncc,
            nc,
            "Update notification channel",
        ),
        "delete": _delete_op(nt, "Delete notification channel"),
    }

    # Bulk import (#131)
    paths["/api/v1/import"] = {
        "post": {
            "tags": ["Import"],
            "summary": "Bulk-import resources from a JSON config",
            "description": (
                "Create connections, uploads, pipelines, and transformations "
                "in a single call. Validates the entire payload first; if any "
                "errors are found, nothing is created (atomic). Uses the same "
                "JSON v2 format as AI_IMPORT_GUIDE.md."
            ),
            "requestBody": {
                "required": True,
                "content": _json_content(_ref("ImportRequest")),
            },
            "responses": {
                "201": {
                    "description": "All resources created",
                    "content": _json_content(_ref("ImportResult")),
                },
                "400": {
                    "description": "Validation errors — nothing created",
                    "content": _err_content,
                },
                "401": _401,
                "429": _429,
            },
        },
    }

    # Catalog (beta — schema may change as we add lineage)
    paths["/api/v1/catalog"] = {
        "get": _list_op("Catalog", "CatalogEntry", "List catalog entries"),
    }
    paths["/api/v1/catalog/{id}"] = {
        "get": _get_op("Catalog", "CatalogEntry", "Get catalog entry"),
    }

    # Annotate every operation with x-stability per the API versioning
    # policy (docs/api_versioning.md, #137). Enterprise procurement
    # requires a machine-readable stability signal in the OpenAPI spec.
    _annotate_stability(paths)

    return {
        "openapi": "3.0.3",
        "info": {
            "title": API_TITLE,
            "description": API_DESCRIPTION,
            "version": API_VERSION,
        },
        "servers": [{"url": "/", "description": "This server"}],
        "components": {
            "securitySchemes": {
                "BearerApiKey": {
                    "type": "http",
                    "scheme": "bearer",
                    "description": ("API key from Settings > API Keys"),
                },
            },
            "schemas": SCHEMAS,
        },
        "security": [{"BearerApiKey": []}],
        "tags": [
            {"name": "Connections"},
            {"name": "Uploads"},
            {"name": "Pipelines"},
            {"name": "Transformations"},
            {"name": "Schedules"},
            {"name": "Runs"},
            {"name": "Notifications"},
            {"name": "Import"},
            {"name": "Catalog"},
        ],
        "paths": paths,
    }


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

_SWAGGER_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Datanika API</title>
  <link rel="stylesheet"
    href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css"/>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src=\
"https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"\
></script>
  <script>
    SwaggerUIBundle({
      url: "/api/v1/openapi.json",
      dom_id: "#swagger-ui"
    });
  </script>
</body>
</html>"""


async def openapi_json(request: Request) -> JSONResponse:
    return JSONResponse(build_openapi_spec())


async def swagger_ui(request: Request) -> HTMLResponse:
    return HTMLResponse(_SWAGGER_HTML)


openapi_routes = [
    Route("/api/v1/openapi.json", openapi_json, methods=["GET"]),
    Route("/api/v1/docs", swagger_ui, methods=["GET"]),
]
