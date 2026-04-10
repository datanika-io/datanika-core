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
    "Connection": _obj({
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "connection_type": {"type": "string"},
        "direction": {"type": "string", "enum": _DIRECTIONS},
        "created_at": _TS,
        "updated_at": _TS,
    }),
    "ConnectionCreate": _obj(
        {
            "name": {"type": "string"},
            "connection_type": {"type": "string"},
            "config": {"type": "object"},
        },
        required=["name", "connection_type", "config"],
    ),
    "Upload": _obj({
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "description": {"type": "string", "nullable": True},
        "source_connection_id": {"type": "integer"},
        "destination_connection_id": {"type": "integer"},
        "dlt_config": {"type": "object"},
        "status": {"type": "string"},
        "created_at": _TS,
        "updated_at": _TS,
    }),
    "UploadCreate": _obj(
        {
            "name": {"type": "string"},
            "source_connection_id": {"type": "integer"},
            "destination_connection_id": {"type": "integer"},
            "description": {"type": "string"},
            "dlt_config": {"type": "object"},
        },
        required=[
            "name", "source_connection_id",
            "destination_connection_id",
        ],
    ),
    "Pipeline": _obj({
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
    }),
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
    "Transformation": _obj({
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "description": {"type": "string", "nullable": True},
        "sql_body": {"type": "string"},
        "materialization": {"type": "string", "enum": _MAT_TYPES},
        "schema_name": {"type": "string"},
        "tests_config": {"type": "object"},
        "destination_connection_id": {
            "type": "integer", "nullable": True,
        },
        "tags": {
            "type": "array",
            "items": {"type": "string"},
            "nullable": True,
        },
        "created_at": _TS,
        "updated_at": _TS,
    }),
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
    "Schedule": _obj({
        "id": {"type": "integer"},
        "target_type": {"type": "string", "enum": _NODE_TYPES},
        "target_id": {"type": "integer"},
        "cron_expression": {"type": "string"},
        "timezone": {"type": "string"},
        "is_active": {"type": "boolean"},
        "created_at": _TS,
        "updated_at": _TS,
    }),
    "ScheduleCreate": _obj(
        {
            "target_type": {
                "type": "string", "enum": _NODE_TYPES,
            },
            "target_id": {"type": "integer"},
            "cron_expression": {"type": "string"},
            "timezone": {"type": "string", "default": "UTC"},
            "is_active": {"type": "boolean", "default": True},
        },
        required=["target_type", "target_id", "cron_expression"],
    ),
    "Run": _obj({
        "id": {"type": "integer"},
        "target_type": {"type": "string"},
        "target_id": {"type": "integer"},
        "status": {"type": "string", "enum": _RUN_STATUSES},
        "started_at": _TS,
        "finished_at": _TS,
        "rows_loaded": {"type": "integer", "nullable": True},
        "error_message": {"type": "string", "nullable": True},
        "created_at": _TS,
    }),
    "RunLogs": _obj({
        "run_id": {"type": "integer"},
        "logs": {"type": "string"},
    }),
    "NotificationChannel": _obj({
        "id": {"type": "integer"},
        "name": {"type": "string"},
        "channel_type": {"type": "string", "enum": _CHANNEL_TYPES},
        "config": {"type": "object"},
        "events": {"type": "array", "items": {"type": "string"}},
        "is_active": {"type": "boolean"},
        "created_at": _TS,
        "updated_at": _TS,
    }),
    "NotificationChannelCreate": _obj(
        {
            "name": {"type": "string"},
            "channel_type": {
                "type": "string", "enum": _CHANNEL_TYPES,
            },
            "config": {"type": "object"},
            "events": {"type": "array", "items": {"type": "string"}},
        },
        required=["name", "channel_type"],
    ),
}

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


def _trigger_op(tag, summary):
    return {
        "tags": [tag],
        "summary": summary,
        "parameters": [_ID_PARAM],
        "responses": {
            "202": {
                "description": "Accepted",
                "content": _json_content(_ref("TriggerResult")),
            },
            "401": _401,
            "404": _404,
            "429": _429,
        },
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
                tag, create_schema, schema, f"Create {cap}",
            ),
        },
        by_id: {
            "get": _get_op(tag, schema, f"Get {cap}"),
            "put": _update_op(
                tag, create_schema, schema, f"Update {cap}",
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
            "Transformations", "Transformation",
            "TransformationCreate", "transformation",
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
            "post": _trigger_op(tag, f"{action} {resource[:-1]}"),
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
            nt, ncc, nc, "Create notification channel",
        ),
    }
    paths["/api/v1/notifications/channels/{id}"] = {
        "get": _get_op(nt, nc, "Get notification channel"),
        "put": _update_op(
            nt, ncc, nc, "Update notification channel",
        ),
        "delete": _delete_op(nt, "Delete notification channel"),
    }

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
                    "description": (
                        "API key from Settings > API Keys"
                    ),
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
