"""REST API v1 — CRUD endpoints for connections, uploads, pipelines,
transformations, schedules, and runs.

All endpoints use @api_endpoint() for authentication + rate limiting.
Tenant isolation via api_key.org_id.
"""

import json
import logging

from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from datanika.models.catalog_entry import CatalogEntryType
from datanika.models.connection import ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.notification_channel import ChannelType
from datanika.models.pipeline import DbtCommand
from datanika.models.run import RunStatus
from datanika.models.transformation import Materialization
from datanika.services.api_middleware import api_endpoint
from datanika.services.catalog_service import CatalogService
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.execution_service import ExecutionService
from datanika.services.notification_service import NotificationService
from datanika.services.pipeline_service import PipelineService
from datanika.services.schedule_service import ScheduleService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService

logger = logging.getLogger(__name__)

_pipeline_svc = PipelineService()
_transform_svc = TransformationService()
_exec_svc = ExecutionService()
_notif_svc = NotificationService()

# Lazy-init services that depend on encryption key (not valid at import time in tests)
_conn_svc = None
_upload_svc = None
_schedule_svc = None


def _get_conn_svc():
    global _conn_svc
    if _conn_svc is None:
        from datanika.config import settings as _s

        _conn_svc = ConnectionService(EncryptionService(_s.credential_encryption_key))
    return _conn_svc


def _get_upload_svc():
    global _upload_svc
    if _upload_svc is None:
        _upload_svc = UploadService(_get_conn_svc())
    return _upload_svc


def _get_schedule_svc():
    global _schedule_svc
    if _schedule_svc is None:
        _schedule_svc = ScheduleService(
            _get_upload_svc(), _transform_svc, pipeline_service=_pipeline_svc
        )
    return _schedule_svc


def _error(status: int, message: str) -> JSONResponse:
    return JSONResponse({"error": {"code": status, "message": message}}, status_code=status)


def _typed_error(status: int, code: str, message: str, **extra) -> JSONResponse:
    """Return an error response where `code` is a machine-readable string.

    Used by Tier 2 agent endpoints (compile / preview) so AI agents can
    branch on the error class without string-matching the message.
    """
    body = {"error": {"code": code, "message": message, **extra}}
    return JSONResponse(body, status_code=status)


async def _body(request: Request) -> dict:
    raw = await request.body()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _body_sync(request: Request) -> dict:
    """Sync equivalent of ``_body`` for sync (``def``) handlers under E12.

    Relies on ``api_middleware`` pre-consuming the body before dispatch
    (``await request.body()`` on POST/PUT/PATCH), so the bytes are already
    cached on ``request._body``. Reading the cached attribute is a pure
    attribute access — no loop needed.
    """
    raw = getattr(request, "_body", b"") or b""
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def _raw_body_sync(request: Request) -> bytes:
    """Sync raw-body reader for handlers that parse non-JSON payloads (YAML)."""
    return getattr(request, "_body", b"") or b""


# ---------------------------------------------------------------------------
# Serializers
# ---------------------------------------------------------------------------


def _ser_connection(c):
    return {
        "id": c.id,
        "name": c.name,
        "connection_type": c.connection_type.value,
        "direction": c.direction.value,
        "created_at": c.created_at.isoformat() if c.created_at else None,
        "updated_at": c.updated_at.isoformat() if c.updated_at else None,
    }


def _ser_upload(u):
    return {
        "id": u.id,
        "name": u.name,
        "description": u.description,
        "source_connection_id": u.source_connection_id,
        "destination_connection_id": u.destination_connection_id,
        "dlt_config": u.dlt_config,
        "status": u.status.value,
        "created_at": u.created_at.isoformat() if u.created_at else None,
        "updated_at": u.updated_at.isoformat() if u.updated_at else None,
    }


def _ser_pipeline(p):
    return {
        "id": p.id,
        "name": p.name,
        "description": p.description,
        "destination_connection_id": p.destination_connection_id,
        "command": p.command.value,
        "full_refresh": p.full_refresh,
        "models": p.models,
        "custom_selector": p.custom_selector,
        "status": p.status.value,
        "created_at": p.created_at.isoformat() if p.created_at else None,
        "updated_at": p.updated_at.isoformat() if p.updated_at else None,
    }


def _ser_transformation(t):
    return {
        "id": t.id,
        "name": t.name,
        "description": t.description,
        "sql_body": t.sql_body,
        "materialization": t.materialization.value,
        "schema_name": t.schema_name,
        "tests_config": t.tests_config,
        "destination_connection_id": t.destination_connection_id,
        "tags": t.tags,
        "created_at": t.created_at.isoformat() if t.created_at else None,
        "updated_at": t.updated_at.isoformat() if t.updated_at else None,
    }


def _ser_schedule(s):
    return {
        "id": s.id,
        "target_type": s.target_type.value,
        "target_id": s.target_id,
        "cron_expression": s.cron_expression,
        "timezone": s.timezone,
        "is_active": s.is_active,
        "created_at": s.created_at.isoformat() if s.created_at else None,
        "updated_at": s.updated_at.isoformat() if s.updated_at else None,
    }


def _ser_run(r):
    return {
        "id": r.id,
        "target_type": r.target_type.value,
        "target_id": r.target_id,
        "status": r.status.value,
        "started_at": r.started_at.isoformat() if r.started_at else None,
        "finished_at": r.finished_at.isoformat() if r.finished_at else None,
        "rows_loaded": r.rows_loaded,
        "error_message": r.error_message,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }


def _ser_channel(ch):
    return {
        "id": ch.id,
        "name": ch.name,
        "channel_type": ch.channel_type.value,
        "config": ch.config,
        "events": ch.events,
        "is_active": ch.is_active,
        "created_at": ch.created_at.isoformat() if ch.created_at else None,
        "updated_at": ch.updated_at.isoformat() if ch.updated_at else None,
    }


def _ser_catalog_entry(e):
    return {
        "id": e.id,
        "entry_type": e.entry_type.value,
        "origin_type": e.origin_type.value,
        "origin_id": e.origin_id,
        "schema_name": e.schema_name,
        "table_name": e.table_name,
        "dataset_name": e.dataset_name,
        "connection_id": e.connection_id,
        "description": e.description,
        "columns": e.columns or [],
        "dbt_config": e.dbt_config or {},
        "created_at": e.created_at.isoformat() if e.created_at else None,
        "updated_at": e.updated_at.isoformat() if e.updated_at else None,
    }


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------


@api_endpoint(required_scope="connections:read")
def list_connections(request, api_key, session):
    items = _get_conn_svc().list_connections(session, api_key.org_id)
    return JSONResponse({"items": [_ser_connection(c) for c in items]})


@api_endpoint(required_scope="connections:read")
def get_connection(request, api_key, session):
    conn_id = int(request.path_params["id"])
    conn = _get_conn_svc().get_connection(session, api_key.org_id, conn_id)
    if conn is None:
        return _error(404, "Connection not found")
    return JSONResponse(_ser_connection(conn))


@api_endpoint(required_scope="connections:write")
def create_connection(request, api_key, session):
    data = _body_sync(request)
    name = data.get("name")
    ct = data.get("connection_type")
    config = data.get("config")
    if not name or not ct or not isinstance(config, dict):
        return _error(400, "name, connection_type, and config (object) are required")
    try:
        connection_type = ConnectionType(ct)
    except ValueError:
        return _error(400, f"Invalid connection_type: {ct}")
    try:
        conn = _get_conn_svc().create_connection(
            session, api_key.org_id, name, connection_type, config
        )
    except (ValueError, Exception) as exc:
        return _error(400, str(exc))
    return JSONResponse(_ser_connection(conn), status_code=201)


@api_endpoint(required_scope="connections:write")
def update_connection(request, api_key, session):
    conn_id = int(request.path_params["id"])
    data = _body_sync(request)
    kwargs = {}
    if "name" in data:
        kwargs["name"] = data["name"]
    if "connection_type" in data:
        try:
            kwargs["connection_type"] = ConnectionType(data["connection_type"])
        except ValueError:
            return _error(400, f"Invalid connection_type: {data['connection_type']}")
    if "config" in data:
        kwargs["config"] = data["config"]
    try:
        conn = _get_conn_svc().update_connection(session, api_key.org_id, conn_id, **kwargs)
    except ValueError as exc:
        return _error(400, str(exc))
    if conn is None:
        return _error(404, "Connection not found")
    return JSONResponse(_ser_connection(conn))


@api_endpoint(required_scope="connections:write")
def delete_connection(request, api_key, session):
    conn_id = int(request.path_params["id"])
    if not _get_conn_svc().delete_connection(session, api_key.org_id, conn_id):
        return _error(404, "Connection not found")
    return JSONResponse({"deleted": True})


@api_endpoint(required_scope="connections:write")
def test_connection(request, api_key, session):
    conn_id = int(request.path_params["id"])
    config = _get_conn_svc().get_connection_config(session, api_key.org_id, conn_id)
    if config is None:
        return _error(404, "Connection not found")
    conn = _get_conn_svc().get_connection(session, api_key.org_id, conn_id)
    ok, msg = ConnectionService.test_connection(config, conn.connection_type)
    return JSONResponse({"success": ok, "message": msg})


def _load_conn_and_config(session, api_key, conn_id):
    """Helper: load connection + decrypted config, or return error tuple."""
    config = _get_conn_svc().get_connection_config(session, api_key.org_id, conn_id)
    if config is None:
        return None, None, _error(404, "Connection not found")
    conn = _get_conn_svc().get_connection(session, api_key.org_id, conn_id)
    return conn, config, None


@api_endpoint(required_scope="connections:read")
def introspect_connection(request, api_key, session):
    """List schemas/tables (and optionally columns) of a source connection."""
    conn_id = int(request.path_params["id"])
    conn, config, err = _load_conn_and_config(session, api_key, conn_id)
    if err is not None:
        return err
    data = _body_sync(request)
    schema = data.get("schema")
    try:
        tables = ConnectionService.list_tables(config, conn.connection_type, schema=schema)
    except ValueError as exc:
        return _error(400, str(exc))
    except Exception as exc:
        logger.exception("Introspection failed for connection %s", conn_id)
        return _error(500, f"Introspection failed: {exc}")
    return JSONResponse({"tables": tables})


@api_endpoint(required_scope="connections:read")
def list_connection_columns(request, api_key, session):
    """List columns of a single table."""
    conn_id = int(request.path_params["id"])
    conn, config, err = _load_conn_and_config(session, api_key, conn_id)
    if err is not None:
        return err
    data = _body_sync(request)
    table = data.get("table")
    if not table:
        return _error(400, "table is required")
    schema = data.get("schema")
    try:
        columns = ConnectionService.list_columns(
            config, conn.connection_type, table=table, schema=schema
        )
    except ValueError as exc:
        return _error(400, str(exc))
    except Exception as exc:
        logger.exception("Column listing failed for connection %s", conn_id)
        return _error(500, f"Column listing failed: {exc}")
    return JSONResponse({"columns": columns})


@api_endpoint(required_scope="connections:read")
def preview_connection(request, api_key, session):
    """Return the first N rows of a table."""
    conn_id = int(request.path_params["id"])
    conn, config, err = _load_conn_and_config(session, api_key, conn_id)
    if err is not None:
        return err
    data = _body_sync(request)
    table = data.get("table")
    if not table:
        return _error(400, "table is required")
    schema = data.get("schema")
    limit = data.get("limit", 100)
    try:
        cols, rows = ConnectionService.preview_table(
            config, conn.connection_type, table=table, schema=schema, limit=limit
        )
    except ValueError as exc:
        return _error(400, str(exc))
    except Exception as exc:
        logger.exception("Preview failed for connection %s", conn_id)
        return _error(500, f"Preview failed: {exc}")
    return JSONResponse({"columns": cols, "rows": rows})


@api_endpoint(required_scope="connections:read")
def query_connection(request, api_key, session):
    """Execute a read-only SQL query against a SQL source connection."""
    conn_id = int(request.path_params["id"])
    conn, config, err = _load_conn_and_config(session, api_key, conn_id)
    if err is not None:
        return err
    data = _body_sync(request)
    query = data.get("query")
    if not query:
        return _error(400, "query is required")
    if not ConnectionService.is_select_only(query):
        return _error(400, "Only single SELECT statements are allowed")
    try:
        cols, rows = ConnectionService.execute_query(config, conn.connection_type, query)
    except ValueError as exc:
        return _error(400, str(exc))
    except Exception as exc:
        logger.exception("Query failed for connection %s", conn_id)
        return _error(500, f"Query failed: {exc}")
    return JSONResponse({"columns": cols, "rows": rows})


# ---------------------------------------------------------------------------
# Uploads
# ---------------------------------------------------------------------------


@api_endpoint(required_scope="uploads:read")
def list_uploads(request, api_key, session):
    items = _get_upload_svc().list_uploads(session, api_key.org_id)
    return JSONResponse({"items": [_ser_upload(u) for u in items]})


@api_endpoint(required_scope="uploads:read")
def get_upload(request, api_key, session):
    upload_id = int(request.path_params["id"])
    upload = _get_upload_svc().get_upload(session, api_key.org_id, upload_id)
    if upload is None:
        return _error(404, "Upload not found")
    return JSONResponse(_ser_upload(upload))


@api_endpoint(required_scope="uploads:write")
def create_upload(request, api_key, session):
    data = _body_sync(request)
    required = ("name", "source_connection_id", "destination_connection_id")
    if not all(data.get(k) for k in required):
        return _error(400, f"{', '.join(required)} are required")
    try:
        upload = _get_upload_svc().create_upload(
            session,
            api_key.org_id,
            name=data["name"],
            description=data.get("description"),
            source_connection_id=int(data["source_connection_id"]),
            destination_connection_id=int(data["destination_connection_id"]),
            dlt_config=data.get("dlt_config", {}),
        )
    except (ValueError, Exception) as exc:
        return _error(400, str(exc))
    return JSONResponse(_ser_upload(upload), status_code=201)


@api_endpoint(required_scope="uploads:write")
def update_upload(request, api_key, session):
    upload_id = int(request.path_params["id"])
    data = _body_sync(request)
    kwargs = {}
    for key in ("name", "description", "dlt_config"):
        if key in data:
            kwargs[key] = data[key]
    try:
        upload = _get_upload_svc().update_upload(session, api_key.org_id, upload_id, **kwargs)
    except ValueError as exc:
        return _error(400, str(exc))
    if upload is None:
        return _error(404, "Upload not found")
    return JSONResponse(_ser_upload(upload))


@api_endpoint(required_scope="uploads:write")
def delete_upload(request, api_key, session):
    upload_id = int(request.path_params["id"])
    if not _get_upload_svc().delete_upload(session, api_key.org_id, upload_id):
        return _error(404, "Upload not found")
    return JSONResponse({"deleted": True})


async def _trigger_and_maybe_wait(request, api_key, run):
    """Shared helper: fire-and-forget (202) or wait for completion (200/408).

    If ``?wait=true`` is set, polls the run until it reaches a terminal
    status or the timeout expires. Default timeout 120s, max 300s.
    """
    wait = request.query_params.get("wait", "").lower() in ("true", "1")
    if not wait:
        return JSONResponse({"run_id": run.id, "status": "pending"}, status_code=202)

    from datanika.services.run_waiter import DEFAULT_TIMEOUT, wait_for_run

    timeout_str = request.query_params.get("timeout", str(DEFAULT_TIMEOUT))
    try:
        timeout = int(timeout_str)
    except (TypeError, ValueError):
        timeout = DEFAULT_TIMEOUT

    final_run = await wait_for_run(run.id, api_key.org_id, timeout=timeout)
    if final_run is None:
        return _error(404, "Run not found after dispatch")

    result = _ser_run(final_run)
    if final_run.status.value in ("pending", "running"):
        result["timed_out"] = True
        return JSONResponse(result, status_code=408)
    return JSONResponse(result)


@api_endpoint(required_scope="uploads:write")
async def trigger_upload(request, api_key, session):
    upload_id = int(request.path_params["id"])
    upload = _get_upload_svc().get_upload(session, api_key.org_id, upload_id)
    if upload is None:
        return _error(404, "Upload not found")
    run = _exec_svc.create_run(session, api_key.org_id, NodeType.UPLOAD, upload_id)
    session.commit()
    from datanika.tasks.upload_tasks import run_upload_task

    run_upload_task.delay(run_id=run.id, org_id=api_key.org_id)
    return await _trigger_and_maybe_wait(request, api_key, run)


# ---------------------------------------------------------------------------
# Pipelines
# ---------------------------------------------------------------------------


@api_endpoint(required_scope="pipelines:read")
def list_pipelines(request, api_key, session):
    items = _pipeline_svc.list_pipelines(session, api_key.org_id)
    return JSONResponse({"items": [_ser_pipeline(p) for p in items]})


@api_endpoint(required_scope="pipelines:read")
def get_pipeline(request, api_key, session):
    pipeline_id = int(request.path_params["id"])
    pipeline = _pipeline_svc.get_pipeline(session, api_key.org_id, pipeline_id)
    if pipeline is None:
        return _error(404, "Pipeline not found")
    return JSONResponse(_ser_pipeline(pipeline))


@api_endpoint(required_scope="pipelines:write")
def create_pipeline(request, api_key, session):
    data = _body_sync(request)
    if not data.get("name") or not data.get("destination_connection_id"):
        return _error(400, "name and destination_connection_id are required")
    command = DbtCommand.RUN
    if "command" in data:
        try:
            command = DbtCommand(data["command"])
        except ValueError:
            return _error(400, f"Invalid command: {data['command']}")
    try:
        pipeline = _pipeline_svc.create_pipeline(
            session,
            api_key.org_id,
            name=data["name"],
            description=data.get("description"),
            destination_connection_id=int(data["destination_connection_id"]),
            command=command,
            full_refresh=data.get("full_refresh", False),
            models=data.get("models"),
            custom_selector=data.get("custom_selector"),
        )
    except (ValueError, Exception) as exc:
        return _error(400, str(exc))
    return JSONResponse(_ser_pipeline(pipeline), status_code=201)


@api_endpoint(required_scope="pipelines:write")
def update_pipeline(request, api_key, session):
    pipeline_id = int(request.path_params["id"])
    data = _body_sync(request)
    kwargs = {}
    for key in ("name", "description", "full_refresh", "models", "custom_selector"):
        if key in data:
            kwargs[key] = data[key]
    if "destination_connection_id" in data:
        kwargs["destination_connection_id"] = int(data["destination_connection_id"])
    if "command" in data:
        try:
            kwargs["command"] = DbtCommand(data["command"])
        except ValueError:
            return _error(400, f"Invalid command: {data['command']}")
    try:
        pipeline = _pipeline_svc.update_pipeline(session, api_key.org_id, pipeline_id, **kwargs)
    except ValueError as exc:
        return _error(400, str(exc))
    if pipeline is None:
        return _error(404, "Pipeline not found")
    return JSONResponse(_ser_pipeline(pipeline))


@api_endpoint(required_scope="pipelines:write")
def delete_pipeline(request, api_key, session):
    pipeline_id = int(request.path_params["id"])
    if not _pipeline_svc.delete_pipeline(session, api_key.org_id, pipeline_id):
        return _error(404, "Pipeline not found")
    return JSONResponse({"deleted": True})


@api_endpoint(required_scope="pipelines:write")
async def trigger_pipeline(request, api_key, session):
    pipeline_id = int(request.path_params["id"])
    pipeline = _pipeline_svc.get_pipeline(session, api_key.org_id, pipeline_id)
    if pipeline is None:
        return _error(404, "Pipeline not found")
    run = _exec_svc.create_run(session, api_key.org_id, NodeType.PIPELINE, pipeline_id)
    session.commit()
    from datanika.tasks.pipeline_tasks import run_pipeline_task

    run_pipeline_task.delay(run_id=run.id, org_id=api_key.org_id)
    return await _trigger_and_maybe_wait(request, api_key, run)


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------


@api_endpoint(required_scope="transformations:read")
def list_transformations(request, api_key, session):
    items = _transform_svc.list_transformations(session, api_key.org_id)
    return JSONResponse({"items": [_ser_transformation(t) for t in items]})


@api_endpoint(required_scope="transformations:read")
def get_transformation(request, api_key, session):
    tid = int(request.path_params["id"])
    t = _transform_svc.get_transformation(session, api_key.org_id, tid)
    if t is None:
        return _error(404, "Transformation not found")
    return JSONResponse(_ser_transformation(t))


@api_endpoint(required_scope="transformations:write")
def create_transformation(request, api_key, session):
    data = _body_sync(request)
    if not data.get("name") or not data.get("sql_body"):
        return _error(400, "name and sql_body are required")
    mat = Materialization.VIEW
    if "materialization" in data:
        try:
            mat = Materialization(data["materialization"])
        except ValueError:
            return _error(400, f"Invalid materialization: {data['materialization']}")
    try:
        t = _transform_svc.create_transformation(
            session,
            api_key.org_id,
            name=data["name"],
            sql_body=data["sql_body"],
            materialization=mat,
            description=data.get("description"),
            schema_name=data.get("schema_name", "staging"),
            tests_config=data.get("tests_config"),
            destination_connection_id=(
                int(data["destination_connection_id"])
                if data.get("destination_connection_id")
                else None
            ),
            tags=data.get("tags"),
            incremental_config=data.get("incremental_config"),
        )
    except (ValueError, Exception) as exc:
        return _error(400, str(exc))
    return JSONResponse(_ser_transformation(t), status_code=201)


@api_endpoint(required_scope="transformations:write")
def update_transformation(request, api_key, session):
    tid = int(request.path_params["id"])
    data = _body_sync(request)
    kwargs = {}
    for key in (
        "name",
        "description",
        "sql_body",
        "schema_name",
        "tests_config",
        "tags",
        "incremental_config",
    ):
        if key in data:
            kwargs[key] = data[key]
    if "materialization" in data:
        try:
            kwargs["materialization"] = Materialization(data["materialization"])
        except ValueError:
            return _error(400, f"Invalid materialization: {data['materialization']}")
    if "destination_connection_id" in data:
        kwargs["destination_connection_id"] = (
            int(data["destination_connection_id"]) if data["destination_connection_id"] else None
        )
    try:
        t = _transform_svc.update_transformation(session, api_key.org_id, tid, **kwargs)
    except ValueError as exc:
        return _error(400, str(exc))
    if t is None:
        return _error(404, "Transformation not found")
    return JSONResponse(_ser_transformation(t))


@api_endpoint(required_scope="transformations:write")
def delete_transformation(request, api_key, session):
    tid = int(request.path_params["id"])
    if not _transform_svc.delete_transformation(session, api_key.org_id, tid):
        return _error(404, "Transformation not found")
    return JSONResponse({"deleted": True})


@api_endpoint(required_scope="transformations:write")
async def trigger_transformation(request, api_key, session):
    tid = int(request.path_params["id"])
    t = _transform_svc.get_transformation(session, api_key.org_id, tid)
    if t is None:
        return _error(404, "Transformation not found")
    run = _exec_svc.create_run(session, api_key.org_id, NodeType.TRANSFORMATION, tid)
    session.commit()
    from datanika.tasks.transformation_tasks import run_transformation_task

    run_transformation_task.delay(run_id=run.id, org_id=api_key.org_id)
    return await _trigger_and_maybe_wait(request, api_key, run)


@api_endpoint(required_scope="transformations:read")
def compile_transformation_endpoint(request, api_key, session):
    """POST /api/v1/transformations/{id}/compile — dbt compile only.

    Agent uses this to validate Jinja + ref/source resolution before
    triggering a full run. No warehouse execution, no side effects
    beyond writing the tenant model file to disk.
    """
    from datanika.services.transformation_compile import (
        TransformationNotFoundError,
        compile_transformation,
    )

    tid = int(request.path_params["id"])
    try:
        result = compile_transformation(session, api_key.org_id, tid)
    except TransformationNotFoundError:
        return _error(404, "Transformation not found")

    if not result.success:
        payload: dict = {}
        if result.line is not None:
            payload["line"] = result.line
        if result.column is not None:
            payload["column"] = result.column
        return _typed_error(
            400,
            "compilation_error",
            result.error_message or "Compilation failed",
            **payload,
        )

    return JSONResponse(
        {
            "compiled_sql": result.compiled_sql,
            "node": result.node,
        }
    )


@api_endpoint(required_scope="transformations:read")
def preview_transformation_endpoint(request, api_key, session):
    """POST /api/v1/transformations/{id}/preview — compile + execute.

    Body: ``{"limit"?: int}`` (default 100, max 1000).

    Returns first N rows from the compiled SQL wrapped in a
    ``SELECT * FROM (...) LIMIT N`` subquery (defense-in-depth: the
    read-only SQL guard re-validates even after dbt compile).
    """
    from datanika.services.transformation_compile import (
        DEFAULT_PREVIEW_LIMIT,
        MissingDestinationError,
        TransformationNotFoundError,
        preview_transformation,
    )

    tid = int(request.path_params["id"])
    data = _body_sync(request)
    limit = data.get("limit", DEFAULT_PREVIEW_LIMIT)
    if not isinstance(limit, int):
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            return _typed_error(400, "invalid_request", "limit must be an integer")

    try:
        result = preview_transformation(session, api_key.org_id, tid, limit=limit)
    except TransformationNotFoundError:
        return _error(404, "Transformation not found")
    except MissingDestinationError as exc:
        return _typed_error(400, "missing_destination", str(exc))

    if not result.success:
        return _typed_error(
            400,
            result.error_code or "preview_failed",
            result.error_message or "Preview failed",
        )

    return JSONResponse(
        {
            "columns": result.columns,
            "rows": result.rows,
            "row_count": result.row_count,
            "truncated": result.truncated,
        }
    )


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------


@api_endpoint(required_scope="schedules:read")
def list_schedules(request, api_key, session):
    items = _get_schedule_svc().list_schedules(session, api_key.org_id)
    return JSONResponse({"items": [_ser_schedule(s) for s in items]})


@api_endpoint(required_scope="schedules:read")
def get_schedule(request, api_key, session):
    sid = int(request.path_params["id"])
    s = _get_schedule_svc().get_schedule(session, api_key.org_id, sid)
    if s is None:
        return _error(404, "Schedule not found")
    return JSONResponse(_ser_schedule(s))


@api_endpoint(required_scope="schedules:write")
def create_schedule(request, api_key, session):
    data = _body_sync(request)
    if not data.get("target_type") or not data.get("target_id") or not data.get("cron_expression"):
        return _error(400, "target_type, target_id, and cron_expression are required")
    try:
        target_type = NodeType(data["target_type"])
    except ValueError:
        return _error(400, f"Invalid target_type: {data['target_type']}")
    try:
        s = _get_schedule_svc().create_schedule(
            session,
            api_key.org_id,
            target_type=target_type,
            target_id=int(data["target_id"]),
            cron_expression=data["cron_expression"],
            timezone=data.get("timezone", "UTC"),
            is_active=data.get("is_active", True),
        )
    except (ValueError, Exception) as exc:
        return _error(400, str(exc))
    return JSONResponse(_ser_schedule(s), status_code=201)


@api_endpoint(required_scope="schedules:write")
def update_schedule(request, api_key, session):
    sid = int(request.path_params["id"])
    data = _body_sync(request)
    kwargs = {}
    for key in ("cron_expression", "timezone", "is_active"):
        if key in data:
            kwargs[key] = data[key]
    try:
        s = _get_schedule_svc().update_schedule(session, api_key.org_id, sid, **kwargs)
    except ValueError as exc:
        return _error(400, str(exc))
    if s is None:
        return _error(404, "Schedule not found")
    return JSONResponse(_ser_schedule(s))


@api_endpoint(required_scope="schedules:write")
def delete_schedule(request, api_key, session):
    sid = int(request.path_params["id"])
    if not _get_schedule_svc().delete_schedule(session, api_key.org_id, sid):
        return _error(404, "Schedule not found")
    return JSONResponse({"deleted": True})


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


@api_endpoint(required_scope="runs:read")
def list_runs(request, api_key, session):
    target_type = request.query_params.get("target_type")
    target_id = request.query_params.get("target_id")
    status = request.query_params.get("status")
    limit = request.query_params.get("limit", "50")

    kwargs = {}
    if target_type:
        try:
            kwargs["target_type"] = NodeType(target_type)
        except ValueError:
            return _error(400, f"Invalid target_type: {target_type}")
    if target_id:
        kwargs["target_id"] = int(target_id)
    if status:
        try:
            kwargs["status"] = RunStatus(status)
        except ValueError:
            return _error(400, f"Invalid status: {status}")
    kwargs["limit"] = min(int(limit), 200)

    items = _exec_svc.list_runs(session, api_key.org_id, **kwargs)
    return JSONResponse({"items": [_ser_run(r) for r in items]})


@api_endpoint(required_scope="runs:read")
def get_run(request, api_key, session):
    run_id = int(request.path_params["id"])
    run = _exec_svc.get_run(session, api_key.org_id, run_id)
    if run is None:
        return _error(404, "Run not found")
    return JSONResponse(_ser_run(run))


@api_endpoint(required_scope="runs:read")
def get_run_logs(request, api_key, session):
    run_id = int(request.path_params["id"])
    run = _exec_svc.get_run(session, api_key.org_id, run_id)
    if run is None:
        return _error(404, "Run not found")
    return JSONResponse({"run_id": run.id, "logs": run.logs or ""})


@api_endpoint(required_scope="runs:write")
def cancel_run(request, api_key, session):
    """POST /api/v1/runs/{id}/cancel — cancel a pending or running run."""
    run_id = int(request.path_params["id"])
    run = _exec_svc.get_run(session, api_key.org_id, run_id)
    if run is None:
        return _error(404, "Run not found")
    if run.status not in (RunStatus.PENDING, RunStatus.RUNNING):
        return _typed_error(
            409,
            "not_cancellable",
            f"Run is already {run.status.value} and cannot be cancelled",
        )
    cancelled = _exec_svc.cancel_run(session, run_id)
    if cancelled is None:
        return _error(500, "Failed to cancel run")
    return JSONResponse(_ser_run(cancelled))


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------


@api_endpoint(required_scope="catalog:read")
def list_catalog_entries(request, api_key, session):
    """List catalog entries for the org. Optional filters: entry_type,
    schema, connection_id."""
    entry_type_str = request.query_params.get("entry_type")
    schema_filter = request.query_params.get("schema")
    connection_id_str = request.query_params.get("connection_id")

    entry_type = None
    if entry_type_str:
        try:
            entry_type = CatalogEntryType(entry_type_str)
        except ValueError:
            return _error(400, f"Invalid entry_type: {entry_type_str}")

    items = CatalogService.list_entries(session, api_key.org_id, entry_type=entry_type)

    if schema_filter:
        items = [e for e in items if e.schema_name == schema_filter]
    if connection_id_str:
        try:
            cid = int(connection_id_str)
        except ValueError:
            return _error(400, "connection_id must be an integer")
        items = [e for e in items if e.connection_id == cid]

    return JSONResponse({"items": [_ser_catalog_entry(e) for e in items]})


@api_endpoint(required_scope="catalog:read")
def get_catalog_entry(request, api_key, session):
    entry_id = int(request.path_params["id"])
    entry = CatalogService.get_entry(session, api_key.org_id, entry_id)
    if entry is None:
        return _error(404, "Catalog entry not found")
    return JSONResponse(_ser_catalog_entry(entry))


# ---------------------------------------------------------------------------
# In-App Notifications
# ---------------------------------------------------------------------------


def _ser_notification(n):
    return {
        "id": n.id,
        "type": n.type.value,
        "title": n.title,
        "message": n.message,
        "resource_type": n.resource_type,
        "resource_id": n.resource_id,
        "read_at": n.read_at.isoformat() if n.read_at else None,
        "created_at": n.created_at.isoformat() if n.created_at else None,
    }


@api_endpoint(required_scope="notifications:read")
def list_in_app_notifications(request, api_key, session):
    from datanika.services.in_app_notification_service import InAppNotificationService

    unread_only = request.query_params.get("unread_only", "").lower() in ("true", "1")
    limit = min(int(request.query_params.get("limit", "20")), 100)
    offset = int(request.query_params.get("offset", "0"))

    svc = InAppNotificationService()
    items = svc.list_for_user(
        session,
        api_key.org_id,
        api_key.user_id,
        unread_only=unread_only,
        limit=limit,
        offset=offset,
    )
    total = svc.total_count(
        session,
        api_key.org_id,
        api_key.user_id,
        unread_only=unread_only,
    )
    unread = svc.unread_count(session, api_key.org_id, api_key.user_id)
    return JSONResponse(
        {
            "items": [_ser_notification(n) for n in items],
            "total": total,
            "unread_count": unread,
        }
    )


@api_endpoint(required_scope="notifications:read")
def get_unread_count(request, api_key, session):
    from datanika.services.in_app_notification_service import InAppNotificationService

    count = InAppNotificationService.unread_count(
        session,
        api_key.org_id,
        api_key.user_id,
    )
    return JSONResponse({"count": count})


@api_endpoint(required_scope="notifications:write")
def mark_notification_read(request, api_key, session):
    from datanika.services.in_app_notification_service import InAppNotificationService

    nid = int(request.path_params["id"])
    notif = InAppNotificationService.mark_read(session, nid, api_key.org_id)
    if notif is None:
        return _error(404, "Notification not found")
    return JSONResponse(_ser_notification(notif))


@api_endpoint(required_scope="notifications:write")
def mark_all_notifications_read(request, api_key, session):
    from datanika.services.in_app_notification_service import InAppNotificationService

    count = InAppNotificationService.mark_all_read(
        session,
        api_key.org_id,
        api_key.user_id,
    )
    return JSONResponse({"marked": count})


@api_endpoint(required_scope="notifications:write")
def dismiss_notification(request, api_key, session):
    from datanika.services.in_app_notification_service import InAppNotificationService

    nid = int(request.path_params["id"])
    if not InAppNotificationService.dismiss(session, nid, api_key.org_id):
        return _error(404, "Notification not found")
    return JSONResponse({"deleted": True})


# ---------------------------------------------------------------------------
# Notification Channels
# ---------------------------------------------------------------------------


@api_endpoint(required_scope="notifications:read")
def list_notification_channels(request, api_key, session):
    items = _notif_svc.list_channels(session, api_key.org_id)
    return JSONResponse({"items": [_ser_channel(ch) for ch in items]})


@api_endpoint(required_scope="notifications:read")
def get_notification_channel(request, api_key, session):
    cid = int(request.path_params["id"])
    ch = _notif_svc._get_channel(session, cid, api_key.org_id)
    if ch is None:
        return _error(404, "Notification channel not found")
    return JSONResponse(_ser_channel(ch))


@api_endpoint(required_scope="notifications:write")
def create_notification_channel(request, api_key, session):
    data = _body_sync(request)
    if not data.get("name") or not data.get("channel_type"):
        return _error(400, "name and channel_type are required")
    try:
        ct = ChannelType(data["channel_type"])
    except ValueError:
        return _error(400, f"Invalid channel_type: {data['channel_type']}")
    try:
        ch = _notif_svc.create_channel(
            session,
            api_key.org_id,
            name=data["name"],
            channel_type=ct,
            config=data.get("config", {}),
            events=data.get("events", []),
        )
    except ValueError as exc:
        return _error(400, str(exc))
    return JSONResponse(_ser_channel(ch), status_code=201)


@api_endpoint(required_scope="notifications:write")
def update_notification_channel(request, api_key, session):
    cid = int(request.path_params["id"])
    data = _body_sync(request)
    kwargs = {}
    for key in ("name", "config", "events", "is_active"):
        if key in data:
            kwargs[key] = data[key]
    try:
        ch = _notif_svc.update_channel(session, cid, api_key.org_id, **kwargs)
    except ValueError as exc:
        return _error(400, str(exc))
    if ch is None:
        return _error(404, "Notification channel not found")
    return JSONResponse(_ser_channel(ch))


@api_endpoint(required_scope="notifications:write")
def delete_notification_channel(request, api_key, session):
    cid = int(request.path_params["id"])
    if not _notif_svc.delete_channel(session, cid, api_key.org_id):
        return _error(404, "Notification channel not found")
    return JSONResponse({"deleted": True})


# ---------------------------------------------------------------------------
# Bulk import — POST /api/v1/import (#131)
# ---------------------------------------------------------------------------


def _validate_import_payload(data: dict, existing_conn_names: dict[str, int]) -> list[dict]:
    """Validate the entire import payload and return a list of error dicts.

    ``existing_conn_names`` maps connection name → id for connections
    already present in the org.
    """
    errors: list[dict] = []

    # --- connections ---
    conn_names_in_payload: set[str] = set()
    for i, c in enumerate(data.get("connections", [])):
        name = c.get("name")
        if not name or not str(name).strip():
            errors.append(
                {"code": "MISSING_FIELD", "message": f"connections[{i}]: name is required"}
            )
            continue
        ct = c.get("connection_type")
        if not ct:
            errors.append(
                {
                    "code": "MISSING_FIELD",
                    "message": f"connections[{i}]: connection_type is required",
                }
            )
        else:
            try:
                ConnectionType(ct)
            except ValueError:
                errors.append(
                    {
                        "code": "INVALID_CONNECTION_TYPE",
                        "message": f"connections[{i}]: invalid connection_type '{ct}'",
                    }
                )
        if not isinstance(c.get("config"), dict):
            errors.append(
                {
                    "code": "MISSING_FIELD",
                    "message": f"connections[{i}]: config (object) is required",
                }
            )
        if name in conn_names_in_payload:
            errors.append(
                {"code": "DUPLICATE_NAME", "message": f"connections[{i}]: duplicate name '{name}'"}
            )
        else:
            conn_names_in_payload.add(name)

    # Build a combined name set for cross-reference checks
    all_conn_names = set(existing_conn_names.keys()) | conn_names_in_payload

    # --- uploads ---
    upload_names: set[str] = set()
    for i, u in enumerate(data.get("uploads", [])):
        name = u.get("name")
        if not name or not str(name).strip():
            errors.append({"code": "MISSING_FIELD", "message": f"uploads[{i}]: name is required"})
        elif name in upload_names:
            errors.append(
                {"code": "DUPLICATE_NAME", "message": f"uploads[{i}]: duplicate name '{name}'"}
            )
        else:
            upload_names.add(name)
        for ref_field in ("source_connection_name", "destination_connection_name"):
            ref = u.get(ref_field)
            if not ref:
                errors.append(
                    {"code": "MISSING_FIELD", "message": f"uploads[{i}]: {ref_field} is required"}
                )
            elif ref not in all_conn_names:
                errors.append(
                    {
                        "code": "UNKNOWN_CONNECTION_REF",
                        "message": f"uploads[{i}]: {ref_field} '{ref}' not found",
                    }
                )

    # --- pipelines ---
    pipeline_names: set[str] = set()
    for i, p in enumerate(data.get("pipelines", [])):
        name = p.get("name")
        if not name or not str(name).strip():
            errors.append({"code": "MISSING_FIELD", "message": f"pipelines[{i}]: name is required"})
        elif name in pipeline_names:
            errors.append(
                {"code": "DUPLICATE_NAME", "message": f"pipelines[{i}]: duplicate name '{name}'"}
            )
        else:
            pipeline_names.add(name)
        ref = p.get("destination_connection_name")
        if not ref:
            errors.append(
                {
                    "code": "MISSING_FIELD",
                    "message": f"pipelines[{i}]: destination_connection_name is required",
                }
            )
        elif ref not in all_conn_names:
            errors.append(
                {
                    "code": "UNKNOWN_CONNECTION_REF",
                    "message": f"pipelines[{i}]: destination_connection_name '{ref}' not found",
                }
            )
        cmd = p.get("command")
        if cmd:
            try:
                DbtCommand(cmd)
            except ValueError:
                errors.append(
                    {
                        "code": "INVALID_ENUM_VALUE",
                        "message": f"pipelines[{i}]: invalid command '{cmd}'",
                    }
                )

    # --- transformations ---
    transform_names: set[str] = set()
    for i, t in enumerate(data.get("transformations", [])):
        name = t.get("name")
        if not name or not str(name).strip():
            errors.append(
                {"code": "MISSING_FIELD", "message": f"transformations[{i}]: name is required"}
            )
        elif name in transform_names:
            errors.append(
                {
                    "code": "DUPLICATE_NAME",
                    "message": f"transformations[{i}]: duplicate name '{name}'",
                }
            )
        else:
            transform_names.add(name)
        if not t.get("sql_body"):
            errors.append(
                {"code": "MISSING_FIELD", "message": f"transformations[{i}]: sql_body is required"}
            )
        mat = t.get("materialization")
        if mat:
            try:
                Materialization(mat)
            except ValueError:
                errors.append(
                    {
                        "code": "INVALID_ENUM_VALUE",
                        "message": f"transformations[{i}]: invalid materialization '{mat}'",
                    }
                )

    return errors


def _execute_validated_import(session, org_id: int, data: dict) -> dict[str, list[int]]:
    """Phase-1..4 creation shared by JSON and YAML import endpoints.

    Caller must have already validated ``data`` via
    ``_validate_import_payload`` — this function assumes the payload is
    shape-correct and proceeds atomically (SQLAlchemy session manages the
    transaction). Returns a ``{"connections": [ids], ...}`` dict.
    """
    existing_conns = _get_conn_svc().list_connections(session, org_id)
    existing_conn_names: dict[str, int] = {c.name: c.id for c in existing_conns}

    created: dict[str, list[int]] = {
        "connections": [],
        "uploads": [],
        "pipelines": [],
        "transformations": [],
    }

    # --- Phase 1: create connections ---
    conn_name_to_id: dict[str, int] = dict(existing_conn_names)
    for c in data.get("connections", []):
        conn = _get_conn_svc().create_connection(
            session,
            org_id,
            name=c["name"],
            connection_type=ConnectionType(c["connection_type"]),
            config=c["config"],
        )
        session.flush()
        conn_name_to_id[c["name"]] = conn.id
        created["connections"].append(conn.id)

    # --- Phase 2: create uploads ---
    for u in data.get("uploads", []):
        upload = _get_upload_svc().create_upload(
            session,
            org_id,
            name=u["name"],
            description=u.get("description"),
            source_connection_id=conn_name_to_id[u["source_connection_name"]],
            destination_connection_id=conn_name_to_id[u["destination_connection_name"]],
            dlt_config=u.get("dlt_config", {}),
        )
        session.flush()
        created["uploads"].append(upload.id)

    # --- Phase 3: create pipelines ---
    for p in data.get("pipelines", []):
        command = DbtCommand.RUN
        if p.get("command"):
            command = DbtCommand(p["command"])
        pipeline = _pipeline_svc.create_pipeline(
            session,
            org_id,
            name=p["name"],
            description=p.get("description"),
            destination_connection_id=conn_name_to_id[p["destination_connection_name"]],
            command=command,
            full_refresh=p.get("full_refresh", False),
            models=p.get("models"),
            custom_selector=p.get("custom_selector"),
        )
        session.flush()
        created["pipelines"].append(pipeline.id)

    # --- Phase 4: create transformations ---
    for t in data.get("transformations", []):
        mat = Materialization.VIEW
        if t.get("materialization"):
            mat = Materialization(t["materialization"])
        dest_conn_id = None
        if t.get("destination_connection_name"):
            dest_conn_id = conn_name_to_id.get(t["destination_connection_name"])
        transformation = _transform_svc.create_transformation(
            session,
            org_id,
            name=t["name"],
            sql_body=t["sql_body"],
            materialization=mat,
            description=t.get("description"),
            schema_name=t.get("schema_name", "staging"),
            tests_config=t.get("tests_config"),
            destination_connection_id=dest_conn_id,
            tags=t.get("tags"),
            incremental_config=t.get("incremental_config"),
        )
        session.flush()
        created["transformations"].append(transformation.id)

    return created


@api_endpoint(required_scope=None)
def bulk_import(request, api_key, session):
    """POST /api/v1/import — create connections, uploads, pipelines,
    and transformations from a single JSON payload.

    Validates the entire payload first; if any errors are found, nothing
    is created (atomic). Uses the same JSON v2 format as AI_IMPORT_GUIDE.md.
    """
    data = _body_sync(request)

    # --- version check ---
    version = data.get("version")
    if version != 2:
        return _error(400, "version 2 is required")

    existing_conns = _get_conn_svc().list_connections(session, api_key.org_id)
    existing_conn_names: dict[str, int] = {c.name: c.id for c in existing_conns}

    errors = _validate_import_payload(data, existing_conn_names)
    if errors:
        return JSONResponse({"errors": errors}, status_code=400)

    created = _execute_validated_import(session, api_key.org_id, data)
    return JSONResponse({"created": created}, status_code=201)


@api_endpoint(required_scope=None)
def bulk_import_yaml(request, api_key, session):
    """POST /api/v1/pipelines/yaml — YAML-formatted alternative to /api/v1/import.

    Accepts the same ``version: 2`` schema as the JSON endpoint, just
    serialized as YAML (more hand-editable, CLI-friendly). Per E8 /
    SPEC §"ELT Optimizations": power-user entry for declarative
    pipeline configs.

    YAML parse errors return 400 with ``{"error": "YAML parse error: ..."}``.
    Schema validation errors return 400 with the same
    ``{"errors": [...]}`` shape as JSON import.

    Payload example:
      version: 2
      connections:
        - name: source_pg
          connection_type: postgres
          config: {host: localhost, port: 5432, user: ..., password: ..., database: ...}
      uploads:
        - name: orders_sync
          source_connection_name: source_pg
          destination_connection_name: dest_bq
          dlt_config:
            mode: single_table
            table: orders
            chunk_size: 10000
            backend: pyarrow
            filters:
              - {op: gt, column: created_at, value: "2024-01-01"}
            incremental:
              cursor_path: updated_at
              initial_value: "2024-01-01"
    """
    from datanika.services._yaml_import import YamlImportError, parse_yaml_import

    raw = _raw_body_sync(request)

    try:
        data = parse_yaml_import(raw)
    except YamlImportError as exc:
        return _error(400, str(exc))

    if data.get("version") != 2:
        return _error(400, "version 2 is required")

    existing_conns = _get_conn_svc().list_connections(session, api_key.org_id)
    existing_conn_names: dict[str, int] = {c.name: c.id for c in existing_conns}

    errors = _validate_import_payload(data, existing_conn_names)
    if errors:
        return JSONResponse({"errors": errors}, status_code=400)

    created = _execute_validated_import(session, api_key.org_id, data)
    return JSONResponse({"created": created}, status_code=201)


# ---------------------------------------------------------------------------
# Route table
# ---------------------------------------------------------------------------

api_v1_routes = [
    # Connections
    Route("/api/v1/connections", list_connections, methods=["GET"]),
    Route("/api/v1/connections/{id:int}", get_connection, methods=["GET"]),
    Route("/api/v1/connections", create_connection, methods=["POST"]),
    Route("/api/v1/connections/{id:int}", update_connection, methods=["PUT"]),
    Route("/api/v1/connections/{id:int}", delete_connection, methods=["DELETE"]),
    Route("/api/v1/connections/{id:int}/test", test_connection, methods=["POST"]),
    Route(
        "/api/v1/connections/{id:int}/introspect",
        introspect_connection,
        methods=["POST"],
    ),
    Route(
        "/api/v1/connections/{id:int}/columns",
        list_connection_columns,
        methods=["POST"],
    ),
    Route(
        "/api/v1/connections/{id:int}/preview",
        preview_connection,
        methods=["POST"],
    ),
    Route(
        "/api/v1/connections/{id:int}/query",
        query_connection,
        methods=["POST"],
    ),
    # Uploads
    Route("/api/v1/uploads", list_uploads, methods=["GET"]),
    Route("/api/v1/uploads/{id:int}", get_upload, methods=["GET"]),
    Route("/api/v1/uploads", create_upload, methods=["POST"]),
    Route("/api/v1/uploads/{id:int}", update_upload, methods=["PUT"]),
    Route("/api/v1/uploads/{id:int}", delete_upload, methods=["DELETE"]),
    Route("/api/v1/uploads/{id:int}/run", trigger_upload, methods=["POST"]),
    # Pipelines
    Route("/api/v1/pipelines", list_pipelines, methods=["GET"]),
    Route("/api/v1/pipelines/{id:int}", get_pipeline, methods=["GET"]),
    Route("/api/v1/pipelines", create_pipeline, methods=["POST"]),
    Route("/api/v1/pipelines/{id:int}", update_pipeline, methods=["PUT"]),
    Route("/api/v1/pipelines/{id:int}", delete_pipeline, methods=["DELETE"]),
    Route("/api/v1/pipelines/{id:int}/run", trigger_pipeline, methods=["POST"]),
    # Transformations
    Route("/api/v1/transformations", list_transformations, methods=["GET"]),
    Route("/api/v1/transformations/{id:int}", get_transformation, methods=["GET"]),
    Route("/api/v1/transformations", create_transformation, methods=["POST"]),
    Route("/api/v1/transformations/{id:int}", update_transformation, methods=["PUT"]),
    Route("/api/v1/transformations/{id:int}", delete_transformation, methods=["DELETE"]),
    Route("/api/v1/transformations/{id:int}/run", trigger_transformation, methods=["POST"]),
    Route(
        "/api/v1/transformations/{id:int}/compile",
        compile_transformation_endpoint,
        methods=["POST"],
    ),
    Route(
        "/api/v1/transformations/{id:int}/preview",
        preview_transformation_endpoint,
        methods=["POST"],
    ),
    # Schedules
    Route("/api/v1/schedules", list_schedules, methods=["GET"]),
    Route("/api/v1/schedules/{id:int}", get_schedule, methods=["GET"]),
    Route("/api/v1/schedules", create_schedule, methods=["POST"]),
    Route("/api/v1/schedules/{id:int}", update_schedule, methods=["PUT"]),
    Route("/api/v1/schedules/{id:int}", delete_schedule, methods=["DELETE"]),
    # Runs
    Route("/api/v1/runs", list_runs, methods=["GET"]),
    Route("/api/v1/runs/{id:int}", get_run, methods=["GET"]),
    Route("/api/v1/runs/{id:int}/logs", get_run_logs, methods=["GET"]),
    Route("/api/v1/runs/{id:int}/cancel", cancel_run, methods=["POST"]),
    # Catalog
    Route("/api/v1/catalog", list_catalog_entries, methods=["GET"]),
    Route("/api/v1/catalog/{id:int}", get_catalog_entry, methods=["GET"]),
    # In-app notifications
    Route("/api/v1/notifications", list_in_app_notifications, methods=["GET"]),
    Route("/api/v1/notifications/unread-count", get_unread_count, methods=["GET"]),
    Route("/api/v1/notifications/{id:int}/read", mark_notification_read, methods=["PATCH"]),
    Route("/api/v1/notifications/read-all", mark_all_notifications_read, methods=["POST"]),
    Route("/api/v1/notifications/{id:int}", dismiss_notification, methods=["DELETE"]),
    # Bulk import
    Route("/api/v1/import", bulk_import, methods=["POST"]),
    # YAML pipeline config (E8) — same schema as JSON import, YAML serialization
    Route("/api/v1/pipelines/yaml", bulk_import_yaml, methods=["POST"]),
    # Notification channels
    Route("/api/v1/notifications/channels", list_notification_channels, methods=["GET"]),
    Route("/api/v1/notifications/channels/{id:int}", get_notification_channel, methods=["GET"]),
    Route("/api/v1/notifications/channels", create_notification_channel, methods=["POST"]),
    Route("/api/v1/notifications/channels/{id:int}", update_notification_channel, methods=["PUT"]),
    Route(
        "/api/v1/notifications/channels/{id:int}",
        delete_notification_channel,
        methods=["DELETE"],
    ),
]
