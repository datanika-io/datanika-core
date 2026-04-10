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

from datanika.models.connection import ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.notification_channel import ChannelType
from datanika.models.pipeline import DbtCommand
from datanika.models.run import RunStatus
from datanika.models.transformation import Materialization
from datanika.services.api_middleware import api_endpoint
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


async def _body(request: Request) -> dict:
    raw = await request.body()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


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


# ---------------------------------------------------------------------------
# Connections
# ---------------------------------------------------------------------------

@api_endpoint(required_scope="connections:read")
async def list_connections(request, api_key, session):
    items = _get_conn_svc().list_connections(session, api_key.org_id)
    return JSONResponse({"items": [_ser_connection(c) for c in items]})


@api_endpoint(required_scope="connections:read")
async def get_connection(request, api_key, session):
    conn_id = int(request.path_params["id"])
    conn = _get_conn_svc().get_connection(session, api_key.org_id, conn_id)
    if conn is None:
        return _error(404, "Connection not found")
    return JSONResponse(_ser_connection(conn))


@api_endpoint(required_scope="connections:write")
async def create_connection(request, api_key, session):
    data = await _body(request)
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
async def update_connection(request, api_key, session):
    conn_id = int(request.path_params["id"])
    data = await _body(request)
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
async def delete_connection(request, api_key, session):
    conn_id = int(request.path_params["id"])
    if not _get_conn_svc().delete_connection(session, api_key.org_id, conn_id):
        return _error(404, "Connection not found")
    return JSONResponse({"deleted": True})


@api_endpoint(required_scope="connections:write")
async def test_connection(request, api_key, session):
    conn_id = int(request.path_params["id"])
    config = _get_conn_svc().get_connection_config(session, api_key.org_id, conn_id)
    if config is None:
        return _error(404, "Connection not found")
    conn = _get_conn_svc().get_connection(session, api_key.org_id, conn_id)
    ok, msg = ConnectionService.test_connection(config, conn.connection_type)
    return JSONResponse({"success": ok, "message": msg})


# ---------------------------------------------------------------------------
# Uploads
# ---------------------------------------------------------------------------

@api_endpoint(required_scope="uploads:read")
async def list_uploads(request, api_key, session):
    items = _get_upload_svc().list_uploads(session, api_key.org_id)
    return JSONResponse({"items": [_ser_upload(u) for u in items]})


@api_endpoint(required_scope="uploads:read")
async def get_upload(request, api_key, session):
    upload_id = int(request.path_params["id"])
    upload = _get_upload_svc().get_upload(session, api_key.org_id, upload_id)
    if upload is None:
        return _error(404, "Upload not found")
    return JSONResponse(_ser_upload(upload))


@api_endpoint(required_scope="uploads:write")
async def create_upload(request, api_key, session):
    data = await _body(request)
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
async def update_upload(request, api_key, session):
    upload_id = int(request.path_params["id"])
    data = await _body(request)
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
async def delete_upload(request, api_key, session):
    upload_id = int(request.path_params["id"])
    if not _get_upload_svc().delete_upload(session, api_key.org_id, upload_id):
        return _error(404, "Upload not found")
    return JSONResponse({"deleted": True})


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
    return JSONResponse({"run_id": run.id, "status": "pending"}, status_code=202)


# ---------------------------------------------------------------------------
# Pipelines
# ---------------------------------------------------------------------------

@api_endpoint(required_scope="pipelines:read")
async def list_pipelines(request, api_key, session):
    items = _pipeline_svc.list_pipelines(session, api_key.org_id)
    return JSONResponse({"items": [_ser_pipeline(p) for p in items]})


@api_endpoint(required_scope="pipelines:read")
async def get_pipeline(request, api_key, session):
    pipeline_id = int(request.path_params["id"])
    pipeline = _pipeline_svc.get_pipeline(session, api_key.org_id, pipeline_id)
    if pipeline is None:
        return _error(404, "Pipeline not found")
    return JSONResponse(_ser_pipeline(pipeline))


@api_endpoint(required_scope="pipelines:write")
async def create_pipeline(request, api_key, session):
    data = await _body(request)
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
async def update_pipeline(request, api_key, session):
    pipeline_id = int(request.path_params["id"])
    data = await _body(request)
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
async def delete_pipeline(request, api_key, session):
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
    return JSONResponse({"run_id": run.id, "status": "pending"}, status_code=202)


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------

@api_endpoint(required_scope="transformations:read")
async def list_transformations(request, api_key, session):
    items = _transform_svc.list_transformations(session, api_key.org_id)
    return JSONResponse({"items": [_ser_transformation(t) for t in items]})


@api_endpoint(required_scope="transformations:read")
async def get_transformation(request, api_key, session):
    tid = int(request.path_params["id"])
    t = _transform_svc.get_transformation(session, api_key.org_id, tid)
    if t is None:
        return _error(404, "Transformation not found")
    return JSONResponse(_ser_transformation(t))


@api_endpoint(required_scope="transformations:write")
async def create_transformation(request, api_key, session):
    data = await _body(request)
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
async def update_transformation(request, api_key, session):
    tid = int(request.path_params["id"])
    data = await _body(request)
    kwargs = {}
    for key in (
        "name", "description", "sql_body", "schema_name",
        "tests_config", "tags", "incremental_config",
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
            int(data["destination_connection_id"])
            if data["destination_connection_id"]
            else None
        )
    try:
        t = _transform_svc.update_transformation(session, api_key.org_id, tid, **kwargs)
    except ValueError as exc:
        return _error(400, str(exc))
    if t is None:
        return _error(404, "Transformation not found")
    return JSONResponse(_ser_transformation(t))


@api_endpoint(required_scope="transformations:write")
async def delete_transformation(request, api_key, session):
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
    return JSONResponse({"run_id": run.id, "status": "pending"}, status_code=202)


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------

@api_endpoint(required_scope="schedules:read")
async def list_schedules(request, api_key, session):
    items = _get_schedule_svc().list_schedules(session, api_key.org_id)
    return JSONResponse({"items": [_ser_schedule(s) for s in items]})


@api_endpoint(required_scope="schedules:read")
async def get_schedule(request, api_key, session):
    sid = int(request.path_params["id"])
    s = _get_schedule_svc().get_schedule(session, api_key.org_id, sid)
    if s is None:
        return _error(404, "Schedule not found")
    return JSONResponse(_ser_schedule(s))


@api_endpoint(required_scope="schedules:write")
async def create_schedule(request, api_key, session):
    data = await _body(request)
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
async def update_schedule(request, api_key, session):
    sid = int(request.path_params["id"])
    data = await _body(request)
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
async def delete_schedule(request, api_key, session):
    sid = int(request.path_params["id"])
    if not _get_schedule_svc().delete_schedule(session, api_key.org_id, sid):
        return _error(404, "Schedule not found")
    return JSONResponse({"deleted": True})


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------

@api_endpoint(required_scope="runs:read")
async def list_runs(request, api_key, session):
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
async def get_run(request, api_key, session):
    run_id = int(request.path_params["id"])
    run = _exec_svc.get_run(session, api_key.org_id, run_id)
    if run is None:
        return _error(404, "Run not found")
    return JSONResponse(_ser_run(run))


@api_endpoint(required_scope="runs:read")
async def get_run_logs(request, api_key, session):
    run_id = int(request.path_params["id"])
    run = _exec_svc.get_run(session, api_key.org_id, run_id)
    if run is None:
        return _error(404, "Run not found")
    return JSONResponse({"run_id": run.id, "logs": run.logs or ""})


# ---------------------------------------------------------------------------
# Notification Channels
# ---------------------------------------------------------------------------

@api_endpoint(required_scope="notifications:read")
async def list_notification_channels(request, api_key, session):
    items = _notif_svc.list_channels(session, api_key.org_id)
    return JSONResponse({"items": [_ser_channel(ch) for ch in items]})


@api_endpoint(required_scope="notifications:read")
async def get_notification_channel(request, api_key, session):
    cid = int(request.path_params["id"])
    ch = _notif_svc._get_channel(session, cid, api_key.org_id)
    if ch is None:
        return _error(404, "Notification channel not found")
    return JSONResponse(_ser_channel(ch))


@api_endpoint(required_scope="notifications:write")
async def create_notification_channel(request, api_key, session):
    data = await _body(request)
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
async def update_notification_channel(request, api_key, session):
    cid = int(request.path_params["id"])
    data = await _body(request)
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
async def delete_notification_channel(request, api_key, session):
    cid = int(request.path_params["id"])
    if not _notif_svc.delete_channel(session, cid, api_key.org_id):
        return _error(404, "Notification channel not found")
    return JSONResponse({"deleted": True})


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
