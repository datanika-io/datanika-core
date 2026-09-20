"""Every scoped REST route reaches a role gate before it acts, and only current members are served.

This is the repository copy of the route-role census for [core#681]. It enumerates every ``Route``
in ``api_v1_routes`` and ``meta_routes`` **from the route tables themselves** — a route with no case
here fails the run rather than being skipped — and drives each one through the real ``api_endpoint``
middleware with real key authentication, once per subject: an org ``admin``, an ``editor``, a
``viewer``, a member whose membership has been soft-deleted, and an ``admin`` holding a key scoped
only to ``catalog:read``.

A timeline records every ``assert_org_role`` call and every **act**: a DB write, a Celery dispatch,
or an outbound dial with the org's stored credentials. Each test below asserts a property of that
timeline. The two load-bearing ones are **derived, not listed** — every acting route passes an
*operation* gate before it acts, and a key whose owner is not a current member is refused
everywhere — so a route added tomorrow is covered without anyone remembering to add it here.

⚠️ *Operation* gate is the load-bearing word, and it is not a refinement for its own sake: the
authentication floor gates every authenticated request, so a census asking only *"did some gate
pass first"* is answered by authentication on every route, whatever the operation thresholds do.
``test_every_acting_route_passes_an_operation_gate_before_it_acts`` is the half with teeth; its
docstring records the mutation that showed the other half green while a service gate was gone.

Why this is a test and not a note. The behaviour it measures is enforced in production, and a
measurement that becomes a floor ships its **instrument**, not its number: a number cannot be
re-derived, and nothing goes red when it moves.

``test_the_instrument_can_see_an_act_that_reached_no_gate`` is the positive control. A guard that
has only ever returned "pass" is not evidence — that one plants a route that acts without reaching
a gate and asserts the census reports it, so the detector is shown returning the verdict every other
test in this file asserts the absence of.
"""

from __future__ import annotations

import ast
import contextlib
import importlib
import itertools
import json
import logging
import pathlib
import sys
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session as SASession
from sqlalchemy.pool import StaticPool
from starlette.applications import Starlette
from starlette.testclient import TestClient

import datanika.services.authorization as authz
import datanika.tasks.pipeline_tasks  # noqa: F401  (so the task attribute exists to patch)
import datanika.tasks.transformation_tasks  # noqa: F401
import datanika.tasks.upload_tasks  # noqa: F401
from datanika.models.api_key import ApiKey
from datanika.models.base import Base
from datanika.models.catalog_entry import CatalogEntry, CatalogEntryType
from datanika.models.connection import ConnectionType
from datanika.models.dependency import NodeType
from datanika.models.notification import Notification, NotificationType
from datanika.models.notification_channel import ChannelType
from datanika.models.pipeline import DbtCommand
from datanika.models.run import Run, RunStatus
from datanika.models.transformation import Materialization
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services import api_v1_routes as routes_mod
from datanika.services import meta_routes as meta_mod
from datanika.services.api_key_service import ApiKeyService
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from datanika.services.notification_service import NotificationService
from datanika.services.pipeline_service import PipelineService
from datanika.services.rate_limit_service import RateLimitResult
from datanika.services.schedule_service import ScheduleService
from datanika.services.transformation_service import TransformationService
from datanika.services.upload_service import UploadService
from tests.factories import make_user

ORG_ID = 10
SUBJECTS = ("admin", "editor", "viewer", "removed", "catalog_read_only_key")
ACT_KINDS = {"write", "dispatch", "dial"}

#: The operation the authentication floor gates under. Every authenticated request passes it, so a
#: census that only asks "did some gate pass first" is answered by authentication on every route.
AUTHENTICATION_GATE = "api_key_authentication"

_REAL_ASSERT = authz.assert_org_role
_RATE_OK = RateLimitResult(
    allowed=True, current_count=1, limit=60, remaining=59, retry_after=0, reset_at=9999999999
)

#: A stored credential and a stored channel secret, each carrying a marker the assertions look for
#: in the response body. They are fixture values; nothing here reads a real credential.
PG = {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "census-pg-MARKER"}
SLACK = "https://hooks.slack.com/services/CENSUS-SLACK-MARKER"
MARKERS = ("census-pg-MARKER", "CENSUS-SLACK-MARKER")

OPENAPI = {
    "openapi": "3.0.0",
    "info": {"title": "t", "version": "1"},
    "servers": [{"url": "https://api.example.com"}],
    "paths": {
        "/items": {
            "get": {
                "responses": {
                    "200": {
                        "description": "ok",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {"id": {"type": "integer"}},
                                    },
                                }
                            }
                        },
                    }
                }
            }
        }
    },
}


# ---------------------------------------------------------------------------------------------
# What this census permits, each entry with its reason
# ---------------------------------------------------------------------------------------------

#: Route-methods a ``viewer``'s key may act on. Not exemptions — the product grants a viewer each
#: of these in the UI too, so a census that refused them would be asserting something the product
#: does not do. An acting route that is not here fails :func:`test_a_viewer_acts_only_where_the_ui
#: _grants_it`, which is the point: a new one has to be looked at.
VIEWER_MAY_ACT: dict[tuple[str, str, str], str] = {
    (
        "POST",
        "/api/v1/connections/{id:int}/introspect",
        "all",
    ): "the UI's table picker is read-only",
    ("POST", "/api/v1/connections/{id:int}/columns", "table"): "same picker, one table deeper",
    ("POST", "/api/v1/connections/{id:int}/preview", "table"): "the model preview carries no gate",
    ("POST", "/api/v1/connections/{id:int}/query", "select"): "the preview's own query path",
    (
        "POST",
        "/api/v1/transformations/{id:int}/compile",
        "default",
    ): "the SQL editor compiles as you type",
    (
        "POST",
        "/api/v1/transformations/{id:int}/preview",
        "default",
    ): "the SQL editor's preview pane",
    ("PATCH", "/api/v1/notifications/{id:int}/read", "own"): "a member's own notification",
    ("POST", "/api/v1/notifications/read-all", "default"): "a member's own notifications",
    ("DELETE", "/api/v1/notifications/{id:int}", "own"): "a member's own notification",
}

#: Route-methods whose threshold is ``admin``, so an ``editor``'s key is refused on them. Asserted
#: in **both** directions: an editor is refused on exactly these and acts elsewhere, so moving a
#: threshold in either direction goes red here rather than silently.
ADMIN_THRESHOLD: frozenset[tuple[str, str]] = frozenset(
    {
        ("DELETE", "/api/v1/connections/{id:int}"),
        ("DELETE", "/api/v1/uploads/{id:int}"),
        ("DELETE", "/api/v1/pipelines/{id:int}"),
        ("DELETE", "/api/v1/transformations/{id:int}"),
        ("DELETE", "/api/v1/schedules/{id:int}"),
        ("POST", "/api/v1/notifications/channels"),
        ("PUT", "/api/v1/notifications/channels/{id:int}"),
        ("DELETE", "/api/v1/notifications/channels/{id:int}"),
    }
)

#: What a key scoped only to ``catalog:read`` may still reach: the catalog it is scoped to, and the
#: meta reads, which describe the product rather than the org and declare no scope.
CATALOG_READ_ONLY_MAY_READ: frozenset[tuple[str, str]] = frozenset(
    {
        ("GET", "/api/v1/catalog"),
        ("GET", "/api/v1/catalog/{id:int}"),
        ("GET", "/api/v1/meta/connection-types"),
        ("GET", "/api/v1/meta/connection-types/{type:str}"),
        ("GET", "/api/v1/meta/dlt-config-schema"),
        ("GET", "/api/v1/meta/dbt-tests"),
        ("GET", "/api/v1/meta/materializations"),
    }
)


# ---------------------------------------------------------------------------------------------
# Where the gate has to be watched
# ---------------------------------------------------------------------------------------------


def _modules_importing_the_gate() -> list[str]:
    """Every module under ``datanika`` that imports ``assert_org_role`` by name.

    Read from the **source tree**, not from ``sys.modules``. A module the gate lives in is only in
    ``sys.modules`` once something has imported it, so discovering targets that way makes the
    census depend on the import order of whatever ran before it — green in one process and red in
    another, for no reason in the product. Reading the tree also means a module that starts
    importing the gate tomorrow is watched on the next run without being listed.
    """
    root = pathlib.Path(authz.__file__).resolve().parent.parent
    # The module that *defines* the gate does not import it, and something may still call it
    # attribute-style through this module rather than by the name it bound at import.
    found: set[str] = {authz.__name__}
    for path in root.rglob("*.py"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except (SyntaxError, UnicodeDecodeError):  # pragma: no cover - not expected in-tree
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and any(
                alias.name == "assert_org_role" for alias in node.names
            ):
                rel = path.relative_to(root.parent).with_suffix("")
                found.add(".".join(rel.parts))
    return sorted(found)


# ---------------------------------------------------------------------------------------------
# The cases
# ---------------------------------------------------------------------------------------------


def _declared_scopes() -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for mod in (routes_mod, meta_mod):
        tree = ast.parse(pathlib.Path(mod.__file__).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and getattr(dec.func, "id", None) == "api_endpoint":
                    scope = None
                    for kw in dec.keywords:
                        if kw.arg == "required_scope":
                            scope = kw.value.value if isinstance(kw.value, ast.Constant) else "<?>"
                    if dec.args:
                        a = dec.args[0]
                        scope = a.value if isinstance(a, ast.Constant) else "<?>"
                    out[node.name] = scope
    return out


def _route_methods() -> list[tuple[str, str, str]]:
    rows = []
    for r in list(routes_mod.api_v1_routes) + list(meta_mod.meta_routes):
        for m in sorted(set(r.methods) - {"HEAD"}):
            rows.append((m, r.path, r.endpoint.__name__))
    return rows


def _cases(ids: dict) -> dict[tuple[str, str], list[tuple[str, object, str, dict]]]:
    """``(method, path) -> [(case, body, body_kind, id_overrides)]``.

    A route with no entry gets one default case with no body. A route whose path needs a fixture id
    with no rule in :data:`_ID_RULES` fails the run.
    """
    j, y = "json", "yaml"
    return {
        ("POST", "/api/v1/connections"): [
            ("create", {"name": "New", "connection_type": "postgres", "config": PG}, j, {})
        ],
        ("POST", "/api/v1/connections/openapi/parse"): [
            ("inline", {"spec_inline": json.dumps(OPENAPI)}, j, {})
        ],
        ("PUT", "/api/v1/connections/{id:int}"): [("rename", {"name": "renamed"}, j, {})],
        ("POST", "/api/v1/connections/{id:int}/introspect"): [("all", {}, j, {})],
        ("POST", "/api/v1/connections/{id:int}/columns"): [("table", {"table": "t"}, j, {})],
        ("POST", "/api/v1/connections/{id:int}/preview"): [("table", {"table": "t"}, j, {})],
        ("POST", "/api/v1/connections/{id:int}/query"): [("select", {"query": "SELECT 1"}, j, {})],
        ("POST", "/api/v1/uploads"): [
            (
                "create",
                {
                    "name": "U2",
                    "source_connection_id": ids["conn"],
                    "destination_connection_id": ids["conn"],
                },
                j,
                {},
            )
        ],
        ("PUT", "/api/v1/uploads/{id:int}"): [("rename", {"name": "renamed"}, j, {})],
        ("POST", "/api/v1/pipelines"): [
            (
                "create",
                {"name": "P2", "destination_connection_id": ids["conn"], "command": "run"},
                j,
                {},
            )
        ],
        ("PUT", "/api/v1/pipelines/{id:int}"): [("rename", {"name": "renamed"}, j, {})],
        ("POST", "/api/v1/transformations"): [
            ("create", {"name": "t2", "sql_body": "select 1"}, j, {})
        ],
        ("PUT", "/api/v1/transformations/{id:int}"): [("rename", {"name": "renamed_t"}, j, {})],
        ("POST", "/api/v1/transformations/{id:int}/preview"): [("default", {}, j, {})],
        ("POST", "/api/v1/schedules"): [
            (
                "create",
                {
                    "target_type": "upload",
                    "target_id": ids["upload"],
                    "cron_expression": "0 * * * *",
                },
                j,
                {},
            )
        ],
        ("PUT", "/api/v1/schedules/{id:int}"): [("cron", {"cron_expression": "0 2 * * *"}, j, {})],
        ("POST", "/api/v1/notifications/channels"): [
            (
                "create",
                {
                    "name": "C2",
                    "channel_type": "slack",
                    "config": {"webhook_url": "https://hooks.slack.com/services/CENSUS"},
                    "events": ["run_failure"],
                },
                j,
                {},
            )
        ],
        ("PUT", "/api/v1/notifications/channels/{id:int}"): [
            ("rename", {"name": "renamed"}, j, {})
        ],
        ("PATCH", "/api/v1/notifications/{id:int}/read"): [
            ("own", None, j, {}),
            ("another-members", None, j, {"notif": ids["notif_other"]}),
        ],
        ("DELETE", "/api/v1/notifications/{id:int}"): [
            ("own", None, j, {}),
            ("another-members", None, j, {"notif": ids["notif_other"]}),
        ],
        ("POST", "/api/v1/import"): [
            (
                "transformation-only",
                {"version": 2, "transformations": [{"name": "imp_t", "sql_body": "select 1"}]},
                j,
                {},
            ),
            (
                "connection",
                {
                    "version": 2,
                    "connections": [{"name": "imp_c", "connection_type": "postgres", "config": PG}],
                },
                j,
                {},
            ),
        ],
        ("POST", "/api/v1/pipelines/yaml"): [
            (
                "transformation-only",
                "version: 2\ntransformations:\n  - name: imp_y\n    sql_body: select 1\n",
                y,
                {},
            ),
            (
                "connection",
                "version: 2\nconnections:\n  - name: imp_yc\n    connection_type: postgres\n"
                "    config: {host: h, port: 5432, database: d, user: u, password: p}\n",
                y,
                {},
            ),
        ],
    }


_ID_RULES = (
    ("/api/v1/notifications/channels/", "channel"),
    ("/api/v1/notifications/", "notif"),
    ("/api/v1/connections/", "conn"),
    ("/api/v1/uploads/", "upload"),
    ("/api/v1/pipelines/", "pipeline"),
    ("/api/v1/transformations/", "transformation"),
    ("/api/v1/schedules/", "schedule"),
    ("/api/v1/runs/", "run"),
    ("/api/v1/catalog/", "catalog"),
)


def _concrete(path: str, ids: dict) -> str:
    if "{type:str}" in path:
        return path.replace("{type:str}", "postgres")
    if "{id:int}" not in path:
        return path
    for prefix, key in _ID_RULES:
        if path.startswith(prefix):
            return path.replace("{id:int}", str(ids[key]))
    raise KeyError(f"no fixture id rule for {path}")


# ---------------------------------------------------------------------------------------------
# The instrumented surface
# ---------------------------------------------------------------------------------------------


class Timeline:
    def __init__(self) -> None:
        self.events: list[tuple[int, str, object]] = []
        self._seq = itertools.count()
        self.armed = False

    def add(self, kind: str, detail: object) -> None:
        if self.armed:
            self.events.append((next(self._seq), kind, detail))


def _recording_gate(tl: Timeline):
    def wrapped(*args, **kwargs):
        required = kwargs.get("required", args[3] if len(args) > 3 else None)
        req = getattr(required, "value", required)
        op = kwargs.get("operation")
        try:
            result = _REAL_ASSERT(*args, **kwargs)
        except Exception as exc:
            tl.add("gate", {"required": req, "operation": op, "raised": type(exc).__name__})
            raise
        tl.add("gate", {"required": req, "operation": op, "raised": None})
        return result

    return wrapped


def _bypassing_gate(_tl: Timeline):
    """A gate that neither checks nor records — the planted defect the control drives."""

    def wrapped(*_args, **_kwargs):
        return None

    return wrapped


def _record(tl: Timeline, kind: str, name: str, ret):
    def f(*_a, **_k):
        tl.add(kind, name)
        return ret

    return f


@contextlib.contextmanager
def _surface(subject: str, gate_targets: list[str], *, gate="record"):
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    Base.metadata.create_all(engine)
    session = SASession(engine)
    tl = Timeline()

    session.add(Organization(id=ORG_ID, name="Acme", slug="acme-census"))
    session.flush()
    role = {
        "admin": MemberRole.ADMIN,
        "editor": MemberRole.EDITOR,
        "viewer": MemberRole.VIEWER,
        "removed": MemberRole.ADMIN,
        "catalog_read_only_key": MemberRole.ADMIN,
    }[subject]
    actor = make_user(session, email=f"census-{subject}@test.io", password_hash="x")
    membership = Membership(user_id=actor.id, org_id=ORG_ID, role=role)
    session.add(membership)
    setup_admin = make_user(session, email="census-setup@test.io", password_hash="x")
    session.add(Membership(user_id=setup_admin.id, org_id=ORG_ID, role=MemberRole.ADMIN))
    session.flush()

    enc = EncryptionService(Fernet.generate_key().decode())
    conn_svc = ConnectionService(enc)
    upload_svc = UploadService(conn_svc)
    pipeline_svc = PipelineService()
    transform_svc = TransformationService()
    schedule_svc = ScheduleService(upload_svc, transform_svc, pipeline_service=pipeline_svc)
    notif_svc = NotificationService()

    conn = conn_svc.create_connection(
        session, ORG_ID, "Existing", ConnectionType.POSTGRES, PG, actor_user_id=setup_admin.id
    )
    session.flush()
    upload = upload_svc.create_upload(
        session, ORG_ID, "Existing upload", None, conn.id, conn.id, {}, actor_user_id=setup_admin.id
    )
    pipeline = pipeline_svc.create_pipeline(
        session,
        ORG_ID,
        "Existing pipeline",
        None,
        conn.id,
        DbtCommand.RUN,
        actor_user_id=setup_admin.id,
    )
    session.flush()
    transformation = transform_svc.create_transformation(
        session,
        ORG_ID,
        "existing_t",
        "select 1",
        Materialization.VIEW,
        actor_user_id=setup_admin.id,
    )
    schedule = schedule_svc.create_schedule(
        session, ORG_ID, NodeType.UPLOAD, upload.id, "0 * * * *", actor_user_id=setup_admin.id
    )
    channel = notif_svc.create_channel(
        session,
        ORG_ID,
        actor_user_id=setup_admin.id,
        name="Existing channel",
        channel_type=ChannelType.SLACK,
        config={"webhook_url": SLACK},
        events=["run_failure"],
    )
    run = Run(
        org_id=ORG_ID,
        target_type=NodeType.UPLOAD,
        target_id=upload.id,
        status=RunStatus.PENDING,
        logs="log",
    )
    session.add(run)
    session.flush()
    notif = Notification(
        org_id=ORG_ID,
        user_id=actor.id,
        type=NotificationType.RUN_FAILED,
        title="own",
        resource_type="run",
        resource_id=run.id,
    )
    notif_other = Notification(
        org_id=ORG_ID,
        user_id=setup_admin.id,
        type=NotificationType.RUN_FAILED,
        title="another member's",
        resource_type="run",
        resource_id=run.id,
    )
    entry = CatalogEntry(
        org_id=ORG_ID,
        entry_type=CatalogEntryType.SOURCE_TABLE,
        origin_type=NodeType.UPLOAD,
        origin_id=upload.id,
        table_name="t",
        schema_name="s",
        dataset_name="d",
        connection_id=conn.id,
    )
    session.add_all([notif, notif_other, entry])
    session.flush()

    scopes = ["catalog:read"] if subject == "catalog_read_only_key" else None
    _key_row, raw_key = ApiKeyService()._mint(session, ORG_ID, actor.id, "K", scopes, None)
    if subject == "removed":
        membership.deleted_at = datetime.now(UTC)
    session.commit()

    ids = {
        "conn": conn.id,
        "upload": upload.id,
        "pipeline": pipeline.id,
        "transformation": transformation.id,
        "schedule": schedule.id,
        "channel": channel.id,
        "run": run.id,
        "notif": notif.id,
        "notif_other": notif_other.id,
        "catalog": entry.id,
    }

    @event.listens_for(session, "before_flush")
    def _before_flush(sess, _ctx, _instances):
        changed = [
            o
            for o in itertools.chain(sess.new, sess.dirty, sess.deleted)
            if not isinstance(o, ApiKey)
            and (o in sess.new or o in sess.deleted or sess.is_modified(o))
        ]
        if changed:
            tl.add("write", sorted({type(o).__name__ for o in changed}))

    @event.listens_for(session, "do_orm_execute")
    def _orm_exec(state):
        if state.is_insert or state.is_update or state.is_delete:
            table = getattr(getattr(state.statement, "table", None), "name", "?")
            if table != "api_keys":
                tl.add("write", [f"dml:{table}"])

    @contextlib.contextmanager
    def fake_session():
        yield session

    class _ErrTap(logging.Handler):
        def emit(self, record):
            if record.exc_info:
                exc = record.exc_info[1]
                tl.add("error", f"{type(exc).__name__}: {str(exc)[:200]}")

    tap = _ErrTap()
    logging.getLogger("datanika.services.api_middleware").addHandler(tap)

    def task_mock(name: str) -> MagicMock:
        m = MagicMock()
        m.delay.side_effect = lambda *a, **k: tl.add("dispatch", name)
        return m

    rl = MagicMock()
    rl.get_limit_for_org.return_value = 60
    rl.check_rate_limit.return_value = _RATE_OK
    rl.preauth_check.return_value = SimpleNamespace(allowed=True, retry_after=0)

    compile_ok = SimpleNamespace(
        success=True, compiled_sql="select 1", node={}, line=None, column=None, error_message=None
    )
    preview_ok = SimpleNamespace(
        success=True,
        columns=[],
        rows=[],
        row_count=0,
        truncated=False,
        error_code=None,
        error_message=None,
    )

    patches = [
        patch("datanika.services.api_middleware._rate_limit_svc", rl),
        patch("datanika.services.api_middleware._get_session", fake_session),
        patch.object(routes_mod, "_get_conn_svc", return_value=conn_svc),
        patch.object(routes_mod, "_get_upload_svc", return_value=upload_svc),
        patch.object(routes_mod, "_get_schedule_svc", return_value=schedule_svc),
        patch("datanika.tasks.upload_tasks.run_upload_task", task_mock("run_upload_task")),
        patch("datanika.tasks.pipeline_tasks.run_pipeline_task", task_mock("run_pipeline_task")),
        patch(
            "datanika.tasks.transformation_tasks.run_transformation_task",
            task_mock("run_transformation_task"),
        ),
        patch.object(
            ConnectionService, "list_tables", staticmethod(_record(tl, "dial", "list_tables", []))
        ),
        patch.object(
            ConnectionService, "list_columns", staticmethod(_record(tl, "dial", "list_columns", []))
        ),
        patch.object(
            ConnectionService,
            "preview_table",
            staticmethod(_record(tl, "dial", "preview_table", ([], []))),
        ),
        patch.object(
            ConnectionService,
            "execute_query",
            staticmethod(_record(tl, "dial", "execute_query", ([], []))),
        ),
        patch(
            "datanika.services.transformation_compile.compile_transformation",
            _record(tl, "dial", "dbt_compile", compile_ok),
        ),
        patch(
            "datanika.services.transformation_compile.preview_transformation",
            _record(tl, "dial", "dbt_preview", preview_ok),
        ),
    ]
    if hasattr(routes_mod, "run_connection_test_bounded"):
        patches.append(
            patch.object(
                routes_mod,
                "run_connection_test_bounded",
                _record(tl, "dial", "connection_test", SimpleNamespace(ok=True, message="ok")),
            )
        )
    gate_fn = (_recording_gate if gate == "record" else _bypassing_gate)(tl)
    for mod_name in gate_targets:
        patches.append(patch.object(sys.modules[mod_name], "assert_org_role", gate_fn))

    with contextlib.ExitStack() as stack:
        for p in patches:
            stack.enter_context(p)
        app = Starlette(routes=list(routes_mod.api_v1_routes) + list(meta_mod.meta_routes))
        yield TestClient(app), ids, tl, raw_key

    logging.getLogger("datanika.services.api_middleware").removeHandler(tap)
    session.close()
    engine.dispose()


def _drive(client, method, path, body, kind, raw_key):
    headers = {"Authorization": f"Bearer {raw_key}"}
    if body is None:
        return client.request(method, path, headers=headers)
    if kind == "yaml":
        headers["Content-Type"] = "application/x-yaml"
        return client.request(method, path, content=body.encode(), headers=headers)
    return client.request(method, path, json=body, headers=headers)


def _outcome(status: int, body_text: str, events) -> dict:
    acts = [e for e in events if e[1] in ACT_KINDS]
    gates = [e for e in events if e[1] == "gate"]
    code = None
    try:
        payload = json.loads(body_text)
        err = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(err, dict):
            code = err.get("code")
        elif isinstance(err, str):
            code = err[:60]
    except ValueError:
        pass
    if acts:
        label = "ACTED"
    elif status == 403 and code == "insufficient_role":
        label = "REFUSED_403"
    elif 200 <= status < 300:
        label = "SERVED"
    else:
        label = f"REJECTED_{status}"
    first_pass_gate = next((e[0] for e in gates if e[2]["raised"] is None), None)
    # The floor fires on every authenticated request, so it is the first passing gate on all of
    # them. Tracked separately because "some gate passed" is therefore true of every route that
    # authenticates at all, which is not the property the operation thresholds are there for.
    first_op_gate = next(
        (
            e[0]
            for e in gates
            if e[2]["raised"] is None and e[2]["operation"] != AUTHENTICATION_GATE
        ),
        None,
    )
    first_act = acts[0][0] if acts else None
    return {
        "label": label,
        "status": status,
        "code": code,
        "acts": [f"{e[1]}:{e[2]}" for e in acts],
        "gates": [e[2] for e in gates],
        "gate_before_act": None
        if first_act is None
        else (first_pass_gate is not None and first_pass_gate < first_act),
        "op_gate_before_act": None
        if first_act is None
        else (first_op_gate is not None and first_op_gate < first_act),
        "markers": [m for m in MARKERS if m in body_text],
        "body": body_text[:160],
    }


# ---------------------------------------------------------------------------------------------
# The census, run once
# ---------------------------------------------------------------------------------------------


@pytest.fixture(scope="module")
def gate_targets() -> list[str]:
    names = _modules_importing_the_gate()
    for name in names:
        importlib.import_module(name)
    return names


@pytest.fixture(scope="module")
def census(gate_targets) -> dict:
    scopes = _declared_scopes()
    uncensused: list[str] = []
    rows: list[dict] = []

    with _surface("admin", gate_targets) as (_c, ids0, _tl, _k):
        cases = _cases(ids0)

    for method, path, handler in _route_methods():
        for case, body, kind, overrides in cases.get(
            (method, path), [("default", None, "json", {})]
        ):
            row = {
                "method": method,
                "path": path,
                "handler": handler,
                "scope": scopes.get(handler, "<undeclared>"),
                "case": case,
                "runs": {},
            }
            for subject in SUBJECTS:
                with _surface(subject, gate_targets) as (client, ids, tl, raw_key):
                    ids = {**ids, **overrides}
                    try:
                        concrete = _concrete(path, ids)
                    except KeyError:
                        uncensused.append(f"{method} {path}")
                        break
                    tl.armed = True
                    resp = _drive(client, method, concrete, body, kind, raw_key)
                    tl.armed = False
                    row["runs"][subject] = _outcome(resp.status_code, resp.text, tl.events)
            rows.append(row)
    return {"rows": rows, "uncensused": uncensused, "gate_targets": gate_targets}


def _key(row) -> tuple[str, str]:
    return (row["method"], row["path"])


# ---------------------------------------------------------------------------------------------
# The control — a guard that has only ever passed is not evidence
# ---------------------------------------------------------------------------------------------


def test_the_instrument_can_see_an_act_that_reached_no_gate(gate_targets):
    """Plant a route that acts without reaching a gate, and read the verdict back.

    Every other test here asserts the **absence** of that verdict, so without this one a broken
    detector and a correct product are the same reading. The gate is replaced by one that neither
    checks nor records, which is what a route with no gate on its path looks like from here.
    """
    with _surface("viewer", gate_targets, gate="bypass") as (client, ids, tl, raw_key):
        tl.armed = True
        resp = _drive(
            client,
            "POST",
            "/api/v1/connections",
            {"name": "New", "connection_type": "postgres", "config": PG},
            "json",
            raw_key,
        )
        tl.armed = False
        outcome = _outcome(resp.status_code, resp.text, tl.events)

    assert outcome["label"] == "ACTED", outcome
    assert outcome["gate_before_act"] is False, outcome
    assert outcome["op_gate_before_act"] is False, outcome
    assert outcome["gates"] == [], outcome


def test_the_census_covers_every_route_the_tables_declare(census):
    """A route added with no case here fails the run rather than being quietly skipped."""
    assert census["uncensused"] == [], census["uncensused"]
    assert len(census["rows"]) >= len(_route_methods())


def test_the_gate_is_watched_in_every_module_that_imports_it(census):
    """An unwatched module is a gate this census cannot see, which reads as a missing gate."""
    targets = census["gate_targets"]
    assert "datanika.services.authorization" in targets
    assert "datanika.services.api_v1_routes" in targets
    assert len(targets) >= 8, targets
    assert all(sys.modules[t].assert_org_role is _REAL_ASSERT for t in targets), targets


# ---------------------------------------------------------------------------------------------
# The invariants
# ---------------------------------------------------------------------------------------------


def test_every_acting_route_passes_a_role_gate_before_it_acts(census):
    """Derived, not listed: whichever routes act, each reaches a passing gate first."""
    acting = [r for r in census["rows"] if r["runs"]["admin"]["label"] == "ACTED"]
    assert len(acting) >= 30, f"only {len(acting)} acting routes — the census stopped reaching them"
    ungated = [
        (r["method"], r["path"], r["case"])
        for r in acting
        if r["runs"]["admin"]["gate_before_act"] is not True
    ]
    assert ungated == [], ungated


def test_every_acting_route_passes_an_operation_gate_before_it_acts(census):
    """The half that has teeth, and the reason it is written separately.

    The authentication floor gates every authenticated request, so it is the first passing gate on
    all of them — which makes *"some gate passed before the act"* true of every route that
    authenticates at all, however the operation thresholds move underneath it. Measured: removing
    the ``create_transformation`` gate entirely leaves the test above **green**, because the floor
    still precedes the write.

    So this one ignores the floor and requires an operation-level gate. The exception is the routes
    whose threshold *is* the floor — any current member — which is exactly :data:`VIEWER_MAY_ACT`;
    for those, the floor is not a loophole, it is the gate, and
    :func:`test_a_key_is_served_only_while_its_owner_is_a_current_member` is what holds them.
    """
    member_threshold = {(m, p) for m, p, _case in VIEWER_MAY_ACT}
    acting = [r for r in census["rows"] if r["runs"]["admin"]["label"] == "ACTED"]
    ungated = [
        (r["method"], r["path"], r["case"])
        for r in acting
        if _key(r) not in member_threshold and r["runs"]["admin"]["op_gate_before_act"] is not True
    ]
    assert ungated == [], ungated
    assert len(acting) - len(member_threshold) >= 20, len(acting)


def test_a_key_is_served_only_while_its_owner_is_a_current_member(census):
    """Derived, not listed: every route-method, no exceptions, and no act on any of them."""
    served = [
        (r["method"], r["path"], r["case"], r["runs"]["removed"]["label"])
        for r in census["rows"]
        if r["runs"]["removed"]["label"] != "REFUSED_403"
    ]
    assert served == [], served
    acted = [
        (r["method"], r["path"], r["runs"]["removed"]["acts"])
        for r in census["rows"]
        if r["runs"]["removed"]["acts"]
    ]
    assert acted == [], acted


def test_a_viewer_acts_only_where_the_ui_grants_it(census):
    """Both directions: a new acting route must be looked at, a listed one must still act."""
    acted = {
        (r["method"], r["path"], r["case"])
        for r in census["rows"]
        if r["runs"]["viewer"]["label"] == "ACTED"
    }
    assert acted - set(VIEWER_MAY_ACT) == set(), sorted(acted - set(VIEWER_MAY_ACT))
    assert set(VIEWER_MAY_ACT) - acted == set(), sorted(set(VIEWER_MAY_ACT) - acted)


def test_an_editor_is_refused_on_exactly_the_admin_threshold_routes(census):
    """Moving a threshold in either direction goes red here rather than silently."""
    refused = {_key(r) for r in census["rows"] if r["runs"]["editor"]["label"] == "REFUSED_403"}
    assert refused == ADMIN_THRESHOLD, {
        "unexpectedly refused": sorted(refused - ADMIN_THRESHOLD),
        "no longer refused": sorted(ADMIN_THRESHOLD - refused),
    }


def test_a_narrowly_scoped_key_reaches_only_what_it_is_scoped_to(census):
    """A key holding one read scope is not a key holding the role its owner holds."""
    reached = {
        _key(r) for r in census["rows"] if r["runs"]["catalog_read_only_key"]["label"] == "SERVED"
    }
    assert reached == CATALOG_READ_ONLY_MAY_READ, {
        "unexpectedly reached": sorted(reached - CATALOG_READ_ONLY_MAY_READ),
        "no longer reached": sorted(CATALOG_READ_ONLY_MAY_READ - reached),
    }
    acted = [
        (r["method"], r["path"], r["case"])
        for r in census["rows"]
        if r["runs"]["catalog_read_only_key"]["acts"]
    ]
    assert acted == [], acted


def test_the_import_routes_require_a_scope(census):
    """A route that declares no scope still answers a narrowly scoped key on its scope."""
    imports = [
        r for r in census["rows"] if r["path"] in ("/api/v1/import", "/api/v1/pipelines/yaml")
    ]
    assert len(imports) == 4, [(_key(r), r["case"]) for r in imports]
    codes = {r["runs"]["catalog_read_only_key"]["code"] for r in imports}
    assert codes == {"insufficient_scope"}, [
        (_key(r), r["case"], r["runs"]["catalog_read_only_key"]["code"]) for r in imports
    ]


def test_another_members_notification_is_not_reachable_by_any_role(census):
    """Object-level, not role-level: an admin does not reach it either."""
    rows = [r for r in census["rows"] if r["case"] == "another-members"]
    assert len(rows) == 2, [(_key(r), r["case"]) for r in rows]
    for row in rows:
        for subject in ("admin", "editor", "viewer"):
            outcome = row["runs"][subject]
            assert outcome["acts"] == [], (subject, _key(row), outcome)
            assert outcome["status"] == 404, (subject, _key(row), outcome)


def test_a_stored_channel_secret_reaches_only_an_admin(census):
    """The marker is planted in the fixture's channel config, so a response carrying it is a read.

    Asserted in both directions: an admin still reads it — otherwise the marker could be absent
    everywhere because nothing serialises it any more, and a guard that cannot see the value is
    not a guard that the value is contained.
    """
    carried = {
        (r["method"], r["path"], subject)
        for r in census["rows"]
        for subject, outcome in r["runs"].items()
        if outcome["markers"]
    }
    assert all(subject == "admin" for _m, _p, subject in carried), sorted(carried)
    assert carried, "no response carried the marker — the census can no longer see it at all"
