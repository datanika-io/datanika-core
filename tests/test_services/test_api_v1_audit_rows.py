"""Mutating v1 routes owe the audit row their UI twin already writes.

SPEC_AUDIT_TRAIL §9, core#1534. The cancel route was ruled on its own (§8,
core#1533) and already writes. This file is the rest of that table.

The structural guard is not a list of today's writers. It classifies every
route, exempts only the four absences §9.4 and §9.5 argue, and fails when a
mutating route outside that set writes no row. A new writer passes. A new
silent mutation does not.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.models.audit_log import AuditAction, AuditLog
from datanika.services import api_v1_routes as routes_mod
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.rate_limit_service import RateLimitResult
from tests.test_services.test_api_v1_routes import _auth_headers, _patch_auth

_ROUTES = Path(routes_mod.__file__)


@pytest.fixture
def fake_api_key():
    key = MagicMock()
    key.id = 1
    key.org_id = 10
    key.user_id = 1
    key.name = "Test Key"
    key.scopes = None
    return key


@pytest.fixture
def rate_limit_ok():
    return RateLimitResult(
        allowed=True,
        current_count=1,
        limit=60,
        remaining=59,
        retry_after=0,
        reset_at=9999999999,
    )


@pytest.fixture
def client():
    return TestClient(Starlette(routes=api_v1_routes))


#: Argued absences, not a writer census. §9.4 the notification centre owes
#: nothing because neither door audits. §9.5 connection test performs no
#: database write. Adding a name here is the decision, and the test below
#: pins the set so a quiet addition is a diff.
EXEMPT = {
    "test_connection": "§9.5 class D: no database write",
    "mark_notification_read": "§9.4 class B: neither door audits",
    "mark_all_notifications_read": "§9.4 class B: neither door audits",
    "dismiss_notification": "§9.4 class B: neither door audits",
}

_AUDIT_HELPERS = frozenset({"_audit", "_audit_run_cancel"})

_PG = {
    "name": "Warehouse",
    "connection_type": "postgres",
    "config": {
        "host": "localhost",
        "port": 5432,
        "user": "u",
        "password": "super-secret-db-password",
        "database": "db",
    },
}


def _callee(node: ast.Call) -> str:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _call_names(fn: ast.AST) -> set[str]:
    return {_callee(n) for n in ast.walk(fn) if isinstance(n, ast.Call)}


def _scope(fn: ast.AST) -> str | None:
    for dec in fn.decorator_list:
        if not isinstance(dec, ast.Call) or _callee(dec) != "api_endpoint":
            continue
        for kw in dec.keywords:
            if kw.arg == "required_scope" and isinstance(kw.value, ast.Constant):
                return kw.value.value
    return None


def _route_methods(tree: ast.Module) -> dict[str, set[str]]:
    assign = next(
        n
        for n in tree.body
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "api_v1_routes" for t in n.targets)
    )
    methods: dict[str, set[str]] = {}
    for call in assign.value.elts:
        endpoint = call.args[1]
        assert isinstance(endpoint, ast.Name)
        found = set()
        for kw in call.keywords:
            if kw.arg == "methods":
                found = {elt.value for elt in kw.value.elts}
        methods.setdefault(endpoint.id, set()).update(found)
    return methods


def _functions(tree: ast.Module) -> dict[str, ast.AST]:
    return {n.name: n for n in tree.body if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)}


def _is_mutating(methods: set[str], scope: str | None) -> bool:
    if methods & {"PUT", "PATCH", "DELETE"}:
        return True
    if "POST" in methods:
        return not (isinstance(scope, str) and scope.endswith(":read"))
    return False


def _writes_audit(fn: ast.AST, fns: dict[str, ast.AST]) -> bool:
    """The handler itself, or one helper it calls, reaches an audit writer."""
    calls = _call_names(fn)
    if calls & _AUDIT_HELPERS:
        return True
    return any(_call_names(fns[name]) & _AUDIT_HELPERS for name in calls if name in fns)


def _classify() -> tuple[set[str], set[str], dict[str, ast.AST]]:
    tree = ast.parse(_ROUTES.read_text(encoding="utf-8"))
    fns = _functions(tree)
    methods = _route_methods(tree)
    mutating: set[str] = set()
    for name, verbs in methods.items():
        if _is_mutating(verbs, _scope(fns[name])):
            mutating.add(name)
    owing = mutating - set(EXEMPT)
    return mutating, owing, fns


def _rows(session, org_id: int) -> list[AuditLog]:
    return list(session.execute(select(AuditLog).where(AuditLog.org_id == org_id)).scalars().all())


class TestTheGuardSeesASilentMutation:
    def test_read_posts_and_the_four_argued_absences_are_not_owed(self):
        mutating, owing, _fns = _classify()
        assert "parse_openapi" not in mutating
        assert "introspect_connection" not in mutating
        assert "query_connection" not in mutating
        assert "compile_transformation_endpoint" not in mutating
        for name in EXEMPT:
            assert name in mutating, name
            assert name not in owing
        for name in (
            "create_connection",
            "update_connection",
            "delete_connection",
            "trigger_upload",
            "trigger_transformation",
            "cancel_run",
            "bulk_import",
            "bulk_import_yaml",
            "update_notification_channel",
        ):
            assert name in owing, name

    def test_every_route_that_owes_a_row_writes_one(self):
        _mutating, owing, fns = _classify()
        missing = sorted(name for name in owing if not _writes_audit(fns[name], fns))
        assert not missing, (
            "SPEC_AUDIT_TRAIL §9: these mutating routes have an auditing UI twin "
            "(or no twin, and still owe a row) and write nothing:\n  " + "\n  ".join(missing)
        )


class TestATriggeredRunIsInTheSameTransaction:
    def test_the_audit_call_precedes_the_handler_commit(self):
        # @api_endpoint replaces the handler with wrapper, so inspect.getsource
        # reads api_middleware.py and cannot see this body.
        tree = ast.parse(_ROUTES.read_text(encoding="utf-8"))
        fns = _functions(tree)
        for name in ("trigger_upload", "trigger_pipeline", "trigger_transformation"):
            audit_at = None
            commit_at = None
            for node in ast.walk(fns[name]):
                if not isinstance(node, ast.Call):
                    continue
                if _callee(node) == "_audit" and audit_at is None:
                    audit_at = node.lineno
                func = node.func
                if (
                    commit_at is None
                    and isinstance(func, ast.Attribute)
                    and func.attr == "commit"
                    and isinstance(func.value, ast.Name)
                    and func.value.id == "session"
                ):
                    commit_at = node.lineno
            assert audit_at is not None and commit_at is not None, name
            assert audit_at < commit_at, name


class TestConnectionRowsMatchTheTwin:
    def test_create_records_the_connection_and_not_the_secret(
        self, client, fake_api_key, rate_limit_ok
    ):
        fake_api_key.name = "ci-bot alice@example.com"
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            resp = client.post("/api/v1/connections", json=_PG, headers=_auth_headers())
            assert resp.status_code == 201
            rows = _rows(session, fake_api_key.org_id)
        assert len(rows) == 1
        row = rows[0]
        assert row.action == AuditAction.CREATE
        assert row.resource_type == "connection"
        assert row.resource_id == resp.json()["id"]
        assert row.old_values is None
        assert row.new_values["name"] == "Warehouse"
        assert row.new_values["connection_type"] == "postgres"
        assert row.new_values["api_key_id"] == fake_api_key.id
        dumped = json.dumps(row.new_values)
        assert "super-secret-db-password" not in dumped
        assert fake_api_key.name not in dumped

    def test_a_name_change_records_the_before_value(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            created = client.post(
                "/api/v1/connections",
                json={**_PG, "name": "Old Name"},
                headers=_auth_headers(),
            )
            assert created.status_code == 201
            conn_id = created.json()["id"]
            updated = client.put(
                f"/api/v1/connections/{conn_id}",
                json={"name": "New Name"},
                headers=_auth_headers(),
            )
            assert updated.status_code == 200
            updates = [
                r
                for r in _rows(session, fake_api_key.org_id)
                if r.action == AuditAction.UPDATE and r.resource_id == conn_id
            ]
        assert len(updates) == 1
        assert updates[0].old_values["name"] == "Old Name"
        assert updates[0].new_values["name"] == "New Name"
        assert updates[0].old_values["name"] != updates[0].new_values["name"]
        assert updates[0].new_values["api_key_id"] == fake_api_key.id
        assert "api_key_id" not in updates[0].old_values

    def test_an_empty_update_writes_no_row(self, client, fake_api_key, rate_limit_ok):
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            created = client.post("/api/v1/connections", json=_PG, headers=_auth_headers())
            assert created.status_code == 201
            conn_id = created.json()["id"]
            updated = client.put(
                f"/api/v1/connections/{conn_id}",
                json={},
                headers=_auth_headers(),
            )
            assert updated.status_code == 200
            rows = _rows(session, fake_api_key.org_id)
        assert any(r.action == AuditAction.CREATE for r in rows)
        assert [r for r in rows if r.action == AuditAction.UPDATE] == []

    def test_delete_records_the_name_and_a_missing_row_does_not(
        self, client, fake_api_key, rate_limit_ok
    ):
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            created = client.post(
                "/api/v1/connections",
                json={**_PG, "name": "To Delete"},
                headers=_auth_headers(),
            )
            conn_id = created.json()["id"]
            deleted = client.delete(f"/api/v1/connections/{conn_id}", headers=_auth_headers())
            assert deleted.status_code == 200
            missing = client.delete("/api/v1/connections/99999", headers=_auth_headers())
            assert missing.status_code == 404
            deletes = [
                r for r in _rows(session, fake_api_key.org_id) if r.action == AuditAction.DELETE
            ]
        assert len(deletes) == 1
        assert deletes[0].resource_type == "connection"
        assert deletes[0].resource_id == conn_id
        assert deletes[0].old_values["name"] == "To Delete"
        assert deletes[0].old_values["connection_type"] == "postgres"
        assert deletes[0].new_values == {"api_key_id": fake_api_key.id}


class TestOneImportRowCarriesTheFourCounts:
    def test_counts_all_four_and_a_zero_import_writes_nothing(
        self, client, fake_api_key, rate_limit_ok
    ):
        from unittest.mock import patch

        created = {
            "connections": [1],
            "uploads": [],
            "pipelines": [2, 3],
            "transformations": [4],
        }
        empty = {key: [] for key in created}
        with _patch_auth(fake_api_key, rate_limit_ok) as session:
            with (
                patch.object(routes_mod, "_import_scope_refusal", return_value=None),
                patch.object(routes_mod, "_validate_import_payload", return_value=[]),
                patch.object(routes_mod, "_execute_validated_import", return_value=created),
            ):
                resp = client.post("/api/v1/import", json={"version": 2}, headers=_auth_headers())
            assert resp.status_code == 201
            rows = [r for r in _rows(session, fake_api_key.org_id) if r.resource_type == "import"]
            assert len(rows) == 1
            assert rows[0].action == AuditAction.CREATE
            assert rows[0].resource_id is None
            assert rows[0].new_values["connections_imported"] == 1
            assert rows[0].new_values["uploads_imported"] == 0
            assert rows[0].new_values["pipelines_imported"] == 2
            assert rows[0].new_values["transformations_imported"] == 1
            assert rows[0].new_values["api_key_id"] == fake_api_key.id
            with (
                patch.object(routes_mod, "_import_scope_refusal", return_value=None),
                patch.object(routes_mod, "_validate_import_payload", return_value=[]),
                patch.object(routes_mod, "_execute_validated_import", return_value=empty),
            ):
                again = client.post("/api/v1/import", json={"version": 2}, headers=_auth_headers())
            assert again.status_code == 201
            rows = [r for r in _rows(session, fake_api_key.org_id) if r.resource_type == "import"]
        assert len(rows) == 1
