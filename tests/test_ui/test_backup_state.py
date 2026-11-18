"""BackupState authorization and the cross-org restore speed bump.

``export_backup`` had no ``_check_role`` at all while ``handle_restore_upload``
required ``admin`` — so the read side of the same feature was open to a
``viewer``, the role you hand a contractor precisely so they cannot reach
credentials.

The role check has to be the *first* thing the handler does. A check placed
after the export is built is not a check; it is a log line. So the test below
asserts position, not presence — ``test_rbac_enforcement.py`` already asserts
presence for every handler in the app.
"""

import ast
import contextlib
import inspect
from unittest.mock import MagicMock

import pytest
from cryptography.fernet import Fernet

import datanika.ui.state.auth_state as auth_state_module
import datanika.ui.state.backup_state as backup_state_module
from datanika.services.auth import AuthService
from datanika.services.backup_service import BackupService
from datanika.ui.state.auth_state import OrgInfo, UserInfo
from datanika.ui.state.backup_state import BackupState
from datanika.ui.state.base_state import BaseState

_SECRET = Fernet.generate_key().decode()


def _handler(name: str) -> ast.AsyncFunctionDef:
    source = inspect.getsource(backup_state_module)
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in backup_state")


def _guards_with_role(node) -> list[str]:
    """Roles required by a `if not await self._check_role(R): return` guard, in order."""
    roles = []
    for stmt in node.body:
        for child in ast.walk(stmt):
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Attribute)
                and child.func.attr == "_check_role"
                and child.args
                and isinstance(child.args[0], ast.Constant)
            ):
                roles.append((stmt, child.args[0].value))
    return roles


class TestExportAuthorization:
    def test_the_probe_finds_the_restore_guard(self):
        """Guard the guard — a broken AST walk would pass the real test vacuously."""
        guards = _guards_with_role(_handler("handle_restore_upload"))
        assert [r for _, r in guards] == ["admin"]

    @pytest.mark.parametrize("handler", ["export_backup", "handle_restore_upload"])
    def test_both_sides_of_backup_require_admin(self, handler):
        guards = _guards_with_role(_handler(handler))
        assert guards, f"{handler} has no _check_role call"
        assert guards[0][1] == "admin", f"{handler} requires '{guards[0][1]}', expected 'admin'"

    @pytest.mark.parametrize("handler", ["export_backup", "handle_restore_upload"])
    def test_the_role_check_is_the_first_statement(self, handler):
        node = _handler(handler)
        guards = _guards_with_role(node)
        assert guards and guards[0][0] is node.body[0], (
            f"{handler}'s role check is not its first statement — anything before it "
            "runs for a user who is about to be denied"
        )


class TestRestoreConfirmationSurface:
    def test_the_state_carries_a_foreign_org_warning_var(self):
        fields = BackupState.get_fields()
        assert "restore_foreign_org" in fields
        assert "restore_pending" in fields

    @pytest.mark.parametrize("handler", ["cancel_restore", "confirm_restore"])
    def test_leaving_the_restore_flow_clears_the_warning(self, handler):
        """A warning left set would re-arm the confirmation on the next, unrelated restore."""
        assigned = {
            t.attr
            for node in ast.walk(_handler(handler))
            if isinstance(node, ast.Assign)
            for t in node.targets
            if isinstance(t, ast.Attribute)
        }
        assert {"restore_foreign_org", "restore_pending"} <= assigned, (
            f"{handler} leaves {{'restore_foreign_org', 'restore_pending'}} - {assigned} set"
        )


def _auth(role: str):
    """An AuthState stand-in. A MagicMock is truthy in every guard and hides the denial."""
    st = MagicMock()
    for name, field in auth_state_module.AuthState.__fields__.items():
        default = field.default_factory() if field.default_factory else field.default
        setattr(st, name, default)
    svc = AuthService(_SECRET)
    st.access_token = svc.create_access_token(1, 10, expires_minutes=10)
    st.refresh_token = svc.create_refresh_token(1)
    st.current_user = UserInfo(id=1, email="a@b.c", full_name="A")
    st.current_org = OrgInfo(id=10, name="Org", slug="org")
    st.current_role = role
    st.action_error = ""
    st.translations = {}
    st._revalidate_session = lambda: auth_state_module.AuthState._revalidate_session(st)
    st._clear_session = lambda: auth_state_module.AuthState._clear_session(st)
    st._get_user_service = lambda: auth_state_module.AuthState._get_user_service(st)
    return st


def _state_as(monkeypatch, role: str) -> BackupState:
    auth = _auth(role)

    async def _get_state(self, state_cls):
        return auth

    monkeypatch.setattr(BackupState, "get_state", _get_state)
    monkeypatch.setattr(auth_state_module.settings, "secret_key", _SECRET)
    return BackupState(parent_state=BaseState(init_substates=False), init_substates=False)


class TestExportDenialIsRuntime:
    async def test_a_viewer_produces_no_backup_and_sets_error_message(self, monkeypatch):
        """core#681. The AST guard is a different regression. This one runs the handler."""
        calls: list = []

        def _export(*args, **kwargs):
            calls.append(kwargs)
            return {"version": 3}

        monkeypatch.setattr(BackupService, "export_backup", _export)
        state = _state_as(monkeypatch, "viewer")

        result = await state.export_backup()

        assert result is None
        assert calls == []
        assert state.error_message, "a viewer export refused nothing the user can see"

    async def test_an_admin_still_reaches_the_export(self, monkeypatch):
        """Without this, the viewer test above is also satisfied by refusing everyone."""
        calls: list = []

        def _export(*args, **kwargs):
            calls.append(kwargs)
            return {"version": 3}

        monkeypatch.setattr(BackupService, "export_backup", _export)
        monkeypatch.setattr(backup_state_module, "EncryptionService", lambda key: object())
        monkeypatch.setattr(
            backup_state_module, "get_sync_session", lambda: contextlib.nullcontext(object())
        )
        downloaded: list = []
        monkeypatch.setattr(
            backup_state_module.rx,
            "download",
            lambda **kwargs: downloaded.append(kwargs) or kwargs,
        )
        state = _state_as(monkeypatch, "admin")

        await state.export_backup()

        assert calls, "an admin was refused, so the viewer denial does not discriminate"
        assert downloaded
        assert state.error_message == ""

    async def test_confirming_a_foreign_restore_passes_the_confirmation(self, monkeypatch):
        captured: dict = {}

        def _import(*args, **kwargs):
            captured.update(kwargs)
            return {
                "connections_imported": 0,
                "uploads_imported": 0,
                "skipped": 0,
                "credentials_required": [],
            }

        monkeypatch.setattr(BackupService, "import_backup", _import)
        monkeypatch.setattr(backup_state_module, "EncryptionService", lambda key: object())
        monkeypatch.setattr(backup_state_module, "ConnectionService", lambda enc: MagicMock())
        monkeypatch.setattr(backup_state_module, "UploadService", lambda conn: MagicMock())
        monkeypatch.setattr(
            backup_state_module, "get_sync_session", lambda: contextlib.nullcontext(MagicMock())
        )
        state = _state_as(monkeypatch, "admin")
        state.restore_data = {
            "version": 3,
            "org": {"id": 9, "name": "Elsewhere", "slug": "elsewhere"},
        }
        state.restore_foreign_org = "Elsewhere"
        state.restore_conflicts = []

        await state.confirm_restore()

        assert captured.get("confirm_foreign_org") is True
