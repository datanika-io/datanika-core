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
import inspect

import pytest

import datanika.ui.state.backup_state as backup_state_module
from datanika.ui.state.backup_state import BackupState


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
