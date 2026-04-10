"""Tests for RBAC enforcement across all UI state handlers.

Verifies that every mutating state method checks the correct minimum role
before proceeding. Uses source code inspection to ensure role checks
aren't accidentally removed or weakened.
"""

import ast
import inspect
import re

import pytest

from datanika.ui.state.base_state import ROLE_HIERARCHY, check_role_hierarchy


# ---------------------------------------------------------------------------
# Source-level verification: every mutating handler has _check_role
# ---------------------------------------------------------------------------

def _extract_check_role_calls(module_path: str) -> dict[str, str]:
    """Parse a Python file and return {method_name: required_role} for _check_role calls."""
    with open(module_path, encoding="utf-8") as f:
        source = f.read()
    tree = ast.parse(source)

    results = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            for child in ast.walk(node):
                if (
                    isinstance(child, ast.Call)
                    and isinstance(child.func, ast.Attribute)
                    and child.func.attr == "_check_role"
                    and child.args
                    and isinstance(child.args[0], ast.Constant)
                ):
                    results[node.name] = child.args[0].value
    return results


# Expected role requirements per state file
EXPECTED_ROLES = {
    "connection_state": {
        "save_connection": "editor",
        "delete_connection": "admin",
    },
    "upload_state": {
        "save_upload": "editor",
        "delete_upload": "admin",
        "run_upload": "editor",
    },
    "pipeline_state": {
        "save_pipeline": "editor",
        "delete_pipeline": "admin",
        "run_pipeline": "editor",
    },
    "transformation_state": {
        "save_transformation": "editor",
        "delete_transformation": "admin",
    },
    "schedule_state": {
        "save_schedule": "editor",
        "toggle_schedule": "editor",
        "delete_schedule": "admin",
    },
    "settings_state": {
        "update_org": "owner",
        "add_member_by_email": "admin",
        "remove_member": "admin",
        "change_member_role": "admin",
        "cancel_invitation": "admin",
    },
    "api_key_state": {
        "create_api_key": "admin",
        "revoke_api_key": "admin",
    },
    "backup_state": {
        "handle_restore_upload": "admin",
    },
    "notification_state": {
        "save_channel": "admin",
        "delete_channel": "admin",
        "toggle_channel_active": "admin",
    },
}


class TestRoleEnforcementInSource:
    """Verify that state handlers have the expected _check_role calls
    by parsing the source code. This catches accidental removal of
    role checks during refactoring."""

    @pytest.mark.parametrize(
        "state_module,expected",
        list(EXPECTED_ROLES.items()),
        ids=list(EXPECTED_ROLES.keys()),
    )
    def test_state_has_correct_role_checks(self, state_module, expected):
        mod = __import__(
            f"datanika.ui.state.{state_module}",
            fromlist=[state_module],
        )
        source_file = inspect.getfile(mod)
        actual = _extract_check_role_calls(source_file)

        for method, required_role in expected.items():
            assert method in actual, (
                f"{state_module}.{method} is missing _check_role() call"
            )
            assert actual[method] == required_role, (
                f"{state_module}.{method} requires '{actual[method]}' "
                f"but expected '{required_role}'"
            )


# ---------------------------------------------------------------------------
# Role hierarchy edge cases
# ---------------------------------------------------------------------------

class TestRoleHierarchyEdgeCases:
    """Additional edge case tests for role hierarchy checks."""

    def test_all_roles_defined(self):
        """Ensure all four roles exist in the hierarchy."""
        assert set(ROLE_HIERARCHY.keys()) == {"owner", "admin", "editor", "viewer"}

    def test_hierarchy_is_strict(self):
        """Each role has a strictly higher value than the one below."""
        assert ROLE_HIERARCHY["owner"] > ROLE_HIERARCHY["admin"]
        assert ROLE_HIERARCHY["admin"] > ROLE_HIERARCHY["editor"]
        assert ROLE_HIERARCHY["editor"] > ROLE_HIERARCHY["viewer"]
        assert ROLE_HIERARCHY["viewer"] > 0

    def test_same_role_is_sufficient(self):
        for role in ROLE_HIERARCHY:
            assert check_role_hierarchy(role, role)

    def test_viewer_blocked_from_all_write_roles(self):
        for role in ("editor", "admin", "owner"):
            assert not check_role_hierarchy("viewer", role)

    def test_editor_blocked_from_delete_and_manage(self):
        assert not check_role_hierarchy("editor", "admin")
        assert not check_role_hierarchy("editor", "owner")

    def test_admin_blocked_from_owner(self):
        assert not check_role_hierarchy("admin", "owner")

    def test_none_role_denied(self):
        assert not check_role_hierarchy(None, "viewer")

    def test_case_sensitive(self):
        assert not check_role_hierarchy("Editor", "editor")
        assert not check_role_hierarchy("ADMIN", "admin")


# ---------------------------------------------------------------------------
# Permission matrix: which roles can do which operations
# ---------------------------------------------------------------------------

class TestPermissionMatrix:
    """Verify the intended permission matrix from the engineering plan."""

    @pytest.mark.parametrize("role", ["owner", "admin", "editor", "viewer"])
    def test_all_roles_can_view(self, role):
        assert check_role_hierarchy(role, "viewer")

    @pytest.mark.parametrize("role", ["owner", "admin", "editor"])
    def test_editor_and_above_can_create(self, role):
        assert check_role_hierarchy(role, "editor")

    def test_viewer_cannot_create(self):
        assert not check_role_hierarchy("viewer", "editor")

    @pytest.mark.parametrize("role", ["owner", "admin"])
    def test_admin_and_above_can_delete(self, role):
        assert check_role_hierarchy(role, "admin")

    def test_editor_cannot_delete(self):
        assert not check_role_hierarchy("editor", "admin")

    def test_only_owner_can_delete_org(self):
        assert check_role_hierarchy("owner", "owner")
        assert not check_role_hierarchy("admin", "owner")
