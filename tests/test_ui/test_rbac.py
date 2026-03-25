"""Tests for RBAC role enforcement."""

from datanika.ui.state.base_state import ROLE_HIERARCHY, check_role_hierarchy


class TestRoleHierarchy:
    """Test the role hierarchy check logic."""

    def test_owner_can_do_everything(self):
        assert check_role_hierarchy("owner", "viewer")
        assert check_role_hierarchy("owner", "editor")
        assert check_role_hierarchy("owner", "admin")
        assert check_role_hierarchy("owner", "owner")

    def test_admin_cannot_do_owner_actions(self):
        assert check_role_hierarchy("admin", "viewer")
        assert check_role_hierarchy("admin", "editor")
        assert check_role_hierarchy("admin", "admin")
        assert not check_role_hierarchy("admin", "owner")

    def test_editor_cannot_do_admin_actions(self):
        assert check_role_hierarchy("editor", "viewer")
        assert check_role_hierarchy("editor", "editor")
        assert not check_role_hierarchy("editor", "admin")
        assert not check_role_hierarchy("editor", "owner")

    def test_viewer_can_only_view(self):
        assert check_role_hierarchy("viewer", "viewer")
        assert not check_role_hierarchy("viewer", "editor")
        assert not check_role_hierarchy("viewer", "admin")
        assert not check_role_hierarchy("viewer", "owner")

    def test_empty_role_denied(self):
        assert not check_role_hierarchy("", "viewer")

    def test_unknown_role_denied(self):
        assert not check_role_hierarchy("superadmin", "viewer")

    def test_hierarchy_values(self):
        assert ROLE_HIERARCHY["owner"] > ROLE_HIERARCHY["admin"]
        assert ROLE_HIERARCHY["admin"] > ROLE_HIERARCHY["editor"]
        assert ROLE_HIERARCHY["editor"] > ROLE_HIERARCHY["viewer"]
