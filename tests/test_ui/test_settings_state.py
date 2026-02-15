"""Tests for settings-related rx.Base data model classes and SettingsState fields."""

from etlfabric.ui.state.settings_state import MemberItem, SettingsState


class TestMemberItem:
    def test_create_with_fields(self):
        item = MemberItem(
            id=1, user_id=5, email="alice@example.com", full_name="Alice", role="admin"
        )
        assert item.id == 1
        assert item.user_id == 5
        assert item.email == "alice@example.com"
        assert item.full_name == "Alice"
        assert item.role == "admin"

    def test_defaults(self):
        item = MemberItem()
        assert item.id == 0
        assert item.user_id == 0
        assert item.email == ""
        assert item.full_name == ""
        assert item.role == ""


class TestSettingsStateFields:
    def test_org_fields_default(self):
        assert SettingsState.__fields__["org_name"].default == ""
        assert SettingsState.__fields__["org_slug"].default == ""

    def test_members_field_default(self):
        field = SettingsState.__fields__["members"]
        default = field.default_factory() if field.default_factory else field.default
        assert default == []

    def test_invite_form_fields_default(self):
        assert SettingsState.__fields__["invite_email"].default == ""
        assert SettingsState.__fields__["invite_role"].default == "viewer"


class TestSettingsStateDefaults:
    def test_invite_email_default(self):
        assert SettingsState.__fields__["invite_email"].default == ""

    def test_invite_role_default(self):
        assert SettingsState.__fields__["invite_role"].default == "viewer"

    def test_members_empty(self):
        field = SettingsState.__fields__["members"]
        default = field.default_factory() if field.default_factory else field.default
        assert isinstance(default, list)
        assert len(default) == 0
