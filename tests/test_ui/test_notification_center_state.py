"""Tests for NotificationCenterState data models and helper logic."""

from pydantic import BaseModel

from datanika.ui.state.notification_center_state import (
    NOTIFICATION_TYPE_ICONS,
    NotificationCenterState,
    NotificationItem,
)


class TestNotificationItem:
    def test_defaults(self):
        item = NotificationItem()
        assert item.id == 0
        assert item.type == ""
        assert item.title == ""
        assert item.message == ""
        assert item.resource_type == ""
        assert item.resource_id == 0
        assert item.read_at == ""
        assert item.created_at == ""

    def test_is_unread_when_read_at_empty(self):
        item = NotificationItem(read_at="")
        assert item.read_at == ""

    def test_is_read_when_read_at_set(self):
        item = NotificationItem(read_at="2026-04-12T10:00:00Z")
        assert item.read_at != ""


class TestTypeIcons:
    def test_all_four_types_have_icons(self):
        assert "run_failed" in NOTIFICATION_TYPE_ICONS
        assert "run_succeeded" in NOTIFICATION_TYPE_ICONS
        assert "quota_warning" in NOTIFICATION_TYPE_ICONS
        assert "quota_exceeded" in NOTIFICATION_TYPE_ICONS

    def test_icons_are_strings(self):
        for icon in NOTIFICATION_TYPE_ICONS.values():
            assert isinstance(icon, str)
            assert len(icon) > 0

    def test_no_deprecated_lucide_names(self):
        # Regression for #88: lucide-react renamed `alert-triangle` to
        # `triangle-alert` in v0.359.0 (March 2024). Reflex emits a
        # deprecation warning at compile time for the old name. Pin
        # the new name so we don't drift back.
        deprecated = {"alert-triangle"}
        for type_name, icon in NOTIFICATION_TYPE_ICONS.items():
            assert icon not in deprecated, (
                f"{type_name} uses deprecated lucide icon {icon!r}; use the new name"
            )

    def test_quota_warning_uses_triangle_alert(self):
        assert NOTIFICATION_TYPE_ICONS["quota_warning"] == "triangle-alert"


class TestNotificationCenterStateStructure:
    def test_has_required_fields(self):
        state_fields = {name for name, _ in NotificationCenterState.__annotations__.items()} | set(
            dir(NotificationCenterState)
        )
        assert "notifications" in state_fields
        assert "unread_count" in state_fields

    def test_has_required_methods(self):
        assert callable(getattr(NotificationCenterState, "load_notifications", None))
        assert callable(getattr(NotificationCenterState, "load_unread_count", None))
        assert callable(getattr(NotificationCenterState, "mark_read", None))
        assert callable(getattr(NotificationCenterState, "mark_all_read", None))
        assert callable(getattr(NotificationCenterState, "dismiss", None))

    def test_notification_item_is_base_model(self):
        assert issubclass(NotificationItem, BaseModel)
