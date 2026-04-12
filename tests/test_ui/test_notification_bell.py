"""Regression tests for the notification bell component (#78).

The bell dropdown was originally a hand-rolled `rx.box` toggled by a
`dropdown_open` state field, with no outside-click or escape handler —
users reported the popup never dismissed. The fix converts it to a
`rx.popover.root`, which Radix wires to outside-click + escape natively.

These tests guard against regressing back to the hand-rolled pattern.
"""

import reflex as rx

from datanika.ui.components.notification_bell import notification_bell
from datanika.ui.state.notification_center_state import NotificationCenterState


class TestNotificationBellPopover:
    def test_returns_radix_popover_root(self):
        component = notification_bell()
        # Radix popover root — Reflex wraps it in a class whose tag identifies it.
        assert "popover" in type(component).__name__.lower() or any(
            "popover" in type(child).__name__.lower()
            for child in getattr(component, "children", [])
        ), "notification_bell must use rx.popover.root so Radix handles outside-click + escape"

    def test_state_has_no_dropdown_open_field(self):
        # Radix manages open/close state internally — we must not reintroduce
        # a Python-side `dropdown_open` flag, which previously caused the
        # outside-click bug because nothing flipped it back to False.
        state_attrs = set(NotificationCenterState.__annotations__.keys()) | set(
            dir(NotificationCenterState)
        )
        assert "dropdown_open" not in state_attrs, (
            "dropdown_open must not exist; let rx.popover.root manage open state"
        )

    def test_state_has_no_toggle_dropdown_method(self):
        assert not hasattr(NotificationCenterState, "toggle_dropdown"), (
            "toggle_dropdown must not exist; rx.popover.trigger handles toggling"
        )

    def test_renders_without_error(self):
        component = notification_bell()
        assert component is not None
        assert isinstance(component, rx.Component)
