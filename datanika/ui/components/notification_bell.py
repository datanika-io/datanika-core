"""Notification bell icon + dropdown for the sidebar."""

import reflex as rx

from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.notification_center_state import (
    NotificationCenterState,
)

_t = I18nState.translations

_KEYS_FOR_SCANNER = (
    _t["notifications.center.title"],
    _t["notifications.center.empty"],
    _t["notifications.center.mark_all_read"],
    _t["notifications.run_failed.title"],
    _t["notifications.run_succeeded.title"],
    _t["notifications.quota_warning.title"],
    _t["notifications.quota_warning.body"],
    _t["notifications.quota_warning.metric.model_runs"],
    _t["notifications.quota_warning.metric.bytes_processed"],
    _t["notifications.quota_warning.metric.connections"],
    _t["notifications.quota_warning.metric.schedules"],
    _t["notifications.quota_warning.metric.seats"],
    _t["notifications.quota_exceeded.title"],
    # V2 P5 Option B — cycle-boundary charge notifications (core#249).
    # Used as fallback copy when the stored title/message is empty; the
    # hook stores English by default (cloud emit, no locale context),
    # but ``NotificationCenterState`` can swap to the localized body via
    # the type registry when rendering the bell / notifications page.
    _t["notifications.charge_incoming.title"],
    _t["notifications.charge_incoming.body"],
    _t["notifications.charge_issued.title"],
    _t["notifications.charge_issued.body"],
    _t["notifications.charge_failed.title"],
    _t["notifications.charge_failed.body"],
    _t["notifications.dismiss"],
    _t["notifications.view_run"],
)


def _type_icon(ntype: rx.Var[str]) -> rx.Component:
    return rx.cond(
        ntype == "run_failed",
        rx.icon("circle-x", size=16, color="var(--red-9)"),
        rx.cond(
            ntype == "run_succeeded",
            rx.icon("circle-check", size=16, color="var(--green-9)"),
            rx.cond(
                ntype == "quota_warning",
                rx.icon("triangle-alert", size=16, color="var(--amber-9)"),
                rx.cond(
                    ntype == "charge_incoming",
                    rx.icon("banknote", size=16, color="var(--amber-11)"),
                    rx.icon("octagon-alert", size=16, color="var(--red-9)"),
                ),
            ),
        ),
    )


def _notification_row(n) -> rx.Component:
    return rx.box(
        rx.hstack(
            _type_icon(n.type),
            rx.vstack(
                rx.text(n.title, size="2", weight="medium"),
                rx.cond(
                    n.message != "",
                    rx.text(n.message, size="1", color="var(--slate-10)", trim="end"),
                    rx.fragment(),
                ),
                spacing="0",
                align="start",
            ),
            rx.spacer(),
            rx.hstack(
                rx.link(
                    rx.icon("external-link", size=14, color="var(--violet-9)"),
                    href=rx.cond(
                        n.resource_type == "run",
                        "/runs",
                        rx.cond(
                            (n.resource_type == "subscription") | (n.resource_type == "charge"),
                            "/settings",
                            "/",
                        ),
                    ),
                ),
                rx.icon_button(
                    rx.icon("x", size=12),
                    on_click=NotificationCenterState.dismiss(n.id),
                    variant="ghost",
                    size="1",
                    color_scheme="gray",
                ),
                spacing="1",
                align="center",
            ),
            align="center",
            spacing="2",
            width="100%",
        ),
        on_click=NotificationCenterState.mark_read(n.id),
        padding="8px",
        border_radius="6px",
        cursor="pointer",
        bg=rx.cond(n.read_at == "", "var(--violet-a2)", "transparent"),
        _hover={"bg": "var(--gray-a3)"},
        width="100%",
    )


def _notification_dropdown_content() -> rx.Component:
    return rx.vstack(
        rx.hstack(
            rx.text(
                _t["notifications.center.title"],
                size="2",
                weight="bold",
            ),
            rx.spacer(),
            rx.cond(
                NotificationCenterState.unread_count > 0,
                rx.button(
                    _t["notifications.center.mark_all_read"],
                    on_click=NotificationCenterState.mark_all_read,
                    variant="ghost",
                    size="1",
                    color_scheme="violet",
                ),
                rx.fragment(),
            ),
            width="100%",
            align="center",
        ),
        rx.separator(),
        rx.cond(
            NotificationCenterState.notifications.length() == 0,
            rx.vstack(
                rx.icon("bell-off", size=24, color="var(--slate-8)"),
                rx.text(
                    _t["notifications.center.empty"],
                    size="2",
                    color="var(--slate-10)",
                ),
                align="center",
                padding_y="4",
            ),
            rx.vstack(
                rx.foreach(
                    NotificationCenterState.notifications,
                    _notification_row,
                ),
                spacing="1",
                width="100%",
            ),
        ),
        spacing="2",
        width="100%",
    )


def notification_bell() -> rx.Component:
    return rx.popover.root(
        rx.popover.trigger(
            rx.icon_button(
                rx.box(
                    rx.icon("bell", size=16),
                    rx.cond(
                        NotificationCenterState.unread_count > 0,
                        rx.badge(
                            NotificationCenterState.unread_count,
                            color_scheme="red",
                            variant="solid",
                            size="1",
                            position="absolute",
                            top="-4px",
                            right="-4px",
                        ),
                        rx.fragment(),
                    ),
                    position="relative",
                ),
                on_click=NotificationCenterState.load_notifications,
                variant="ghost",
                size="1",
            ),
        ),
        rx.popover.content(
            _notification_dropdown_content(),
            width="320px",
            max_height="400px",
            side="top",
            align="start",
        ),
    )
