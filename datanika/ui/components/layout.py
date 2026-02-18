"""Sidebar navigation and page wrapper layout."""

import reflex as rx

from datanika.ui.components.language_switcher import language_switcher
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def sidebar_link(text: rx.Var[str], href: str, icon: str) -> rx.Component:
    return rx.link(
        rx.hstack(
            rx.icon(icon, size=18),
            rx.text(text, size="3"),
            spacing="2",
            align="center",
            width="100%",
            padding_x="12px",
            padding_y="8px",
            border_radius="6px",
            _hover={"bg": "var(--gray-a3)"},
        ),
        href=href,
        underline="none",
        width="100%",
    )


def sidebar_user_section() -> rx.Component:
    return rx.vstack(
        rx.separator(),
        language_switcher(),
        sidebar_link(_t["nav.settings"], "/settings", "settings"),
        rx.hstack(
            rx.vstack(
                rx.text(AuthState.current_user.full_name, size="2", weight="medium"),
                rx.text(AuthState.current_org.name, size="1", color="gray"),
                spacing="0",
            ),
            rx.spacer(),
            rx.icon_button(
                rx.icon("log-out", size=16),
                on_click=AuthState.logout,
                variant="ghost",
                size="1",
            ),
            width="100%",
            padding_x="12px",
            padding_y="8px",
            align="center",
        ),
        spacing="1",
        width="100%",
        padding="8px",
    )


def sidebar() -> rx.Component:
    return rx.box(
        rx.vstack(
            rx.heading(_t["app.name"], size="5", padding="16px"),
            rx.separator(),
            rx.vstack(
                sidebar_link(_t["nav.dashboard"], "/", "layout-dashboard"),
                sidebar_link(_t["nav.connections"], "/connections", "plug"),
                sidebar_link(_t["nav.pipelines"], "/pipelines", "git-branch"),
                sidebar_link(_t["nav.transformations"], "/transformations", "code"),
                sidebar_link(_t["nav.schedules"], "/schedules", "clock"),
                sidebar_link(_t["nav.runs"], "/runs", "play"),
                sidebar_link(_t["nav.dependencies"], "/dag", "network"),
                sidebar_link(_t["nav.models"], "/models", "database"),
                spacing="1",
                width="100%",
                padding="8px",
            ),
            rx.spacer(),
            sidebar_user_section(),
            spacing="0",
            height="100vh",
        ),
        width="240px",
        border_right="1px solid var(--gray-a5)",
        bg="var(--gray-a2)",
        position="fixed",
        left="0",
        top="0",
    )


def page_layout(*children, title: rx.Var[str] | str = "") -> rx.Component:
    return rx.box(
        sidebar(),
        rx.box(
            rx.vstack(
                rx.cond(title != "", rx.heading(title, size="6"), rx.fragment()),
                *children,
                spacing="4",
                width="100%",
            ),
            margin_left="240px",
            padding="24px",
            width="calc(100% - 240px)",
        ),
    )
