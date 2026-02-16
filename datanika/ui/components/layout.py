"""Sidebar navigation and page wrapper layout."""

import reflex as rx

from etlfabric.ui.state.auth_state import AuthState


def sidebar_link(text: str, href: str, icon: str) -> rx.Component:
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
        sidebar_link("Settings", "/settings", "settings"),
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
            rx.heading("ETL Fabric", size="5", padding="16px"),
            rx.separator(),
            rx.vstack(
                sidebar_link("Dashboard", "/", "layout-dashboard"),
                sidebar_link("Connections", "/connections", "plug"),
                sidebar_link("Pipelines", "/pipelines", "git-branch"),
                sidebar_link("Transformations", "/transformations", "code"),
                sidebar_link("Schedules", "/schedules", "clock"),
                sidebar_link("Runs", "/runs", "play"),
                sidebar_link("Dependencies", "/dag", "network"),
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


def page_layout(*children, title: str = "") -> rx.Component:
    return rx.box(
        sidebar(),
        rx.box(
            rx.vstack(
                rx.heading(title, size="6") if title else rx.fragment(),
                *children,
                spacing="4",
                width="100%",
            ),
            margin_left="240px",
            padding="24px",
            width="calc(100% - 240px)",
        ),
    )
