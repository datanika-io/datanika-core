"""Sidebar navigation and page wrapper layout."""

import reflex as rx


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
