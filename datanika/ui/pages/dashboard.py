"""Dashboard page — dynamic stats and recent runs."""

import reflex as rx

from etlfabric.ui.components.layout import page_layout
from etlfabric.ui.state.dashboard_state import DashboardState


def _status_color(status: rx.Var[str]) -> rx.Var[str]:
    return rx.cond(
        status == "success",
        "green",
        rx.cond(
            status == "failed",
            "red",
            rx.cond(status == "running", "blue", "gray"),
        ),
    )


def stat_card(title: str, value: rx.Var, icon: str) -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.icon(icon, size=24),
            rx.text(title, weight="bold"),
            rx.heading(value, size="6"),
            align="center",
            spacing="2",
        ),
        width="180px",
    )


def recent_runs_table() -> rx.Component:
    return rx.box(
        rx.heading("Recent Runs", size="4"),
        rx.table.root(
            rx.table.header(
                rx.table.row(
                    rx.table.column_header_cell("ID"),
                    rx.table.column_header_cell("Target"),
                    rx.table.column_header_cell("Status"),
                    rx.table.column_header_cell("Started"),
                    rx.table.column_header_cell("Rows"),
                ),
            ),
            rx.table.body(
                rx.foreach(
                    DashboardState.recent_runs,
                    lambda r: rx.table.row(
                        rx.table.cell(r.id),
                        rx.table.cell(rx.text(f"{r.target_type} #{r.target_id}")),
                        rx.table.cell(
                            rx.badge(r.status, color_scheme=_status_color(r.status)),
                        ),
                        rx.table.cell(r.started_at),
                        rx.table.cell(r.rows_loaded),
                    ),
                ),
            ),
            width="100%",
        ),
        width="100%",
    )


def dashboard_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            rx.card(
                rx.text("Welcome to ETL Fabric", size="4"),
                rx.text(
                    "Manage your data pipelines, transformations, and schedules.",
                    color="gray",
                ),
                width="100%",
            ),
            rx.hstack(
                stat_card(
                    "Pipelines",
                    DashboardState.stats.total_pipelines,
                    "git-branch",
                ),
                stat_card(
                    "Transformations",
                    DashboardState.stats.total_transformations,
                    "code",
                ),
                stat_card(
                    "Schedules",
                    DashboardState.stats.total_schedules,
                    "clock",
                ),
                stat_card(
                    "Runs (OK)",
                    DashboardState.stats.recent_runs_success,
                    "circle_check",
                ),
                stat_card(
                    "Runs (Fail)",
                    DashboardState.stats.recent_runs_failed,
                    "circle_x",
                ),
                spacing="4",
                wrap="wrap",
            ),
            recent_runs_table(),
            spacing="6",
            width="100%",
        ),
        title="Dashboard",
    )
