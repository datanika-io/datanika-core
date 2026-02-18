"""Dashboard page — dynamic stats and recent runs."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.dashboard_state import DashboardState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


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


def stat_card(title: rx.Var[str], value: rx.Var, icon: str) -> rx.Component:
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
        rx.heading(_t["dashboard.recent_runs"], size="4"),
        rx.table.root(
            rx.table.header(
                rx.table.row(
                    rx.table.column_header_cell(_t["common.id"]),
                    rx.table.column_header_cell(_t["dashboard.target"]),
                    rx.table.column_header_cell(_t["common.status"]),
                    rx.table.column_header_cell(_t["dashboard.started"]),
                    rx.table.column_header_cell(_t["dashboard.rows"]),
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
                rx.text(_t["dashboard.welcome"], size="4"),
                rx.text(
                    _t["dashboard.welcome_sub"],
                    color="gray",
                ),
                width="100%",
            ),
            rx.hstack(
                stat_card(
                    _t["dashboard.pipelines"],
                    DashboardState.stats.total_pipelines,
                    "git-branch",
                ),
                stat_card(
                    _t["dashboard.transformations"],
                    DashboardState.stats.total_transformations,
                    "code",
                ),
                stat_card(
                    _t["dashboard.schedules"],
                    DashboardState.stats.total_schedules,
                    "clock",
                ),
                stat_card(
                    _t["dashboard.runs_ok"],
                    DashboardState.stats.recent_runs_success,
                    "circle_check",
                ),
                stat_card(
                    _t["dashboard.runs_fail"],
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
        title=_t["nav.dashboard"],
    )
