"""Dashboard page — dynamic stats and recent runs."""

import reflex as rx

from datanika.ui.components.getting_started_checklist import getting_started_checklist
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
                        rx.table.cell(rx.text(r.target_name)),
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


def _guide_step(title: rx.Var[str], desc: rx.Var[str]) -> rx.Component:
    return rx.box(
        rx.text(title, size="2", weight="bold"),
        rx.text(desc, size="2", color="gray"),
        padding_y="2",
    )


def usage_bar() -> rx.Component:
    return rx.cond(
        DashboardState.has_usage_data,
        rx.card(
            rx.vstack(
                rx.hstack(
                    rx.text(_t["dashboard.usage_title"], weight="bold", size="3"),
                    rx.badge(DashboardState.plan_name, color_scheme="violet", size="1"),
                    align="center",
                    spacing="2",
                ),
                rx.hstack(
                    rx.text(
                        DashboardState.runs_used,
                        " / ",
                        DashboardState.runs_limit,
                        " ",
                        _t["dashboard.usage_runs"],
                        size="2",
                    ),
                    rx.text(
                        DashboardState.runs_percent,
                        "%",
                        size="2",
                        weight="bold",
                        color=rx.cond(
                            DashboardState.runs_percent >= 80,
                            "var(--red-11)",
                            rx.cond(
                                DashboardState.runs_percent >= 60,
                                "var(--amber-11)",
                                "var(--green-11)",
                            ),
                        ),
                    ),
                    justify="between",
                    width="100%",
                ),
                rx.progress(
                    value=DashboardState.runs_percent,
                    max=100,
                    color_scheme=DashboardState.runs_color,
                    width="100%",
                ),
                rx.cond(
                    DashboardState.runs_percent >= 80,
                    rx.hstack(
                        rx.icon("triangle-alert", size=14, color="var(--red-11)"),
                        rx.link(
                            rx.text(
                                _t["dashboard.usage_upgrade"],
                                size="2",
                                color="var(--red-11)",
                                weight="medium",
                            ),
                            href="/settings?tab=billing",
                        ),
                        align="center",
                        spacing="1",
                    ),
                ),
                spacing="2",
                width="100%",
            ),
            width="100%",
        ),
    )


def getting_started_card() -> rx.Component:
    return rx.accordion.root(
        rx.accordion.item(
            header=rx.hstack(
                rx.icon("book-open", size=18),
                rx.text(_t["guide.title"], size="3", weight="bold"),
                align="center",
                spacing="2",
            ),
            content=rx.vstack(
                _guide_step(_t["guide.step1_title"], _t["guide.step1_desc"]),
                _guide_step(_t["guide.step2_title"], _t["guide.step2_desc"]),
                _guide_step(_t["guide.step3_title"], _t["guide.step3_desc"]),
                _guide_step(_t["guide.step4_title"], _t["guide.step4_desc"]),
                _guide_step(_t["guide.step5_title"], _t["guide.step5_desc"]),
                _guide_step(_t["guide.step6_title"], _t["guide.step6_desc"]),
                _guide_step(_t["guide.step7_title"], _t["guide.step7_desc"]),
                rx.link(
                    rx.hstack(
                        rx.icon("external-link", size=14),
                        rx.text(_t["guide.docs_link"], size="2"),
                        align="center",
                        spacing="1",
                    ),
                    href="https://datanika.io/docs",
                    is_external=True,
                    color_scheme="violet",
                    padding_top="2",
                ),
                spacing="1",
                width="100%",
            ),
            value="guide",
        ),
        collapsible=True,
        variant="ghost",
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
            getting_started_checklist(),
            getting_started_card(),
            rx.hstack(
                stat_card(
                    _t["dashboard.uploads"],
                    DashboardState.stats.total_uploads,
                    "upload",
                ),
                stat_card(
                    _t["dashboard.transformations"],
                    DashboardState.stats.total_transformations,
                    "code",
                ),
                stat_card(
                    _t["dashboard.pipelines"],
                    DashboardState.stats.total_pipelines,
                    "git-branch",
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
            usage_bar(),
            recent_runs_table(),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.dashboard"],
    )
