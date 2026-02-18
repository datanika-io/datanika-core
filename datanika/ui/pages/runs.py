"""Runs page — list with status and target_type filters + logs viewer."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.run_state import RunState

_t = I18nState.translations


def filters_bar() -> rx.Component:
    return rx.hstack(
        rx.text(_t["runs.status_filter"], size="2"),
        rx.select(
            ["pending", "running", "success", "failed", "cancelled"],
            value=RunState.filter_status,
            on_change=RunState.set_filter,
            placeholder="All",
            width="100%",
        ),
        rx.text(_t["runs.target_type_filter"], size="2"),
        rx.select(
            ["upload", "transformation", "pipeline"],
            value=RunState.filter_target_type,
            on_change=RunState.set_target_type_filter,
            placeholder="All",
            width="100%",
        ),
        spacing="3",
        align="center",
    )


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


def runs_table() -> rx.Component:
    return rx.table.root(
        rx.table.header(
            rx.table.row(
                rx.table.column_header_cell(_t["common.id"]),
                rx.table.column_header_cell(_t["runs.target"]),
                rx.table.column_header_cell(_t["common.status"]),
                rx.table.column_header_cell(_t["runs.started"]),
                rx.table.column_header_cell(_t["runs.finished"]),
                rx.table.column_header_cell(_t["runs.rows"]),
                rx.table.column_header_cell(_t["runs.error"]),
                rx.table.column_header_cell(_t["runs.logs"]),
            ),
        ),
        rx.table.body(
            rx.foreach(
                RunState.runs,
                lambda r: rx.table.row(
                    rx.table.cell(r.id),
                    rx.table.cell(rx.text(r.target_name)),
                    rx.table.cell(
                        rx.badge(r.status, color_scheme=_status_color(r.status)),
                    ),
                    rx.table.cell(r.started_at),
                    rx.table.cell(r.finished_at),
                    rx.table.cell(r.rows_loaded),
                    rx.table.cell(
                        rx.cond(
                            r.error_message,
                            rx.tooltip(
                                rx.icon("circle_alert", size=16, color="red"),
                                content=r.error_message,
                            ),
                            rx.text(""),
                        ),
                    ),
                    rx.table.cell(
                        rx.icon_button(
                            rx.icon("file_text", size=16),
                            size="1",
                            variant="ghost",
                            on_click=RunState.view_logs(r.id),
                        ),
                    ),
                ),
            ),
        ),
        width="100%",
    )


def logs_panel() -> rx.Component:
    return rx.cond(
        RunState.selected_run_id > 0,
        rx.card(
            rx.vstack(
                rx.hstack(
                    rx.heading(
                        _t["runs.logs_run"],
                        RunState.selected_run_id,
                        size="4",
                    ),
                    rx.spacer(),
                    rx.icon_button(
                        rx.icon("x", size=16),
                        size="1",
                        variant="ghost",
                        on_click=RunState.close_logs,
                    ),
                    width="100%",
                    align="center",
                ),
                rx.code_block(
                    RunState.selected_run_logs,
                    language="log",
                    width="100%",
                    wrap_long_lines=True,
                ),
                spacing="4",
                width="100%",
            ),
            width="100%",
        ),
    )


def runs_page() -> rx.Component:
    return page_layout(
        rx.vstack(filters_bar(), runs_table(), logs_panel(), spacing="6", width="100%"),
        title=_t["nav.runs"],
    )
