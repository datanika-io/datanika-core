"""Runs page — list with status and target_type filters + logs viewer."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.run_state import RunState


def filters_bar() -> rx.Component:
    return rx.hstack(
        rx.text("Status:", size="2"),
        rx.select(
            ["pending", "running", "success", "failed", "cancelled"],
            value=RunState.filter_status,
            on_change=RunState.set_filter,
            placeholder="All",
            width="100%",
        ),
        rx.text("Target type:", size="2"),
        rx.select(
            ["pipeline", "transformation"],
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
                rx.table.column_header_cell("ID"),
                rx.table.column_header_cell("Target"),
                rx.table.column_header_cell("Status"),
                rx.table.column_header_cell("Started"),
                rx.table.column_header_cell("Finished"),
                rx.table.column_header_cell("Rows"),
                rx.table.column_header_cell("Error"),
                rx.table.column_header_cell("Logs"),
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
                        f"Logs — Run #{RunState.selected_run_id}",
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
        title="Runs",
    )
