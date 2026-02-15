"""Runs page — list with status and target_type filters."""

import reflex as rx

from etlfabric.ui.components.layout import page_layout
from etlfabric.ui.state.run_state import RunState


def filters_bar() -> rx.Component:
    return rx.hstack(
        rx.text("Status:", size="2"),
        rx.select(
            ["pending", "running", "success", "failed", "cancelled"],
            value=RunState.filter_status,
            on_change=RunState.set_filter,
            placeholder="All",
        ),
        rx.text("Target type:", size="2"),
        rx.select(
            ["pipeline", "transformation"],
            value=RunState.filter_target_type,
            on_change=RunState.set_target_type_filter,
            placeholder="All",
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
            ),
        ),
        rx.table.body(
            rx.foreach(
                RunState.runs,
                lambda r: rx.table.row(
                    rx.table.cell(r.id),
                    rx.table.cell(
                        rx.text(f"{r.target_type} #{r.target_id}"),
                    ),
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
                ),
            ),
        ),
        width="100%",
    )


def runs_page() -> rx.Component:
    return page_layout(
        rx.vstack(filters_bar(), runs_table(), spacing="6", width="100%"),
        title="Runs",
    )
