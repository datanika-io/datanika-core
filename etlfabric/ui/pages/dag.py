"""DAG page — dependency edge table with add/remove."""

import reflex as rx

from etlfabric.ui.components.layout import page_layout
from etlfabric.ui.state.dag_state import DagState


def add_dependency_form() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading("Add Dependency", size="4"),
            rx.hstack(
                rx.vstack(
                    rx.text("Upstream Type", size="2"),
                    rx.select(
                        ["pipeline", "transformation"],
                        value=DagState.form_upstream_type,
                        on_change=DagState.set_form_upstream_type,
                    ),
                    spacing="1",
                ),
                rx.vstack(
                    rx.text("Upstream ID", size="2"),
                    rx.input(
                        value=DagState.form_upstream_id,
                        on_change=DagState.set_form_upstream_id,
                        placeholder="ID",
                        width="100px",
                    ),
                    spacing="1",
                ),
                rx.icon("arrow-right", size=20),
                rx.vstack(
                    rx.text("Downstream Type", size="2"),
                    rx.select(
                        ["pipeline", "transformation"],
                        value=DagState.form_downstream_type,
                        on_change=DagState.set_form_downstream_type,
                    ),
                    spacing="1",
                ),
                rx.vstack(
                    rx.text("Downstream ID", size="2"),
                    rx.input(
                        value=DagState.form_downstream_id,
                        on_change=DagState.set_form_downstream_id,
                        placeholder="ID",
                        width="100px",
                    ),
                    spacing="1",
                ),
                rx.button("Add", on_click=DagState.add_dependency),
                spacing="3",
                align="end",
            ),
            rx.cond(
                DagState.error_message,
                rx.callout(DagState.error_message, icon="triangle_alert", color_scheme="red"),
                rx.fragment(),
            ),
            spacing="3",
            width="100%",
        ),
        width="100%",
    )


def dependency_table() -> rx.Component:
    return rx.table.root(
        rx.table.header(
            rx.table.row(
                rx.table.column_header_cell("ID"),
                rx.table.column_header_cell("Upstream"),
                rx.table.column_header_cell(""),
                rx.table.column_header_cell("Downstream"),
                rx.table.column_header_cell("Actions"),
            ),
        ),
        rx.table.body(
            rx.foreach(
                DagState.dependencies,
                lambda d: rx.table.row(
                    rx.table.cell(d.id),
                    rx.table.cell(
                        rx.text(f"{d.upstream_type} #{d.upstream_id}"),
                    ),
                    rx.table.cell(rx.icon("arrow-right", size=14)),
                    rx.table.cell(
                        rx.text(f"{d.downstream_type} #{d.downstream_id}"),
                    ),
                    rx.table.cell(
                        rx.button(
                            rx.icon("trash-2", size=14),
                            on_click=DagState.remove_dependency(d.id),
                            variant="ghost",
                            color_scheme="red",
                            size="1",
                        ),
                    ),
                ),
            ),
        ),
        width="100%",
    )


def dag_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            add_dependency_form(),
            dependency_table(),
            spacing="6",
            width="100%",
        ),
        title="Dependencies",
    )
