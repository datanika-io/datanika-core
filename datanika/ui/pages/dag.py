"""DAG page — dependency edge table with add/remove."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.dag_state import DagState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def add_dependency_form() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading(_t["dag.add_dependency"], size="4"),
            rx.hstack(
                rx.vstack(
                    rx.text(_t["dag.upstream_type"], size="2"),
                    rx.select(
                        ["upload", "transformation", "pipeline"],
                        value=DagState.form_upstream_type,
                        on_change=DagState.set_form_upstream_type,
                        width="100%",
                    ),
                    spacing="1",
                ),
                rx.vstack(
                    rx.text(_t["dag.upstream_id"], size="2"),
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
                    rx.text(_t["dag.downstream_type"], size="2"),
                    rx.select(
                        ["upload", "transformation", "pipeline"],
                        value=DagState.form_downstream_type,
                        on_change=DagState.set_form_downstream_type,
                        width="100%",
                    ),
                    spacing="1",
                ),
                rx.vstack(
                    rx.text(_t["dag.downstream_id"], size="2"),
                    rx.input(
                        value=DagState.form_downstream_id,
                        on_change=DagState.set_form_downstream_id,
                        placeholder="ID",
                        width="100px",
                    ),
                    spacing="1",
                ),
                rx.button(_t["common.add"], on_click=DagState.add_dependency),
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
                rx.table.column_header_cell(_t["common.id"]),
                rx.table.column_header_cell(_t["dag.upstream"]),
                rx.table.column_header_cell(""),
                rx.table.column_header_cell(_t["dag.downstream"]),
                rx.table.column_header_cell(_t["common.actions"]),
            ),
        ),
        rx.table.body(
            rx.foreach(
                DagState.dependencies,
                lambda d: rx.table.row(
                    rx.table.cell(d.id),
                    rx.table.cell(rx.text(d.upstream_name)),
                    rx.table.cell(rx.icon("arrow-right", size=14)),
                    rx.table.cell(rx.text(d.downstream_name)),
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
        title=_t["nav.dependencies"],
    )
