"""Transformations page — list + create form."""

import reflex as rx

from etlfabric.ui.components.layout import page_layout
from etlfabric.ui.state.transformation_state import TransformationState


def transformation_form() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading("New Transformation", size="4"),
            rx.input(
                placeholder="Model name",
                value=TransformationState.form_name,
                on_change=TransformationState.set_form_name,
            ),
            rx.input(
                placeholder="Description",
                value=TransformationState.form_description,
                on_change=TransformationState.set_form_description,
            ),
            rx.text_area(
                placeholder="SELECT * FROM ...",
                value=TransformationState.form_sql_body,
                on_change=TransformationState.set_form_sql_body,
                min_height="120px",
            ),
            rx.select(
                ["view", "table", "incremental", "ephemeral"],
                value=TransformationState.form_materialization,
                on_change=TransformationState.set_form_materialization,
            ),
            rx.input(
                placeholder="Schema name (default: staging)",
                value=TransformationState.form_schema_name,
                on_change=TransformationState.set_form_schema_name,
            ),
            rx.cond(
                TransformationState.error_message,
                rx.callout(
                    TransformationState.error_message,
                    icon="alert-triangle",
                    color_scheme="red",
                ),
            ),
            rx.button(
                "Create Transformation",
                on_click=TransformationState.create_transformation,
            ),
            spacing="3",
            width="100%",
        ),
        width="100%",
    )


def transformations_table() -> rx.Component:
    return rx.table.root(
        rx.table.header(
            rx.table.row(
                rx.table.column_header_cell("ID"),
                rx.table.column_header_cell("Name"),
                rx.table.column_header_cell("Materialization"),
                rx.table.column_header_cell("Schema"),
                rx.table.column_header_cell("Actions"),
            ),
        ),
        rx.table.body(
            rx.foreach(
                TransformationState.transformations,
                lambda t: rx.table.row(
                    rx.table.cell(t.id),
                    rx.table.cell(t.name),
                    rx.table.cell(rx.badge(t.materialization)),
                    rx.table.cell(t.schema_name),
                    rx.table.cell(
                        rx.button(
                            "Delete",
                            color_scheme="red",
                            size="1",
                            on_click=TransformationState.delete_transformation(t.id),
                        ),
                    ),
                ),
            ),
        ),
        width="100%",
    )


def transformations_page() -> rx.Component:
    return page_layout(
        rx.vstack(transformation_form(), transformations_table(), spacing="6", width="100%"),
        title="Transformations",
    )
