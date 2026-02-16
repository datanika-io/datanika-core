"""Transformations page — list + create/edit form + test config."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.transformation_state import TransformationState


def transformation_form() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading(
                rx.cond(
                    TransformationState.editing_transformation_id,
                    "Edit Transformation",
                    "New Transformation",
                ),
                size="4",
            ),
            rx.input(
                placeholder="Model name",
                value=TransformationState.form_name,
                on_change=TransformationState.set_form_name,
                width="100%",
            ),
            rx.input(
                placeholder="Description",
                value=TransformationState.form_description,
                on_change=TransformationState.set_form_description,
                width="100%",
            ),
            rx.text_area(
                placeholder="SELECT * FROM ...",
                value=TransformationState.form_sql_body,
                on_change=TransformationState.set_form_sql_body,
                min_height="120px",
                width="100%",
            ),
            rx.select(
                ["view", "table", "incremental", "ephemeral"],
                value=TransformationState.form_materialization,
                on_change=TransformationState.set_form_materialization,
                width="100%",
            ),
            rx.input(
                placeholder="Schema name (default: staging)",
                value=TransformationState.form_schema_name,
                on_change=TransformationState.set_form_schema_name,
                width="100%",
            ),
            rx.text("Tests Config (JSON)", size="2", weight="bold"),
            rx.text_area(
                placeholder='{"columns": {"id": ["not_null", "unique"]}}',
                value=TransformationState.form_tests_config,
                on_change=TransformationState.set_form_tests_config,
                min_height="80px",
                width="100%",
            ),
            rx.cond(
                TransformationState.error_message,
                rx.callout(
                    TransformationState.error_message,
                    icon="triangle_alert",
                    color_scheme="red",
                ),
            ),
            rx.hstack(
                rx.button(
                    rx.cond(
                        TransformationState.editing_transformation_id,
                        "Save Changes",
                        "Create Transformation",
                    ),
                    on_click=TransformationState.save_transformation,
                ),
                rx.cond(
                    TransformationState.editing_transformation_id,
                    rx.button(
                        "Cancel",
                        variant="outline",
                        on_click=TransformationState.cancel_edit,
                    ),
                ),
                spacing="2",
            ),
            spacing="3",
            width="100%",
        ),
        width="100%",
    )


def transformations_table() -> rx.Component:
    return rx.vstack(
        rx.cond(
            TransformationState.test_result_message,
            rx.callout(
                TransformationState.test_result_message,
                icon="flask-conical",
                color_scheme="blue",
            ),
        ),
        rx.cond(
            TransformationState.preview_sql,
            rx.card(
                rx.vstack(
                    rx.heading("Compiled SQL Preview", size="3"),
                    rx.code_block(
                        TransformationState.preview_sql,
                        language="sql",
                        width="100%",
                    ),
                    spacing="2",
                ),
                width="100%",
            ),
        ),
        rx.table.root(
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
                            rx.hstack(
                                rx.button(
                                    "Edit",
                                    size="1",
                                    variant="outline",
                                    on_click=TransformationState.edit_transformation(t.id),
                                ),
                                rx.button(
                                    "Copy",
                                    size="1",
                                    variant="outline",
                                    on_click=TransformationState.copy_transformation(t.id),
                                ),
                                rx.button(
                                    "Preview SQL",
                                    size="1",
                                    variant="outline",
                                    on_click=TransformationState.preview_compiled_sql(t.id),
                                ),
                                rx.button(
                                    "Run Tests",
                                    size="1",
                                    variant="outline",
                                    on_click=TransformationState.run_tests(t.id),
                                ),
                                rx.button(
                                    "Delete",
                                    color_scheme="red",
                                    size="1",
                                    on_click=TransformationState.delete_transformation(t.id),
                                ),
                                spacing="2",
                            ),
                        ),
                    ),
                ),
            ),
            width="100%",
        ),
        spacing="3",
        width="100%",
    )


def transformations_page() -> rx.Component:
    return page_layout(
        rx.vstack(transformation_form(), transformations_table(), spacing="6", width="100%"),
        title="Transformations",
    )
