"""Model detail page — view/edit columns, description, dbt config."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.model_detail_state import ModelDetailState


def _type_color(entry_type: rx.Var[str]) -> rx.Var[str]:
    return rx.cond(entry_type == "source_table", "blue", "purple")


def header_section() -> rx.Component:
    return rx.hstack(
        rx.badge(
            ModelDetailState.entry_type,
            color_scheme=_type_color(ModelDetailState.entry_type),
            size="2",
        ),
        rx.heading(ModelDetailState.table_name, size="5"),
        rx.text(
            f"Schema: {ModelDetailState.schema_name} | Origin: {ModelDetailState.origin_name}",
            color="gray",
            size="2",
        ),
        spacing="3",
        align="center",
        wrap="wrap",
    )


def description_section() -> rx.Component:
    return rx.vstack(
        rx.text("Description", size="3", weight="bold"),
        rx.text_area(
            value=ModelDetailState.form_description,
            on_change=ModelDetailState.set_form_description,
            placeholder="Add a description for this model...",
            width="100%",
            rows="3",
        ),
        spacing="2",
        width="100%",
    )


def config_section() -> rx.Component:
    return rx.vstack(
        rx.text("dbt Config (JSON)", size="3", weight="bold"),
        rx.text_area(
            value=ModelDetailState.form_dbt_config,
            on_change=ModelDetailState.set_form_dbt_config,
            placeholder='{"materialized": "view"}',
            width="100%",
            rows="4",
        ),
        spacing="2",
        width="100%",
    )


def columns_table() -> rx.Component:
    return rx.vstack(
        rx.text("Columns", size="3", weight="bold"),
        rx.table.root(
            rx.table.header(
                rx.table.row(
                    rx.table.column_header_cell("Name"),
                    rx.table.column_header_cell("Data Type"),
                    rx.table.column_header_cell("Description"),
                    rx.table.column_header_cell("Tests"),
                ),
            ),
            rx.table.body(
                rx.foreach(
                    ModelDetailState.columns,
                    lambda col: rx.table.row(
                        rx.table.cell(rx.code(col.name)),
                        rx.table.cell(rx.badge(col.data_type, variant="outline")),
                        rx.table.cell(rx.text(col.description)),
                        rx.table.cell(
                            rx.cond(
                                col.tests.length() > 0,
                                rx.text(col.tests.to_string()),
                                rx.text("-", color="gray"),
                            ),
                        ),
                    ),
                ),
            ),
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def actions_section() -> rx.Component:
    return rx.hstack(
        rx.button(
            "Save",
            on_click=ModelDetailState.save_model_detail,
            color_scheme="blue",
        ),
        rx.link(
            rx.button("Back to Models", variant="outline"),
            href="/models",
        ),
        spacing="3",
    )


def model_detail_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            rx.cond(
                ModelDetailState.error_message,
                rx.callout(
                    ModelDetailState.error_message,
                    icon="alert_triangle",
                    color_scheme="red",
                ),
                rx.fragment(),
            ),
            header_section(),
            rx.separator(),
            description_section(),
            config_section(),
            columns_table(),
            rx.separator(),
            actions_section(),
            spacing="5",
            width="100%",
        ),
        title="Model Detail",
    )
