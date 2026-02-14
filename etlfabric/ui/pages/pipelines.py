"""Pipelines page — list + create form + run button."""

import reflex as rx

from etlfabric.ui.components.layout import page_layout
from etlfabric.ui.state.pipeline_state import PipelineState


def pipeline_form() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading("New Pipeline", size="4"),
            rx.input(
                placeholder="Pipeline name",
                value=PipelineState.form_name,
                on_change=PipelineState.set_form_name,
            ),
            rx.input(
                placeholder="Description",
                value=PipelineState.form_description,
                on_change=PipelineState.set_form_description,
            ),
            rx.input(
                placeholder="Source connection ID",
                value=PipelineState.form_source_id,
                on_change=PipelineState.set_form_source_id,
            ),
            rx.input(
                placeholder="Destination connection ID",
                value=PipelineState.form_dest_id,
                on_change=PipelineState.set_form_dest_id,
            ),
            rx.text_area(
                placeholder='{"write_disposition": "append"}',
                value=PipelineState.form_config,
                on_change=PipelineState.set_form_config,
            ),
            rx.cond(
                PipelineState.error_message,
                rx.callout(PipelineState.error_message, icon="alert-triangle", color_scheme="red"),
            ),
            rx.button("Create Pipeline", on_click=PipelineState.create_pipeline),
            spacing="3",
            width="100%",
        ),
        width="100%",
    )


def pipelines_table() -> rx.Component:
    return rx.table.root(
        rx.table.header(
            rx.table.row(
                rx.table.column_header_cell("ID"),
                rx.table.column_header_cell("Name"),
                rx.table.column_header_cell("Status"),
                rx.table.column_header_cell("Source"),
                rx.table.column_header_cell("Destination"),
                rx.table.column_header_cell("Actions"),
            ),
        ),
        rx.table.body(
            rx.foreach(
                PipelineState.pipelines,
                lambda pipe: rx.table.row(
                    rx.table.cell(pipe.id),
                    rx.table.cell(pipe.name),
                    rx.table.cell(
                        rx.badge(
                            pipe.status,
                            color_scheme=rx.cond(pipe.status == "active", "green", "gray"),
                        ),
                    ),
                    rx.table.cell(pipe.source_connection_id),
                    rx.table.cell(pipe.destination_connection_id),
                    rx.table.cell(
                        rx.hstack(
                            rx.button(
                                "Run",
                                size="1",
                                on_click=PipelineState.run_pipeline(pipe.id),
                            ),
                            rx.button(
                                "Delete",
                                color_scheme="red",
                                size="1",
                                on_click=PipelineState.delete_pipeline(pipe.id),
                            ),
                            spacing="2",
                        ),
                    ),
                ),
            ),
        ),
        width="100%",
    )


def pipelines_page() -> rx.Component:
    return page_layout(
        rx.vstack(pipeline_form(), pipelines_table(), spacing="6", width="100%"),
        title="Pipelines",
    )
