"""Pipelines page — list + create/edit form with structured mode fields + run button."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.pipeline_state import PipelineState


def _mode_fields() -> rx.Component:
    """Conditional fields that depend on selected mode."""
    return rx.fragment(
        # single_table fields
        rx.cond(
            PipelineState.form_mode == "single_table",
            rx.fragment(
                rx.input(
                    placeholder="Table name (e.g. customers)",
                    value=PipelineState.form_table,
                    on_change=PipelineState.set_form_table,
                    width="100%",
                ),
                rx.checkbox(
                    "Enable incremental loading",
                    checked=PipelineState.form_enable_incremental,
                    on_change=PipelineState.set_form_enable_incremental,
                ),
                rx.cond(
                    PipelineState.form_enable_incremental,
                    rx.vstack(
                        rx.input(
                            placeholder="Cursor path (e.g. updated_at)",
                            value=PipelineState.form_cursor_path,
                            on_change=PipelineState.set_form_cursor_path,
                            width="100%",
                        ),
                        rx.input(
                            placeholder="Initial value (optional)",
                            value=PipelineState.form_initial_value,
                            on_change=PipelineState.set_form_initial_value,
                            width="100%",
                        ),
                        rx.select(
                            ["asc", "desc"],
                            value=PipelineState.form_row_order,
                            on_change=PipelineState.set_form_row_order,
                            placeholder="Row order (optional)",
                            width="100%",
                        ),
                        spacing="2",
                        width="100%",
                    ),
                ),
            ),
        ),
        # full_database fields
        rx.cond(
            PipelineState.form_mode == "full_database",
            rx.input(
                placeholder="Table names (comma-separated, optional)",
                value=PipelineState.form_table_names,
                on_change=PipelineState.set_form_table_names,
                width="100%",
            ),
        ),
    )


def pipeline_form() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading(
                rx.cond(
                    PipelineState.editing_pipeline_id,
                    "Edit Pipeline",
                    "New Pipeline",
                ),
                size="4",
            ),
            rx.input(
                placeholder="Pipeline name",
                value=PipelineState.form_name,
                on_change=PipelineState.set_form_name,
                width="100%",
            ),
            rx.input(
                placeholder="Description",
                value=PipelineState.form_description,
                on_change=PipelineState.set_form_description,
                width="100%",
            ),
            rx.select(
                PipelineState.source_conn_options,
                value=PipelineState.form_source_id,
                on_change=PipelineState.set_form_source_id,
                placeholder="Source connection",
                width="100%",
            ),
            rx.select(
                PipelineState.dest_conn_options,
                value=PipelineState.form_dest_id,
                on_change=PipelineState.set_form_dest_id,
                placeholder="Destination connection",
                width="100%",
            ),
            # Mode selection
            rx.select(
                ["full_database", "single_table"],
                value=PipelineState.form_mode,
                on_change=PipelineState.set_form_mode,
                width="100%",
            ),
            # Write disposition
            rx.select(
                ["append", "replace", "merge"],
                value=PipelineState.form_write_disposition,
                on_change=PipelineState.set_form_write_disposition,
                width="100%",
            ),
            # Primary key (merge only)
            rx.cond(
                PipelineState.form_write_disposition == "merge",
                rx.input(
                    placeholder="Primary key (required for merge)",
                    value=PipelineState.form_primary_key,
                    on_change=PipelineState.set_form_primary_key,
                    width="100%",
                ),
            ),
            # Source schema
            rx.input(
                placeholder="Source schema (optional, e.g. public)",
                value=PipelineState.form_source_schema,
                on_change=PipelineState.set_form_source_schema,
                width="100%",
            ),
            # Mode-specific fields
            _mode_fields(),
            # Batch size
            rx.input(
                placeholder="Batch size (optional, default 10000)",
                value=PipelineState.form_batch_size,
                on_change=PipelineState.set_form_batch_size,
                width="100%",
            ),
            # Schema contract
            rx.text("Schema Contract (optional)", size="2", weight="bold"),
            rx.hstack(
                rx.text("Tables", size="2", weight="bold", width="33%"),
                rx.text("Columns", size="2", weight="bold", width="33%"),
                rx.text("Data Type", size="2", weight="bold", width="33%"),
                spacing="2",
                width="100%",
            ),
            rx.hstack(
                rx.select(
                    ["evolve", "freeze", "discard_value", "discard_row"],
                    value=PipelineState.form_sc_tables,
                    on_change=PipelineState.set_form_sc_tables,
                    placeholder="Tables",
                    width="33%",
                ),
                rx.select(
                    ["evolve", "freeze", "discard_value", "discard_row"],
                    value=PipelineState.form_sc_columns,
                    on_change=PipelineState.set_form_sc_columns,
                    placeholder="Columns",
                    width="33%",
                ),
                rx.select(
                    ["evolve", "freeze", "discard_value", "discard_row"],
                    value=PipelineState.form_sc_data_type,
                    on_change=PipelineState.set_form_sc_data_type,
                    placeholder="Data type",
                    width="33%",
                ),
                spacing="2",
                width="100%",
            ),
            # Raw JSON toggle
            rx.checkbox(
                "Use raw JSON config",
                checked=PipelineState.form_use_raw_json,
                on_change=PipelineState.set_form_use_raw_json,
            ),
            rx.cond(
                PipelineState.form_use_raw_json,
                rx.text_area(
                    placeholder='{"write_disposition": "append"}',
                    value=PipelineState.form_config,
                    on_change=PipelineState.set_form_config,
                    width="100%",
                ),
            ),
            rx.cond(
                PipelineState.error_message,
                rx.callout(PipelineState.error_message, icon="triangle_alert", color_scheme="red"),
            ),
            rx.hstack(
                rx.button(
                    rx.cond(
                        PipelineState.editing_pipeline_id,
                        "Save Changes",
                        "Create Pipeline",
                    ),
                    on_click=PipelineState.save_pipeline,
                ),
                rx.cond(
                    PipelineState.editing_pipeline_id,
                    rx.button(
                        "Cancel",
                        variant="outline",
                        on_click=PipelineState.cancel_edit,
                    ),
                ),
                spacing="2",
            ),
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
                    rx.table.cell(pipe.source_connection_name),
                    rx.table.cell(pipe.destination_connection_name),
                    rx.table.cell(
                        rx.hstack(
                            rx.button(
                                "Edit",
                                size="1",
                                variant="outline",
                                on_click=PipelineState.edit_pipeline(pipe.id),
                            ),
                            rx.button(
                                "Copy",
                                size="1",
                                variant="outline",
                                on_click=PipelineState.copy_pipeline(pipe.id),
                            ),
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
