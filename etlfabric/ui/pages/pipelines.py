"""Pipelines page — list + create form with structured mode fields + run button."""

import reflex as rx

from etlfabric.ui.components.layout import page_layout
from etlfabric.ui.state.pipeline_state import PipelineState


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
                        ),
                        rx.input(
                            placeholder="Initial value (optional)",
                            value=PipelineState.form_initial_value,
                            on_change=PipelineState.set_form_initial_value,
                        ),
                        rx.select(
                            ["", "asc", "desc"],
                            value=PipelineState.form_row_order,
                            on_change=PipelineState.set_form_row_order,
                            placeholder="Row order (optional)",
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
            ),
        ),
    )


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
            # Mode selection
            rx.select(
                ["full_database", "single_table"],
                value=PipelineState.form_mode,
                on_change=PipelineState.set_form_mode,
            ),
            # Write disposition
            rx.select(
                ["append", "replace", "merge"],
                value=PipelineState.form_write_disposition,
                on_change=PipelineState.set_form_write_disposition,
            ),
            # Primary key (merge only)
            rx.cond(
                PipelineState.form_write_disposition == "merge",
                rx.input(
                    placeholder="Primary key (required for merge)",
                    value=PipelineState.form_primary_key,
                    on_change=PipelineState.set_form_primary_key,
                ),
            ),
            # Source schema
            rx.input(
                placeholder="Source schema (optional, e.g. public)",
                value=PipelineState.form_source_schema,
                on_change=PipelineState.set_form_source_schema,
            ),
            # Mode-specific fields
            _mode_fields(),
            # Batch size
            rx.input(
                placeholder="Batch size (optional, default 10000)",
                value=PipelineState.form_batch_size,
                on_change=PipelineState.set_form_batch_size,
            ),
            # Schema contract
            rx.text("Schema Contract (optional)", size="2", weight="bold"),
            rx.hstack(
                rx.select(
                    ["", "evolve", "freeze", "discard_value", "discard_row"],
                    value=PipelineState.form_sc_tables,
                    on_change=PipelineState.set_form_sc_tables,
                    placeholder="Tables",
                ),
                rx.select(
                    ["", "evolve", "freeze", "discard_value", "discard_row"],
                    value=PipelineState.form_sc_columns,
                    on_change=PipelineState.set_form_sc_columns,
                    placeholder="Columns",
                ),
                rx.select(
                    ["", "evolve", "freeze", "discard_value", "discard_row"],
                    value=PipelineState.form_sc_data_type,
                    on_change=PipelineState.set_form_sc_data_type,
                    placeholder="Data type",
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
                ),
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
