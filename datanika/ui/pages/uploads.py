"""Uploads page — list + create/edit form with structured mode fields + run button."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.upload_state import UploadState

_t = I18nState.translations


def _mode_fields() -> rx.Component:
    """Conditional fields that depend on selected mode."""
    return rx.fragment(
        # single_table fields
        rx.cond(
            UploadState.form_mode == "single_table",
            rx.fragment(
                rx.input(
                    placeholder="Table name (e.g. customers)",
                    value=UploadState.form_table,
                    on_change=UploadState.set_form_table,
                    width="100%",
                ),
                rx.checkbox(
                    _t["uploads.enable_incremental"],
                    checked=UploadState.form_enable_incremental,
                    on_change=UploadState.set_form_enable_incremental,
                ),
                rx.cond(
                    UploadState.form_enable_incremental,
                    rx.vstack(
                        rx.input(
                            placeholder="Cursor path (e.g. updated_at)",
                            value=UploadState.form_cursor_path,
                            on_change=UploadState.set_form_cursor_path,
                            width="100%",
                        ),
                        rx.input(
                            placeholder="Initial value (optional)",
                            value=UploadState.form_initial_value,
                            on_change=UploadState.set_form_initial_value,
                            width="100%",
                        ),
                        rx.select(
                            ["asc", "desc"],
                            value=UploadState.form_row_order,
                            on_change=UploadState.set_form_row_order,
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
            UploadState.form_mode == "full_database",
            rx.input(
                placeholder="Table names (comma-separated, optional)",
                value=UploadState.form_table_names,
                on_change=UploadState.set_form_table_names,
                width="100%",
            ),
        ),
    )


def upload_form() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading(
                rx.cond(
                    UploadState.editing_upload_id,
                    _t["uploads.edit"],
                    _t["uploads.new"],
                ),
                size="4",
            ),
            rx.input(
                placeholder="Upload name",
                value=UploadState.form_name,
                on_change=UploadState.set_form_name,
                width="100%",
            ),
            rx.input(
                placeholder="Description",
                value=UploadState.form_description,
                on_change=UploadState.set_form_description,
                width="100%",
            ),
            rx.select(
                UploadState.source_conn_options,
                value=UploadState.form_source_id,
                on_change=UploadState.set_form_source_id,
                placeholder="Source connection",
                width="100%",
            ),
            rx.select(
                UploadState.dest_conn_options,
                value=UploadState.form_dest_id,
                on_change=UploadState.set_form_dest_id,
                placeholder="Destination connection",
                width="100%",
            ),
            # Mode selection
            rx.select(
                ["full_database", "single_table"],
                value=UploadState.form_mode,
                on_change=UploadState.set_form_mode,
                width="100%",
            ),
            # Write disposition
            rx.select(
                ["append", "replace", "merge"],
                value=UploadState.form_write_disposition,
                on_change=UploadState.set_form_write_disposition,
                width="100%",
            ),
            # Primary key (merge only)
            rx.cond(
                UploadState.form_write_disposition == "merge",
                rx.input(
                    placeholder="Primary key (required for merge)",
                    value=UploadState.form_primary_key,
                    on_change=UploadState.set_form_primary_key,
                    width="100%",
                ),
            ),
            # Source schema
            rx.input(
                placeholder="Source schema (optional, e.g. public)",
                value=UploadState.form_source_schema,
                on_change=UploadState.set_form_source_schema,
                width="100%",
            ),
            # Mode-specific fields
            _mode_fields(),
            # Batch size
            rx.input(
                placeholder="Batch size (optional, default 10000)",
                value=UploadState.form_batch_size,
                on_change=UploadState.set_form_batch_size,
                width="100%",
            ),
            # Schema contract
            rx.text(_t["uploads.schema_contract"], size="2", weight="bold"),
            rx.hstack(
                rx.text(_t["uploads.tables"], size="2", weight="bold", width="33%"),
                rx.text(_t["uploads.columns"], size="2", weight="bold", width="33%"),
                rx.text(_t["uploads.data_type"], size="2", weight="bold", width="33%"),
                spacing="2",
                width="100%",
            ),
            rx.hstack(
                rx.select(
                    ["evolve", "freeze", "discard_value", "discard_row"],
                    value=UploadState.form_sc_tables,
                    on_change=UploadState.set_form_sc_tables,
                    placeholder="Tables",
                    width="33%",
                ),
                rx.select(
                    ["evolve", "freeze", "discard_value", "discard_row"],
                    value=UploadState.form_sc_columns,
                    on_change=UploadState.set_form_sc_columns,
                    placeholder="Columns",
                    width="33%",
                ),
                rx.select(
                    ["evolve", "freeze", "discard_value", "discard_row"],
                    value=UploadState.form_sc_data_type,
                    on_change=UploadState.set_form_sc_data_type,
                    placeholder="Data type",
                    width="33%",
                ),
                spacing="2",
                width="100%",
            ),
            # Raw JSON toggle
            rx.checkbox(
                _t["uploads.use_raw_json"],
                checked=UploadState.form_use_raw_json,
                on_change=UploadState.set_form_use_raw_json,
            ),
            rx.cond(
                UploadState.form_use_raw_json,
                rx.text_area(
                    placeholder='{"write_disposition": "append"}',
                    value=UploadState.form_config,
                    on_change=UploadState.set_form_config,
                    width="100%",
                ),
            ),
            rx.cond(
                UploadState.error_message,
                rx.callout(UploadState.error_message, icon="triangle_alert", color_scheme="red"),
            ),
            rx.hstack(
                rx.button(
                    rx.cond(
                        UploadState.editing_upload_id,
                        _t["common.save_changes"],
                        _t["uploads.create"],
                    ),
                    on_click=UploadState.save_upload,
                ),
                rx.cond(
                    UploadState.editing_upload_id,
                    rx.button(
                        _t["common.cancel"],
                        variant="outline",
                        on_click=UploadState.cancel_edit,
                    ),
                ),
                spacing="2",
            ),
            spacing="3",
            width="100%",
        ),
        width="100%",
    )


def uploads_table() -> rx.Component:
    return rx.table.root(
        rx.table.header(
            rx.table.row(
                rx.table.column_header_cell(_t["common.id"]),
                rx.table.column_header_cell(_t["common.name"]),
                rx.table.column_header_cell(_t["common.status"]),
                rx.table.column_header_cell(_t["uploads.source"]),
                rx.table.column_header_cell(_t["uploads.destination"]),
                rx.table.column_header_cell(_t["common.actions"]),
            ),
        ),
        rx.table.body(
            rx.foreach(
                UploadState.uploads,
                lambda u: rx.table.row(
                    rx.table.cell(u.id),
                    rx.table.cell(u.name),
                    rx.table.cell(
                        rx.badge(
                            u.status,
                            color_scheme=rx.cond(u.status == "active", "green", "gray"),
                        ),
                    ),
                    rx.table.cell(u.source_connection_name),
                    rx.table.cell(u.destination_connection_name),
                    rx.table.cell(
                        rx.hstack(
                            rx.button(
                                _t["common.edit"],
                                size="1",
                                variant="outline",
                                on_click=UploadState.edit_upload(u.id),
                            ),
                            rx.button(
                                _t["common.copy"],
                                size="1",
                                variant="outline",
                                on_click=UploadState.copy_upload(u.id),
                            ),
                            rx.button(
                                _t["common.run"],
                                size="1",
                                on_click=UploadState.run_upload(u.id),
                            ),
                            rx.button(
                                _t["common.delete"],
                                color_scheme="red",
                                size="1",
                                on_click=UploadState.delete_upload(u.id),
                            ),
                            spacing="2",
                        ),
                    ),
                ),
            ),
        ),
        width="100%",
    )


def uploads_page() -> rx.Component:
    return page_layout(
        rx.vstack(upload_form(), uploads_table(), spacing="6", width="100%"),
        title=_t["nav.uploads"],
    )
