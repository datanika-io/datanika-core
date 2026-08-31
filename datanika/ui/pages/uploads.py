"""Uploads page — list + create/edit form with structured mode fields + run button."""

import reflex as rx

from datanika.ui.components.info_tooltip import info_tooltip
from datanika.ui.components.layout import page_layout
from datanika.ui.components.quota_callout import error_or_quota_callout
from datanika.ui.components.searchable_select import searchable_select
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.upload_state import UploadState

_t = I18nState.translations


def _run_button_color(status: rx.Var[str]) -> rx.Var[str]:
    return rx.cond(
        (status == "running") | (status == "pending"),
        "yellow",
        "gray",
    )


def _mode_fields() -> rx.Component:
    """Conditional fields that depend on selected mode."""
    return rx.fragment(
        # single_table fields
        rx.cond(
            UploadState.form_mode == "single_table",
            rx.fragment(
                rx.input(
                    placeholder=_t["uploads.ph_table_name"],
                    value=UploadState.form_table,
                    on_change=UploadState.set_form_table,
                    width="100%",
                ),
                rx.hstack(
                    rx.checkbox(
                        _t["uploads.enable_incremental"],
                        checked=UploadState.form_enable_incremental,
                        on_change=UploadState.set_form_enable_incremental,
                    ),
                    info_tooltip("tooltip.incremental_cursor"),
                    align="center",
                    spacing="1",
                ),
                rx.cond(
                    UploadState.form_enable_incremental,
                    rx.vstack(
                        rx.input(
                            placeholder=_t["uploads.ph_cursor_path"],
                            value=UploadState.form_cursor_path,
                            on_change=UploadState.set_form_cursor_path,
                            width="100%",
                        ),
                        rx.input(
                            placeholder=_t["uploads.ph_initial_value"],
                            value=UploadState.form_initial_value,
                            on_change=UploadState.set_form_initial_value,
                            width="100%",
                        ),
                        rx.select(
                            ["asc", "desc"],
                            value=UploadState.form_row_order,
                            on_change=UploadState.set_form_row_order,
                            placeholder=_t["uploads.ph_row_order"],
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
                placeholder=_t["uploads.ph_table_names"],
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
                placeholder=_t["uploads.ph_name"],
                value=UploadState.form_name,
                on_change=UploadState.set_form_name,
                width="100%",
            ),
            rx.input(
                placeholder=_t["uploads.ph_description"],
                value=UploadState.form_description,
                on_change=UploadState.set_form_description,
                width="100%",
            ),
            searchable_select(
                UploadState.source_conn_options,
                value=UploadState.form_source_id,
                on_change=UploadState.set_form_source_id,
                placeholder=_t["uploads.ph_source"],
                width="100%",
            ),
            searchable_select(
                UploadState.dest_conn_options,
                value=UploadState.form_dest_id,
                on_change=UploadState.set_form_dest_id,
                placeholder=_t["uploads.ph_destination"],
                width="100%",
            ),
            # SaaS endpoint selector (shown for SaaS sources)
            rx.cond(
                UploadState.form_is_saas_source,
                rx.vstack(
                    rx.text(_t["uploads.select_endpoints"], size="2", weight="bold"),
                    rx.foreach(
                        UploadState.form_available_endpoints,
                        lambda ep: rx.hstack(
                            rx.checkbox(
                                ep,
                                checked=UploadState.form_selected_endpoints.contains(ep),
                                on_change=lambda _val: UploadState.toggle_endpoint(ep),
                            ),
                            spacing="2",
                        ),
                    ),
                    spacing="2",
                    width="100%",
                ),
            ),
            # Google Sheets: sheet names
            rx.cond(
                UploadState.form_source_id.contains("google_sheets"),
                rx.vstack(
                    rx.text(_t["uploads.sheet_names"], size="2", weight="bold"),
                    rx.input(
                        placeholder=_t["uploads.ph_sheet_names"],
                        value=UploadState.form_sheet_names,
                        on_change=UploadState.set_form_sheet_names,
                        width="100%",
                    ),
                    spacing="2",
                    width="100%",
                ),
            ),
            # MongoDB: collection names
            rx.cond(
                UploadState.form_source_id.contains("mongodb"),
                rx.vstack(
                    rx.text(_t["uploads.collection_names"], size="2", weight="bold"),
                    rx.input(
                        placeholder=_t["uploads.ph_collection_names"],
                        value=UploadState.form_collection_names,
                        on_change=UploadState.set_form_collection_names,
                        width="100%",
                    ),
                    spacing="2",
                    width="100%",
                ),
            ),
            # S3 source: file glob pattern (CSV/JSON/Parquet use uploaded files, no glob needed)
            rx.cond(
                UploadState.form_source_id.contains("(s3)"),
                rx.vstack(
                    rx.text(_t["uploads.file_glob"], size="2", weight="bold"),
                    rx.input(
                        placeholder=_t["uploads.ph_file_glob"],
                        value=UploadState.form_file_glob,
                        on_change=UploadState.set_form_file_glob,
                        width="100%",
                    ),
                    spacing="2",
                    width="100%",
                ),
            ),
            # File sources: format knobs the runner reads from dlt_config.
            # Before core#499 these were reachable only through raw JSON config,
            # so a semicolon CSV loaded as one fused column and an s3 connection
            # on the default `*` glob failed with an error naming a setting the
            # user had no way to set.
            rx.cond(
                UploadState.form_is_file_source,
                rx.vstack(
                    rx.text(_t["uploads.file_format"], size="2", weight="bold"),
                    rx.select(
                        ["auto", "csv", "json", "parquet"],
                        value=UploadState.form_file_format,
                        on_change=UploadState.set_form_file_format,
                        placeholder=_t["uploads.ph_file_format"],
                        width="100%",
                    ),
                    rx.text(_t["uploads.delimiter"], size="2", weight="bold"),
                    rx.input(
                        placeholder=_t["uploads.ph_delimiter"],
                        value=UploadState.form_delimiter,
                        on_change=UploadState.set_form_delimiter,
                        width="100%",
                    ),
                    rx.text(_t["uploads.encoding"], size="2", weight="bold"),
                    rx.input(
                        placeholder=_t["uploads.ph_encoding"],
                        value=UploadState.form_encoding,
                        on_change=UploadState.set_form_encoding,
                        width="100%",
                    ),
                    spacing="2",
                    width="100%",
                ),
            ),
            # SQL-specific fields (hidden for non-SQL sources)
            rx.cond(
                ~UploadState.form_is_non_sql_source,
                rx.vstack(
                    rx.hstack(
                        rx.text(_t["uploads.load_mode"], size="2", weight="bold"),
                        info_tooltip("tooltip.load_mode"),
                        align="center",
                        spacing="1",
                    ),
                    rx.select(
                        ["full_database", "single_table"],
                        value=UploadState.form_mode,
                        on_change=UploadState.set_form_mode,
                        width="100%",
                    ),
                    rx.hstack(
                        rx.text(_t["uploads.write_disposition"], size="2", weight="bold"),
                        info_tooltip("tooltip.write_disposition"),
                        align="center",
                        spacing="1",
                    ),
                    rx.select(
                        ["append", "replace", "merge"],
                        value=UploadState.form_write_disposition,
                        on_change=UploadState.set_form_write_disposition,
                        width="100%",
                    ),
                    # Primary key (merge + single_table only)
                    rx.cond(
                        (UploadState.form_write_disposition == "merge")
                        & (UploadState.form_mode == "single_table"),
                        rx.input(
                            placeholder=_t["uploads.ph_primary_key"],
                            value=UploadState.form_primary_key,
                            on_change=UploadState.set_form_primary_key,
                            width="100%",
                        ),
                    ),
                    # Merge config (merge + full_database only)
                    rx.cond(
                        (UploadState.form_write_disposition == "merge")
                        & (UploadState.form_mode == "full_database"),
                        rx.text_area(
                            placeholder=_t["uploads.ph_merge_config"],
                            value=UploadState.form_merge_config,
                            on_change=UploadState.set_form_merge_config,
                            width="100%",
                        ),
                    ),
                    # Source schema
                    rx.input(
                        placeholder=_t["uploads.ph_source_schema"],
                        value=UploadState.form_source_schema,
                        on_change=UploadState.set_form_source_schema,
                        width="100%",
                    ),
                    # Mode-specific fields
                    _mode_fields(),
                    spacing="2",
                    width="100%",
                ),
            ),
            # Batch size
            rx.input(
                placeholder=_t["uploads.ph_batch_size"],
                value=UploadState.form_batch_size,
                on_change=UploadState.set_form_batch_size,
                width="100%",
            ),
            # Schema contract
            rx.hstack(
                rx.text(_t["uploads.schema_contract"], size="2", weight="bold"),
                info_tooltip("tooltip.schema_contract"),
                align="center",
                spacing="1",
            ),
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
                    placeholder=_t["uploads.ph_tables"],
                    width="33%",
                ),
                rx.select(
                    ["evolve", "freeze", "discard_value", "discard_row"],
                    value=UploadState.form_sc_columns,
                    on_change=UploadState.set_form_sc_columns,
                    placeholder=_t["uploads.ph_columns"],
                    width="33%",
                ),
                rx.select(
                    ["evolve", "freeze", "discard_value", "discard_row"],
                    value=UploadState.form_sc_data_type,
                    on_change=UploadState.set_form_sc_data_type,
                    placeholder=_t["uploads.ph_data_type"],
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
                    placeholder=_t["uploads.ph_raw_json"],
                    value=UploadState.form_config,
                    on_change=UploadState.set_form_config,
                    width="100%",
                ),
            ),
            error_or_quota_callout(UploadState),
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


def _connection_cell(name, is_deleted) -> rx.Component:
    """A connection reference that stays readable after the connection is retired.

    Soft-deleting a connection dropped it out of the name map, so the row fell
    through to a bare ``#31`` — a raw internal identifier, in no i18n key, and
    indistinguishable from a connection with an empty name (core#805). The name
    was never gone; only the join filtered it out.
    """
    return rx.hstack(
        rx.text(name, size="2"),
        rx.cond(
            is_deleted,
            rx.badge(_t["common.deleted"], color_scheme="red", variant="soft", size="1"),
        ),
        spacing="1",
        align="center",
    )


def _delete_upload_dialog(u) -> rx.Component:
    """core#804 AC3 — /uploads Delete had the same missing confirmation.

    Self-gating on ``can_delete`` for the reason given in
    ``connections._delete_connection_dialog``: the RBAC visibility guard scans
    lexically, so a control moved into a helper must carry its own gate.
    """
    return rx.cond(
        AuthState.can_delete,
        rx.alert_dialog.root(
            rx.alert_dialog.trigger(
                rx.button(_t["common.delete"], color_scheme="red", size="1"),
            ),
            rx.alert_dialog.content(
                rx.alert_dialog.title(_t["uploads.delete_title"]),
                rx.alert_dialog.description(_t["uploads.delete_body"]),
                rx.card(
                    rx.text("#", u.id, "  ", u.name, size="2", weight="bold"),
                    margin_top="12px",
                ),
                rx.flex(
                    rx.alert_dialog.cancel(
                        rx.button(_t["common.cancel"], variant="soft", color_scheme="gray"),
                    ),
                    rx.alert_dialog.action(
                        rx.button(
                            _t["uploads.delete_confirm"],
                            color_scheme="red",
                            on_click=UploadState.delete_upload(u.id),
                        ),
                    ),
                    spacing="3",
                    justify="end",
                    margin_top="16px",
                ),
                max_width="480px",
            ),
        ),
    )


def _run_control(u) -> rx.Component:
    """Run, or a disabled Run that says why (core#805 AC3).

    A live control that queues a doomed run is worse than a disabled one: the
    failure surfaces only in the run, and for a scheduled upload that is at
    03:00 with nobody watching. The reason is rendered beside the button rather
    than in a tooltip because a disabled button takes no pointer events, so its
    tooltip never fires — the explanation would be there and unreachable.

    Self-gating on ``can_edit``, same reason as the delete dialogs.
    """
    return rx.cond(
        AuthState.can_edit,
        rx.cond(
            u.is_blocked,
            rx.hstack(
                rx.button(_t["common.run"], size="1", color_scheme="gray", disabled=True),
                rx.text(
                    _t["uploads.run_blocked_reason"],
                    size="1",
                    color="var(--gray-9)",
                ),
                spacing="1",
                align="center",
            ),
            rx.button(
                _t["common.run"],
                size="1",
                color_scheme=_run_button_color(u.last_run_status),
                on_click=UploadState.run_upload(u.id),
            ),
        ),
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
                            # An upload whose connection is gone must not read
                            # `active` — it cannot run (core#805 AC2).
                            rx.cond(u.is_blocked, _t["uploads.status_blocked"], u.status),
                            color_scheme=u.status_color,
                        ),
                    ),
                    rx.table.cell(
                        _connection_cell(u.source_connection_name, u.source_connection_deleted),
                    ),
                    rx.table.cell(
                        _connection_cell(
                            u.destination_connection_name, u.destination_connection_deleted
                        ),
                    ),
                    rx.table.cell(
                        rx.hstack(
                            rx.cond(
                                AuthState.can_edit,
                                rx.button(
                                    _t["common.edit"],
                                    size="1",
                                    variant="outline",
                                    on_click=UploadState.edit_upload(u.id),
                                ),
                            ),
                            rx.cond(
                                AuthState.can_edit,
                                rx.button(
                                    _t["common.copy"],
                                    size="1",
                                    variant="outline",
                                    on_click=UploadState.copy_upload(u.id),
                                ),
                            ),
                            # Both helpers carry their own role gate.
                            _run_control(u),
                            _delete_upload_dialog(u),
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
        rx.vstack(
            rx.cond(AuthState.can_edit, upload_form()),
            uploads_table(),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.uploads"],
    )
