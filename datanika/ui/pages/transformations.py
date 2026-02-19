"""Transformations page — list + create/edit form + test config."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.components.sql_autocomplete import (
    REF_AUTOCOMPLETE_JS,
    ref_hidden_buttons,
    ref_popover,
)
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.transformation_state import TransformationState

_t = I18nState.translations


def _schema_select() -> rx.Component:
    """Schema combobox with 'Add new...' option."""
    return rx.vstack(
        rx.select(
            TransformationState.schema_options,
            value=TransformationState.form_schema_name,
            on_change=TransformationState.set_form_schema_name,
            width="100%",
        ),
        rx.cond(
            TransformationState.adding_new_schema,
            rx.hstack(
                rx.input(
                    placeholder=_t["transformations.ph_schema"],
                    value=TransformationState.form_schema_name,
                    on_change=TransformationState.set_new_schema_name,
                    width="100%",
                ),
                rx.button(
                    _t["common.add"],
                    size="1",
                    on_click=TransformationState.confirm_new_schema,
                ),
                width="100%",
            ),
            rx.fragment(),
        ),
        spacing="2",
        width="100%",
    )


def preview_display() -> rx.Component:
    """Preview sections for compiled SQL and query result, shared between pages."""
    return rx.vstack(
        rx.cond(
            TransformationState.preview_result_message,
            rx.callout(
                TransformationState.preview_result_message,
                icon="info",
                color_scheme="blue",
            ),
        ),
        rx.cond(
            TransformationState.preview_result_columns.length() > 0,
            rx.card(
                rx.vstack(
                    rx.heading(_t["transformations.preview_result_heading"], size="3"),
                    rx.table.root(
                        rx.table.header(
                            rx.table.row(
                                rx.foreach(
                                    TransformationState.preview_result_columns,
                                    lambda col: rx.table.column_header_cell(col),
                                ),
                            ),
                        ),
                        rx.table.body(
                            rx.foreach(
                                TransformationState.preview_result_rows,
                                lambda row: rx.table.row(
                                    rx.foreach(
                                        row,
                                        lambda cell: rx.table.cell(cell),
                                    ),
                                ),
                            ),
                        ),
                        width="100%",
                    ),
                    spacing="2",
                ),
                width="100%",
            ),
        ),
        rx.cond(
            TransformationState.preview_sql,
            rx.card(
                rx.vstack(
                    rx.heading(_t["transformations.compiled_sql_preview"], size="3"),
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
        spacing="3",
        width="100%",
    )


def _sql_action_buttons() -> rx.Component:
    """Upload SQL, SQL Editor, Preview SQL, Preview Result buttons below the textarea."""
    return rx.hstack(
        rx.upload(
            rx.button(
                _t["transformations.upload_sql"],
                size="1",
                variant="outline",
                type="button",
            ),
            accept={".sql": ["text/plain", "application/sql"]},
            max_files=1,
            on_drop=TransformationState.handle_sql_file_upload(rx.upload_files()),  # type: ignore
            no_click=False,
            no_drag=True,
            border="none",
            padding="0",
        ),
        rx.link(
            rx.button(
                _t["transformations.sql_editor"],
                size="1",
                variant="outline",
                type="button",
            ),
            href="/transformations/sql-editor",
        ),
        rx.button(
            _t["transformations.preview_sql"],
            size="1",
            variant="outline",
            on_click=TransformationState.preview_compiled_sql_from_form,
            disabled=~TransformationState.can_preview,
        ),
        rx.button(
            _t["transformations.preview_result"],
            size="1",
            variant="outline",
            on_click=TransformationState.preview_result_from_form,
            disabled=~TransformationState.can_preview,
        ),
        spacing="2",
        wrap="wrap",
    )


def transformation_form() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading(
                rx.cond(
                    TransformationState.editing_transformation_id,
                    _t["transformations.edit"],
                    _t["transformations.new"],
                ),
                size="4",
            ),
            rx.input(
                placeholder=_t["transformations.ph_name"],
                value=TransformationState.form_name,
                on_change=TransformationState.set_form_name,
                width="100%",
            ),
            rx.input(
                placeholder=_t["transformations.ph_description"],
                value=TransformationState.form_description,
                on_change=TransformationState.set_form_description,
                width="100%",
            ),
            rx.text(_t["transformations.dest_connection"], size="2", weight="bold"),
            rx.select(
                TransformationState.dest_conn_options,
                placeholder=_t["transformations.ph_connection"],
                value=TransformationState.form_connection_option,
                on_change=TransformationState.set_form_connection_option,
                width="100%",
            ),
            rx.text(_t["transformations.sql"], size="2", weight="bold"),
            rx.box(
                rx.text_area(
                    placeholder=_t["transformations.ph_sql"],
                    value=TransformationState.form_sql_body,
                    on_change=TransformationState.set_form_sql_body,
                    id="sql-editor",
                    min_height="120px",
                    width="100%",
                ),
                ref_popover(),
                position="relative",
                width="100%",
            ),
            ref_hidden_buttons(),
            rx.script(REF_AUTOCOMPLETE_JS),
            _sql_action_buttons(),
            rx.text(_t["transformations.materialization"], size="2", weight="bold"),
            rx.select(
                ["view", "table", "incremental", "ephemeral"],
                value=TransformationState.form_materialization,
                on_change=TransformationState.set_form_materialization,
                width="100%",
            ),
            rx.cond(
                TransformationState.form_materialization == "incremental",
                rx.card(
                    rx.vstack(
                        rx.text(_t["transformations.incremental_config"], size="2", weight="bold"),
                        rx.input(
                            placeholder=_t["transformations.ph_unique_key"],
                            value=TransformationState.form_unique_key,
                            on_change=TransformationState.set_form_unique_key,
                            width="100%",
                        ),
                        rx.text(_t["transformations.strategy"], size="2"),
                        rx.select(
                            ["append", "delete+insert", "merge"],
                            placeholder=_t["transformations.ph_strategy"],
                            value=TransformationState.form_strategy,
                            on_change=TransformationState.set_form_strategy,
                            width="100%",
                        ),
                        rx.input(
                            placeholder=_t["transformations.ph_updated_at"],
                            value=TransformationState.form_updated_at,
                            on_change=TransformationState.set_form_updated_at,
                            width="100%",
                        ),
                        rx.text(_t["transformations.on_schema_change"], size="2"),
                        rx.select(
                            ["ignore", "fail", "append_new_columns", "sync_all_columns"],
                            value=TransformationState.form_on_schema_change,
                            on_change=TransformationState.set_form_on_schema_change,
                            width="100%",
                        ),
                        spacing="2",
                        width="100%",
                    ),
                    width="100%",
                ),
            ),
            rx.text(_t["transformations.schema"], size="2", weight="bold"),
            _schema_select(),
            rx.text(_t["transformations.tags"], size="2", weight="bold"),
            rx.input(
                placeholder=_t["transformations.ph_tags"],
                value=TransformationState.form_tags,
                on_change=TransformationState.set_form_tags,
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
                        _t["common.save_changes"],
                        _t["transformations.create"],
                    ),
                    on_click=TransformationState.save_transformation,
                ),
                rx.cond(
                    TransformationState.editing_transformation_id,
                    rx.button(
                        _t["common.cancel"],
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
        rx.table.root(
            rx.table.header(
                rx.table.row(
                    rx.table.column_header_cell(_t["common.id"]),
                    rx.table.column_header_cell(_t["common.name"]),
                    rx.table.column_header_cell(_t["transformations.connection"]),
                    rx.table.column_header_cell(_t["transformations.materialization"]),
                    rx.table.column_header_cell(_t["transformations.schema"]),
                    rx.table.column_header_cell(_t["transformations.tags"]),
                    rx.table.column_header_cell(_t["common.actions"]),
                ),
            ),
            rx.table.body(
                rx.foreach(
                    TransformationState.transformations,
                    lambda t: rx.table.row(
                        rx.table.cell(t.id),
                        rx.table.cell(t.name),
                        rx.table.cell(t.connection_name),
                        rx.table.cell(rx.badge(t.materialization)),
                        rx.table.cell(t.schema_name),
                        rx.table.cell(t.tags),
                        rx.table.cell(
                            rx.hstack(
                                rx.button(
                                    _t["common.edit"],
                                    size="1",
                                    variant="outline",
                                    on_click=TransformationState.edit_transformation(t.id),
                                ),
                                rx.button(
                                    _t["common.copy"],
                                    size="1",
                                    variant="outline",
                                    on_click=TransformationState.copy_transformation(t.id),
                                ),
                                rx.button(
                                    _t["transformations.preview_sql"],
                                    size="1",
                                    variant="outline",
                                    on_click=TransformationState.preview_compiled_sql(t.id),
                                ),
                                rx.button(
                                    _t["transformations.preview_result"],
                                    size="1",
                                    variant="outline",
                                    on_click=TransformationState.preview_result(t.id),
                                ),
                                rx.button(
                                    _t["common.delete"],
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
        rx.vstack(
            transformation_form(),
            preview_display(),
            transformations_table(),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.transformations"],
    )
