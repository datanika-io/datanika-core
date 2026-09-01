"""Transformations page — list + create/edit form + test config."""

import reflex as rx

from datanika.ui.components.info_tooltip import info_tooltip
from datanika.ui.components.layout import page_layout
from datanika.ui.components.searchable_select import searchable_select
from datanika.ui.components.sql_autocomplete import (
    REF_AUTOCOMPLETE_JS,
    ref_hidden_buttons,
    ref_popover,
)
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.transformation_state import TransformationState

_t = I18nState.translations


def _schema_select() -> rx.Component:
    """Schema combobox with 'Add new...' option."""
    return rx.vstack(
        searchable_select(
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
            searchable_select(
                TransformationState.dest_conn_options,
                value=TransformationState.form_connection_option,
                on_change=TransformationState.set_form_connection_option,
                placeholder=_t["transformations.ph_connection"],
                width="100%",
            ),
            # core#862 — say WHY the list is short. A connection the user
            # created, can see on /connections, and may already use as a
            # source, vanishing from this dropdown with no explanation is the
            # core#805 shape: the product behaving as though something is true
            # without saying it. Withdrawing an advertised capability needs a
            # sentence; a silently shorter list produces a bug report.
            rx.text(
                _t["transformations.destination_help"],
                size="1",
                color="var(--gray-9)",
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
            rx.hstack(
                rx.text(_t["transformations.materialization"], size="2", weight="bold"),
                info_tooltip("tooltip.materialization"),
                align="center",
                spacing="1",
            ),
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


def _delete_transformation_dialog(t) -> rx.Component:
    """Ask before deleting a transformation (core#851).

    The reversibility line is the load-bearing sentence here and it is narrower
    than it looks: deleting the transformation does **not** drop the table dbt
    already materialised in the warehouse. Without saying so, the safe reading
    of a red Delete on a page about building tables is that the table goes too,
    and the user who believes that will not click — or will click and then go
    looking for data they still have.

    Self-gating on ``can_delete`` — see ``pipelines._delete_pipeline_dialog``.
    """
    return rx.cond(
        AuthState.can_delete,
        rx.alert_dialog.root(
            rx.alert_dialog.trigger(
                rx.button(_t["common.delete"], color_scheme="red", size="1"),
            ),
            rx.alert_dialog.content(
                rx.alert_dialog.title(_t["transformations.delete_title"]),
                rx.alert_dialog.description(_t["transformations.delete_body"]),
                rx.vstack(
                    rx.card(
                        rx.text("#", t.id, "  ", t.name, size="2", weight="bold"),
                        rx.text(
                            t.materialization,
                            "  ",
                            t.schema_name,
                            size="1",
                            color="var(--gray-9)",
                        ),
                    ),
                    rx.text(
                        _t["transformations.delete_reversible"],
                        size="1",
                        color="var(--gray-9)",
                    ),
                    spacing="3",
                    width="100%",
                    margin_top="12px",
                ),
                rx.flex(
                    rx.alert_dialog.cancel(
                        rx.button(_t["common.cancel"], variant="soft", color_scheme="gray"),
                    ),
                    rx.alert_dialog.action(
                        rx.button(
                            _t["transformations.delete_confirm"],
                            color_scheme="red",
                            on_click=TransformationState.delete_transformation(t.id),
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
                                rx.cond(
                                    AuthState.can_edit,
                                    rx.button(
                                        _t["common.edit"],
                                        size="1",
                                        variant="outline",
                                        on_click=TransformationState.edit_transformation(t.id),
                                    ),
                                ),
                                rx.cond(
                                    AuthState.can_edit,
                                    rx.button(
                                        _t["common.copy"],
                                        size="1",
                                        variant="outline",
                                        on_click=TransformationState.copy_transformation(t.id),
                                    ),
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
                                _delete_transformation_dialog(t),
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
            rx.cond(AuthState.can_edit, transformation_form()),
            transformations_table(),
            preview_display(),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.transformations"],
    )
