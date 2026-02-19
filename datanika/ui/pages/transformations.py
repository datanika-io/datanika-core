"""Transformations page — list + create/edit form + test config."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.transformation_state import TransformationState

_t = I18nState.translations

_REF_AUTOCOMPLETE_JS = """
(function() {
    if (window.__refAutocompleteBound) return;
    window.__refAutocompleteBound = true;
    var debounceTimer = null;
    document.addEventListener('keydown', function(e) {
        var ta = document.getElementById('sql-editor');
        if (!ta || document.activeElement !== ta) return;
        if (!document.getElementById('ref-popover-box')) return;
        var map = {
            ArrowDown: 'ref-nav-down', ArrowUp: 'ref-nav-up',
            Enter: 'ref-select', Escape: 'ref-dismiss'
        };
        var btn = map[e.key];
        if (btn) {
            e.preventDefault();
            var el = document.getElementById(btn);
            if (el) el.click();
        }
    }, true);
    document.addEventListener('input', function(e) {
        if (e.target.id !== 'sql-editor') return;
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(function() {
            var el = document.getElementById('ref-detect');
            if (el) el.click();
        }, 300);
    });
})();
"""


def _ref_hidden_buttons() -> rx.Component:
    """Hidden buttons that JavaScript clicks programmatically to trigger state events."""
    return rx.box(
        rx.el.button(
            id="ref-nav-up",
            on_click=TransformationState.ref_navigate_up,
        ),
        rx.el.button(
            id="ref-nav-down",
            on_click=TransformationState.ref_navigate_down,
        ),
        rx.el.button(
            id="ref-select",
            on_click=TransformationState.ref_select_current,
        ),
        rx.el.button(
            id="ref-dismiss",
            on_click=TransformationState.ref_dismiss,
        ),
        rx.el.button(
            id="ref-detect",
            on_click=TransformationState.detect_ref_suggestions,
        ),
        display="none",
    )


def _ref_popover() -> rx.Component:
    """Autocomplete popover that appears when typing {{ ref(' in the SQL editor."""
    return rx.cond(
        TransformationState.show_ref_popover,
        rx.box(
            rx.foreach(
                TransformationState.ref_suggestions,
                lambda name: rx.box(
                    rx.text(name, size="2"),
                    padding="4px 8px",
                    cursor="pointer",
                    background=rx.cond(
                        name == TransformationState.ref_selected_name,
                        "var(--accent-3)",
                        "transparent",
                    ),
                    _hover={"background": "var(--accent-4)"},
                    on_click=TransformationState.select_ref_suggestion(name),
                ),
            ),
            id="ref-popover-box",
            position="absolute",
            bottom="0",
            left="0",
            width="100%",
            max_height="160px",
            overflow_y="auto",
            background="var(--color-background)",
            border="1px solid var(--gray-6)",
            border_radius="6px",
            box_shadow="0 4px 12px rgba(0,0,0,0.15)",
            z_index="10",
        ),
        rx.fragment(),
    )


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
                _ref_popover(),
                position="relative",
                width="100%",
            ),
            _ref_hidden_buttons(),
            rx.script(_REF_AUTOCOMPLETE_JS),
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
        rx.vstack(transformation_form(), transformations_table(), spacing="6", width="100%"),
        title=_t["nav.transformations"],
    )
