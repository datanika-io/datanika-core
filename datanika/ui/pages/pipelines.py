"""Pipelines page — list + create/edit form for dbt pipeline orchestration."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.pipeline_state import PipelineState

_t = I18nState.translations


def _run_button_color(status: rx.Var[str]) -> rx.Var[str]:
    return rx.cond(
        (status == "running") | (status == "pending"),
        "yellow",
        "gray",
    )


COMMAND_OPTIONS = ["build", "run", "test", "seed", "snapshot", "compile"]

_MODEL_AUTOCOMPLETE_JS = """
(function() {
    if (window.__modelAutocompleteBound) return;
    window.__modelAutocompleteBound = true;
    document.addEventListener('keydown', function(e) {
        var input = document.getElementById('model-name-input');
        if (!input || document.activeElement !== input) return;
        if (!document.getElementById('model-popover-box')) return;
        var map = {
            ArrowDown: 'model-nav-down', ArrowUp: 'model-nav-up',
            Enter: 'model-select', Escape: 'model-dismiss'
        };
        var btn = map[e.key];
        if (btn) {
            e.preventDefault();
            var el = document.getElementById(btn);
            if (el) el.click();
        }
    }, true);
})();
"""


def _model_hidden_buttons() -> rx.Component:
    """Hidden buttons for keyboard navigation of model suggestions."""
    return rx.box(
        rx.el.button(id="model-nav-up", on_click=PipelineState.model_nav_up),
        rx.el.button(id="model-nav-down", on_click=PipelineState.model_nav_down),
        rx.el.button(id="model-select", on_click=PipelineState.model_select_current),
        rx.el.button(id="model-dismiss", on_click=PipelineState.model_dismiss),
        display="none",
    )


def _model_suggestions_popover() -> rx.Component:
    """Autocomplete popover for model names from transformations."""
    return rx.cond(
        PipelineState.show_model_suggestions,
        rx.box(
            rx.foreach(
                PipelineState.model_suggestions,
                lambda name, idx: rx.box(
                    rx.text(name, size="2"),
                    padding="4px 8px",
                    cursor="pointer",
                    background=rx.cond(
                        idx == PipelineState.model_suggestion_index,
                        "var(--accent-3)",
                        "transparent",
                    ),
                    _hover={"background": "var(--accent-4)"},
                    on_click=PipelineState.select_model_suggestion(name),
                ),
            ),
            id="model-popover-box",
            position="absolute",
            top="100%",
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


def _models_section() -> rx.Component:
    """Models sub-table with add/remove and upstream/downstream toggles."""
    return rx.vstack(
        rx.text(_t["pipelines.models"], size="3", weight="bold"),
        rx.table.root(
            rx.table.header(
                rx.table.row(
                    rx.table.column_header_cell(_t["pipelines.model_name"]),
                    rx.table.column_header_cell(_t["pipelines.include_upstream"]),
                    rx.table.column_header_cell(_t["pipelines.include_downstream"]),
                    rx.table.column_header_cell(""),
                ),
            ),
            rx.table.body(
                rx.foreach(
                    PipelineState.form_models,
                    lambda m, idx: rx.table.row(
                        rx.table.cell(m.name),
                        rx.table.cell(
                            rx.checkbox(
                                checked=m.upstream,
                                on_change=lambda _v: PipelineState.toggle_model_upstream(idx),
                            ),
                        ),
                        rx.table.cell(
                            rx.checkbox(
                                checked=m.downstream,
                                on_change=lambda _v: PipelineState.toggle_model_downstream(idx),
                            ),
                        ),
                        rx.table.cell(
                            rx.button(
                                _t["pipelines.remove"],
                                size="1",
                                color_scheme="red",
                                variant="outline",
                                on_click=PipelineState.remove_model(idx),
                            ),
                        ),
                    ),
                ),
            ),
            width="100%",
        ),
        rx.hstack(
            rx.box(
                rx.input(
                    id="model-name-input",
                    placeholder=_t["pipelines.ph_model_name"],
                    value=PipelineState.form_new_model_name,
                    on_change=PipelineState.set_form_new_model_name,
                    width="100%",
                ),
                _model_suggestions_popover(),
                position="relative",
                width="100%",
            ),
            rx.button(
                _t["pipelines.add_model"],
                on_click=PipelineState.add_model,
                size="2",
                variant="outline",
            ),
            spacing="2",
            width="100%",
        ),
        rx.cond(
            PipelineState.model_warning,
            rx.callout(
                rx.text(
                    _t["pipelines.model_not_found"],
                    rx.text(PipelineState.model_warning, weight="bold", as_="span"),
                ),
                icon="triangle_alert",
                color_scheme="orange",
                size="1",
            ),
        ),
        _model_hidden_buttons(),
        rx.script(_MODEL_AUTOCOMPLETE_JS),
        spacing="2",
        width="100%",
    )


def pipeline_form() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading(
                rx.cond(
                    PipelineState.editing_pipeline_id,
                    _t["pipelines.edit"],
                    _t["pipelines.new"],
                ),
                size="4",
            ),
            rx.input(
                placeholder=_t["pipelines.ph_name"],
                value=PipelineState.form_name,
                on_change=PipelineState.set_form_name,
                width="100%",
            ),
            rx.input(
                placeholder=_t["pipelines.ph_description"],
                value=PipelineState.form_description,
                on_change=PipelineState.set_form_description,
                width="100%",
            ),
            # Destination connection
            rx.text(_t["pipelines.connection"], size="2", weight="bold"),
            rx.select(
                PipelineState.dest_conn_options,
                value=PipelineState.form_dest_id,
                on_change=PipelineState.set_form_dest_id,
                placeholder=_t["pipelines.ph_destination"],
                width="100%",
            ),
            # dbt command
            rx.text(_t["pipelines.command"], size="2", weight="bold"),
            rx.select(
                COMMAND_OPTIONS,
                value=PipelineState.form_command,
                on_change=PipelineState.set_form_command,
                width="100%",
            ),
            rx.match(
                PipelineState.form_command,
                ("build", rx.text(_t["pipelines.cmd_build_hint"], size="1", color="gray")),
                ("run", rx.text(_t["pipelines.cmd_run_hint"], size="1", color="gray")),
                ("test", rx.text(_t["pipelines.cmd_test_hint"], size="1", color="gray")),
                ("seed", rx.text(_t["pipelines.cmd_seed_hint"], size="1", color="gray")),
                ("snapshot", rx.text(_t["pipelines.cmd_snapshot_hint"], size="1", color="gray")),
                ("compile", rx.text(_t["pipelines.cmd_compile_hint"], size="1", color="gray")),
                rx.fragment(),
            ),
            # Full refresh
            rx.checkbox(
                _t["pipelines.full_refresh"],
                checked=PipelineState.form_full_refresh,
                on_change=PipelineState.set_form_full_refresh,
            ),
            # Models section
            _models_section(),
            # Custom selector
            rx.text(_t["pipelines.custom_selector"], size="2", weight="bold"),
            rx.input(
                placeholder=_t["pipelines.ph_custom_selector"],
                value=PipelineState.form_custom_selector,
                on_change=PipelineState.set_form_custom_selector,
                width="100%",
            ),
            # Error
            rx.cond(
                PipelineState.error_message,
                rx.callout(PipelineState.error_message, icon="triangle_alert", color_scheme="red"),
            ),
            # Buttons
            rx.hstack(
                rx.button(
                    rx.cond(
                        PipelineState.editing_pipeline_id,
                        _t["common.save_changes"],
                        _t["pipelines.create"],
                    ),
                    on_click=PipelineState.save_pipeline,
                ),
                rx.cond(
                    PipelineState.editing_pipeline_id,
                    rx.button(
                        _t["common.cancel"],
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
                rx.table.column_header_cell(_t["common.id"]),
                rx.table.column_header_cell(_t["common.name"]),
                rx.table.column_header_cell(_t["pipelines.command"]),
                rx.table.column_header_cell(_t["pipelines.connection"]),
                rx.table.column_header_cell(_t["pipelines.models"]),
                rx.table.column_header_cell(_t["common.status"]),
                rx.table.column_header_cell(_t["common.actions"]),
            ),
        ),
        rx.table.body(
            rx.foreach(
                PipelineState.pipelines,
                lambda p: rx.table.row(
                    rx.table.cell(p.id),
                    rx.table.cell(p.name),
                    rx.table.cell(rx.badge(p.command)),
                    rx.table.cell(p.connection_name),
                    rx.table.cell(p.model_count),
                    rx.table.cell(
                        rx.badge(
                            p.status,
                            color_scheme=rx.cond(p.status == "active", "green", "gray"),
                        ),
                    ),
                    rx.table.cell(
                        rx.hstack(
                            rx.button(
                                _t["common.edit"],
                                size="1",
                                variant="outline",
                                on_click=PipelineState.edit_pipeline(p.id),
                            ),
                            rx.button(
                                _t["common.copy"],
                                size="1",
                                variant="outline",
                                on_click=PipelineState.copy_pipeline(p.id),
                            ),
                            rx.button(
                                _t["common.run"],
                                size="1",
                                color_scheme=_run_button_color(p.last_run_status),
                                on_click=PipelineState.run_pipeline(p.id),
                            ),
                            rx.button(
                                _t["common.delete"],
                                color_scheme="red",
                                size="1",
                                on_click=PipelineState.delete_pipeline(p.id),
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
        title=_t["nav.pipelines"],
    )
