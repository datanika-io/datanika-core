"""DAG page — dependency edge table with add/remove."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.dag_state import DagState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations

_DAG_AUTOCOMPLETE_JS = """
(function() {
    if (window.__dagAutocompleteBound) return;
    window.__dagAutocompleteBound = true;
    document.addEventListener('keydown', function(e) {
        var upInput = document.getElementById('dag-upstream-input');
        var downInput = document.getElementById('dag-downstream-input');
        if (!upInput && !downInput) return;
        var isUp = (document.activeElement === upInput)
            && document.getElementById('dag-upstream-popover-box');
        var isDown = (document.activeElement === downInput)
            && document.getElementById('dag-downstream-popover-box');
        if (!isUp && !isDown) return;
        var prefix = isUp ? 'dag-upstream' : 'dag-downstream';
        var map = {
            ArrowDown: prefix + '-nav-down',
            ArrowUp: prefix + '-nav-up',
            Enter: prefix + '-select',
            Escape: prefix + '-dismiss'
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


def _dag_hidden_buttons() -> rx.Component:
    """Hidden buttons for keyboard navigation of upstream/downstream suggestions."""
    return rx.box(
        # Upstream
        rx.el.button(id="dag-upstream-nav-up", on_click=DagState.upstream_nav_up),
        rx.el.button(id="dag-upstream-nav-down", on_click=DagState.upstream_nav_down),
        rx.el.button(id="dag-upstream-select", on_click=DagState.upstream_select_current),
        rx.el.button(id="dag-upstream-dismiss", on_click=DagState.upstream_dismiss),
        # Downstream
        rx.el.button(id="dag-downstream-nav-up", on_click=DagState.downstream_nav_up),
        rx.el.button(id="dag-downstream-nav-down", on_click=DagState.downstream_nav_down),
        rx.el.button(
            id="dag-downstream-select",
            on_click=DagState.downstream_select_current,
        ),
        rx.el.button(id="dag-downstream-dismiss", on_click=DagState.downstream_dismiss),
        display="none",
    )


def _upstream_popover() -> rx.Component:
    """Autocomplete popover for upstream node names."""
    return rx.cond(
        DagState.show_upstream_suggestions,
        rx.box(
            rx.foreach(
                DagState.upstream_suggestions,
                lambda name, idx: rx.box(
                    rx.text(name, size="2"),
                    padding="4px 8px",
                    cursor="pointer",
                    background=rx.cond(
                        idx == DagState.upstream_suggestion_index,
                        "var(--accent-3)",
                        "transparent",
                    ),
                    _hover={"background": "var(--accent-4)"},
                    on_click=DagState.select_upstream_suggestion(name),
                ),
            ),
            id="dag-upstream-popover-box",
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
            z_index="50",
        ),
        rx.fragment(),
    )


def _downstream_popover() -> rx.Component:
    """Autocomplete popover for downstream node names."""
    return rx.cond(
        DagState.show_downstream_suggestions,
        rx.box(
            rx.foreach(
                DagState.downstream_suggestions,
                lambda name, idx: rx.box(
                    rx.text(name, size="2"),
                    padding="4px 8px",
                    cursor="pointer",
                    background=rx.cond(
                        idx == DagState.downstream_suggestion_index,
                        "var(--accent-3)",
                        "transparent",
                    ),
                    _hover={"background": "var(--accent-4)"},
                    on_click=DagState.select_downstream_suggestion(name),
                ),
            ),
            id="dag-downstream-popover-box",
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
            z_index="50",
        ),
        rx.fragment(),
    )


def add_dependency_form() -> rx.Component:
    return rx.box(
        rx.vstack(
            rx.heading(_t["dag.add_dependency"], size="4"),
            rx.hstack(
                rx.vstack(
                    rx.text(_t["dag.upstream_type"], size="2"),
                    rx.select(
                        ["upload", "transformation", "pipeline"],
                        value=DagState.form_upstream_type,
                        on_change=DagState.set_form_upstream_type,
                        width="100%",
                    ),
                    spacing="1",
                ),
                rx.vstack(
                    rx.text(_t["dag.upstream_name"], size="2"),
                    rx.box(
                        rx.input(
                            id="dag-upstream-input",
                            value=DagState.form_upstream_name,
                            on_change=DagState.set_form_upstream_name,
                            on_focus=DagState.show_upstream_all,
                            placeholder=_t["dag.ph_name"],
                            width="160px",
                        ),
                        _upstream_popover(),
                        position="relative",
                    ),
                    spacing="1",
                ),
                rx.icon("arrow-right", size=20),
                rx.vstack(
                    rx.text(_t["dag.downstream_type"], size="2"),
                    rx.select(
                        ["upload", "transformation", "pipeline"],
                        value=DagState.form_downstream_type,
                        on_change=DagState.set_form_downstream_type,
                        width="100%",
                    ),
                    spacing="1",
                ),
                rx.vstack(
                    rx.text(_t["dag.downstream_name"], size="2"),
                    rx.box(
                        rx.input(
                            id="dag-downstream-input",
                            value=DagState.form_downstream_name,
                            on_change=DagState.set_form_downstream_name,
                            on_focus=DagState.show_downstream_all,
                            placeholder=_t["dag.ph_name"],
                            width="160px",
                        ),
                        _downstream_popover(),
                        position="relative",
                    ),
                    spacing="1",
                ),
                rx.vstack(
                    rx.text(_t["dag.check_timeframe"], size="2"),
                    rx.hstack(
                        rx.input(
                            value=DagState.form_check_timeframe_value,
                            on_change=DagState.set_form_check_timeframe_value,
                            placeholder="—",
                            width="70px",
                            type="number",
                        ),
                        rx.select(
                            ["minutes", "hours"],
                            value=DagState.form_check_timeframe_unit,
                            on_change=DagState.set_form_check_timeframe_unit,
                            width="100px",
                        ),
                        spacing="1",
                    ),
                    spacing="1",
                ),
                rx.button(_t["common.add"], on_click=DagState.add_dependency),
                spacing="3",
                align="end",
            ),
            rx.cond(
                DagState.error_message,
                rx.callout(
                    DagState.error_message,
                    icon="triangle_alert",
                    color_scheme="red",
                ),
                rx.fragment(),
            ),
            _dag_hidden_buttons(),
            rx.script(_DAG_AUTOCOMPLETE_JS),
            spacing="3",
            width="100%",
        ),
        width="100%",
        padding="16px",
        border="1px solid var(--gray-6)",
        border_radius="var(--radius-3)",
        background="var(--color-panel-solid)",
    )


def dependency_table() -> rx.Component:
    return rx.table.root(
        rx.table.header(
            rx.table.row(
                rx.table.column_header_cell(_t["common.id"]),
                rx.table.column_header_cell(_t["dag.upstream"]),
                rx.table.column_header_cell(""),
                rx.table.column_header_cell(_t["dag.downstream"]),
                rx.table.column_header_cell(_t["dag.check_timeframe"]),
                rx.table.column_header_cell(_t["common.actions"]),
            ),
        ),
        rx.table.body(
            rx.foreach(
                DagState.dependencies,
                lambda d: rx.table.row(
                    rx.table.cell(d.id),
                    rx.table.cell(rx.text(d.upstream_name)),
                    rx.table.cell(rx.icon("arrow-right", size=14)),
                    rx.table.cell(rx.text(d.downstream_name)),
                    rx.table.cell(rx.text(d.check_timeframe_display)),
                    rx.table.cell(
                        rx.button(
                            rx.icon("trash-2", size=14),
                            on_click=DagState.remove_dependency(d.id),
                            variant="ghost",
                            color_scheme="red",
                            size="1",
                        ),
                    ),
                ),
            ),
        ),
        width="100%",
    )


def dag_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            add_dependency_form(),
            dependency_table(),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.dependencies"],
    )
