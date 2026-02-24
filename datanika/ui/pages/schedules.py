"""Schedules page — list + create/edit form + toggle."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.schedule_state import ScheduleState

_t = I18nState.translations

_SCHEDULE_AUTOCOMPLETE_JS = """
(function() {
    if (window.__scheduleAutocompleteBound) return;
    window.__scheduleAutocompleteBound = true;
    document.addEventListener('keydown', function(e) {
        var input = document.getElementById('schedule-target-input');
        if (!input || document.activeElement !== input) return;
        if (!document.getElementById('schedule-target-popover-box')) return;
        var map = {
            ArrowDown: 'schedule-target-nav-down',
            ArrowUp: 'schedule-target-nav-up',
            Enter: 'schedule-target-select',
            Escape: 'schedule-target-dismiss'
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


def _schedule_hidden_buttons() -> rx.Component:
    """Hidden buttons for keyboard navigation of target suggestions."""
    return rx.box(
        rx.el.button(id="schedule-target-nav-up", on_click=ScheduleState.target_nav_up),
        rx.el.button(id="schedule-target-nav-down", on_click=ScheduleState.target_nav_down),
        rx.el.button(
            id="schedule-target-select",
            on_click=ScheduleState.target_select_current,
        ),
        rx.el.button(id="schedule-target-dismiss", on_click=ScheduleState.target_dismiss),
        display="none",
    )


def _target_popover() -> rx.Component:
    """Autocomplete popover for target names."""
    return rx.cond(
        ScheduleState.show_target_suggestions,
        rx.box(
            rx.foreach(
                ScheduleState.target_suggestions,
                lambda name, idx: rx.box(
                    rx.text(name, size="2"),
                    padding="4px 8px",
                    cursor="pointer",
                    background=rx.cond(
                        idx == ScheduleState.target_suggestion_index,
                        "var(--accent-3)",
                        "transparent",
                    ),
                    _hover={"background": "var(--accent-4)"},
                    on_click=ScheduleState.select_target_suggestion(name),
                ),
            ),
            id="schedule-target-popover-box",
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


def schedule_form() -> rx.Component:
    return rx.box(
        rx.vstack(
            rx.heading(
                rx.cond(
                    ScheduleState.editing_schedule_id,
                    _t["schedules.edit"],
                    _t["schedules.new"],
                ),
                size="4",
            ),
            rx.select(
                ["upload", "transformation", "pipeline"],
                value=ScheduleState.form_target_type,
                on_change=ScheduleState.set_form_target_type,
                width="100%",
            ),
            rx.box(
                rx.input(
                    id="schedule-target-input",
                    placeholder=_t["schedules.ph_target_name"],
                    value=ScheduleState.form_target_name,
                    on_change=ScheduleState.set_form_target_name,
                    on_focus=ScheduleState.show_target_all,
                    width="100%",
                ),
                _target_popover(),
                position="relative",
                width="100%",
            ),
            rx.input(
                placeholder=_t["schedules.ph_cron"],
                value=ScheduleState.form_cron,
                on_change=ScheduleState.set_form_cron,
                width="100%",
            ),
            rx.input(
                placeholder=_t["schedules.ph_timezone"],
                value=ScheduleState.form_timezone,
                on_change=ScheduleState.set_form_timezone,
                width="100%",
            ),
            rx.cond(
                ScheduleState.error_message,
                rx.callout(
                    ScheduleState.error_message,
                    icon="triangle_alert",
                    color_scheme="red",
                ),
            ),
            rx.hstack(
                rx.button(
                    rx.cond(
                        ScheduleState.editing_schedule_id,
                        _t["common.save_changes"],
                        _t["schedules.create"],
                    ),
                    on_click=ScheduleState.save_schedule,
                ),
                rx.cond(
                    ScheduleState.editing_schedule_id,
                    rx.button(
                        _t["common.cancel"],
                        variant="outline",
                        on_click=ScheduleState.cancel_edit,
                    ),
                ),
                spacing="2",
            ),
            _schedule_hidden_buttons(),
            rx.script(_SCHEDULE_AUTOCOMPLETE_JS),
            spacing="3",
            width="100%",
        ),
        width="100%",
        padding="16px",
        border="1px solid var(--gray-6)",
        border_radius="var(--radius-3)",
        background="var(--color-panel-solid)",
    )


def schedules_table() -> rx.Component:
    return rx.table.root(
        rx.table.header(
            rx.table.row(
                rx.table.column_header_cell(_t["common.id"]),
                rx.table.column_header_cell(_t["dashboard.target"]),
                rx.table.column_header_cell(_t["schedules.cron"]),
                rx.table.column_header_cell(_t["schedules.timezone"]),
                rx.table.column_header_cell(_t["schedules.active"]),
                rx.table.column_header_cell(_t["common.actions"]),
            ),
        ),
        rx.table.body(
            rx.foreach(
                ScheduleState.schedules,
                lambda s: rx.table.row(
                    rx.table.cell(s.id),
                    rx.table.cell(rx.text(s.target_name)),
                    rx.table.cell(rx.code(s.cron_expression)),
                    rx.table.cell(s.timezone),
                    rx.table.cell(
                        rx.badge(
                            rx.cond(
                                s.is_active,
                                _t["schedules.active"],
                                _t["schedules.inactive"],
                            ),
                            color_scheme=rx.cond(s.is_active, "green", "gray"),
                        ),
                    ),
                    rx.table.cell(
                        rx.hstack(
                            rx.button(
                                _t["common.edit"],
                                size="1",
                                variant="outline",
                                on_click=ScheduleState.edit_schedule(s.id),
                            ),
                            rx.button(
                                _t["common.copy"],
                                size="1",
                                variant="outline",
                                on_click=ScheduleState.copy_schedule(s.id),
                            ),
                            rx.button(
                                rx.cond(
                                    s.is_active,
                                    _t["schedules.pause"],
                                    _t["schedules.resume"],
                                ),
                                size="1",
                                variant="outline",
                                on_click=ScheduleState.toggle_schedule(s.id),
                            ),
                            rx.button(
                                _t["common.delete"],
                                color_scheme="red",
                                size="1",
                                on_click=ScheduleState.delete_schedule(s.id),
                            ),
                            spacing="2",
                        ),
                    ),
                ),
            ),
        ),
        width="100%",
    )


def schedules_page() -> rx.Component:
    return page_layout(
        rx.vstack(schedule_form(), schedules_table(), spacing="6", width="100%"),
        title=_t["nav.schedules"],
    )
