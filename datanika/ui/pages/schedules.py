"""Schedules page — list + create/edit form + toggle."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.schedule_state import ScheduleState

_t = I18nState.translations


def schedule_form() -> rx.Component:
    return rx.card(
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
            rx.input(
                placeholder=_t["schedules.ph_target_id"],
                value=ScheduleState.form_target_id,
                on_change=ScheduleState.set_form_target_id,
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
                rx.callout(ScheduleState.error_message, icon="triangle_alert", color_scheme="red"),
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
            spacing="3",
            width="100%",
        ),
        width="100%",
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
                                rx.cond(s.is_active, _t["schedules.pause"], _t["schedules.resume"]),
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
