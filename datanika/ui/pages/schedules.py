"""Schedules page — list + create/edit form + toggle."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.schedule_state import ScheduleState


def schedule_form() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading(
                rx.cond(
                    ScheduleState.editing_schedule_id,
                    "Edit Schedule",
                    "New Schedule",
                ),
                size="4",
            ),
            rx.select(
                ["pipeline", "transformation"],
                value=ScheduleState.form_target_type,
                on_change=ScheduleState.set_form_target_type,
                width="100%",
            ),
            rx.input(
                placeholder="Target ID",
                value=ScheduleState.form_target_id,
                on_change=ScheduleState.set_form_target_id,
                width="100%",
            ),
            rx.input(
                placeholder="Cron expression (e.g. 0 * * * *)",
                value=ScheduleState.form_cron,
                on_change=ScheduleState.set_form_cron,
                width="100%",
            ),
            rx.input(
                placeholder="Timezone (default: UTC)",
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
                        "Save Changes",
                        "Create Schedule",
                    ),
                    on_click=ScheduleState.save_schedule,
                ),
                rx.cond(
                    ScheduleState.editing_schedule_id,
                    rx.button(
                        "Cancel",
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
                rx.table.column_header_cell("ID"),
                rx.table.column_header_cell("Target"),
                rx.table.column_header_cell("Cron"),
                rx.table.column_header_cell("Timezone"),
                rx.table.column_header_cell("Active"),
                rx.table.column_header_cell("Actions"),
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
                            rx.cond(s.is_active, "Active", "Inactive"),
                            color_scheme=rx.cond(s.is_active, "green", "gray"),
                        ),
                    ),
                    rx.table.cell(
                        rx.hstack(
                            rx.button(
                                "Edit",
                                size="1",
                                variant="outline",
                                on_click=ScheduleState.edit_schedule(s.id),
                            ),
                            rx.button(
                                "Copy",
                                size="1",
                                variant="outline",
                                on_click=ScheduleState.copy_schedule(s.id),
                            ),
                            rx.button(
                                rx.cond(s.is_active, "Pause", "Resume"),
                                size="1",
                                variant="outline",
                                on_click=ScheduleState.toggle_schedule(s.id),
                            ),
                            rx.button(
                                "Delete",
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
        title="Schedules",
    )
