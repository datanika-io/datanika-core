"""Runs page — list with status and target_type filters + logs viewer."""

import reflex as rx

from datanika.config import settings
from datanika.models.run import RunStatus
from datanika.ui.components.elt_nudge_card import elt_nudge_card
from datanika.ui.components.info_tooltip import info_tooltip
from datanika.ui.components.layout import page_layout
from datanika.ui.components.run_status import run_status_color
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.run_state import RunState

_t = I18nState.translations


def filters_bar() -> rx.Component:
    return rx.hstack(
        rx.text(_t["runs.status_filter"], size="2"),
        rx.select(
            # Derived: a status the product can return must be findable by filter.
            [s.value for s in RunStatus],
            value=RunState.filter_status,
            on_change=RunState.set_filter,
            placeholder=_t["common.all"],
            width="100%",
            custom_attrs={"aria-label": _t["runs.status_filter"]},
        ),
        rx.text(_t["runs.target_type_filter"], size="2"),
        rx.select(
            ["upload", "transformation", "pipeline"],
            value=RunState.filter_target_type,
            on_change=RunState.set_target_type_filter,
            placeholder=_t["common.all"],
            width="100%",
            custom_attrs={"aria-label": _t["runs.target_type_filter"]},
        ),
        spacing="3",
        align="center",
    )


def _status_badge(r) -> rx.Component:
    """The status, or *Stopping…* while a requested stop is unconfirmed (§5.3).

    Raw enum values stay untranslated, per the standing i18n rule; *Stopping…* is copy, not an
    enum value, so it is translated.
    """
    return rx.cond(
        r.stopping,
        rx.badge(_t["runs.stopping"], color_scheme=run_status_color(r.status)),
        rx.badge(r.status, color_scheme=run_status_color(r.status)),
    )


def _cancel_dialog(r) -> rx.Component:
    """Ask before stopping a run, and say what stopping does (core#657, AC9/AC10/AC12).

    ``rx.alert_dialog`` renders a real ``role="alertdialog"``, and the handler hangs off the
    dialog's ACTION — a trigger wired to the handler would stop the run and then ask.

    The copy is ``SPEC_RUN_CANCELLATION`` D3a, not D3: a load already in progress is not
    interrupted, and the dialog says so rather than promising a stop the product cannot make.
    It names the run by id *and* target — the id is what an agent aims by, the name is what a
    person recognises — and labels its buttons by outcome, never a bare "Are you sure?".

    ⚠️ The trigger is a labelled text button, not an icon: an icon-only button inside an
    ``alert_dialog.trigger`` is the shape core#1409 found reported as ``button-name`` and
    ``aria-allowed-attr``.
    """
    return rx.alert_dialog.root(
        rx.alert_dialog.trigger(
            rx.button(_t["runs.cancel"], size="1", variant="soft", color_scheme="red"),
        ),
        rx.alert_dialog.content(
            rx.alert_dialog.title(_t["runs.cancel_title"]),
            rx.alert_dialog.description(_t["runs.cancel_body"]),
            rx.vstack(
                rx.card(
                    rx.text("#", r.id, "  ", r.target_name, size="2", weight="bold"),
                ),
                rx.cond(
                    RunState.bills_usage,
                    rx.text(_t["runs.cancel_billing"], size="2"),
                ),
                spacing="3",
                width="100%",
                margin_top="12px",
            ),
            rx.flex(
                rx.alert_dialog.cancel(
                    rx.button(_t["runs.cancel_keep"], variant="soft", color_scheme="gray"),
                ),
                rx.alert_dialog.action(
                    rx.button(
                        _t["runs.cancel_confirm"],
                        color_scheme="red",
                        on_click=RunState.cancel_run(r.id),
                    ),
                ),
                spacing="3",
                justify="end",
                margin_top="16px",
            ),
            max_width="480px",
        ),
    )


def _cancel_control(r) -> rx.Component:
    """The row's Cancel control: offered, disabled while stopping, or absent once finished.

    🚨 ``AuthState.can_edit`` is the affordance, and only that. ``RunState.cancel_run`` refuses a
    ``viewer`` on its own, because hiding a control is not authorization (§5.3).

    The branches read ``r.can_cancel`` and ``r.stopping``, which the state derives from the
    model's status sets — never ``r.status == "running"``, which is the hand-maintained status
    list §4 exists to retire.
    """
    return rx.cond(
        AuthState.can_edit,
        rx.cond(
            r.can_cancel,
            _cancel_dialog(r),
            rx.cond(
                r.stopping,
                rx.button(
                    _t["runs.cancel"],
                    size="1",
                    variant="soft",
                    color_scheme="gray",
                    disabled=True,
                ),
            ),
        ),
    )


def runs_table() -> rx.Component:
    return rx.table.root(
        rx.table.header(
            rx.table.row(
                rx.table.column_header_cell(_t["common.id"]),
                rx.table.column_header_cell(_t["runs.target"]),
                rx.table.column_header_cell(_t["common.status"]),
                rx.table.column_header_cell(_t["runs.started"]),
                rx.table.column_header_cell(_t["runs.finished"]),
                # core#1170 AC4.3. AC4.2 made the count honest — `None` for
                # "not measured" instead of a coerced 0 — and `_format_rows`
                # renders that as an em dash. Nothing said what the dash meant,
                # so an unmeasured count became indistinguishable from a
                # genuinely empty load: the same conflation, moved one layer out.
                rx.table.column_header_cell(
                    rx.hstack(
                        rx.text(_t["runs.rows"]),
                        info_tooltip("tooltip.rows_loaded"),
                        spacing="1",
                        align="center",
                    )
                ),
                rx.table.column_header_cell(_t["runs.error"]),
                rx.table.column_header_cell(_t["runs.logs"]),
                rx.table.column_header_cell(_t["common.actions"]),
            ),
        ),
        rx.table.body(
            rx.foreach(
                RunState.runs,
                lambda r: rx.table.row(
                    rx.table.cell(r.id),
                    rx.table.cell(rx.text(r.target_name)),
                    rx.table.cell(_status_badge(r)),
                    rx.table.cell(r.started_at),
                    rx.table.cell(r.finished_at),
                    rx.table.cell(r.rows_loaded),
                    rx.table.cell(
                        rx.cond(
                            r.error_message,
                            rx.tooltip(
                                rx.icon("circle_alert", size=16, color="red"),
                                content=r.error_message,
                            ),
                            rx.text(""),
                        ),
                    ),
                    rx.table.cell(
                        rx.icon_button(
                            rx.icon("file_text", size=16),
                            size="1",
                            variant="ghost",
                            on_click=RunState.view_logs(r.id),
                            aria_label=_t["runs.logs"],
                        ),
                    ),
                    rx.table.cell(_cancel_control(r)),
                ),
            ),
        ),
        width="100%",
    )


def logs_panel() -> rx.Component:
    return rx.cond(
        RunState.selected_run_id > 0,
        rx.card(
            rx.vstack(
                rx.hstack(
                    rx.heading(
                        _t["runs.logs_run"],
                        RunState.selected_run_id,
                        size="4",
                    ),
                    rx.spacer(),
                    rx.icon_button(
                        rx.icon("x", size=16),
                        size="1",
                        variant="ghost",
                        on_click=RunState.close_logs,
                        aria_label=_t["common.close"],
                    ),
                    width="100%",
                    align="center",
                ),
                rx.code_block(
                    RunState.selected_run_logs,
                    language="log",
                    width="100%",
                    wrap_long_lines=True,
                ),
                spacing="4",
                width="100%",
            ),
            width="100%",
        ),
    )


def runs_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            filters_bar(),
            runs_table(),
            # V2 pricing pivot — ELT nudge card, gated by feature flag.
            (elt_nudge_card() if settings.datanika_dual_mode_ux_enabled else rx.fragment()),
            logs_panel(),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.runs"],
    )
