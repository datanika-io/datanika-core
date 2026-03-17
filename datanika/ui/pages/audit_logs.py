"""Audit Logs page — view organization activity history."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.audit_state import AuditLogItem, AuditState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def _log_row(log: AuditLogItem) -> rx.Component:
    return rx.table.row(
        rx.table.cell(log.created_at),
        rx.table.cell(rx.badge(log.action)),
        rx.table.cell(log.resource_type),
        rx.table.cell(log.resource_id),
        rx.table.cell(log.ip_address),
    )


def audit_logs_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            # Filters
            rx.hstack(
                rx.vstack(
                    rx.text(_t["audit.filter_action"], size="2", weight="medium"),
                    rx.select(
                        ["all", "create", "update", "delete", "login", "logout", "run"],
                        value=AuditState.filter_action,
                        on_change=AuditState.set_filter_action,
                        size="2",
                    ),
                    spacing="1",
                ),
                rx.vstack(
                    rx.text(_t["audit.filter_resource"], size="2", weight="medium"),
                    rx.select(
                        [
                            "all",
                            "connection",
                            "upload",
                            "transformation",
                            "pipeline",
                            "schedule",
                            "membership",
                            "api_key",
                        ],
                        value=AuditState.filter_resource_type,
                        on_change=AuditState.set_filter_resource_type,
                        size="2",
                    ),
                    spacing="1",
                ),
                rx.button(
                    _t["audit.apply"],
                    on_click=AuditState.apply_filters,
                    size="2",
                ),
                spacing="4",
                align="end",
            ),
            # Logs table
            rx.cond(
                AuditState.logs.length() > 0,
                rx.table.root(
                    rx.table.header(
                        rx.table.row(
                            rx.table.column_header_cell(_t["audit.timestamp"]),
                            rx.table.column_header_cell(_t["audit.action"]),
                            rx.table.column_header_cell(_t["audit.resource_type"]),
                            rx.table.column_header_cell(_t["audit.resource_id"]),
                            rx.table.column_header_cell(_t["audit.ip_address"]),
                        ),
                    ),
                    rx.table.body(
                        rx.foreach(AuditState.logs, _log_row),
                    ),
                    width="100%",
                ),
                rx.text(_t["audit.no_logs"], color="gray"),
            ),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.audit_log"],
    )
