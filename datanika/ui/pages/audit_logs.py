"""Audit Logs page — view organization activity history."""

import reflex as rx

from datanika.models.audit_log import AuditAction, AuditResourceType
from datanika.ui.components.layout import page_layout
from datanika.ui.components.searchable_select import searchable_select
from datanika.ui.state.audit_state import AuditLogItem, AuditState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations

#: Derived from the vocabularies themselves, never hand-listed beside them (core#1128).
#:
#: Both lists used to be literals inline in the component, and both drifted from what the
#: writers actually write — the resource list in *both directions at once*: it offered
#: ``membership``, which nothing has ever written, and omitted ``member``, which carries 7
#: of the 13 written types. So *"who removed this person?"* returned an empty table, and an
#: empty audit table does not read as a broken filter. It reads as **nobody did it**.
#:
#: This is the correction ``PII_PAYLOAD_KEYS`` already made for the redactor, for the same
#: reason its docstring gives: *"a hand list is what this whole change is correcting."*
#: Adding a member to ``AuditResourceType`` now makes that type filterable with no edit
#: here, and ``test_audit_call_site_vocabulary.py`` fails if a writer and the enum disagree
#: in either direction.
#:
#: ⚠️ ``"all"`` is the sentinel ``AuditState.load_audit_logs`` reads as *no filter*; it is
#: not a resource type and is deliberately not in the enum.
ACTION_FILTER_OPTIONS: list[str] = ["all", *sorted(a.value for a in AuditAction)]

RESOURCE_FILTER_OPTIONS: list[str] = ["all", *sorted(t.value for t in AuditResourceType)]


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
                    searchable_select(
                        ACTION_FILTER_OPTIONS,
                        value=AuditState.filter_action,
                        on_change=AuditState.set_filter_action,
                        size="2",
                    ),
                    spacing="1",
                    width="220px",
                ),
                rx.vstack(
                    rx.text(_t["audit.filter_resource"], size="2", weight="medium"),
                    searchable_select(
                        RESOURCE_FILTER_OPTIONS,
                        value=AuditState.filter_resource_type,
                        on_change=AuditState.set_filter_resource_type,
                        size="2",
                    ),
                    spacing="1",
                    width="220px",
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
