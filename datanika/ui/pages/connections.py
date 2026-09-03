"""Connections page — list + create form with dynamic config fields."""

import reflex as rx

from datanika.plugin_registry import get_page_scripts
from datanika.ui.components.connection_config_fields import type_fields
from datanika.ui.components.layout import page_layout
from datanika.ui.components.quota_callout import error_or_quota_callout
from datanika.ui.components.searchable_select import searchable_select
from datanika.ui.components.secure_input import config_input, config_text_area
from datanika.ui.components.table_loading import table_loading
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.connection_state import ConnectionState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations

# Picker options for the connection-type dropdown. Must cover every
# ConnectionType the backend dispatches on — see the coverage test at
# tests/test_ui/test_connection_picker_coverage.py. Grouped by category
# for UX (databases first, then warehouses, files, APIs, SaaS, analytics,
# messaging), not alphabetically.
PICKER_TYPES: list[str] = [
    # Databases
    "postgres",
    "mysql",
    "mssql",
    "oracle",
    "sqlite",
    "redshift",
    "synapse",
    "clickhouse",
    "duckdb",
    "mongodb",
    # Cloud warehouses
    "bigquery",
    "snowflake",
    "databricks",
    # File / blob
    # `s3` withdrawn — core#863; see WITHDRAWN_SOURCE_TYPES.
    "csv",
    "json",
    "parquet",
    # Generic APIs
    "rest_api",
    "openapi",
    "google_sheets",
    # SaaS / CRM
    "stripe",
    "salesforce",
    "hubspot",
    "shopify",
    "zendesk",
    "airtable",
    "notion",
    "pipedrive",
    "freshdesk",
    "asana",
    # Dev tools
    "github",
    "jira",
    "slack",
    # Analytics / ads
    "google_analytics",
    "google_ads",
    "facebook_ads",
    # Messaging
    "kafka",
]


def connection_form() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading(
                rx.cond(
                    ConnectionState.editing_conn_id > 0,
                    _t["connections.edit"],
                    _t["connections.new"],
                ),
                size="4",
            ),
            rx.text(_t["connections.name"], size="2", weight="bold"),
            # config_input, not rx.input: this is the first text field on the
            # form, so it is exactly the slot Chrome fills with the saved
            # username when it pairs against a password input below (core#618).
            config_input(
                "name",
                placeholder=_t["connections.ph_name"],
                value=ConnectionState.form_name,
                on_change=ConnectionState.set_form_name,
                required=True,
                size="3",
            ),
            searchable_select(
                PICKER_TYPES,
                value=ConnectionState.form_type,
                on_change=ConnectionState.set_form_type,
                placeholder=_t["connections.ph_type"],
                width="100%",
            ),
            # `s3` is withdrawn (core#863). The picker is a *searchable* select,
            # so someone looking for S3 types "s3" and gets nothing back — this
            # line is the difference between that and a silently shorter list.
            # ⚠️ Remove it in the same commit that returns `s3` to PICKER_TYPES.
            rx.text(
                _t["connections.s3_withdrawn"],
                size="1",
                color_scheme="gray",
            ),
            # Dynamic config fields (hidden when raw JSON is active)
            rx.cond(
                ~ConnectionState.form_use_raw_json,
                type_fields(),
            ),
            # Raw JSON toggle
            rx.checkbox(
                _t["connections.use_raw_json"],
                checked=ConnectionState.form_use_raw_json,
                on_change=ConnectionState.set_form_use_raw_json,
            ),
            rx.cond(
                ConnectionState.form_use_raw_json,
                # The raw-JSON escape hatch carries the same credentials the
                # generated fields do, so it gets the same opt-out.
                config_text_area(
                    "raw_json",
                    placeholder=_t["connections.ph_raw_json"],
                    value=ConnectionState.form_config,
                    on_change=ConnectionState.set_form_config,
                ),
            ),
            error_or_quota_callout(ConnectionState),
            rx.cond(
                ConnectionState.test_message,
                rx.callout(
                    ConnectionState.test_message,
                    # core#821: three states. `not tested` must not be green
                    # (that was the bug — 20 connector types reported success
                    # having made no request) and must not be red either, since
                    # the connection may well be fine.
                    icon=rx.cond(
                        ConnectionState.test_untested,
                        "info",
                        rx.cond(ConnectionState.test_success, "check", "triangle_alert"),
                    ),
                    color_scheme=rx.cond(
                        ConnectionState.test_untested,
                        "gray",
                        rx.cond(ConnectionState.test_success, "green", "red"),
                    ),
                ),
            ),
            rx.hstack(
                rx.button(
                    rx.cond(
                        ConnectionState.editing_conn_id > 0,
                        _t["common.save_changes"],
                        _t["connections.create"],
                    ),
                    on_click=ConnectionState.save_connection,
                ),
                rx.button(
                    _t["connections.test"],
                    variant="outline",
                    on_click=ConnectionState.test_connection_from_form,
                ),
                rx.cond(
                    ConnectionState.editing_conn_id > 0,
                    rx.button(
                        _t["common.cancel"],
                        variant="soft",
                        color_scheme="gray",
                        on_click=ConnectionState.cancel_edit,
                    ),
                ),
                spacing="3",
            ),
            spacing="3",
            width="100%",
        ),
        width="100%",
    )


def _delete_connection_dialog(conn) -> rx.Component:
    """Ask before retiring a connection (core#804).

    Delete used to be wired straight to the handler, so one click removed a
    production connection with no dialog, no toast and no undo affordance —
    and the safety procedure we wrote for ourselves in `WORKFLOW_RULES.md`
    §7b told agents to *"assert the dialog is open"*, describing a control that
    did not exist. Following that literally means concluding the click failed
    and clicking again, which on this page is a second deletion.

    `rx.alert_dialog` renders Radix's AlertDialog, i.e. a real
    ``role="alertdialog"`` element — that is what makes the rule true again
    rather than merely differently worded.

    Self-gating on ``can_delete``: moving the button into a helper put it out of
    reach of the lexical gate-scan in `tests/test_ui/test_rbac_ui_visibility.py`,
    so the gate lives *here* rather than at the call site. A viewer that reaches
    a delete control by accident is the regression that guard exists for, and
    keeping it able to see this button matters more than where the `rx.cond` is
    written.
    """
    return rx.cond(
        AuthState.can_delete,
        rx.alert_dialog.root(
            rx.alert_dialog.trigger(
                rx.button(_t["common.delete"], color_scheme="red", size="1"),
            ),
            rx.alert_dialog.content(
                rx.alert_dialog.title(_t["connections.delete_title"]),
                rx.alert_dialog.description(_t["connections.delete_body"]),
                rx.vstack(
                    # Name what is about to go, by id *and* name: the id alone
                    # is what an agent aims by, the name is what a human
                    # recognises.
                    rx.card(
                        rx.text("#", conn.id, "  ", conn.name, size="2", weight="bold"),
                        rx.text(conn.connection_type, size="1", color="var(--gray-9)"),
                    ),
                    rx.cond(
                        conn.dependent_count > 0,
                        rx.callout(
                            rx.vstack(
                                rx.hstack(
                                    rx.text(conn.dependent_count, size="2", weight="bold"),
                                    rx.text(_t["connections.delete_dependents"], size="2"),
                                    spacing="1",
                                ),
                                rx.text(conn.dependent_names, size="1"),
                                spacing="1",
                                align="start",
                            ),
                            icon="triangle_alert",
                            color_scheme="amber",
                            width="100%",
                        ),
                    ),
                    rx.text(_t["connections.delete_reversible"], size="1", color="var(--gray-9)"),
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
                            _t["connections.delete_confirm"],
                            color_scheme="red",
                            on_click=ConnectionState.delete_connection(conn.id),
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


def connections_table() -> rx.Component:
    """The table, or the third state that says it has not arrived yet (core#872).

    An empty `connections` list is two different facts. Rendering the table
    unconditionally makes them pixel-identical, and the user's recovery action
    for "nothing happened" is to click Create again — which, with quota
    enforcement live, is the click that gets refused.
    """
    return rx.cond(
        ConnectionState.connections_loaded,
        _connections_table_loaded(),
        table_loading(),
    )


def _connections_table_loaded() -> rx.Component:
    return rx.table.root(
        rx.table.header(
            rx.table.row(
                rx.table.column_header_cell(_t["common.id"]),
                rx.table.column_header_cell(_t["common.name"]),
                rx.table.column_header_cell(_t["common.type"]),
                rx.table.column_header_cell(_t["common.actions"], padding_left="34px"),
            ),
        ),
        rx.table.body(
            rx.foreach(
                ConnectionState.connections,
                lambda conn: rx.table.row(
                    rx.table.cell(conn.id),
                    rx.table.cell(conn.name),
                    rx.table.cell(conn.connection_type),
                    rx.table.cell(
                        rx.hstack(
                            rx.icon(
                                rx.cond(
                                    conn.test_status == "ok",
                                    "circle-check",
                                    "circle-x",
                                ),
                                color=rx.cond(
                                    conn.test_status == "ok",
                                    "green",
                                    "red",
                                ),
                                size=16,
                                visibility=rx.cond(
                                    conn.test_status != "",
                                    "visible",
                                    "hidden",
                                ),
                            ),
                            rx.button(
                                _t["common.test"],
                                variant="outline",
                                size="1",
                                on_click=ConnectionState.test_saved_connection(conn.id),
                            ),
                            rx.cond(
                                AuthState.can_edit,
                                rx.button(
                                    _t["common.edit"],
                                    variant="outline",
                                    size="1",
                                    on_click=ConnectionState.edit_connection(conn.id),
                                ),
                            ),
                            rx.cond(
                                AuthState.can_edit,
                                rx.button(
                                    _t["common.copy"],
                                    variant="outline",
                                    size="1",
                                    on_click=ConnectionState.copy_connection(conn.id),
                                ),
                            ),
                            # Gate lives inside the helper — see its docstring.
                            _delete_connection_dialog(conn),
                            spacing="2",
                            align="center",
                        ),
                    ),
                ),
            ),
        ),
        width="100%",
    )


def connections_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            rx.cond(AuthState.can_edit, connection_form()),
            connections_table(),
            # Plugin-contributed scripts (e.g. cloud-edition Plausible
            # ``template_prefill_applied`` event on ?template=slug
            # landings). Empty in open-source builds.
            *get_page_scripts("connections"),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.connections"],
    )
