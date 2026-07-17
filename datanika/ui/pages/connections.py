"""Connections page — list + create form with dynamic config fields."""

import reflex as rx

from datanika.plugin_registry import get_page_scripts
from datanika.ui.components.connection_config_fields import type_fields
from datanika.ui.components.layout import page_layout
from datanika.ui.components.quota_callout import error_or_quota_callout
from datanika.ui.components.searchable_select import searchable_select
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
    "s3",
    "csv",
    "json",
    "parquet",
    # Generic APIs
    "rest_api",
    "google_sheets",
    # SaaS / CRM
    "stripe",
    "salesforce",
    "hubspot",
    "shopify",
    "zendesk",
    "airtable",
    "notion",
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
            rx.input(
                placeholder=_t["connections.ph_name"],
                value=ConnectionState.form_name,
                on_change=ConnectionState.set_form_name,
                required=True,
                size="3",
                width="100%",
            ),
            searchable_select(
                PICKER_TYPES,
                value=ConnectionState.form_type,
                on_change=ConnectionState.set_form_type,
                placeholder=_t["connections.ph_type"],
                width="100%",
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
                rx.text_area(
                    placeholder=_t["connections.ph_raw_json"],
                    value=ConnectionState.form_config,
                    on_change=ConnectionState.set_form_config,
                    width="100%",
                ),
            ),
            error_or_quota_callout(ConnectionState),
            rx.cond(
                ConnectionState.test_message,
                rx.callout(
                    ConnectionState.test_message,
                    icon=rx.cond(ConnectionState.test_success, "check", "triangle_alert"),
                    color_scheme=rx.cond(ConnectionState.test_success, "green", "red"),
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


def connections_table() -> rx.Component:
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
                            rx.cond(
                                AuthState.can_delete,
                                rx.button(
                                    _t["common.delete"],
                                    color_scheme="red",
                                    size="1",
                                    on_click=ConnectionState.delete_connection(conn.id),
                                ),
                            ),
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
