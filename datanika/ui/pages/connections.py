"""Connections page — list + create form with dynamic config fields."""

import reflex as rx

from datanika.ui.components.connection_config_fields import type_fields
from datanika.ui.components.layout import page_layout
from datanika.ui.state.connection_state import ConnectionState


def connection_form() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading(
                rx.cond(
                    ConnectionState.editing_conn_id > 0,
                    "Edit Connection",
                    "New Connection",
                ),
                size="4",
            ),
            rx.text("Connection Name *", size="2", weight="bold"),
            rx.input(
                placeholder="Connection name",
                value=ConnectionState.form_name,
                on_change=ConnectionState.set_form_name,
                required=True,
                size="3",
                width="100%",
            ),
            rx.select(
                [
                    "postgres",
                    "mysql",
                    "mssql",
                    "sqlite",
                    "rest_api",
                    "bigquery",
                    "snowflake",
                    "redshift",
                    "s3",
                    "csv",
                    "json",
                    "parquet",
                ],
                value=ConnectionState.form_type,
                on_change=ConnectionState.set_form_type,
                placeholder="Connection type",
            ),
            # Dynamic config fields (hidden when raw JSON is active)
            rx.cond(
                ~ConnectionState.form_use_raw_json,
                type_fields(),
            ),
            # Raw JSON toggle
            rx.checkbox(
                "Use raw JSON config",
                checked=ConnectionState.form_use_raw_json,
                on_change=ConnectionState.set_form_use_raw_json,
            ),
            rx.cond(
                ConnectionState.form_use_raw_json,
                rx.text_area(
                    placeholder='{"host": "localhost", "port": 5432}',
                    value=ConnectionState.form_config,
                    on_change=ConnectionState.set_form_config,
                ),
            ),
            rx.cond(
                ConnectionState.error_message,
                rx.callout(
                    ConnectionState.error_message, icon="triangle_alert", color_scheme="red"
                ),
            ),
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
                        "Save Changes",
                        "Create Connection",
                    ),
                    on_click=ConnectionState.save_connection,
                ),
                rx.button(
                    "Test Connection",
                    variant="outline",
                    on_click=ConnectionState.test_connection_from_form,
                ),
                rx.cond(
                    ConnectionState.editing_conn_id > 0,
                    rx.button(
                        "Cancel",
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
                rx.table.column_header_cell("ID"),
                rx.table.column_header_cell("Name"),
                rx.table.column_header_cell("Type"),
                rx.table.column_header_cell("Actions", padding_left="34px"),
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
                                "Test",
                                variant="outline",
                                size="1",
                                on_click=ConnectionState.test_saved_connection(
                                    conn.id
                                ),
                            ),
                            rx.button(
                                "Edit",
                                variant="outline",
                                size="1",
                                on_click=ConnectionState.edit_connection(conn.id),
                            ),
                            rx.button(
                                "Copy",
                                variant="outline",
                                size="1",
                                on_click=ConnectionState.copy_connection(conn.id),
                            ),
                            rx.button(
                                "Delete",
                                color_scheme="red",
                                size="1",
                                on_click=ConnectionState.delete_connection(conn.id),
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
        rx.vstack(connection_form(), connections_table(), spacing="6", width="100%"),
        title="Connections",
    )
