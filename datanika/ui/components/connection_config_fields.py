"""Connection type-specific config form fields."""

import reflex as rx

from datanika.ui.state.connection_state import ConnectionState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def db_fields() -> rx.Component:
    """Fields for postgres / mysql / mssql / redshift."""
    return rx.vstack(
        rx.text(_t["connections.host"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_host"],
            value=ConnectionState.form_host,
            on_change=ConnectionState.set_form_host,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.port"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_port"],
            value=ConnectionState.form_port,
            on_change=ConnectionState.set_form_port,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.user"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_user"],
            value=ConnectionState.form_user,
            on_change=ConnectionState.set_form_user,
            width="100%",
        ),
        rx.text(_t["connections.password"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_password"],
            value=ConnectionState.form_password,
            on_change=ConnectionState.set_form_password,
            type="password",
            width="100%",
        ),
        rx.text(_t["connections.database"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_database"],
            value=ConnectionState.form_database,
            on_change=ConnectionState.set_form_database,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.schema_optional"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_schema"],
            value=ConnectionState.form_schema,
            on_change=ConnectionState.set_form_schema,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def sqlite_fields() -> rx.Component:
    """Fields for sqlite."""
    return rx.vstack(
        rx.text(_t["connections.db_path"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_db_path"],
            value=ConnectionState.form_path,
            on_change=ConnectionState.set_form_path,
            required=True,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def bigquery_fields() -> rx.Component:
    """Fields for bigquery."""
    return rx.vstack(
        rx.text(_t["connections.gcp_project"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_gcp_project"],
            value=ConnectionState.form_project,
            on_change=ConnectionState.set_form_project,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.dataset"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_dataset"],
            value=ConnectionState.form_dataset,
            on_change=ConnectionState.set_form_dataset,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.service_account_json_optional"], size="2", weight="bold"),
        rx.text_area(
            placeholder=_t["connections.ph_service_account_json"],
            value=ConnectionState.form_keyfile_json,
            on_change=ConnectionState.set_form_keyfile_json,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def snowflake_fields() -> rx.Component:
    """Fields for snowflake."""
    return rx.vstack(
        rx.text(_t["connections.account"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_snowflake_account"],
            value=ConnectionState.form_account,
            on_change=ConnectionState.set_form_account,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.user"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_snowflake_user"],
            value=ConnectionState.form_user,
            on_change=ConnectionState.set_form_user,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.password"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_password"],
            value=ConnectionState.form_password,
            on_change=ConnectionState.set_form_password,
            type="password",
            width="100%",
        ),
        rx.text(_t["connections.database"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_snowflake_database"],
            value=ConnectionState.form_database,
            on_change=ConnectionState.set_form_database,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.warehouse"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_snowflake_warehouse"],
            value=ConnectionState.form_warehouse,
            on_change=ConnectionState.set_form_warehouse,
            width="100%",
        ),
        rx.text(_t["connections.role"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_snowflake_role"],
            value=ConnectionState.form_role,
            on_change=ConnectionState.set_form_role,
            width="100%",
        ),
        rx.text(_t["connections.schema"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_snowflake_schema"],
            value=ConnectionState.form_schema,
            on_change=ConnectionState.set_form_schema,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def s3_fields() -> rx.Component:
    """Fields for s3."""
    return rx.vstack(
        rx.text(_t["connections.bucket_url"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_bucket_url"],
            value=ConnectionState.form_bucket_url,
            on_change=ConnectionState.set_form_bucket_url,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.aws_access_key"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_aws_access_key"],
            value=ConnectionState.form_aws_access_key_id,
            on_change=ConnectionState.set_form_aws_access_key_id,
            width="100%",
        ),
        rx.text(_t["connections.aws_secret_key"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_secret_key"],
            value=ConnectionState.form_aws_secret_access_key,
            on_change=ConnectionState.set_form_aws_secret_access_key,
            type="password",
            width="100%",
        ),
        rx.text(_t["connections.region"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_region"],
            value=ConnectionState.form_region_name,
            on_change=ConnectionState.set_form_region_name,
            width="100%",
        ),
        rx.text(_t["connections.endpoint_url"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_endpoint_url"],
            value=ConnectionState.form_endpoint_url,
            on_change=ConnectionState.set_form_endpoint_url,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def file_upload_fields() -> rx.Component:
    """File upload widget + fallback path for csv/json/parquet connections."""
    return rx.vstack(
        rx.text(_t["connections.upload_file"], size="2", weight="bold"),
        rx.upload(
            rx.vstack(
                rx.text(_t["connections.drag_drop"]),
                rx.text(
                    _t["connections.max_file_size"],
                    size="1",
                    color_scheme="gray",
                ),
                align="center",
                spacing="1",
            ),
            id="file_upload",
            max_size=20 * 1024 * 1024,
            accept={
                "text/csv": [".csv"],
                "application/json": [".json"],
                "application/octet-stream": [".parquet"],
            },
            on_drop=ConnectionState.handle_file_upload(
                rx.upload_files(upload_id="file_upload")
            ),
            border="1px dashed var(--gray-7)",
            padding="4",
            width="100%",
        ),
        rx.cond(
            ConnectionState.form_uploaded_file_name != "",
            rx.hstack(
                rx.icon("file", size=16),
                rx.text(
                    _t["connections.file_uploaded"],
                    size="2",
                ),
                rx.text(ConnectionState.form_uploaded_file_name, size="2"),
                spacing="2",
            ),
        ),
        rx.text(_t["connections.or_enter_path"], size="1", color_scheme="gray"),
        rx.input(
            placeholder=_t["connections.ph_file_path"],
            value=ConnectionState.form_bucket_url,
            on_change=ConnectionState.set_form_bucket_url,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def rest_api_fields() -> rx.Component:
    """Fields for rest_api."""
    return rx.vstack(
        rx.text(_t["connections.base_url"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_base_url"],
            value=ConnectionState.form_base_url,
            on_change=ConnectionState.set_form_base_url,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.api_key"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_api_key"],
            value=ConnectionState.form_api_key,
            on_change=ConnectionState.set_form_api_key,
            type="password",
            width="100%",
        ),
        rx.text(_t["connections.extra_headers"], size="2", weight="bold"),
        rx.text_area(
            placeholder=_t["connections.ph_extra_headers"],
            value=ConnectionState.form_extra_headers,
            on_change=ConnectionState.set_form_extra_headers,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def google_sheets_fields() -> rx.Component:
    """Fields for google_sheets connection."""
    return rx.vstack(
        rx.text(_t["connections.spreadsheet_url"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_spreadsheet_url"],
            value=ConnectionState.form_spreadsheet_url,
            on_change=ConnectionState.set_form_spreadsheet_url,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.service_account_json"], " *", size="2", weight="bold"),
        rx.text_area(
            placeholder=_t["connections.ph_service_account_json"],
            value=ConnectionState.form_service_account_json,
            on_change=ConnectionState.set_form_service_account_json,
            width="100%",
            min_height="120px",
        ),
        rx.callout(
            _t["connections.share_spreadsheet"],
            icon="info",
        ),
        spacing="2",
        width="100%",
    )


def type_fields() -> rx.Component:
    """Render the appropriate config fields based on selected connection type."""
    return rx.fragment(
        rx.cond(
            (ConnectionState.form_type == "postgres")
            | (ConnectionState.form_type == "mysql")
            | (ConnectionState.form_type == "mssql")
            | (ConnectionState.form_type == "redshift"),
            db_fields(),
        ),
        rx.cond(ConnectionState.form_type == "sqlite", sqlite_fields()),
        rx.cond(ConnectionState.form_type == "bigquery", bigquery_fields()),
        rx.cond(ConnectionState.form_type == "snowflake", snowflake_fields()),
        rx.cond(ConnectionState.form_type == "s3", s3_fields()),
        rx.cond(
            (ConnectionState.form_type == "csv")
            | (ConnectionState.form_type == "json")
            | (ConnectionState.form_type == "parquet"),
            file_upload_fields(),
        ),
        rx.cond(ConnectionState.form_type == "rest_api", rest_api_fields()),
        rx.cond(ConnectionState.form_type == "google_sheets", google_sheets_fields()),
    )
