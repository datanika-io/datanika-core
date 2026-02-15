"""Connection type-specific config form fields."""

import reflex as rx

from etlfabric.ui.state.connection_state import ConnectionState


def db_fields() -> rx.Component:
    """Fields for postgres / mysql / mssql / redshift."""
    return rx.vstack(
        rx.text("Host", size="2", weight="bold"),
        rx.input(
            placeholder="localhost",
            value=ConnectionState.form_host,
            on_change=ConnectionState.set_form_host,
            width="100%",
        ),
        rx.text("Port", size="2", weight="bold"),
        rx.input(
            placeholder="5432",
            value=ConnectionState.form_port,
            on_change=ConnectionState.set_form_port,
            width="100%",
        ),
        rx.text("User", size="2", weight="bold"),
        rx.input(
            placeholder="postgres",
            value=ConnectionState.form_user,
            on_change=ConnectionState.set_form_user,
            width="100%",
        ),
        rx.text("Password", size="2", weight="bold"),
        rx.input(
            placeholder="Password",
            value=ConnectionState.form_password,
            on_change=ConnectionState.set_form_password,
            type="password",
            width="100%",
        ),
        rx.text("Database", size="2", weight="bold"),
        rx.input(
            placeholder="mydb",
            value=ConnectionState.form_database,
            on_change=ConnectionState.set_form_database,
            width="100%",
        ),
        rx.text("Schema (optional)", size="2", weight="bold"),
        rx.input(
            placeholder="public",
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
        rx.text("Database Path", size="2", weight="bold"),
        rx.input(
            placeholder="/data/my.db",
            value=ConnectionState.form_path,
            on_change=ConnectionState.set_form_path,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def bigquery_fields() -> rx.Component:
    """Fields for bigquery."""
    return rx.vstack(
        rx.text("GCP Project ID", size="2", weight="bold"),
        rx.input(
            placeholder="my-gcp-project",
            value=ConnectionState.form_project,
            on_change=ConnectionState.set_form_project,
            width="100%",
        ),
        rx.text("Dataset", size="2", weight="bold"),
        rx.input(
            placeholder="raw_data",
            value=ConnectionState.form_dataset,
            on_change=ConnectionState.set_form_dataset,
            width="100%",
        ),
        rx.text("Service Account JSON (optional)", size="2", weight="bold"),
        rx.text_area(
            placeholder='{"type": "service_account", ...}',
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
        rx.text("Account", size="2", weight="bold"),
        rx.input(
            placeholder="abc123.us-east-1",
            value=ConnectionState.form_account,
            on_change=ConnectionState.set_form_account,
            width="100%",
        ),
        rx.text("User", size="2", weight="bold"),
        rx.input(
            placeholder="SNOW_USER",
            value=ConnectionState.form_user,
            on_change=ConnectionState.set_form_user,
            width="100%",
        ),
        rx.text("Password", size="2", weight="bold"),
        rx.input(
            placeholder="Password",
            value=ConnectionState.form_password,
            on_change=ConnectionState.set_form_password,
            type="password",
            width="100%",
        ),
        rx.text("Database", size="2", weight="bold"),
        rx.input(
            placeholder="ANALYTICS",
            value=ConnectionState.form_database,
            on_change=ConnectionState.set_form_database,
            width="100%",
        ),
        rx.text("Warehouse", size="2", weight="bold"),
        rx.input(
            placeholder="COMPUTE_WH",
            value=ConnectionState.form_warehouse,
            on_change=ConnectionState.set_form_warehouse,
            width="100%",
        ),
        rx.text("Role", size="2", weight="bold"),
        rx.input(
            placeholder="SYSADMIN",
            value=ConnectionState.form_role,
            on_change=ConnectionState.set_form_role,
            width="100%",
        ),
        rx.text("Schema", size="2", weight="bold"),
        rx.input(
            placeholder="PUBLIC",
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
        rx.text("Bucket URL", size="2", weight="bold"),
        rx.input(
            placeholder="s3://my-bucket/path",
            value=ConnectionState.form_bucket_url,
            on_change=ConnectionState.set_form_bucket_url,
            width="100%",
        ),
        rx.text("AWS Access Key ID", size="2", weight="bold"),
        rx.input(
            placeholder="AKIAIOSFODNN7EXAMPLE",
            value=ConnectionState.form_aws_access_key_id,
            on_change=ConnectionState.set_form_aws_access_key_id,
            width="100%",
        ),
        rx.text("AWS Secret Access Key", size="2", weight="bold"),
        rx.input(
            placeholder="Secret key",
            value=ConnectionState.form_aws_secret_access_key,
            on_change=ConnectionState.set_form_aws_secret_access_key,
            type="password",
            width="100%",
        ),
        rx.text("Region", size="2", weight="bold"),
        rx.input(
            placeholder="us-east-1",
            value=ConnectionState.form_region_name,
            on_change=ConnectionState.set_form_region_name,
            width="100%",
        ),
        rx.text("Endpoint URL (optional)", size="2", weight="bold"),
        rx.input(
            placeholder="https://minio.local:9000",
            value=ConnectionState.form_endpoint_url,
            on_change=ConnectionState.set_form_endpoint_url,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def local_file_fields() -> rx.Component:
    """Fields for csv / json / parquet."""
    return rx.vstack(
        rx.text("File Path", size="2", weight="bold"),
        rx.input(
            placeholder="/data/files",
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
        rx.text("Base URL", size="2", weight="bold"),
        rx.input(
            placeholder="https://api.example.com",
            value=ConnectionState.form_base_url,
            on_change=ConnectionState.set_form_base_url,
            width="100%",
        ),
        rx.text("API Key (optional)", size="2", weight="bold"),
        rx.input(
            placeholder="API key for Authorization header",
            value=ConnectionState.form_api_key,
            on_change=ConnectionState.set_form_api_key,
            type="password",
            width="100%",
        ),
        rx.text("Extra Headers (optional, JSON)", size="2", weight="bold"),
        rx.text_area(
            placeholder='{"X-Custom-Header": "value"}',
            value=ConnectionState.form_extra_headers,
            on_change=ConnectionState.set_form_extra_headers,
            width="100%",
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
            local_file_fields(),
        ),
        rx.cond(ConnectionState.form_type == "rest_api", rest_api_fields()),
    )
