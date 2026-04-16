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
            rx.button(
                rx.icon("upload", size=16),
                _t["connections.upload_file"],
                variant="outline",
                cursor="pointer",
            ),
            id="file_upload",
            max_size=20 * 1024 * 1024,
            accept={
                "text/csv": [".csv"],
                "application/json": [".json"],
                "application/octet-stream": [".parquet"],
            },
            on_drop=ConnectionState.handle_file_upload(rx.upload_files(upload_id="file_upload")),
            no_drag=True,
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


def clickhouse_fields() -> rx.Component:
    """Fields for clickhouse — extends db_fields with secure + cluster replication."""
    return rx.vstack(
        db_fields(),
        rx.flex(
            rx.checkbox(
                _t["connections.secure"],
                checked=ConnectionState.form_secure,
                on_change=ConnectionState.set_form_secure,
            ),
            spacing="2",
            align="center",
        ),
        rx.flex(
            rx.checkbox(
                _t["connections.cluster_replication"],
                checked=ConnectionState.form_cluster_replication,
                on_change=ConnectionState.set_form_cluster_replication,
            ),
            spacing="2",
            align="center",
        ),
        rx.cond(
            ConnectionState.form_cluster_replication,
            rx.callout(
                _t["connections.cluster_hint"],
                icon="info",
                size="1",
            ),
        ),
        spacing="2",
        width="100%",
    )


def duckdb_fields() -> rx.Component:
    """Fields for duckdb — path to database file."""
    return rx.vstack(
        rx.text(_t["connections.db_path"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_duckdb_path"],
            value=ConnectionState.form_path,
            on_change=ConnectionState.set_form_path,
            required=True,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def stripe_fields() -> rx.Component:
    """Fields for stripe source."""
    return rx.vstack(
        rx.text(_t["connections.api_key"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_stripe_key"],
            value=ConnectionState.form_api_key,
            on_change=ConnectionState.set_form_api_key,
            type="password",
            required=True,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def github_fields() -> rx.Component:
    """Fields for github source."""
    return rx.vstack(
        rx.text(_t["connections.access_token"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_github_token"],
            value=ConnectionState.form_api_key,
            on_change=ConnectionState.set_form_api_key,
            type="password",
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.owner"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_github_owner"],
            value=ConnectionState.form_owner,
            on_change=ConnectionState.set_form_owner,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.repo"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_github_repo"],
            value=ConnectionState.form_repo,
            on_change=ConnectionState.set_form_repo,
            required=True,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def databricks_fields() -> rx.Component:
    """Fields for databricks."""
    return rx.vstack(
        rx.text(_t["connections.host"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_databricks_host"],
            value=ConnectionState.form_host,
            on_change=ConnectionState.set_form_host,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.http_path"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_http_path"],
            value=ConnectionState.form_http_path,
            on_change=ConnectionState.set_form_http_path,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.token"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_token"],
            value=ConnectionState.form_token,
            on_change=ConnectionState.set_form_token,
            type="password",
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.catalog"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_catalog"],
            value=ConnectionState.form_catalog,
            on_change=ConnectionState.set_form_catalog,
            width="100%",
        ),
        rx.text(_t["connections.schema"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_schema"],
            value=ConnectionState.form_schema,
            on_change=ConnectionState.set_form_schema,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def saas_api_key_fields() -> rx.Component:
    """Shared fields for SaaS sources that only need an API key (HubSpot, Slack)."""
    return rx.vstack(
        rx.text(_t["connections.api_key"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_api_key"],
            value=ConnectionState.form_api_key,
            on_change=ConnectionState.set_form_api_key,
            type="password",
            required=True,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def salesforce_fields() -> rx.Component:
    """Fields for salesforce source."""
    return rx.vstack(
        rx.text(_t["connections.access_token"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_salesforce_token"],
            value=ConnectionState.form_api_key,
            on_change=ConnectionState.set_form_api_key,
            type="password",
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.instance_url"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_instance_url"],
            value=ConnectionState.form_instance_url,
            on_change=ConnectionState.set_form_instance_url,
            required=True,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def shopify_fields() -> rx.Component:
    """Fields for shopify source."""
    return rx.vstack(
        rx.text(_t["connections.api_key"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_shopify_key"],
            value=ConnectionState.form_api_key,
            on_change=ConnectionState.set_form_api_key,
            type="password",
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.store_name"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_store_name"],
            value=ConnectionState.form_store,
            on_change=ConnectionState.set_form_store,
            required=True,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def jira_fields() -> rx.Component:
    """Fields for jira source."""
    return rx.vstack(
        rx.text(_t["connections.jira_domain"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_jira_domain"],
            value=ConnectionState.form_domain,
            on_change=ConnectionState.set_form_domain,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.jira_email"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_jira_email"],
            value=ConnectionState.form_email,
            on_change=ConnectionState.set_form_email,
            width="100%",
        ),
        rx.text(_t["connections.api_key"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_jira_token"],
            value=ConnectionState.form_api_key,
            on_change=ConnectionState.set_form_api_key,
            type="password",
            required=True,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def google_analytics_fields() -> rx.Component:
    """Fields for google_analytics source."""
    return rx.vstack(
        rx.text(_t["connections.property_id"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_property_id"],
            value=ConnectionState.form_property_id,
            on_change=ConnectionState.set_form_property_id,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.service_account_json_optional"], size="2", weight="bold"),
        rx.text_area(
            placeholder=_t["connections.ph_service_account_json"],
            value=ConnectionState.form_api_key,
            on_change=ConnectionState.set_form_api_key,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def google_ads_fields() -> rx.Component:
    """Fields for google_ads source."""
    return rx.vstack(
        rx.text(_t["connections.customer_id"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_customer_id"],
            value=ConnectionState.form_customer_id,
            on_change=ConnectionState.set_form_customer_id,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.service_account_json_optional"], size="2", weight="bold"),
        rx.text_area(
            placeholder=_t["connections.ph_service_account_json"],
            value=ConnectionState.form_api_key,
            on_change=ConnectionState.set_form_api_key,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def facebook_ads_fields() -> rx.Component:
    """Fields for facebook_ads source."""
    return rx.vstack(
        rx.text(_t["connections.access_token"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_fb_token"],
            value=ConnectionState.form_api_key,
            on_change=ConnectionState.set_form_api_key,
            type="password",
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.account_id"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_fb_account_id"],
            value=ConnectionState.form_account_id,
            on_change=ConnectionState.set_form_account_id,
            required=True,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def zendesk_fields() -> rx.Component:
    """Fields for zendesk source."""
    return rx.vstack(
        rx.text(_t["connections.zendesk_subdomain"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_zendesk_subdomain"],
            value=ConnectionState.form_domain,
            on_change=ConnectionState.set_form_domain,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.jira_email"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_jira_email"],
            value=ConnectionState.form_email,
            on_change=ConnectionState.set_form_email,
            width="100%",
        ),
        rx.text(_t["connections.api_key"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_zendesk_token"],
            value=ConnectionState.form_api_key,
            on_change=ConnectionState.set_form_api_key,
            type="password",
            required=True,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def airtable_fields() -> rx.Component:
    """Fields for airtable source."""
    return rx.vstack(
        rx.text(_t["connections.api_key"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_airtable_key"],
            value=ConnectionState.form_api_key,
            on_change=ConnectionState.set_form_api_key,
            type="password",
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.base_id"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_base_id"],
            value=ConnectionState.form_base_id,
            on_change=ConnectionState.set_form_base_id,
            required=True,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def kafka_fields() -> rx.Component:
    """Fields for kafka source."""
    return rx.vstack(
        rx.text(_t["connections.bootstrap_servers"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_bootstrap_servers"],
            value=ConnectionState.form_bootstrap_servers,
            on_change=ConnectionState.set_form_bootstrap_servers,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.topics"], " *", size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_topics"],
            value=ConnectionState.form_topics,
            on_change=ConnectionState.set_form_topics,
            required=True,
            width="100%",
        ),
        rx.text(_t["connections.group_id"], size="2", weight="bold"),
        rx.input(
            placeholder=_t["connections.ph_group_id"],
            value=ConnectionState.form_group_id,
            on_change=ConnectionState.set_form_group_id,
            width="100%",
        ),
        spacing="2",
        width="100%",
    )


def mongodb_fields() -> rx.Component:
    """Fields for mongodb (host/port/user/pass/database — no schema)."""
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
            | (ConnectionState.form_type == "redshift")
            | (ConnectionState.form_type == "synapse"),
            db_fields(),
        ),
        rx.cond(ConnectionState.form_type == "clickhouse", clickhouse_fields()),
        rx.cond(ConnectionState.form_type == "duckdb", duckdb_fields()),
        rx.cond(ConnectionState.form_type == "databricks", databricks_fields()),
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
        rx.cond(ConnectionState.form_type == "mongodb", mongodb_fields()),
        rx.cond(ConnectionState.form_type == "stripe", stripe_fields()),
        rx.cond(ConnectionState.form_type == "github", github_fields()),
        rx.cond(
            (ConnectionState.form_type == "hubspot") | (ConnectionState.form_type == "slack"),
            saas_api_key_fields(),
        ),
        rx.cond(ConnectionState.form_type == "salesforce", salesforce_fields()),
        rx.cond(ConnectionState.form_type == "shopify", shopify_fields()),
        rx.cond(ConnectionState.form_type == "jira", jira_fields()),
        rx.cond(ConnectionState.form_type == "google_analytics", google_analytics_fields()),
        rx.cond(ConnectionState.form_type == "google_ads", google_ads_fields()),
        rx.cond(ConnectionState.form_type == "facebook_ads", facebook_ads_fields()),
        rx.cond(ConnectionState.form_type == "zendesk", zendesk_fields()),
        rx.cond(ConnectionState.form_type == "airtable", airtable_fields()),
        rx.cond(
            ConnectionState.form_type == "notion",
            saas_api_key_fields(),
        ),
        rx.cond(ConnectionState.form_type == "kafka", kafka_fields()),
    )
