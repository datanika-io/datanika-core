"""Centralized JSON Schemas for connection configs.

Single source of truth for what fields each connection type expects.
Used by:
- /api/v1/meta/connection-types discovery endpoint
- (future) UI form rendering
- (future) Connection.config validation

Each schema follows JSON Schema Draft 7. Sensitive fields are marked
with `"format": "password"` so the UI can render them as password inputs
and AI agents know not to log them.
"""

from datanika.models.connection import ConnectionType
from datanika.services.connection_service import (
    DESTINATION_TYPES,
    SOURCE_TYPES,
    infer_direction,
)


def _str(description: str, sensitive: bool = False) -> dict:
    s = {"type": "string", "description": description}
    if sensitive:
        s["format"] = "password"
    return s


def _int(description: str, default: int | None = None) -> dict:
    s = {"type": "integer", "description": description}
    if default is not None:
        s["default"] = default
    return s


def _bool(description: str, default: bool = False) -> dict:
    return {"type": "boolean", "description": description, "default": default}


def _schema(properties: dict, required: list[str]) -> dict:
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


# JSON Schemas for each connection type's `config` field
CONFIG_SCHEMAS: dict[str, dict] = {
    # ---- Databases ----
    "postgres": _schema(
        {
            "host": _str("Database hostname"),
            "port": _int("Port number", default=5432),
            "database": _str("Database name"),
            "user": _str("Username"),
            "password": _str("Password", sensitive=True),
        },
        required=["host", "database", "user", "password"],
    ),
    "mysql": _schema(
        {
            "host": _str("Database hostname"),
            "port": _int("Port number", default=3306),
            "database": _str("Database name"),
            "user": _str("Username"),
            "password": _str("Password", sensitive=True),
        },
        required=["host", "database", "user", "password"],
    ),
    "mssql": _schema(
        {
            "host": _str("SQL Server hostname"),
            "port": _int("Port number", default=1433),
            "database": _str("Database name"),
            "user": _str("Username"),
            "password": _str("Password", sensitive=True),
        },
        required=["host", "database", "user", "password"],
    ),
    "oracle": _schema(
        {
            "host": _str("Oracle hostname"),
            "port": _int("Port number", default=1521),
            "database": _str(
                "Service name (e.g. a PDB like XEPDB1, RAC, or Autonomous service); "
                "or the SID when 'use_sid' is enabled"
            ),
            "user": _str("Username"),
            "password": _str("Password", sensitive=True),
            "use_sid": _bool(
                "Connect by SID instead of service name (legacy single-instance Oracle)"
            ),
        },
        required=["host", "database", "user", "password"],
    ),
    "sqlite": _schema(
        {
            "path": _str("Path to SQLite file (or :memory:)"),
        },
        required=["path"],
    ),
    "clickhouse": _schema(
        {
            "host": _str("ClickHouse hostname"),
            "port": _int("HTTP port", default=8123),
            "database": _str("Database name"),
            "user": _str("Username"),
            "password": _str("Password", sensitive=True),
            "secure": _bool("Use HTTPS (enable for ClickHouse Cloud and TLS instances)"),
            "cluster_replication": _bool("Enable cluster replication (ReplicatedMergeTree)"),
        },
        required=["host", "database", "user", "password"],
    ),
    "duckdb": _schema(
        {
            "path": _str("Path to DuckDB file (or :memory:)"),
        },
        required=["path"],
    ),
    # ---- Cloud warehouses ----
    "bigquery": _schema(
        {
            "project": _str("GCP project ID"),
            "dataset": _str("BigQuery dataset name"),
            "service_account_json": _str(
                "Service account JSON (paste the full JSON)",
                sensitive=True,
            ),
        },
        required=["project", "dataset", "service_account_json"],
    ),
    "snowflake": _schema(
        {
            "account": _str("Snowflake account identifier (e.g. xy12345.us-east-1)"),
            "user": _str("Username"),
            "password": _str("Password", sensitive=True),
            "database": _str("Database name"),
            "warehouse": _str("Warehouse name"),
            "schema": _str("Schema name"),
            "role": _str("Role (optional)"),
        },
        required=["account", "user", "password", "database", "warehouse", "schema"],
    ),
    "redshift": _schema(
        {
            "host": _str("Redshift cluster endpoint"),
            "port": _int("Port number", default=5439),
            "database": _str("Database name"),
            "user": _str("Username"),
            "password": _str("Password", sensitive=True),
        },
        required=["host", "database", "user", "password"],
    ),
    "databricks": _schema(
        {
            "host": _str("Databricks workspace host"),
            "http_path": _str("SQL warehouse HTTP path"),
            "token": _str("Personal access token", sensitive=True),
            "catalog": _str("Unity Catalog name"),
        },
        required=["host", "http_path", "token", "catalog"],
    ),
    "synapse": _schema(
        {
            "host": _str("Synapse SQL endpoint"),
            "port": _int("Port number", default=1433),
            "database": _str("Database/pool name"),
            "user": _str("Username"),
            "password": _str("Password", sensitive=True),
        },
        required=["host", "database", "user", "password"],
    ),
    # ---- SaaS APIs (source only) ----
    "stripe": _schema(
        {"api_key": _str("Stripe secret key", sensitive=True)},
        required=["api_key"],
    ),
    "github": _schema(
        {
            "access_token": _str("GitHub personal access token", sensitive=True),
            "owner": _str("Repository owner (org or user)"),
            "repo": _str("Repository name"),
        },
        required=["access_token", "owner", "repo"],
    ),
    "hubspot": _schema(
        {"api_key": _str("HubSpot API key", sensitive=True)},
        required=["api_key"],
    ),
    "salesforce": _schema(
        {
            "client_id": _str("Connected app client ID"),
            "client_secret": _str("Connected app client secret", sensitive=True),
            "username": _str("Salesforce username"),
            "password": _str("Salesforce password", sensitive=True),
            "security_token": _str("Security token", sensitive=True),
        },
        required=[
            "client_id",
            "client_secret",
            "username",
            "password",
            "security_token",
        ],
    ),
    "shopify": _schema(
        {
            "shop_url": _str("Shop URL (e.g. my-store.myshopify.com)"),
            "access_token": _str("Admin API access token", sensitive=True),
        },
        required=["shop_url", "access_token"],
    ),
    "jira": _schema(
        {
            "server_url": _str("Jira server URL"),
            "email": _str("Account email"),
            "api_token": _str("API token", sensitive=True),
        },
        required=["server_url", "email", "api_token"],
    ),
    "slack": _schema(
        {"token": _str("Slack bot token", sensitive=True)},
        required=["token"],
    ),
    "zendesk": _schema(
        {
            "subdomain": _str("Zendesk subdomain"),
            "email": _str("Account email"),
            "api_token": _str("API token", sensitive=True),
        },
        required=["subdomain", "email", "api_token"],
    ),
    "airtable": _schema(
        {
            "api_key": _str("Airtable personal access token", sensitive=True),
            "base_id": _str("Base ID"),
        },
        required=["api_key", "base_id"],
    ),
    "notion": _schema(
        {"api_key": _str("Notion integration token", sensitive=True)},
        required=["api_key"],
    ),
    "pipedrive": _schema(
        {"api_key": _str("Pipedrive API token", sensitive=True)},
        required=["api_key"],
    ),
    "freshdesk": _schema(
        {
            "domain": _str("Freshdesk domain (the <domain> in <domain>.freshdesk.com)"),
            "api_key": _str("Freshdesk API key", sensitive=True),
        },
        required=["domain", "api_key"],
    ),
    "asana": _schema(
        {"api_key": _str("Asana personal access token", sensitive=True)},
        required=["api_key"],
    ),
    "google_analytics": _schema(
        {
            "property_id": _str("GA4 property ID"),
            "service_account_json": _str(
                "Service account JSON",
                sensitive=True,
            ),
        },
        required=["property_id", "service_account_json"],
    ),
    # "google_ads" is deliberately absent (core#555). It was schema'd with
    # `customer_id` + `service_account_json`, but every Google Ads API request
    # also needs a `developer-token` header — issued per manager account through
    # an application to Google, not something a user pastes from a settings
    # page. Nothing we stored could authenticate, so the connector could only
    # ever be created and then fail.
    #
    # Left out rather than completed because collecting the token is a product
    # decision with a much longer setup guide attached; see core#555 for the
    # route back. `ConnectionType.GOOGLE_ADS` remains so rows already stored
    # keep resolving.
    "facebook_ads": _schema(
        {
            "access_token": _str("Marketing API access token", sensitive=True),
            "account_id": _str("Ad account ID"),
        },
        required=["access_token", "account_id"],
    ),
    "google_sheets": _schema(
        {
            "spreadsheet_id": _str("Google Sheets spreadsheet ID"),
            "service_account_json": _str(
                "Service account JSON",
                sensitive=True,
            ),
        },
        required=["spreadsheet_id", "service_account_json"],
    ),
    "mongodb": _schema(
        {
            "host": _str("MongoDB host"),
            "port": _int("MongoDB port", default=27017),
            "user": _str("Username"),
            "password": _str("Password", sensitive=True),
            "database": _str("Database name"),
            # Exposed, not just defaulted (core#550). Defaulting to `admin`
            # fixes the standard deployment, but anyone whose user lives inside
            # the target database needs a way to say so — and a setting with no
            # surface is the core#499 mistake.
            "auth_source": _str(
                "Authentication database (default: admin — where MongoDB users "
                "are normally created; set to the database name if yours is not)"
            ),
        },
        required=["host", "database"],
    ),
    # ---- REST API ----
    "rest_api": _schema(
        {
            "base_url": _str("API base URL"),
            "auth_type": {
                "type": "string",
                "enum": ["none", "bearer", "api_key", "basic"],
                "default": "none",
                "description": "Authentication type",
            },
            "auth_token": _str("Auth token (for bearer/api_key)", sensitive=True),
            "auth_user": _str("Username (for basic auth)"),
            "auth_password": _str("Password (for basic auth)", sensitive=True),
        },
        required=["base_url"],
    ),
    "openapi": _schema(
        {
            "spec_inline": _str(
                "The OpenAPI 3.x spec (JSON or YAML) this connector was built from"
            ),
            "base_url": _str("API base URL (from the spec's servers; override if needed)"),
            "auth": {
                "type": "object",
                "description": (
                    "Auth config: {type: bearer|api_key|http_basic, ...}. "
                    "Fill credentials matching the spec's securitySchemes."
                ),
            },
            "headers": {"type": "object", "description": "Optional static request headers"},
            "resources": {
                "type": "array",
                "items": {"type": "object"},
                "description": (
                    "Derived resource catalog (from POST /connections/openapi/parse). "
                    "Each item is a rest_api resource plus derived columns."
                ),
            },
        },
        required=["base_url", "resources"],
    ),
    # ---- Files ----
    "s3": _schema(
        {
            "bucket_url": _str("S3 bucket URL, e.g. s3://my-bucket/path/prefix/"),
            "aws_access_key_id": _str("AWS access key ID (optional with IAM role)", sensitive=True),
            "aws_secret_access_key": _str(
                "AWS secret access key (optional with IAM role)", sensitive=True
            ),
            "region_name": _str("AWS region, e.g. us-east-1 (optional, auto-detected)"),
            "endpoint_url": _str("S3-compatible endpoint URL (MinIO, Backblaze B2, Cloudflare R2)"),
        },
        required=["bucket_url"],
    ),
    "csv": _schema(
        {"path": _str("Path to CSV file or directory")},
        required=["path"],
    ),
    "json": _schema(
        {"path": _str("Path to JSON file or directory")},
        required=["path"],
    ),
    "parquet": _schema(
        {"path": _str("Path to Parquet file or directory")},
        required=["path"],
    ),
    # ---- Streaming ----
    "kafka": _schema(
        {
            "bootstrap_servers": _str("Comma-separated Kafka brokers"),
            "topics": _str("Comma-separated topic names"),
            "group_id": _str("Consumer group ID"),
        },
        required=["bootstrap_servers", "topics", "group_id"],
    ),
}


def list_connection_types() -> list[dict]:
    """Return a list of all connection types with their schemas and direction."""
    items = []
    for ct in ConnectionType:
        slug = ct.value
        if slug not in CONFIG_SCHEMAS:
            # Should not happen — every connection type should have a schema
            continue
        direction = infer_direction(ct).value
        items.append(
            {
                "type": slug,
                "direction": direction,
                "is_source": slug in SOURCE_TYPES,
                "is_destination": slug in DESTINATION_TYPES,
                "config_schema": CONFIG_SCHEMAS[slug],
            }
        )
    return items


def get_connection_type(slug: str) -> dict | None:
    """Return a single connection type's schema, or None if unknown."""
    if slug not in CONFIG_SCHEMAS:
        return None
    try:
        ct = ConnectionType(slug)
    except ValueError:
        return None
    return {
        "type": slug,
        "direction": infer_direction(ct).value,
        "is_source": slug in SOURCE_TYPES,
        "is_destination": slug in DESTINATION_TYPES,
        "config_schema": CONFIG_SCHEMAS[slug],
    }
