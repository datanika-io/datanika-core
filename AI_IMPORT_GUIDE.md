# Datanika AI Import Guide

You are helping a user set up data pipelines in Datanika. The user will describe what they want in natural language. Your job is to produce a JSON file that they can upload to Datanika via **Settings > Import File**.

## JSON Format (version 2)

```json
{
  "version": 2,
  "connections": [...],
  "uploads": [...],
  "pipelines": [...],
  "transformations": [...]
}
```

All sections are optional. Include only what the user needs. The import is **non-destructive** — it merges with existing data, never deletes anything.

---

## Connections

A connection is a source or destination database/API/file that Datanika connects to.

```json
{
  "name": "My Postgres Source",
  "connection_type": "postgres",
  "config": {
    "host": "db.example.com",
    "port": 5432,
    "database": "mydb",
    "user": "readonly",
    "password": "CHANGE_ME"
  }
}
```

### Required fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Unique name within the organization |
| `connection_type` | string | One of the supported types below |
| `config` | object | Connection-specific settings (see below) |

Direction (source/destination/both) is automatically determined from `connection_type`. You don't need to specify it.

### Optional fields

| Field | Type | Description |
|-------|------|-------------|
| `freshness_config` | object | dbt source freshness settings (loaded_at_field, warn_after, error_after) |

### Supported connection types with config examples

Set sensitive values (`password`, `api_key`, `aws_secret_access_key`, `service_account_json`) to `"CHANGE_ME"`. The user will enter real credentials after import.

#### Databases (source + destination)

**postgres**
```json
{"connection_type": "postgres", "config": {"host": "db.example.com", "port": 5432, "database": "mydb", "user": "admin", "password": "CHANGE_ME"}}
```

**mysql**
```json
{"connection_type": "mysql", "config": {"host": "mysql.example.com", "port": 3306, "database": "mydb", "user": "root", "password": "CHANGE_ME"}}
```

**mssql**
```json
{"connection_type": "mssql", "config": {"host": "sql.example.com", "port": 1433, "database": "mydb", "user": "sa", "password": "CHANGE_ME"}}
```

**sqlite**
```json
{"connection_type": "sqlite", "config": {"path": "/data/local.db"}}
```

**clickhouse**
```json
{"connection_type": "clickhouse", "config": {"host": "ch.example.com", "port": 8123, "database": "default", "user": "default", "password": "CHANGE_ME", "secure": false}}
```

**duckdb**
```json
{"connection_type": "duckdb", "config": {"path": "/data/analytics.duckdb"}}
```

#### Cloud Warehouses (destination, some also source)

**bigquery**
```json
{"connection_type": "bigquery", "config": {"project": "my-gcp-project", "dataset": "raw_data", "service_account_json": "CHANGE_ME"}}
```

**snowflake**
```json
{"connection_type": "snowflake", "config": {"account": "xy12345.us-east-1", "user": "ETL_USER", "password": "CHANGE_ME", "database": "ANALYTICS", "warehouse": "ETL_WH", "schema": "RAW", "role": "ETL_ROLE"}}
```

**redshift**
```json
{"connection_type": "redshift", "config": {"host": "cluster.abc.us-east-1.redshift.amazonaws.com", "port": 5439, "database": "analytics", "user": "etl", "password": "CHANGE_ME"}}
```

**databricks**
```json
{"connection_type": "databricks", "config": {"host": "adb-123.azuredatabricks.net", "http_path": "/sql/1.0/warehouses/abc", "token": "CHANGE_ME", "catalog": "main"}}
```

**synapse**
```json
{"connection_type": "synapse", "config": {"host": "synapse.sql.azuresynapse.net", "port": 1433, "database": "pool", "user": "sqladmin", "password": "CHANGE_ME"}}
```

#### SaaS APIs (source only)

**stripe**
```json
{"connection_type": "stripe", "config": {"api_key": "CHANGE_ME"}}
```

**github**
```json
{"connection_type": "github", "config": {"access_token": "CHANGE_ME", "owner": "my-org", "repo": "my-repo"}}
```

**hubspot**
```json
{"connection_type": "hubspot", "config": {"api_key": "CHANGE_ME"}}
```

**salesforce**
```json
{"connection_type": "salesforce", "config": {"client_id": "CHANGE_ME", "client_secret": "CHANGE_ME", "username": "user@example.com", "password": "CHANGE_ME", "security_token": "CHANGE_ME"}}
```

**shopify**
```json
{"connection_type": "shopify", "config": {"shop_url": "my-store.myshopify.com", "access_token": "CHANGE_ME"}}
```

**jira**
```json
{"connection_type": "jira", "config": {"server_url": "https://mycompany.atlassian.net", "email": "user@example.com", "api_token": "CHANGE_ME"}}
```

**slack**
```json
{"connection_type": "slack", "config": {"token": "CHANGE_ME"}}
```

**zendesk**
```json
{"connection_type": "zendesk", "config": {"subdomain": "mycompany", "email": "admin@example.com", "api_token": "CHANGE_ME"}}
```

**airtable**
```json
{"connection_type": "airtable", "config": {"api_key": "CHANGE_ME", "base_id": "appXXXXXXXXXXXXXX"}}
```

**notion**
```json
{"connection_type": "notion", "config": {"api_key": "CHANGE_ME"}}
```

**google_analytics**
```json
{"connection_type": "google_analytics", "config": {"property_id": "123456789", "service_account_json": "CHANGE_ME"}}
```

**google_ads**
```json
{"connection_type": "google_ads", "config": {"customer_id": "123-456-7890", "service_account_json": "CHANGE_ME"}}
```

**facebook_ads**
```json
{"connection_type": "facebook_ads", "config": {"access_token": "CHANGE_ME", "account_id": "act_123456789"}}
```

**google_sheets**
```json
{"connection_type": "google_sheets", "config": {"spreadsheet_id": "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms", "service_account_json": "CHANGE_ME"}}
```

**mongodb**
```json
{"connection_type": "mongodb", "config": {"connection_string": "mongodb://user:CHANGE_ME@mongo.example.com:27017", "database": "mydb"}}
```

#### Files (source only)

**s3**
```json
{"connection_type": "s3", "config": {"bucket": "my-data-bucket", "aws_access_key_id": "CHANGE_ME", "aws_secret_access_key": "CHANGE_ME", "region": "us-east-1"}}
```

**csv**
```json
{"connection_type": "csv", "config": {"path": "/data/exports/orders.csv"}}
```

**json**
```json
{"connection_type": "json", "config": {"path": "/data/exports/events.json"}}
```

**parquet**
```json
{"connection_type": "parquet", "config": {"path": "/data/exports/transactions.parquet"}}
```

#### Streaming (source only)

**kafka**
```json
{"connection_type": "kafka", "config": {"bootstrap_servers": "kafka1:9092,kafka2:9092", "topics": "orders,events", "group_id": "datanika-consumer"}}
```

---

## Uploads (Extract + Load)

An upload extracts data from a source connection and loads it into a destination using dlt.

```json
{
  "name": "Load orders from MySQL",
  "description": "Full database sync of the orders table",
  "source_connection_name": "MySQL Production",
  "destination_connection_name": "Analytics Postgres",
  "dlt_config": {
    "load_mode": "single_table",
    "table_name": "orders",
    "write_disposition": "replace"
  }
}
```

### Required fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Unique name within the organization |
| `source_connection_name` | string | Must match a connection name (in this file or already in Datanika) |
| `destination_connection_name` | string | Must match a connection name |
| `dlt_config` | object | dlt extraction configuration |

### Optional fields

| Field | Type | Description |
|-------|------|-------------|
| `description` | string | Human-readable description |
| `status` | string | `"draft"` (default) or `"active"` |

### dlt_config options

| Field | Type | Description |
|-------|------|-------------|
| `load_mode` | string | `"single_table"` or `"full_database"` |
| `table_name` | string | Required for single_table mode |
| `write_disposition` | string | `"append"`, `"replace"`, or `"merge"` |
| `primary_key` | string | Column name for merge disposition |
| `incremental_key` | string | Column name for incremental loading |
| `schema_name` | string | Target schema in destination |
| `tables` | list of strings | Table filter for full_database mode |
| `schema_contract` | string | `"evolve"`, `"freeze"`, or `"discard"` |
| `filters` | list of objects | Row-level filters: `{"column": "...", "operator": "...", "value": "..."}` |

Filter operators: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `not_in`.

### Upload examples

**Single table, full replace:**
```json
{"name": "Load products", "source_connection_name": "MySQL Prod", "destination_connection_name": "Warehouse", "dlt_config": {"load_mode": "single_table", "table_name": "products", "write_disposition": "replace", "schema_name": "raw"}}
```

**Single table, incremental merge:**
```json
{"name": "Sync orders", "source_connection_name": "MySQL Prod", "destination_connection_name": "Warehouse", "dlt_config": {"load_mode": "single_table", "table_name": "orders", "write_disposition": "merge", "primary_key": "id", "incremental_key": "updated_at", "schema_name": "raw"}}
```

**Full database sync:**
```json
{"name": "Replicate CRM", "source_connection_name": "CRM Postgres", "destination_connection_name": "Warehouse", "dlt_config": {"load_mode": "full_database", "write_disposition": "replace", "schema_name": "raw_crm"}}
```

**Full database with table filter:**
```json
{"name": "Load selected tables", "source_connection_name": "MySQL Prod", "destination_connection_name": "Warehouse", "dlt_config": {"load_mode": "full_database", "tables": ["orders", "customers", "products"], "write_disposition": "append", "schema_name": "raw"}}
```

**With row-level filter:**
```json
{"name": "Load active users", "source_connection_name": "App DB", "destination_connection_name": "Warehouse", "dlt_config": {"load_mode": "single_table", "table_name": "users", "write_disposition": "replace", "filters": [{"column": "status", "operator": "eq", "value": "active"}]}}
```

---

## Pipelines (dbt Transform Orchestration)

A pipeline runs dbt commands (run, build, test, seed, snapshot, compile) against a destination connection.

```json
{
  "name": "Build analytics models",
  "description": "Run all staging + marts models",
  "destination_connection_name": "Analytics Postgres",
  "command": "build",
  "full_refresh": false,
  "models": [
    {"name": "staging"},
    {"name": "marts"}
  ]
}
```

### Required fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Unique name within the organization |
| `destination_connection_name` | string | Must match a connection name |
| `command` | string | `"run"`, `"build"`, `"test"`, `"seed"`, `"snapshot"`, or `"compile"` |

### Optional fields

| Field | Type | Description |
|-------|------|-------------|
| `description` | string | Human-readable description |
| `full_refresh` | bool | Run with `--full-refresh` flag (default: false) |
| `models` | list of objects | Each object must have a `"name"` key (dbt selector) |
| `custom_selector` | string | Raw dbt `--select` expression (overrides models list) |
| `status` | string | `"draft"` (default), `"active"`, `"paused"`, or `"error"` |

### Pipeline examples

**Run all models:**
```json
{"name": "Run everything", "destination_connection_name": "Warehouse", "command": "run"}
```

**Build specific model groups:**
```json
{"name": "Build staging", "destination_connection_name": "Warehouse", "command": "build", "models": [{"name": "staging"}]}
```

**Run tests only:**
```json
{"name": "Test data quality", "destination_connection_name": "Warehouse", "command": "test", "models": [{"name": "marts"}]}
```

**Full refresh with custom selector:**
```json
{"name": "Full rebuild marts", "destination_connection_name": "Warehouse", "command": "run", "full_refresh": true, "custom_selector": "marts.* --exclude marts.mart_temp"}
```

**Run dbt seed (load CSV reference data):**
```json
{"name": "Load seeds", "destination_connection_name": "Warehouse", "command": "seed"}
```

---

## Transformations (dbt SQL Models)

A transformation is a dbt SQL model that transforms loaded data.

```json
{
  "name": "stg_orders",
  "description": "Staging model for orders — clean and cast columns",
  "sql_body": "SELECT\n  id,\n  customer_id,\n  CAST(order_date AS DATE) AS order_date,\n  amount::numeric(12,2) AS amount,\n  status\nFROM {{ source('raw', 'orders') }}\nWHERE status != 'cancelled'",
  "materialization": "view",
  "schema_name": "staging",
  "tags": ["staging", "orders"]
}
```

### Required fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Valid dbt model name: letters, digits, underscores, hyphens; must start with letter or underscore |
| `sql_body` | string | dbt-compatible SQL (supports `{{ ref() }}`, `{{ source() }}`, Jinja) |
| `materialization` | string | `"view"`, `"table"`, `"incremental"`, `"ephemeral"`, or `"snapshot"` |

### Optional fields

| Field | Type | Description |
|-------|------|-------------|
| `description` | string | Human-readable description |
| `schema_name` | string | Target schema (default: `"staging"`) |
| `destination_connection_name` | string | Override the org's default destination |
| `tags` | list of strings | dbt tags for selection |
| `tests_config` | object | dbt generic tests (see below) |
| `incremental_config` | object | For incremental materialization: `{"unique_key": "id", "strategy": "merge"}` |

### tests_config format

```json
{
  "columns": {
    "id": {"not_null": true, "unique": true},
    "email": {"not_null": true},
    "status": {"accepted_values": ["active", "inactive", "pending"]}
  }
}
```

Supported tests: `not_null`, `unique`, `accepted_values`, `relationships`.

### Transformation examples

**Simple staging view:**
```json
{"name": "stg_orders", "sql_body": "SELECT id, customer_id, CAST(amount AS numeric(12,2)) AS amount, status, created_at FROM {{ source('raw', 'orders') }} WHERE status != 'cancelled'", "materialization": "view", "schema_name": "staging", "tags": ["staging"]}
```

**Table materialization with tests:**
```json
{"name": "mart_revenue", "sql_body": "SELECT date_trunc('month', o.created_at) AS month, SUM(o.amount) AS total_revenue, COUNT(*) AS order_count FROM {{ ref('stg_orders') }} o GROUP BY 1", "materialization": "table", "schema_name": "marts", "tags": ["marts", "finance"], "tests_config": {"columns": {"month": {"not_null": true, "unique": true}, "total_revenue": {"not_null": true}}}}
```

**Incremental model:**
```json
{"name": "fct_events", "sql_body": "SELECT id, user_id, event_type, created_at FROM {{ source('raw', 'events') }} {% if is_incremental() %} WHERE created_at > (SELECT MAX(created_at) FROM {{ this }}) {% endif %}", "materialization": "incremental", "schema_name": "analytics", "incremental_config": {"unique_key": "id", "strategy": "merge"}}
```

**Ephemeral (CTE-only, not materialized):**
```json
{"name": "int_active_users", "sql_body": "SELECT id, name, email FROM {{ ref('stg_users') }} WHERE status = 'active' AND last_login_at > CURRENT_DATE - INTERVAL '90 days'", "materialization": "ephemeral", "schema_name": "intermediate"}
```

**With relationships test:**
```json
{"name": "stg_order_items", "sql_body": "SELECT id, order_id, product_id, quantity, price FROM {{ source('raw', 'order_items') }}", "materialization": "view", "schema_name": "staging", "tests_config": {"columns": {"id": {"not_null": true, "unique": true}, "order_id": {"not_null": true, "relationships": {"to": "ref('stg_orders')", "field": "id"}}, "product_id": {"not_null": true}}}}
```

---

## Complete Example

User request: *"I have a MySQL database with orders and customers. I want to load them into Postgres, then create staging models and a mart that joins them."*

```json
{
  "version": 2,
  "connections": [
    {
      "name": "MySQL Production",
      "connection_type": "mysql",
      "config": {
        "host": "mysql.example.com",
        "port": 3306,
        "database": "production",
        "user": "readonly",
        "password": "CHANGE_ME"
      }
    },
    {
      "name": "Analytics Postgres",
      "connection_type": "postgres",
      "config": {
        "host": "pg.example.com",
        "port": 5432,
        "database": "analytics",
        "user": "etl",
        "password": "CHANGE_ME"
      }
    }
  ],
  "uploads": [
    {
      "name": "Load orders",
      "description": "Incremental sync of orders table",
      "source_connection_name": "MySQL Production",
      "destination_connection_name": "Analytics Postgres",
      "dlt_config": {
        "load_mode": "single_table",
        "table_name": "orders",
        "write_disposition": "merge",
        "primary_key": "id",
        "incremental_key": "updated_at",
        "schema_name": "raw"
      }
    },
    {
      "name": "Load customers",
      "description": "Full sync of customers table",
      "source_connection_name": "MySQL Production",
      "destination_connection_name": "Analytics Postgres",
      "dlt_config": {
        "load_mode": "single_table",
        "table_name": "customers",
        "write_disposition": "replace",
        "schema_name": "raw"
      }
    }
  ],
  "pipelines": [
    {
      "name": "Build analytics",
      "description": "Run all staging and mart models",
      "destination_connection_name": "Analytics Postgres",
      "command": "build",
      "models": [
        {"name": "staging"},
        {"name": "marts"}
      ]
    }
  ],
  "transformations": [
    {
      "name": "stg_orders",
      "description": "Clean and cast order columns",
      "sql_body": "SELECT\n  id,\n  customer_id,\n  CAST(order_date AS DATE) AS order_date,\n  amount::numeric(12,2) AS amount,\n  status\nFROM {{ source('raw', 'orders') }}\nWHERE status != 'cancelled'",
      "materialization": "view",
      "schema_name": "staging",
      "tags": ["staging", "orders"],
      "tests_config": {
        "columns": {
          "id": {"not_null": true, "unique": true},
          "customer_id": {"not_null": true}
        }
      }
    },
    {
      "name": "stg_customers",
      "description": "Clean customer data",
      "sql_body": "SELECT\n  id,\n  TRIM(name) AS name,\n  LOWER(email) AS email,\n  created_at\nFROM {{ source('raw', 'customers') }}",
      "materialization": "view",
      "schema_name": "staging",
      "tags": ["staging", "customers"],
      "tests_config": {
        "columns": {
          "id": {"not_null": true, "unique": true},
          "email": {"not_null": true, "unique": true}
        }
      }
    },
    {
      "name": "mart_customer_orders",
      "description": "Customer orders summary — total orders, revenue, last order date",
      "sql_body": "SELECT\n  c.id AS customer_id,\n  c.name,\n  c.email,\n  COUNT(o.id) AS total_orders,\n  SUM(o.amount) AS total_revenue,\n  MAX(o.order_date) AS last_order_date\nFROM {{ ref('stg_customers') }} c\nLEFT JOIN {{ ref('stg_orders') }} o ON o.customer_id = c.id\nGROUP BY c.id, c.name, c.email",
      "materialization": "table",
      "schema_name": "marts",
      "tags": ["marts", "customers", "orders"]
    }
  ]
}
```

## Validation Rules

The import validates the entire file before making any changes. If any errors are found, nothing is imported. All errors are returned at once so you can fix them in one pass.

### Connection rules
- Required fields: `name`, `connection_type`, `config`
- `name` must be a non-empty string (after trimming whitespace)
- `connection_type` must be one of the supported types listed above
- Direction is auto-determined from `connection_type` (no need to specify)
- No two connections in the same file can have the same name

### Upload rules
- Required fields: `name`, `source_connection_name`, `destination_connection_name`
- `name` must be non-empty
- `source_connection_name` must reference a connection in the file OR one already in Datanika
- `destination_connection_name` must reference a connection in the file OR one already in Datanika
- Source connection must be a type that supports reading (databases, APIs, files)
- Destination connection must be a type that supports writing (databases, warehouses)
- `status` (if provided) must be `"draft"`, `"active"`, `"paused"`, or `"error"`
- No duplicate upload names in the file

### Pipeline rules
- Required fields: `name`, `destination_connection_name`
- `name` must be non-empty
- `destination_connection_name` must reference a valid destination connection
- `command` (if provided) must be `"run"`, `"build"`, `"test"`, `"seed"`, `"snapshot"`, or `"compile"`
- No duplicate pipeline names in the file

### Transformation rules
- Required fields: `name`, `sql_body`
- `name` must match pattern: starts with a letter or underscore, followed by letters, digits, underscores, or hyphens (`^[a-zA-Z_][a-zA-Z0-9_-]*$`)
- `sql_body` must be non-empty
- `materialization` (if provided) must be `"view"`, `"table"`, `"incremental"`, `"ephemeral"`, or `"snapshot"`
- No duplicate transformation names in the file

### Error codes

If validation fails, you'll get errors with these codes:

| Code | Description |
|------|-------------|
| `MISSING_FIELD` | A required field is not present in the JSON |
| `EMPTY_FIELD` | A required field is empty or whitespace-only |
| `INVALID_CONNECTION_TYPE` | `connection_type` is not a recognized value |
| `INVALID_ENUM_VALUE` | An enum field (`status`, `command`, `materialization`) has an invalid value |
| `INVALID_NAME_FORMAT` | Transformation name doesn't match the required pattern |
| `DUPLICATE_NAME` | Two items of the same type have the same name in the file |
| `UNKNOWN_CONNECTION_REF` | Upload/pipeline/transformation references a connection that doesn't exist |

---

## How to Import

1. Save the JSON as a `.json` file
2. Log in to Datanika at https://app.datanika.io
3. Go to **Settings** (sidebar)
4. In the **Backup & Import** section, click **Import File**
5. Upload the JSON file
6. If any items already exist, choose: **Skip** (keep existing), **Overwrite** (replace), or **Rename** (create as copy)
7. After import, go to each connection and enter the real passwords (they're set to `CHANGE_ME`)
8. Run your uploads, then your pipelines
