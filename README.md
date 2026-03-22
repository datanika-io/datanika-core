# Datanika

Build and run modern data pipelines in minutes.

Datanika is an all-in-one platform for extracting, loading, transforming, and orchestrating data pipelines through a simple web UI.

Think Airbyte + dbt Cloud + orchestration — in one platform.

---

## Why Datanika?

Modern data teams are forced to combine multiple tools:

- Connectors: Airbyte / Fivetran
- Transformations: dbt Cloud
- Orchestration: Airflow / Prefect
- Monitoring: custom dashboards

This stack is complex, expensive, and hard to maintain.

Datanika replaces it with one platform.

---

## What You Can Do

- Connect databases, SaaS APIs, and files (32 connectors)
- Load data into warehouses with incremental sync
- Transform data using dbt SQL models
- Schedule pipelines with cron
- Monitor runs in real time
- Manage multiple organizations securely
- Invite team members by email with role-based access
- Browse schemas and tables in the data catalog
- Write and preview SQL in a full-screen editor
- Upload CSV, JSON, and Parquet files directly
- Back up and restore your metadata

All from one UI.

---

## Who It's For

Datanika is built for:

- Small SaaS teams
- Startups without dedicated data engineers
- Agencies managing pipelines for clients
- Companies needing self-hosted data stack

---

## Key Features

- **32 connectors** — databases (PostgreSQL, MySQL, MSSQL, SQLite, ClickHouse, DuckDB, Databricks, Synapse), warehouses (BigQuery, Snowflake, Redshift), SaaS (Stripe, GitHub, HubSpot, Salesforce, Shopify, Jira, Slack, Zendesk, Airtable, Notion), analytics (Google Analytics, Google Ads, Facebook Ads), files (S3, CSV, JSON, Parquet), streams (Kafka), plus Google Sheets, MongoDB, and REST API
- **dbt transformations** — SQL models with materialization control, tests, snapshots, packages, and source freshness
- **DAG orchestration** — dependency graphs with topological execution
- **Cron scheduling** — persistent schedules with APScheduler
- **Incremental loading** — single-table and full-database extraction modes
- **Schema evolution** — evolve, freeze, or discard per entity
- **Data quality** — row-level filters with 8 operators
- **Data catalog** — auto-generated from uploads and transformations
- **SQL editor** — full-screen editor with autocomplete and compiled SQL preview
- **File uploads** — drag-and-drop CSV/JSON/Parquet ingestion
- **Backups** — metadata backup and restore
- **Email invitations** — invite team members by email, accept via link, JWT-based tokens
- **Email verification** — optional signup verification (configurable via SMTP settings)
- **9 languages** — en, ru, el, de, fr, es, zh, ar, sr with runtime switching
- **Multi-tenant** — org-level isolation with 4-tier RBAC (owner/admin/editor/viewer)
- **Role-based access & audit logs** — full action history with old/new values
- **API keys** — service account tokens with scoping and expiry
- **OAuth SSO** — Google + GitHub social login
- **Bot protection** — reCAPTCHA v3 on login/signup
- **Monitoring** — Prometheus + Grafana + Node Exporter + cAdvisor
- **Automated maintenance** — hourly cleanup of orphaned pipeline files, stale artifacts, old runs
- **Hooks system** — event bus for plugin extensibility
- **Python-native stack** — no JavaScript frontend to maintain

---

## How It Works

Sources → dlt (extract + load) → dbt (transform) → Analytics

---

## Tech Stack

- Python + Reflex UI + PostgreSQL + Celery + Redis
- dlt for extraction & loading (32 source/destination types)
- dbt-core for transformations (11 adapters)

---

## Status

Core platform is live at https://app.datanika.io and under active development.

**Current counts**: 15 tables, 31 services, 18 pages, 18 state classes, 7 components, 62 test files, 1224 unit tests, 9 locales.

Looking for design partners and early adopters.

Contact: info@datanika.io

---

## Roadmap

- [x] 32 connectors (databases, SaaS APIs, files, streams)
- [x] Usage-based billing (datanika-cloud plugin)
- [x] Email invitations and verification
- [x] Automated maintenance and cleanup
- [x] Security test suite (73 tests)
- [x] E2E test suite (22 tests on real databases)
- [ ] Slack / Telegram alerts
- [ ] Kubernetes deployment

---

## Quick Start

```bash
# Via Docker (recommended)
set -a && source .env.docker && set +a
docker compose up -d --build

# Or locally for development
uv venv && source .venv/bin/activate && uv pip install -e ".[dev]"
set -a && source .env.docker && set +a && docker-compose up -d postgres redis
uv run reflex run
```

See full setup instructions in [CLAUDE.md](CLAUDE.md).

---

## Why Not Airbyte or dbt Cloud?

| Feature | Datanika | Airbyte | dbt Cloud |
|-----------|-----------|-----------|-----------|
| All-in-one platform | Yes | No | No |
| Multi-tenant SaaS-ready | Yes | No | No |
| Self-hosted | Yes | Yes | No |
| Python-native stack | Yes | No | No |
| 32 built-in connectors | Yes | 300+ (separate) | No |
| dbt transformations | Yes | No | Yes |

---

## Open-Core Strategy

Core platform is open-source (AGPL-3.0).
Cloud version adds billing, quotas, and usage metering via the `datanika-cloud` plugin.

---

## Contributing

We welcome contributors and design partners.

Open an issue or contact info@datanika.io

---

## License

[AGPL-3.0](LICENSE)
