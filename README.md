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

- Connect databases, APIs, and files
- Load data into warehouses with incremental sync
- Transform data using dbt SQL models
- Schedule pipelines with cron
- Monitor runs in real time
- Manage multiple organizations securely
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

- **15 connectors** — PostgreSQL, MySQL, MSSQL, SQLite, BigQuery, Snowflake, Redshift, ClickHouse, MongoDB, Google Sheets, REST API, S3, CSV, JSON, Parquet
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
- **9 languages** — en, ru, el, de, fr, es, zh, ar, sr with runtime switching
- **Multi-tenant** — org-level isolation with 4-tier RBAC (owner/admin/editor/viewer)
- **Role-based access & audit logs** — full action history with old/new values
- **API keys** — service account tokens with scoping and expiry
- **OAuth SSO** — Google + GitHub social login
- **Bot protection** — reCAPTCHA v3 on login/signup
- **Monitoring** — Prometheus + Grafana + Node Exporter + cAdvisor
- **Hooks system** — event bus for plugin extensibility
- **Python-native stack** — no JavaScript frontend to maintain

---

## How It Works

Sources → dlt (extract + load) → dbt (transform) → Analytics

---

## Tech Stack

- Python + Reflex UI + PostgreSQL + Celery + Redis
- dlt for extraction & loading
- dbt-core for transformations

---

## Status

Core platform works locally and is under active development.

**Current counts**: 14 tables, 26 services, 15 pages, 15 state classes, 5 components, 43 test files, 9 locales.

Looking for design partners and early adopters.

Contact: founders@datanika.io

---

## Roadmap

- [x] ClickHouse connector
- [x] Usage-based billing (datanika-cloud plugin)
- [ ] Slack / Telegram alerts
- [ ] Kubernetes deployment
- [ ] Hosted SaaS version

---

## Quick Start

```bash
# Source environment for docker-compose
set -a && source .env.docker && set +a

# Start infrastructure
docker-compose up -d postgres redis

# Create virtualenv and install
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"

# Run app
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

---

## Open-Core Strategy

Core platform will be open-source.
Hosted cloud version with monitoring, autoscaling, and enterprise security will be paid.

---

## Contributing

We welcome contributors and design partners.

Open an issue or contact founders@datanika.io

---

## License

[AGPL-3.0](LICENSE)
