# Datanika

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL_3.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/Python-3.12+-blue.svg)](https://www.python.org)
[![Built with Reflex](https://img.shields.io/badge/Built_with-Reflex-purple.svg)](https://reflex.dev)
[![dlt](https://img.shields.io/badge/Extract%20%2B%20Load-dlt-orange.svg)](https://dlthub.com)
[![dbt-core](https://img.shields.io/badge/Transform-dbt--core-green.svg)](https://www.getdbt.com)

**Open-source data pipeline platform — Extract, Load, Transform, and Orchestrate from a single UI.**

Datanika combines [dlt](https://dlthub.com) (extract + load) with [dbt-core](https://www.getdbt.com) (transform) and adds visual pipeline management, scheduling, and monitoring — all in one Python-native platform.

> Think Airbyte + dbt Cloud + Airflow — in one tool, self-hostable with Docker Compose.

---

## Features

🔌 **32 Connectors** — PostgreSQL, MySQL, BigQuery, Snowflake, Stripe, HubSpot, Salesforce, Kafka, S3, and more
🔄 **dbt Transformations** — SQL models, tests, snapshots, packages, and source freshness built in
📊 **Visual Pipeline Builder** — DAG editor with dependency management
⏰ **Scheduling** — Cron-based with APScheduler, persistent across restarts
📈 **Monitoring** — Run history, streaming logs, and dashboard stats
🔐 **Enterprise Security** — RBAC, SSO (SAML/OIDC), audit logging, encrypted credentials
🌍 **9 Languages** — English, German, French, Spanish, Russian, Greek, Chinese, Arabic, Serbian
🔌 **REST API** — Full CRUD with OpenAPI/Swagger docs, rate limiting, and scoped API keys
🤖 **AI-Agent Ready** — `/llms.txt`, agent-guide.md, 5-tier capability API, compile+preview validation, typed error codes, `?wait=true`, `Idempotency-Key`, run cancellation
🚀 **Pipeline Templates** — One-click starter templates (Stripe→Postgres, Postgres→BigQuery, CSV→DuckDB) with prefilled connection configs
🔔 **Notifications** — Slack, Telegram, email, and webhook alerts on run completion, plus an in-app notification center
📦 **Self-Hostable** — Single `docker compose up` — no Kubernetes required

---

## Quick Start

### Docker (recommended)

```bash
git clone https://github.com/datanika-io/datanika-core.git
cd datanika-core
cp .env.example .env
# Edit .env with your settings
docker compose up -d
```

App available at http://localhost:3000

### Development

```bash
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
docker compose up -d postgres redis  # infrastructure only
uv run reflex run                     # starts on :3000 + :8000
```

---

## Why Datanika?

| | Datanika | Airbyte | Fivetran | dbt Cloud |
|---|---|---|---|---|
| Extract + Load | ✅ 32 connectors | ✅ 400+ | ✅ 500+ | ❌ |
| Transformations | ✅ dbt built-in | ❌ | ❌ (add-on) | ✅ |
| Scheduling | ✅ Cron + DAG | ✅ Basic | ✅ Basic | ✅ |
| Pipeline DAG | ✅ Visual | ❌ | ❌ | ❌ |
| Self-host | ✅ Docker | ⚠️ Needs K8s | ❌ SaaS only | ❌ SaaS only |
| Open source | ✅ AGPL-3.0 | ⚠️ ELv2 | ❌ | ❌ |
| Notifications | ✅ Slack/Telegram/Email/Webhook | ✅ | ✅ | ✅ |
| Pricing | Free forever | Free tier limited | ~$250+/mo | ~$100+/mo |

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Frontend | [Reflex](https://reflex.dev) (Python → React) |
| Backend | Starlette (via Reflex) |
| Extract + Load | [dlt](https://dlthub.com) |
| Transform | [dbt-core](https://www.getdbt.com) |
| Database | PostgreSQL 16 |
| Task Queue | Celery + Redis |
| Scheduling | APScheduler |

---

## Roadmap

- [x] 32 connectors (databases, SaaS APIs, files, streaming)
- [x] dbt transformations, tests, snapshots, packages
- [x] REST API v1 with OpenAPI/Swagger and typed per-connector inline schemas
- [x] AI-agent compatibility (`/llms.txt`, agent-guide, 5-tier API, golden-path loop, `?wait=true`, `Idempotency-Key`, run cancel, MCP server)
- [x] Pipeline templates (one-click setup)
- [x] In-app notification center with Slack, Telegram, Email, Webhook channels
- [x] SSO (SAML/OIDC) for Enterprise
- [x] Usage-based billing (cloud plugin)
- [x] 1,700+ tests across unit, security, and E2E (SQLite in-memory for speed)
- [ ] Kubernetes Helm chart
- [ ] Data lineage visualization

---

## AI Agent Integration

Datanika ships a first-class [MCP](https://modelcontextprotocol.io/) server so AI agents (Claude Desktop, Claude Code, etc.) can browse connections, preview data, compile transformations, and manage pipelines natively.

```bash
# Install and run (read-only by default)
uvx --from "git+https://github.com/datanika-io/datanika-core#subdirectory=datanika-mcp" \
    datanika-mcp --url https://app.datanika.io --api-key YOUR_KEY
```

See [`datanika-mcp/README.md`](datanika-mcp/README.md) for Claude Desktop config snippets, the full tool list, and the `--allow-write` flag.

Additional agent resources:
- [`/llms.txt`](https://app.datanika.io/llms.txt) — discovery document
- [`/api/v1/openapi.json`](https://app.datanika.io/api/v1/openapi.json) — OpenAPI spec with typed inline schemas
- [`/api/v1/meta/agent-tiers`](https://app.datanika.io/api/v1/meta/agent-tiers) — 5-tier capability stack (JSON)
- [`docs/api_versioning.md`](docs/api_versioning.md) — stability tiers and deprecation policy

---

## Open-Core Strategy

Core platform is open-source (AGPL-3.0).
Cloud version adds billing, quotas, and usage metering via the `datanika-cloud` plugin.

---

## Links

- 🌐 **Website**: [datanika.io](https://datanika.io)
- 🚀 **Cloud Platform**: [app.datanika.io](https://app.datanika.io)
- 📖 **Documentation**: [datanika.io/docs](https://datanika.io/docs)
- 🔌 **Connectors**: [datanika.io/connectors](https://datanika.io/connectors)
- 📡 **API Reference**: [datanika.io/docs/api](https://datanika.io/docs/api)

---

## Security

Found a vulnerability? See [SECURITY.md](SECURITY.md) for our disclosure
policy, supported versions, and reporting instructions.

---

## Contributing

We welcome contributors and design partners. Open an issue or contact info@datanika.io.

---

## License

[AGPL-3.0](LICENSE)
