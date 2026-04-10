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
🔔 **Notifications** — Slack, Telegram, email, and webhook alerts on run completion
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
- [x] REST API v1 with OpenAPI/Swagger
- [x] Notification channels (Slack, Telegram, Email, Webhook)
- [x] SSO (SAML/OIDC) for Enterprise
- [x] Usage-based billing (cloud plugin)
- [x] 73 security tests + 22 E2E tests
- [ ] Kubernetes Helm chart
- [ ] Pipeline templates (one-click setup)
- [ ] Data lineage visualization

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

## Contributing

We welcome contributors and design partners. Open an issue or contact info@datanika.io.

---

## License

[AGPL-3.0](LICENSE)
