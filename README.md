# Datanika

Build and run modern data pipelines in minutes.

Datanika is an all-in-one platform for extracting, loading, transforming, and orchestrating data pipelines through a simple web UI.

Think Airbyte + dbt Cloud + orchestration — in one platform.

---

## Why Datanika?

Modern data teams are forced to combine multiple tools:

• Connectors → Airbyte / Fivetran  
• Transformations → dbt Cloud  
• Orchestration → Airflow / Prefect  
• Monitoring → custom dashboards  

This stack is complex, expensive, and hard to maintain.

Datanika replaces it with one platform.

---

## What You Can Do

• Connect databases, APIs, and files  
• Load data into warehouses with incremental sync  
• Transform data using dbt SQL models  
• Schedule pipelines with cron  
• Monitor runs in real time  
• Manage multiple organizations securely  

All from one UI.

---

## Who It’s For

Datanika is built for:

• Small SaaS teams  
• Startups without dedicated data engineers  
• Agencies managing pipelines for clients  
• Companies needing self-hosted data stack  

---

## Key Features

• Multi-tenant architecture  
• Built-in dbt transformations  
• DAG orchestration  
• Incremental loading  
• Schema evolution control  
• Role-based access & audit logs  
• Python-native stack  

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
Not production-hardened yet.

Looking for design partners and early adopters.

👉 Contact: founders@datanika.io

---

## Roadmap

- [ ] ClickHouse connector  
- [ ] Slack / Telegram alerts  
- [ ] Kubernetes deployment  
- [ ] Usage-based billing  
- [ ] Hosted SaaS version  

---

## Quick Start

```bash
docker-compose up -d postgres redis
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
uv run reflex run
```

See full setup instructions below.

---

## Why Not Airbyte or dbt Cloud?

| Feature | Datanika | Airbyte | dbt Cloud |
|-----------|-----------|-----------|-----------|
| All-in-one platform | ✅ | ❌ | ❌ |
| Multi-tenant SaaS-ready | ✅ | ❌ | ❌ |
| Self-hosted | ✅ | ✅ | ❌ |
| Python-native stack | ✅ | ❌ | ❌ |

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

TBD
