# ETL Fabric

Multi-tenant data pipeline management platform built with **dlt** (Extract+Load) and **dbt-core** (Transform).

## Stack

- **UI**: Reflex (Python → React)
- **Database**: PostgreSQL 16 (async via asyncpg + SQLAlchemy 2.0)
- **Task Queue**: Celery + Redis
- **Scheduling**: APScheduler with PostgreSQL job store
- **Auth**: JWT (access + refresh tokens), bcrypt password hashing, RBAC (owner/admin/editor/viewer)
- **Credential Storage**: Fernet symmetric encryption at rest

## Quick Start

```bash
# Prerequisites: Docker, Python 3.12+, uv

# 1. Start infrastructure
docker-compose up -d   # PostgreSQL + Redis

# 2. Install dependencies
uv venv
source .venv/bin/activate   # or .venv/Scripts/activate on Windows
uv pip install -e ".[dev]"

# 3. Configure
cp .env.example .env
# Edit .env — generate CREDENTIAL_ENCRYPTION_KEY:
#   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# 4. Run migrations
uv run alembic upgrade head

# 5. Start the app
uv run reflex run

# 6. Start Celery worker (separate terminal)
uv run celery -A etlfabric.tasks worker -l info
```

## Development

```bash
# Run tests
uv run pytest tests/ -v

# Lint & format
uv run ruff check etlfabric tests
uv run ruff format etlfabric tests

# Create migration
uv run alembic revision --autogenerate -m "description"
```

## Project Structure

```
etlfabric/
├── models/          # SQLAlchemy ORM models (9 tables)
├── services/        # Business logic (auth, encryption, tenant)
├── tasks/           # Celery async tasks
├── migrations/      # Alembic database migrations
├── dbt_projects/    # Per-tenant dbt projects (generated)
└── ui/              # Reflex frontend
    ├── state/       # Reactive state classes
    ├── pages/       # Route handlers
    └── components/  # Reusable UI components
```

See [PLAN.md](PLAN.md) for the full implementation roadmap.
