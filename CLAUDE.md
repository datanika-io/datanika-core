# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

ETL Fabric — a multi-tenant data pipeline management platform. Uses dlt (Extract+Load) and dbt-core (Transform) with a Reflex (Python→React) UI, PostgreSQL, Celery+Redis task queue, and APScheduler.

## Commands

All commands must run inside the venv. Use `uv` for all dependency and environment management.

```bash
# Create venv and install
uv venv
source .venv/bin/activate  # or .venv/Scripts/activate on Windows
uv pip install -e ".[dev]"

# Add a new dependency
uv add <package>           # core dependency
uv add --dev <package>     # dev dependency

# Infrastructure (PostgreSQL 16 + Redis 7)
docker-compose up -d

# Run app (starts frontend on :3000, backend on :8000)
uv run reflex run

# Celery worker (separate terminal)
uv run celery -A etlfabric.tasks worker -l info

# Lint
uv run ruff check etlfabric tests
uv run ruff format etlfabric tests

# Tests
uv run pytest tests/
uv run pytest tests/test_models/test_user.py          # single file
uv run pytest -k "auth" tests/                         # pattern match
uv run pytest tests/ --cov=etlfabric --cov-report=html # with coverage

# Migrations
uv run alembic revision --autogenerate -m "description"
uv run alembic upgrade head
```

## Architecture

### Multi-Tenancy (schema-per-tenant)
- `public` schema: users, orgs, memberships (shared app metadata)
- `tenant_{org_id}`: per-org pipeline configs, connections, schedules
- `tenant_{org_id}_raw/staging/marts`: per-org pipeline data (dlt loads → dbt transforms)
- Tenant context is injected from JWT token via middleware

### Data Flow
Sources → **dlt** (extract+load into `_raw` schema) → **dbt** (transform into `_staging`/`_marts` schemas) → Analytics

### Key Patterns
- **Async DB everywhere**: SQLAlchemy 2.0+ with `asyncpg`, use `async with` sessions from `db.get_session()`
- **ORM models**: Inherit from `Base` + `TimestampMixin` + `TenantMixin`. Use `Mapped[type]` annotations, UUID primary keys
- **Config**: Pydantic Settings from `.env` — access via `from etlfabric.config import settings`
- **Credentials**: Encrypted at rest with `cryptography.fernet` (key in env var)
- **Long-running work**: Celery tasks (JSON serializer, Redis broker). Tasks named `etlfabric.{action}_{entity}`
- **Reflex UI**: Pages are functions returning `rx.Component`, state classes manage reactive data, entry point is `etlfabric/etlfabric.py`

### Layer Responsibilities
- `models/` — SQLAlchemy ORM (data shape only, no logic)
- `services/` — business logic, DB queries, dlt/dbt orchestration
- `tasks/` — Celery async tasks that call services
- `ui/state/` — Reflex state classes (bridge between UI and services)
- `ui/pages/` — route handlers returning components
- `ui/components/` — reusable UI building blocks

## Development Approach — TDD

Follow test-driven development for all new code:

1. **Write a failing test first** — before implementing any model, service, task, or component, write a test that defines the expected behavior.
2. **Make it pass** — write the minimum code to pass the test.
3. **Refactor** — clean up while keeping tests green.

Test placement mirrors source layout:
- `etlfabric/models/user.py` → `tests/test_models/test_user.py`
- `etlfabric/services/pipeline_service.py` → `tests/test_services/test_pipeline_service.py`
- `etlfabric/tasks/pipeline_tasks.py` → `tests/test_tasks/test_pipeline_tasks.py`

When implementing a new feature, the PR should contain tests *committed before or alongside* the implementation — never implementation without tests.

## Code Style

Ruff enforces: line length 100, Python 3.11+ target, rules `E,F,I,N,UP,B,SIM`. Pytest uses `asyncio_mode = "auto"`.
