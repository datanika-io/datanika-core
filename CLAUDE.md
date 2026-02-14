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
uv run pytest tests/test_models/test_all_models.py     # single file
uv run pytest -k "auth" tests/                          # pattern match
uv run pytest tests/ --cov=etlfabric --cov-report=html  # with coverage

# Migrations
uv run alembic revision --autogenerate -m "description"
uv run alembic upgrade head
```

## Architecture

### Multi-Tenancy (schema-per-tenant)
- `public` schema: users, orgs, memberships (shared app metadata)
- `tenant_{org_id}`: per-org config schema (created by TenantService)
- Data destination schemas (raw, staging, dds, etc.) are **user-configured** per pipeline/transformation — not hardcoded
- Tenant context is injected from JWT token via middleware

### Data Flow
Sources → **dlt** (extract+load into user-chosen schema) → **dbt** (transform into user-chosen schema) → Analytics

### Key Patterns
- **Async DB everywhere**: SQLAlchemy 2.0+ with `asyncpg`, use `async with` sessions from `db.get_session()`
- **ORM models**: Inherit from `Base` + `TimestampMixin` + `TenantMixin`. Use `Mapped[type]` annotations, **integer autoincrement primary keys** (PostgreSQL IDENTITY — no UUIDs for PKs)
- **Soft delete**: All timestamped models have `deleted_at` (nullable) via `TimestampMixin`
- **Config**: Pydantic Settings from `.env` — access via `from etlfabric.config import settings`
- **Auth**: `AuthService` — bcrypt password hashing (direct, not passlib), JWT access+refresh tokens via python-jose, RBAC with 4 roles (owner > admin > editor > viewer)
- **Credentials**: `EncryptionService` — Fernet encrypt/decrypt for connection credentials stored in DB
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

## Current Implementation Status

**Completed (Steps 1-3 of PLAN.md):**
- Step 1: Project setup — pyproject.toml, docker-compose, .env, rxconfig, Alembic, Celery config
- Step 2: Database models — 9 tables (Organization, User, Membership, Connection, Pipeline, Transformation, Dependency, Schedule, Run) with integer PKs, TenantMixin, TimestampMixin with soft-delete
- Step 3: Auth & encryption — AuthService (bcrypt + JWT + RBAC), EncryptionService (Fernet)

**Test suite: 77 tests, all passing** (51 model tests + 18 auth tests + 6 encryption tests + 2 tenant tests)

**Next up: Step 4 (Alembic migrations with multi-tenant awareness), then Phase 2 (Steps 5-7: Connection management UI, Pipeline builder, Pipeline execution via Celery)**

## Important Decisions Made
- **No passlib** — uses `bcrypt` library directly (passlib has compatibility issues with newer bcrypt versions)
- **No UUIDs for PKs** — all primary keys are integer autoincrement, managed by PostgreSQL as IDENTITY columns
- **Tenant data schemas are user-defined** — TenantService only creates config schema `tenant_{org_id}`, data schemas (raw/staging/dds/refined/etc.) are chosen by user per pipeline/transformation
- **SQLite for tests** — model tests use in-memory SQLite for speed; PK columns use `mapped_column(primary_key=True, autoincrement=True)` without explicit BigInteger to stay SQLite-compatible
- **FK columns use BigInteger** — non-PK foreign key columns and polymorphic reference columns use `BigInteger` explicitly

## Code Style

Ruff enforces: line length 100, Python 3.12+ target, rules `E,F,I,N,UP,B,SIM`. Pytest uses `asyncio_mode = "auto"`.
