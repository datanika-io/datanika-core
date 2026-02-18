# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Datanika — a multi-tenant data pipeline management platform. Uses dlt (Extract+Load) and dbt-core (Transform) with a Reflex (Python→React) UI, PostgreSQL, Celery+Redis task queue, and APScheduler.

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
uv run celery -A datanika.tasks worker -l info

# Lint
uv run ruff check datanika tests
uv run ruff format datanika tests

# Tests
uv run pytest tests/
uv run pytest tests/test_models/test_all_models.py     # single file
uv run pytest -k "auth" tests/                          # pattern match
uv run pytest tests/ --cov=datanika --cov-report=html  # with coverage

# Migrations
uv run alembic revision --autogenerate -m "description"
uv run alembic upgrade head
```

## Architecture

### Multi-Tenancy (org_id isolation in public schema)
- **All tables live in `public` schema** — `PUBLIC_TABLES` in `migrations/helpers.py` must include every model table
- Tenant isolation is via `org_id` column (TenantMixin), not per-tenant schemas
- `tenant_{org_id}` schemas are reserved for future data isolation only (not config tables)
- Data destination schemas (raw, staging, dds, etc.) are **user-configured** per pipeline/transformation — not hardcoded
- Tenant context is injected from JWT token via middleware

### Data Flow
Sources → **dlt** (extract+load into user-chosen schema) → **dbt** (transform into user-chosen schema) → Analytics

### Key Patterns
- **Async DB everywhere**: SQLAlchemy 2.0+ with `asyncpg`, use `async with` sessions from `db.get_session()`
- **ORM models**: Inherit from `Base` + `TimestampMixin` + `TenantMixin`. Use `Mapped[type]` annotations, **integer autoincrement primary keys** (PostgreSQL IDENTITY — no UUIDs for PKs)
- **Soft delete**: All timestamped models have `deleted_at` (nullable) via `TimestampMixin`
- **Config**: Pydantic Settings from `.env` — access via `from datanika.config import settings`
- **Auth**: `AuthService` — bcrypt password hashing (direct, not passlib), JWT access+refresh tokens via python-jose, RBAC with 4 roles (owner > admin > editor > viewer)
- **Credentials**: `EncryptionService` — Fernet encrypt/decrypt for connection credentials stored in DB
- **Long-running work**: Celery tasks (JSON serializer, Redis broker). Tasks named `datanika.{action}_{entity}`
- **Reflex UI**: Pages are functions returning `rx.Component`, state classes manage reactive data, entry point is `datanika/datanika.py`

### Layer Responsibilities
- `models/` — SQLAlchemy ORM (data shape only, no logic)
- `services/` — business logic, DB queries, dlt/dbt orchestration
- `tasks/` — Celery async tasks that call services
- `ui/state/` — Reflex state classes (bridge between UI and services)
- `ui/pages/` — route handlers returning components
- `ui/components/` — reusable UI building blocks
- `i18n/` — translation JSON files and loader

## i18n: Translation Maintenance

Every user-visible string in the UI **must** have a translation key. When adding or changing text that users see (headings, button labels, form labels, table headers, badge text, callout messages, navigation links):

1. Add the key to `datanika/i18n/en.json` first
2. Add the same key with a translated value to all other locale files: `ru.json`, `el.json`, `de.json`, `fr.json`, `es.json`
3. Reference it in the page/component via `_t = I18nState.translations` then `_t["your.key"]`

**What to translate:** headings, button labels, form field labels, table column headers, navigation text, badge labels, callout/info text, checkbox labels.

**What to skip:** input placeholders, dynamic error messages from services, status enum values (success/failed/running), technical identifiers, OAuth provider names (Google/GitHub).

Locale files live in `datanika/i18n/`. The test `tests/test_i18n/test_i18n.py::test_all_locales_have_same_keys` will fail if any locale is missing a key present in `en.json`.

## Development Approach — TDD

Follow test-driven development for all new code:

1. **Write a failing test first** — before implementing any model, service, task, or component, write a test that defines the expected behavior.
2. **Make it pass** — write the minimum code to pass the test.
3. **Refactor** — clean up while keeping tests green.

Test placement mirrors source layout:
- `datanika/models/user.py` → `tests/test_models/test_user.py`
- `datanika/services/pipeline_service.py` → `tests/test_services/test_pipeline_service.py`
- `datanika/tasks/pipeline_tasks.py` → `tests/test_tasks/test_pipeline_tasks.py`

When implementing a new feature, the PR should contain tests *committed before or alongside* the implementation — never implementation without tests.

**Bug fixes require a regression test.** Every bug fix must include a test that would have caught the bug — this prevents regressions and documents the fix. Write the test first (red), then fix the bug (green).

## Important Decisions Made
- **No passlib** — uses `bcrypt` library directly (passlib has compatibility issues with newer bcrypt versions)
- **No UUIDs for PKs** — all primary keys are integer autoincrement, managed by PostgreSQL as IDENTITY columns
- **All tables in public schema** — originally designed as schema-per-tenant for config tables, changed to public schema with org_id filtering after discovering services don't set search_path. `PUBLIC_TABLES` must include every model table or Alembic won't create them.
- **Alembic env.py requires explicit connection.commit()** — SQLAlchemy 2.0 autobegin doesn't auto-commit DDL after `context.begin_transaction()`. Without explicit commit, migrations log as successful but tables don't persist.
- **SQLite for tests** — model tests use in-memory SQLite for speed; PK columns use `mapped_column(primary_key=True, autoincrement=True)` without explicit BigInteger to stay SQLite-compatible
- **FK columns use BigInteger** — non-PK foreign key columns and polymorphic reference columns use `BigInteger` explicitly
- **Reflex 0.8.x uses Starlette, not FastAPI** — custom API routes use `starlette.routing.Route` and are appended to `app._api.routes`, not FastAPI router

## Code Style

Ruff enforces: line length 100, Python 3.12+ target, rules `E,F,I,N,UP,B,SIM`. Pytest uses `asyncio_mode = "auto"`.
