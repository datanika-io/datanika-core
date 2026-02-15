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

**Completed (Steps 1-23, 28-29, 33 of PLAN.md):**
- Step 1: Project setup — pyproject.toml, docker-compose, .env, rxconfig, Alembic, Celery config
- Step 2: Database models — 9 tables (Organization, User, Membership, Connection, Pipeline, Transformation, Dependency, Schedule, Run) with integer PKs, TenantMixin, TimestampMixin with soft-delete
- Step 3: Auth & encryption — AuthService (bcrypt + JWT + RBAC), EncryptionService (Fernet)
- Step 4: Multi-tenant Alembic — migration helpers (PUBLIC_TABLES, is_public_table, is_tenant_table), two-phase env.py (public then tenant schemas), TenantService creates tenant tables on provisioning
- Step 5: ConnectionService — CRUD with encrypted credentials, soft delete, org isolation, basic test_connection validation
- Step 6: PipelineService — CRUD with dlt_config validation (write_disposition, merge requires primary_key), connection direction enforcement, PipelineConfigError
- Step 7: Pipeline execution — ExecutionService (run lifecycle: create/start/complete/fail/cancel), run_pipeline function with mocked dlt, Celery task wrapper
- Step 8: TransformationService — CRUD with sql_body/schema_name/tests_config validation, soft delete, org isolation, TransformationConfigError
- Step 9: ScheduleService — CRUD with cron validation (5 fields), target validation (pipeline/transformation existence), toggle active, ScheduleConfigError
- Step 10: Minimal Reflex UI — sidebar layout, 6 pages (dashboard, connections, pipelines, transformations, schedules, runs), state classes with rx.Base data models, hardcoded org_id=1
- Step 11: DependencyService — CRUD with validation (self-reference/duplicate rejection, upstream/downstream node existence), get_upstream/get_downstream queries, soft delete, org isolation, DependencyConfigError
- Step 12: Transformation execution — run_transformation function with mocked dbt, Celery task wrapper, same dual-mode pattern as run_pipeline (external session for tests, internal for Celery)
- Step 13: Enhanced UI — DashboardState with dynamic stats (pipeline/transformation/schedule counts, recent runs), DagState with dependency CRUD, /dag page with edge table + add/remove form, runs page target_type filter, sidebar Dependencies link
- Step 14: UserService — User/Org/Membership CRUD, registration with email lowering, authenticate with JWT scoped to first org, authenticate_for_org, last-owner protection on remove/demote, UserServiceError
- Step 15: Auth UI integration — AuthState (login/signup/logout/switch_org/check_auth), BaseState.org_id reads from AuthState via get_state(), login/signup pages, auth guards on all protected routes, sidebar user info + logout
- Step 16: Settings page — SettingsState with org profile editing and member management (invite/role change/remove), /settings page with org card + members table
- Step 17: DltRunnerService — dlt pipeline/source/destination factory with batch processing (build_destination, build_source, build_pipeline, execute), supports postgres/mysql/mssql/sqlite, configurable batch_size, log_callback for progress; pipeline_tasks.py uses DltRunnerService instead of inline dlt calls
- Step 18: DbtProjectService — per-tenant dbt project scaffolding (ensure_project, write_model, generate_profiles_yml, run_model, remove_model), dbt_project.yml/profiles.yml generation, dbtRunner API execution; transformation_tasks.py uses DbtProjectService instead of _execute_dbt stub
- Step 19: SchedulerIntegrationService — APScheduler bridge with PostgreSQL job store (sync_schedule, remove_schedule, sync_all, get_job, _dispatch_target), CronTrigger from 5-field cron, coalesce/max_instances/misfire_grace_time config; ScheduleService accepts optional scheduler_integration for auto-sync on create/update/delete/toggle
- Step 20: Pipeline load modes & source filtering — two modes (single_table with sql_table+incremental, full_database with sql_database+table_names filter), dlt_config validation (mode, table, table_names, incremental, batch_size, source_schema), DltRunnerService branches build_source on mode, execute() filters INTERNAL_CONFIG_KEYS and extracts batch_size, structured UI form with mode dropdown/conditional fields/raw JSON fallback
- Step 21: dbt tests from UI — structured tests_config validation (not_null/unique/accepted_values/relationships per column), DbtProjectService.write_tests_config() generates schema.yml test entries, DbtProjectService.run_test() invokes dbt test, TransformationState.run_tests() action, Run Tests button in UI
- Step 22: dbt compile (SQL preview) — DbtProjectService.compile_model() invokes dbt compile and returns compiled SQL, TransformationState.preview_compiled_sql() action, Preview SQL button + code block display in UI
- Step 23: Schema contract / evolution control — schema_contract validation in PipelineService (tables/columns/data_type entities, evolve/freeze/discard_value/discard_row values), passes through to dlt pipeline.run(), UI dropdowns for each entity
- Step 28: API keys & service accounts — ApiKey model (org_id, user_id, name, key_hash, scopes, expires_at, last_used_at, soft delete), ApiKeyService (create with etf_ prefix + sha256 hash, authenticate with expiry/scope check + last_used tracking, list, revoke)
- Step 29: Audit logging — AuditLog model (org_id, user_id, action, resource_type, resource_id, old_values, new_values, ip_address), AuditAction enum (create/update/delete/login/logout/run), AuditService (log_action, list_logs with action/resource_type/user_id/limit filters)
- Step 33: Data quality filters — filters validation in PipelineService (list of {column, op, value} dicts, 8 ops: eq/ne/gt/gte/lt/lte/in/not_in), DltRunnerService applies source.add_filter() with operator functions, filters in INTERNAL_CONFIG_KEYS

**Test suite: 497 tests, all passing** (51 model + 18 auth + 6 encryption + 2 tenant + 19 migration helpers + 23 connection service + 64 pipeline service + 20 execution service + 3 pipeline tasks + 21 dependency service + 5 transformation tasks + 36 transformation service + 26 schedule service + 15 UI state models + 48 user service + 12 auth state + 8 settings state + 38 dlt runner + 35 dbt project + 20 scheduler integration + 19 api key service + 11 audit service)

**Next up: Steps 24-27, 30-32 (REST API source, file sources, cloud destinations, SSO, dbt snapshots, source freshness, dbt packages)**

## Important Decisions Made
- **No passlib** — uses `bcrypt` library directly (passlib has compatibility issues with newer bcrypt versions)
- **No UUIDs for PKs** — all primary keys are integer autoincrement, managed by PostgreSQL as IDENTITY columns
- **Tenant data schemas are user-defined** — TenantService only creates config schema `tenant_{org_id}`, data schemas (raw/staging/dds/refined/etc.) are chosen by user per pipeline/transformation
- **SQLite for tests** — model tests use in-memory SQLite for speed; PK columns use `mapped_column(primary_key=True, autoincrement=True)` without explicit BigInteger to stay SQLite-compatible
- **FK columns use BigInteger** — non-PK foreign key columns and polymorphic reference columns use `BigInteger` explicitly

## Code Style

Ruff enforces: line length 100, Python 3.12+ target, rules `E,F,I,N,UP,B,SIM`. Pytest uses `asyncio_mode = "auto"`.
