# Technical Design

This document describes the internal architecture, design patterns, and technical decisions behind Datanika. For a product overview, see [README.md](README.md).

## Architecture

```
                    +---------------------+
                    |     Reflex UI       |
                    |  (Python -> React)  |
                    |  :3000 / :8000      |
                    +----------+----------+
                               |
              +----------------+----------------+
              |                                 |
    +---------v---------+          +------------v-----------+
    |   State Classes   |          |   Starlette API        |
    |  (auth, upload,   |          |  (OAuth callbacks)     |
    |   pipeline, ...)  |          +-----------+------------+
    +---------+---------+                      |
              |                                |
    +---------v-----------------------------------v---------+
    |                   Services Layer                      |
    |  AuthService, UploadService, PipelineService,         |
    |  DltRunnerService, DbtProjectService, AuditService... |
    +-------+---------------------+---------------------+---+
            |                     |                     |
    +-------v-------+    +-------v-------+    +--------v-------+
    | PostgreSQL 16 |    | Celery+Redis  |    | APScheduler    |
    | (metadata +   |    | (async tasks) |    | (cron triggers)|
    |  credentials) |    +-------+-------+    +--------+-------+
    +---------------+            |                     |
                        +--------v---------------------v--------+
                        |          Execution Engine             |
                        |   dlt (extract+load) | dbt (transform)|
                        +---+------------------+---+------------+
                            |                      |
                   +--------v--------+    +--------v--------+
                   |   Data Sources  |    |  Destinations   |
                   | PG, MySQL, MSSQL|    | PG, BQ, SF, RS  |
                   | REST, S3, Files |    |  MySQL, MSSQL   |
                   +-----------------+    +-----------------+
```

### Layer Responsibilities

| Layer | Directory | Role |
|-------|-----------|------|
| **Models** | `datanika/models/` | SQLAlchemy ORM — data shape only, no logic |
| **Services** | `datanika/services/` | Business logic, DB queries, dlt/dbt orchestration |
| **Tasks** | `datanika/tasks/` | Celery async wrappers that call services |
| **UI State** | `datanika/ui/state/` | Reflex state classes — bridge between UI and services |
| **UI Pages** | `datanika/ui/pages/` | Route handlers returning Reflex components |
| **UI Components** | `datanika/ui/components/` | Reusable building blocks |
| **Migrations** | `migrations/` | Alembic database migrations |

### Data Flow

```
Sources -> dlt (extract + load into user-chosen schema)
        -> dbt (transform into user-chosen schema)
        -> Analytics
```

## Tech Stack

| Component | Technology |
|-----------|-----------|
| **Language** | Python 3.12+ |
| **UI** | Reflex 0.7+ (compiles Python to React) |
| **Database** | PostgreSQL 16, SQLAlchemy 2.0 async (asyncpg) |
| **Migrations** | Alembic |
| **Task Queue** | Celery 5.4+ with Redis 7 broker |
| **Scheduling** | APScheduler with PostgreSQL job store |
| **Extract & Load** | dlt with adapters for Postgres, Snowflake, BigQuery, MSSQL |
| **Transform** | dbt-core with adapters for Postgres, Snowflake, BigQuery, Redshift, MySQL, MSSQL, SQLite |
| **Auth** | bcrypt + JWT (python-jose), Google/GitHub OAuth2 |
| **Encryption** | Fernet (cryptography) |
| **Package Manager** | uv |
| **Linting** | Ruff |
| **i18n** | 6 languages (en, ru, el, de, fr, es) with runtime switching |
| **Testing** | pytest + pytest-asyncio, SQLite in-memory for model tests |
| **Monitoring** | Prometheus (metrics collection), Grafana (dashboards), Node Exporter (host metrics), cAdvisor (container metrics) |

## Database Design

### ORM Mixins

All models inherit from `DeclarativeBase` and compose two mixins:

- **TimestampMixin** — `created_at`, `updated_at` (auto-set via `func.now()` / `onupdate`), `deleted_at` (nullable, for soft deletes)
- **TenantMixin** — `org_id: BigInteger` FK to `organizations.id` with index, provides row-level tenant isolation

### Primary Key Strategy

Integer autoincrement PKs (`mapped_column(primary_key=True, autoincrement=True)`) without explicit `BigInteger` type — this keeps SQLite compatibility for tests while PostgreSQL uses IDENTITY columns in production. Non-PK foreign key columns use `BigInteger` explicitly.

### Soft Delete

All timestamped models use `deleted_at IS NULL` filtering. Records are never hard-deleted — they're preserved for audit and can be restored.

### Schema Layout

All configuration tables live in the `public` schema, isolated by `org_id`. Data destination schemas (raw, staging, dds) are user-configured per pipeline — not hardcoded. `tenant_{org_id}` schemas are reserved for future data isolation only.

`PUBLIC_TABLES` in `migrations/helpers.py` must include every model table name or Alembic won't generate migrations for them.

### Async Session Management

Two session types coexist:

- **Async** (`create_async_engine` + `async_sessionmaker` → `AsyncSession`) — used by Reflex state classes and services. `expire_on_commit=False` prevents unnecessary reloading after commits.
- **Sync** (`create_engine` + `sessionmaker`) — used by Celery workers and APScheduler callbacks, which don't run in an async context.

Sessions are yielded via `async def get_session()` / `def get_sync_session()` context managers.

## Multi-Tenancy

Tenant isolation is enforced at the service layer — every query filters by `org_id` extracted from the JWT token. There is no schema-per-tenant for config tables; all rows coexist in `public` with org_id discrimination.

Per-tenant isolation exists only for dbt projects: each org gets its own directory at `{DBT_PROJECTS_DIR}/tenant_{org_id}/` with an independent `profiles.yml`, `dbt_project.yml`, and model files.

## Authentication & Authorization

### Password Auth

bcrypt hashing directly (no passlib — it has compatibility issues with newer bcrypt versions). JWT access tokens (15 min) + refresh tokens (7 days) via python-jose.

### OAuth2 (Google + GitHub)

OAuth routes are plain Starlette `Route` objects (not FastAPI — Reflex 0.8.x uses Starlette internally):

1. `/api/auth/login/{provider}` — generates random state, sets HMAC-signed state cookie (httponly, samesite=lax, 10-min expiry), redirects to provider
2. `/api/auth/callback/{provider}` — verifies HMAC state signature (CSRF protection), exchanges code for tokens, creates/links user, redirects to frontend with `?token=...&refresh=...&is_new=0|1`

Routes are mounted by appending to `app._api.routes` after `rx.App()` creation.

### RBAC

Four roles: **owner > admin > editor > viewer**. Role checks happen in services before any mutating operation.

### API Keys

`etf_`-prefixed tokens, SHA-256 hashed in DB, scoped with expiry dates. Used for service account access.

## Task Queue (Celery)

### Configuration

- Redis broker and backend
- JSON serializer for cross-language compatibility
- `task_acks_late=True` + `worker_prefetch_multiplier=1` — serial execution per worker, tasks re-queued on crash
- `task_track_started=True` — tracks long-running operations
- Explicit module paths in `celery_app.conf.include` (not `autodiscover_tasks`) to avoid import issues

### Task Naming

Convention: `datanika.{action}_{entity}` — e.g. `datanika.run_upload`, `datanika.run_transformation`, `datanika.run_pipeline`.

All tasks are `@celery_app.task(bind=True)` with signature `(self, run_id: int, org_id: int)`. They use sync DB sessions internally.

## Scheduling (APScheduler)

A single `BackgroundScheduler` with `SQLAlchemyJobStore` (sync PostgreSQL URL) for persistence across restarts.

### Job Lifecycle

1. `sync_schedule(schedule)` — adds/updates a `CronTrigger` job (ID: `schedule_{id}`) if `is_active`, removes if inactive
2. `_dispatch_target(schedule_id)` — callback creates a `Run` record (PENDING), then dispatches the appropriate Celery task based on `target_type`
3. `sync_all()` — called on app startup to load all active, non-deleted schedules from DB

### Job Defaults

`coalesce=True` (skip missed runs), `max_instances=1` (no overlapping runs), `misfire_grace_time=300s`.

## dlt Integration (Extract + Load)

`DltRunnerService` builds dlt source and destination objects from connection config:

- Source factory selects adapter by connection type (postgres, mysql, mssql, sqlite, rest_api, s3, csv, json, parquet)
- Destination factory selects dlt destination (postgres, bigquery, snowflake, redshift, mssql, mysql)
- Supports two extraction modes: **single_table** (one table with optional incremental key) and **full_database** (all tables or filtered subset)
- Write dispositions: append, replace, merge
- Schema evolution control per entity: evolve, freeze, discard
- Row-level data quality filters with 8 operators

## dbt Integration (Transform)

`DbtProjectService` manages per-tenant dbt projects:

### Project Scaffold

`ensure_project(org_id)` creates `tenant_{org_id}/` with subdirectories (models, macros, tests, snapshots) and generates `dbt_project.yml` with profile `tenant_{org_id}`.

### Model Management

`write_model()` writes `.sql` files under `models/{schema_name}/`, generates/updates `schema.yml` with materialization config and column tests. Identifier validation regex `^[a-zA-Z_][a-zA-Z0-9_-]*$` prevents path traversal and SQL injection.

### Profile Generation

`generate_profiles_yml()` builds adapter-specific connection dicts from decrypted credentials. Supports postgres, mysql, mssql, sqlite, bigquery, snowflake, redshift.

### Command Execution

Uses `dbtRunner().invoke()` with dynamic args (selector expressions, full-refresh flag). Parses `adapter_response.rows_affected` from result nodes. Returns `{success, rows_affected, logs}`.

## Reflex UI Integration

### App Entry Point (`datanika/datanika.py`)

1. `rx.App()` creates the Reflex application
2. Pages registered via `app.add_page()` — protected pages include `on_load=[AuthState.check_auth, ...]`
3. OAuth Starlette routes appended to `app._api.routes`
4. APScheduler started and synced on app startup

### State Pattern

State classes in `ui/state/` bridge UI and services. Common patterns:

- **Edit/Copy**: `editing_*_id: int = 0` (0 = create mode, >0 = edit mode). `save_*()` branches on this value.
- **Connection options**: formatted as `"{id} — {name} ({type})"` for select dropdowns
- **Name resolution**: build `{id: name}` dicts from service list methods for display

## Security

| Concern | Approach |
|---------|----------|
| **Password storage** | bcrypt hash (12 rounds) |
| **Session tokens** | JWT with HMAC-SHA256, short-lived access + long-lived refresh |
| **OAuth CSRF** | HMAC-signed state cookie, verified on callback |
| **Credential storage** | Fernet symmetric encryption at rest, decrypted only at execution time |
| **API keys** | SHA-256 hashed in DB, `etf_` prefix, scoped with expiry |
| **Bot protection** | reCAPTCHA v3 on login/signup (optional) |
| **Authorization** | 4-tier RBAC enforced in service layer |
| **Audit trail** | Tracks create/update/delete/login/logout/run with old/new values |
| **Soft delete** | Records preserved for audit, never hard-removed |
| **Input validation** | Identifier regex, path traversal prevention in dbt file writes |

## Monitoring

The platform includes a full observability stack deployed via docker-compose.

### Components

| Service | Image | Port | Role |
|---------|-------|------|------|
| **Prometheus** | `prom/prometheus` | 9090 | Metrics collection and storage (30-day retention) |
| **Grafana** | `grafana/grafana` | 3001 | Dashboards and alerting, auto-provisioned with Prometheus datasource |
| **Node Exporter** | `prom/node-exporter` | 9100 | Host-level metrics (CPU, memory, disk, network) |
| **cAdvisor** | `gcr.io/cadvisor/cadvisor` | 8080 | Container-level metrics (per-container CPU, memory, I/O) |

### Configuration

- **Prometheus config**: `monitoring/prometheus.yml` — 15-second scrape interval, scrapes itself, Node Exporter, and cAdvisor
- **Grafana datasource**: `monitoring/grafana/provisioning/datasources/datasource.yml` — auto-provisions Prometheus as the default datasource
- **Grafana credentials**: `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` in `.env.docker`

## Testing Strategy

- **Framework**: pytest + pytest-asyncio with `asyncio_mode = "auto"`
- **Database**: In-memory SQLite for speed (no Docker dependency in CI)
- **Layout**: Test files mirror source — `datanika/services/foo.py` → `tests/test_services/test_foo.py`
- **TDD**: Failing test first, then implementation, then refactor
- **Bug fixes**: Every fix requires a regression test

## Project Structure

```
datanika/
├── models/            # SQLAlchemy ORM (12 tables)
│   ├── user.py        #   User, Organization, Membership
│   ├── connection.py  #   Connection (encrypted credentials)
│   ├── upload.py      #   Upload (dlt extract+load config)
│   ├── transformation.py  # Transformation (dbt SQL)
│   ├── pipeline.py    #   Pipeline (dbt command orchestration)
│   ├── dependency.py  #   DAG edges
│   ├── schedule.py    #   Cron schedules
│   ├── run.py         #   Execution history
│   ├── api_key.py     #   Service account keys
│   └── audit_log.py   #   Audit trail
├── i18n/              # Translations (en, ru, el, de, fr, es)
├── services/          # Business logic (18 services)
│   ├── auth.py        #   JWT + bcrypt + RBAC
│   ├── user_service.py    # Registration, org provisioning
│   ├── connection_service.py  # Encrypted connection CRUD
│   ├── upload_service.py      # Upload (dlt) validation + CRUD
│   ├── pipeline_service.py    # Pipeline (dbt) validation + CRUD
│   ├── dlt_runner.py      # dlt source/destination factory
│   ├── transformation_service.py  # dbt model CRUD
│   ├── dbt_project.py     # Per-tenant dbt project + command execution
│   ├── schedule_service.py    # Cron validation + CRUD
│   ├── scheduler_integration.py  # APScheduler bridge
│   ├── execution_service.py   # Run lifecycle management
│   ├── dependency_service.py  # DAG validation
│   ├── encryption.py      # Fernet encrypt/decrypt
│   ├── api_key_service.py     # API key management
│   ├── audit_service.py       # Audit logging
│   ├── oauth_service.py       # Google + GitHub OAuth2
│   ├── oauth_routes.py        # Starlette OAuth2 callback routes
│   └── tenant.py              # Tenant provisioning
├── tasks/             # Celery async tasks
│   ├── upload_tasks.py        # run_upload (dlt extract+load)
│   ├── pipeline_tasks.py      # run_pipeline (dbt commands)
│   └── transformation_tasks.py    # run_transformation
├── ui/
│   ├── state/         # Reflex state classes (11 files)
│   ├── pages/         # Route handlers (12 pages)
│   └── components/    # Reusable UI components
├── migrations/        # Alembic migrations
└── dbt_projects/      # Generated per-tenant dbt projects
```
