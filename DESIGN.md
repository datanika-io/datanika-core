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
    |  (auth, upload,   |          |  (OAuth callbacks,     |
    |   pipeline, ...)  |          |   webhooks)            |
    +---------+---------+          +-----------+------------+
              |                                |
    +---------v-----------------------------------v---------+
    |                   Services Layer                      |
    |  AuthService, UploadService, PipelineService,         |
    |  DltRunnerService, DbtProjectService, AuditService,   |
    |  CatalogService, BackupService, FileUploadService...  |
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
                   | REST, S3, Files |    | MySQL, MSSQL,   |
                   | Google Sheets,  |    | ClickHouse      |
                   | MongoDB         |    |                 |
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
| **Hooks** | `datanika/hooks.py` | Event bus for plugin extensibility |
| **i18n** | `datanika/i18n/` | Translation JSON files (9 locales) and loader |
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
| **UI** | Reflex 0.8+ (compiles Python to React) |
| **Database** | PostgreSQL 16, SQLAlchemy 2.0 async (asyncpg) |
| **Migrations** | Alembic |
| **Task Queue** | Celery 5.4+ with Redis 7 broker |
| **Scheduling** | APScheduler with PostgreSQL job store |
| **Extract & Load** | dlt with adapters for Postgres, Snowflake, BigQuery, MSSQL, ClickHouse, MongoDB, Google Sheets |
| **Transform** | dbt-core with adapters for Postgres, Snowflake, BigQuery, Redshift, MySQL, MSSQL, SQLite |
| **Auth** | bcrypt + JWT (python-jose), Google/GitHub OAuth2 |
| **Encryption** | Fernet (cryptography) |
| **Package Manager** | uv |
| **Linting** | Ruff |
| **i18n** | 9 languages (en, ru, el, de, fr, es, zh, ar, sr) with runtime switching |
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

### Tables (14)

| Table | Model File | Description |
|-------|-----------|-------------|
| `organizations` | `user.py` | Tenant organizations |
| `users` | `user.py` | User accounts (global) |
| `memberships` | `user.py` | User↔org relationships with roles |
| `connections` | `connection.py` | Source/destination connections (encrypted credentials) |
| `uploads` | `upload.py` | dlt extract+load configurations |
| `pipelines` | `pipeline.py` | dbt pipeline orchestrations |
| `transformations` | `transformation.py` | SQL transformations (dbt models) |
| `dependencies` | `dependency.py` | DAG edges between pipelines/transforms |
| `runs` | `run.py` | Execution history |
| `schedules` | `schedule.py` | Cron schedules |
| `api_keys` | `api_key.py` | Service account API keys |
| `audit_logs` | `audit_log.py` | User action audit trail |
| `catalog_entries` | `catalog_entry.py` | Data catalog (schemas, tables, columns) |
| `uploaded_files` | `uploaded_file.py` | File upload references |

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

- Source factory selects adapter by connection type (postgres, mysql, mssql, sqlite, rest_api, s3, csv, json, parquet, google_sheets, mongodb)
- Destination factory selects dlt destination (postgres, bigquery, snowflake, redshift, mssql, mysql, clickhouse)
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

## Hooks System

A generic event bus (`datanika/hooks.py`) for plugin extensibility. Plugins (like `datanika-cloud`) register handlers at startup; core services emit events at key lifecycle points.

### API

```python
from datanika.hooks import on, off, emit, clear

on(event, handler)      # Register a handler for an event
off(event, handler)     # Remove a handler
emit(event, **kwargs)   # Emit event to all registered handlers
clear()                 # Remove all handlers (testing)
```

### Events Emitted by Core

| Event | Emitted By | kwargs | Purpose |
|-------|-----------|--------|---------|
| `connection.before_create` | `connection_service.py` | `session`, `org_id` | Pre-creation hook for quota checks |
| `schedule.before_create` | `schedule_service.py` | `session`, `org_id` | Pre-creation hook for quota checks |
| `membership.before_create` | `user_service.py` | `session`, `org_id` | Pre-creation hook for seat limit checks |
| `run.upload_completed` | `upload_tasks.py` | `org_id`, `table_count` | Post-upload metering |
| `run.models_completed` | `pipeline_tasks.py` | `org_id`, `count` | Post-pipeline metering (billable model runs) |
| `run.transformation_completed` | `transformation_tasks.py` | `org_id` | Post-transformation metering |

## Configuration

All settings are managed via Pydantic Settings (`datanika/config.py`), loaded from `.env` file.

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `database_url` | str | `postgresql+asyncpg://...` | Async database connection |
| `database_url_sync` | str | `postgresql://...` | Sync database connection (Celery/APScheduler) |
| `redis_url` | str | `redis://localhost:6379/0` | Redis broker URL |
| `secret_key` | str | *(insecure default)* | JWT signing key |
| `access_token_expire_minutes` | int | `15` | JWT access token TTL |
| `refresh_token_expire_days` | int | `7` | JWT refresh token TTL |
| `credential_encryption_key` | str | *(insecure default)* | Fernet key for credential encryption |
| `google_client_id` | str | `""` | Google OAuth client ID |
| `google_client_secret` | str | `""` | Google OAuth client secret |
| `github_client_id` | str | `""` | GitHub OAuth client ID |
| `github_client_secret` | str | `""` | GitHub OAuth client secret |
| `oauth_redirect_base_url` | str | `http://localhost:8000` | Base URL for OAuth callbacks |
| `frontend_url` | str | `http://localhost:3000` | Frontend URL for redirects |
| `recaptcha_site_key` | str | `""` | reCAPTCHA v3 site key (disabled when empty) |
| `recaptcha_secret_key` | str | `""` | reCAPTCHA v3 secret key |
| `dbt_projects_dir` | str | `./dbt_projects` | Per-tenant dbt project root |
| `file_uploads_dir` | str | `./uploaded_files` | File upload storage path |
| `app_name` | str | `Datanika` | Application display name |
| `debug` | bool | `False` | Debug mode flag |
| `datanika_edition` | str | `core` | Edition: `core` (open-source) or `cloud` (SaaS) |

## Reflex UI Integration

### App Entry Point (`datanika/datanika.py`)

1. `rx.App()` creates the Reflex application
2. Pages registered via `app.add_page()` — protected pages include `on_load=[AuthState.check_auth, ...]`
3. OAuth Starlette routes appended to `app._api.routes`
4. APScheduler started and synced on app startup
5. If `DATANIKA_EDITION=cloud`, calls `init_cloud(app)` to bootstrap the billing plugin

### State Pattern

State classes in `ui/state/` bridge UI and services. Common patterns:

- **Edit/Copy**: `editing_*_id: int = 0` (0 = create mode, >0 = edit mode). `save_*()` branches on this value.
- **Connection options**: formatted as `"{id} — {name} ({type})"` for select dropdowns
- **Name resolution**: build `{id: name}` dicts from service list methods for display

## Docker Compose

All services are defined in `docker-compose.yml` (requires `source .env.docker` before running).

| Service | Image | Port | Role |
|---------|-------|------|------|
| **postgres** | `postgres:16` | 5432 | Primary database |
| **redis** | `redis:7` | 6379 | Celery broker + cache |
| **app** | *(built from Dockerfile)* | 3000, 8000 | Reflex app (frontend + backend) |
| **celery** | *(built from Dockerfile)* | — | Celery worker for async tasks |
| **prometheus** | `prom/prometheus` | 9090 | Metrics collection (30-day retention) |
| **grafana** | `grafana/grafana` | 3001 | Dashboards and alerting |
| **node-exporter** | `prom/node-exporter` | 9100 | Host-level metrics (CPU, memory, disk) |
| **cadvisor** | `gcr.io/cadvisor/cadvisor` | 8080 | Container-level metrics |

### Monitoring Configuration

- **Prometheus config**: `monitoring/prometheus.yml` — 15-second scrape interval, scrapes itself, Node Exporter, and cAdvisor
- **Grafana datasource**: `monitoring/grafana/provisioning/datasources/datasource.yml` — auto-provisions Prometheus as default
- **Grafana credentials**: `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD` in `.env.docker`

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

## Testing Strategy

- **Framework**: pytest + pytest-asyncio with `asyncio_mode = "auto"`
- **Database**: In-memory SQLite for speed (no Docker dependency in CI)
- **Layout**: Test files mirror source — `datanika/services/foo.py` → `tests/test_services/test_foo.py`
- **TDD**: Failing test first, then implementation, then refactor
- **Bug fixes**: Every fix requires a regression test
- **Test files**: 43 files across 6 directories (`test_models/`, `test_services/`, `test_tasks/`, `test_ui/`, `test_i18n/`, `test_migrations/`, plus top-level hook tests)

## Project Structure

```
datanika/
├── models/            # SQLAlchemy ORM (14 tables)
│   ├── base.py        #   Base, TimestampMixin, TenantMixin
│   ├── user.py        #   User, Organization, Membership
│   ├── connection.py  #   Connection (encrypted credentials)
│   ├── upload.py      #   Upload (dlt extract+load config)
│   ├── transformation.py  # Transformation (dbt SQL)
│   ├── pipeline.py    #   Pipeline (dbt command orchestration)
│   ├── dependency.py  #   DAG edges
│   ├── schedule.py    #   Cron schedules
│   ├── run.py         #   Execution history
│   ├── api_key.py     #   Service account keys
│   ├── audit_log.py   #   Audit trail
│   ├── catalog_entry.py   # Data catalog
│   └── uploaded_file.py   # File upload references
├── hooks.py           # Event bus (on/off/emit/clear)
├── config.py          # Pydantic Settings from .env
├── i18n/              # Translations (en, ru, el, de, fr, es, zh, ar, sr)
├── services/          # Business logic (26 services)
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
│   ├── dependency_check.py    # Pre-delete dependency checks
│   ├── encryption.py      # Fernet encrypt/decrypt
│   ├── api_key_service.py     # API key management
│   ├── audit_service.py       # Audit logging
│   ├── oauth_service.py       # Google + GitHub OAuth2
│   ├── oauth_routes.py        # Starlette OAuth2 callback routes
│   ├── tenant.py              # Tenant provisioning
│   ├── captcha_service.py     # reCAPTCHA v3 verification
│   ├── catalog_service.py     # Data catalog management
│   ├── backup_service.py      # Database backup/restore
│   ├── file_upload_service.py # File upload handling
│   ├── google_sheets_source.py # Google Sheets dlt source
│   ├── mongodb_source.py      # MongoDB dlt source
│   └── naming.py              # Name/slug generation utilities
├── tasks/             # Celery async tasks (4 task files)
│   ├── celery_app.py          # Celery configuration
│   ├── upload_tasks.py        # run_upload (dlt extract+load)
│   ├── pipeline_tasks.py      # run_pipeline (dbt commands)
│   ├── transformation_tasks.py    # run_transformation
│   └── dependency_helpers.py  # DAG resolution utilities
├── ui/
│   ├── state/         # Reflex state classes (15 files)
│   │   ├── base_state.py      # Base state with auth context
│   │   ├── auth_state.py      # Login/signup/session
│   │   ├── i18n_state.py      # Language switching
│   │   ├── dashboard_state.py # Dashboard stats
│   │   ├── connection_state.py # Connection management
│   │   ├── upload_state.py    # Upload management
│   │   ├── pipeline_state.py  # Pipeline management
│   │   ├── transformation_state.py # Transformation management
│   │   ├── schedule_state.py  # Schedule management
│   │   ├── run_state.py       # Run history
│   │   ├── dag_state.py       # DAG visualization
│   │   ├── settings_state.py  # User/org settings
│   │   ├── backup_state.py    # Backup management
│   │   ├── model_state.py     # Data catalog browse
│   │   └── model_detail_state.py # Data catalog detail
│   ├── pages/         # Route handlers (15 pages)
│   │   ├── login.py           # /login
│   │   ├── signup.py          # /signup
│   │   ├── auth_complete.py   # /auth/complete (OAuth)
│   │   ├── dashboard.py       # /
│   │   ├── connections.py     # /connections
│   │   ├── uploads.py         # /uploads
│   │   ├── pipelines.py       # /pipelines
│   │   ├── transformations.py # /transformations
│   │   ├── sql_editor.py      # /sql-editor
│   │   ├── schedules.py       # /schedules
│   │   ├── runs.py            # /runs
│   │   ├── dag.py             # /dag
│   │   ├── settings.py        # /settings
│   │   ├── models.py          # /models (data catalog)
│   │   └── model_detail.py    # /models/{id} (catalog detail)
│   └── components/    # Reusable UI components (5 files)
│       ├── layout.py              # Sidebar + header layout
│       ├── connection_config_fields.py # Dynamic connection form
│       ├── language_switcher.py   # Language selection dropdown
│       ├── captcha.py             # reCAPTCHA v3 widget
│       └── sql_autocomplete.py    # SQL editor autocomplete
├── migrations/        # Alembic migrations
└── dbt_projects/      # Generated per-tenant dbt projects
```
