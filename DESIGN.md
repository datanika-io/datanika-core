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
| **Hooks** | `datanika/hooks.py` | Event bus for plugin extensibility (`on`/`emit`/`collect_events`) |
| **Plugin Registry** | `datanika/plugin_registry.py` | Plugin-contributed head components + per-page inline scripts |
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
| **Extract & Load** | dlt with 27 source types + 11 destination types (databases, SaaS APIs, files, streams) |
| **Transform** | dbt-core with 11 adapters (Postgres, Snowflake, BigQuery, Redshift, MySQL, MSSQL, SQLite, ClickHouse, DuckDB, Databricks, Synapse) |
| **Auth** | bcrypt + JWT (python-jose), Google/GitHub OAuth2, email verification |
| **Email** | SMTP via smtplib (verification + invitations), async via Celery |
| **Encryption** | Fernet (cryptography) |
| **Package Manager** | uv |
| **Linting** | Ruff |
| **i18n** | 9 languages (en, ru, el, de, fr, es, zh, ar, sr) with runtime switching |
| **Testing** | pytest + pytest-asyncio, SQLite in-memory, 1,700+ tests across unit / security / E2E |
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

### Tables (18 core + 3 cloud-plugin)

`PUBLIC_TABLES` in `migrations/helpers.py` is the canonical list. The 3 cloud-plugin tables (`plans`, `subscriptions`, `usage_ledger`) live alongside core tables in the `public` schema — they're owned by `datanika-cloud` and only populated when `DATANIKA_EDITION=cloud`.

| Table | Model File | Description |
|-------|-----------|-------------|
| `organizations` | `user.py` | Tenant organizations |
| `users` | `user.py` | User accounts (global, with `email_verified` flag) |
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
| `invitations` | `invitation.py` | Pending org invitations (email, role, JWT token, expiry) |
| `sso_configs` | `sso_config.py` | SAML/OIDC SSO configuration per org (Enterprise plan) |
| `notification_channels` | `notification_channel.py` | Slack/Telegram/Email/Webhook channel definitions per org |
| `notifications` | `notification.py` | In-app notification records (unread count, category, action URL) |

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
from datanika.hooks import on, off, emit, collect_events, clear

on(event, handler)              # Register a handler for an event
off(event, handler)             # Remove a handler
emit(event, **kwargs)           # Emit event to all registered handlers (return values discarded)
collect_events(event, **kwargs) # Emit + gather non-None handler returns into a flat list
clear()                         # Remove all handlers (testing)
```

`collect_events` is the emit variant used when core code needs to splice plugin-contributed return values into its own. List returns are flattened one level, `None` returns are skipped, scalar returns are appended, handler order is preserved. The signup flow uses it to let the cloud plugin contribute an `rx.call_script` (Google Ads conversion) that core then splices into its post-signup Reflex event list before the redirect.

### Events Emitted by Core

| Event | Emitted By | kwargs | Purpose | Collector |
|-------|-----------|--------|---------|-----------|
| `connection.before_create` | `connection_service.py` | `session`, `org_id` | Pre-creation hook for quota checks | `emit` |
| `schedule.before_create` | `schedule_service.py` | `session`, `org_id` | Pre-creation hook for quota checks | `emit` |
| `membership.before_create` | `user_service.py` | `session`, `org_id` | Pre-creation hook for seat limit checks | `emit` |
| `sso_config.before_create` | `sso_service.py` | `session`, `org_id` | Pre-creation hook for SSO-on-Enterprise quota | `emit` |
| `run.before_execute` | `upload_tasks.py`, `pipeline_tasks.py`, `transformation_tasks.py` | `session`, `org_id` | Pre-execution hook for run quota (Free plan hard cap) | `emit` |
| `run.upload_completed` | `upload_tasks.py` | `org_id`, `table_count` | Post-upload metering | `emit` |
| `run.models_completed` | `pipeline_tasks.py` | `org_id`, `count` | Post-pipeline metering (billable model runs) | `emit` |
| `run.transformation_completed` | `transformation_tasks.py` | `org_id` | Post-transformation metering | `emit` |
| `user.signup_completed` | `ui/state/auth_state.py::signup` | `user_id` | Post-signup, plugins can contribute Reflex events (e.g. conversion tracking) | `collect_events` |

Handlers raising exceptions propagate to the emitter — this is how quota enforcement blocks resource creation (cloud's `QuotaExceededError` subclasses `ValueError`).

## Plugin Registry

`datanika/plugin_registry.py` is the other half of the plugin seam. It lets a plugin contribute UI artifacts (head components, per-page inline scripts) into the Reflex app without the core page files importing the plugin. Two halves:

**Head components**: `register_head_component(c)` + `plugin_head_components() -> list[rx.Component]`. Plugin calls `register_head_component` during its early bootstrap phase; `datanika/datanika.py` reads the list and includes it in `rx.App(head_components=[...])` before construction.

**Page scripts**: `register_page_script(page_key, js)` + `get_page_scripts(page_key) -> list[rx.Component]`. Plugin registers inline JS under a short key like `pipeline_templates` or `connections`; core page functions splat the result with `*get_page_scripts("pipeline_templates")`. In open-source builds with no plugin loaded, `get_page_scripts` returns `[]` and no scripts are emitted.

Both halves are additive — there's no `unregister` API. Plugins register once at module import time and stay registered for the process lifetime.

This seam is how the cloud plugin contributes Plausible + Google Ads instrumentation without a single SaaS-specific reference in the open-source core.

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

The startup flow is a strict two-phase cloud plugin init, load-bearing because plugin-contributed head components must reach `rx.App(head_components=[...])` at construction time:

1. **Phase 1 — `bootstrap_cloud()` (if `DATANIKA_EDITION=cloud`)**. Runs *before* `rx.App(...)`. The plugin subscribes every hook handler (metering, quota, signup conversion, …) and contributes head components + per-page inline scripts into `plugin_registry`. No app instance exists yet.
2. **`_head_components` assembled** — favicon + `plugin_head_components()`.
3. **`rx.App(head_components=_head_components)` constructs the Reflex app.**
4. **Phase 2 — `init_cloud(app)` (if `DATANIKA_EDITION=cloud`)**. Runs *after* `rx.App(...)`. Registers the billing page, sidebar link, i18n overrides, and Paddle webhook route — all concerns that need the live app instance.
5. **Core pages registered** via `app.add_page()` — protected pages include `on_load=[AuthState.check_auth, ...]`.
6. **Core Starlette routes appended** to `app._api.routes` — OAuth, email verification, SSO, REST API v1, discovery meta routes, agent docs (`/llms.txt`, `/api/v1/agent-guide.md`, `/api/v1/meta/agent-tiers`), OpenAPI/Swagger, health checks, Prometheus metrics.
7. **APScheduler started and synced** on app startup.
8. **Notification hooks wired** (`NotificationService.register_hooks` + `register_in_app_notification_hooks`) for run-completion fanout to channels and the in-app notification center.

The two-phase split is pinned by `tests/test_app_plugin_init.py::TestCloudInitOrdering`, which source-scans `datanika.py` and fails if `bootstrap_cloud()` moves after `rx.App(...)` or `init_cloud(app)` moves before it.

### State Pattern

State classes in `ui/state/` bridge UI and services. Common patterns:

- **Edit/Copy**: `editing_*_id: int = 0` (0 = create mode, >0 = edit mode). `save_*()` branches on this value.
- **Connection options**: formatted as `"{id} — {name} ({type})"` for select dropdowns
- **Name resolution**: build `{id: name}` dicts from service list methods for display

## AI Agent Compatibility

Datanika exposes a 5-tier capability stack designed for autonomous LLM agents to build complete data pipelines without human intervention. The surface is intentionally narrow — an agent only needs to learn five idea-clusters before it can go end-to-end.

### The five tiers

| Tier | Name | What it gives the agent |
|------|------|-------------------------|
| 1 | Discover & Introspect | Full JSON Schema for every connection type, dlt config, dbt test, and materialization via `/api/v1/meta/*`; list tables, inspect columns, preview rows, run read-only SQL on any source connection |
| 2 | Build | Full CRUD for connections, uploads, pipelines, transformations, schedules, notification channels |
| 3 | Validate | Compile dbt transformations without touching the warehouse (`POST /transformations/{id}/compile`); preview output rows via a sandboxed `SELECT * FROM (...) LIMIT N` (`POST /transformations/{id}/preview`). Both surface typed error codes |
| 4 | Execute & Control | `POST /{resource}/{id}/run?wait=true` for synchronous completion (default 120s, max 300s), `POST /runs/{id}/cancel`, `Idempotency-Key` header for safe retry, `/runs`, `/runs/{id}/logs`, `/catalog` |
| 5 | Machine-Readable Discovery | Plain-text / Markdown / JSON documents an agent fetches without auth before it has an API key: `/llms.txt`, `/api/v1/agent-guide.md`, `/api/v1/openapi.json`, `/api/v1/meta/agent-tiers` |

### Single source of truth

`datanika/services/agent_tiers.py` is a pure-Python frozen-dataclass module that defines the 5-tier structure, the 7-capability decomposition, the 17-step golden-path loop, the 6 typed error codes (`compilation_error`, `execution_error`, `missing_destination`, `unsafe_sql`, `invalid_request`, `not_cancellable`), and the 6 UI-only operations. `services/agent_docs.py` renders `LLMS_TXT` and `AGENT_GUIDE_MD` from this SoT at import time, and `GET /api/v1/meta/agent-tiers` serializes the same structure as JSON. Consumers (landing site, blog posts, docs) fetch the JSON endpoint at build time instead of hardcoding tier counts — this makes the 5-vs-6 drift bug class (core PR #80, landing PR #97) structurally impossible.

### Typed error codes

Tier 3/4 endpoints return a new typed `error_code` field alongside the human message, built via `_typed_error()` in the API middleware. Agents branch on the error class without regex-matching messages. Error code invariants are frozen by contract tests in `tests/test_services/test_agent_tiers.py`.

### Inline OpenAPI schemas with discriminator

`datanika/services/openapi_inline.py` walks the `CONFIG_SCHEMAS` dict at import time and injects per-connector config schemas into the OpenAPI spec as a `oneOf` with a discriminator on `connection_type`. Adding a new connection type auto-updates the spec — zero manual sync. OpenAPI 3.0.3 native discriminator was chosen over `allOf` / JSON Schema `if/then` because it's the widest-supported pattern across LLM agents and codegen tools.

### Compile + preview safety

`transformation_compile.py` runs `dbt compile` via `dbtRunner().invoke()`, wraps the resulting SQL in `SELECT * FROM (...) LIMIT N`, and re-validates via `is_select_only()` before executing against the warehouse. Defense in depth: a compromised or buggy dbt compile output cannot reach DDL/DML. The read-only guard uses regex word boundaries (not a full SQL parser) for lightness while still rejecting DDL, DML, multi-statement, and CTE-with-mutation payloads.

## Notification Center

Datanika has two notification surfaces that share the same `notifications` table:

**In-app notification center** — bell icon in the top header (`datanika/ui/components/notification_bell.py` built on `rx.popover`), drop-down with unread count, category, and action URL per entry. Managed by `notification_center_state.py` + `services/in_app_notification_service.py`. Hooks in `services/in_app_notification_hooks.py` create `Notification` rows on run completion events (upload / pipeline / transformation) — no explicit API calls from the task code.

**Channel fanout** — `notification_channels` table stores per-org Slack / Telegram / Email / Webhook channel definitions, `services/notification_service.py` routes events to each configured channel, and `register_hooks()` subscribes to the same `run.*_completed` events.

REST API surface (5 endpoints, all tenant-scoped):

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/v1/notifications` | List notifications for the current user |
| `GET` | `/api/v1/notifications/unread_count` | Poll for badge count |
| `POST` | `/api/v1/notifications/{id}/mark_read` | Mark a single notification read |
| `POST` | `/api/v1/notifications/mark_all_read` | Mark every notification read |
| `POST` | `/api/v1/notifications/channels` | CRUD for channel definitions (also used by the Settings page) |

The whole subsystem was shipped in PR #70 (backend) + PR #74 (UI). Bell dropdown conversion from hand-rolled `rx.box` to `rx.popover.root` was a PR #82 regression fix; the audit confirmed `notification_bell.py` was the only hand-rolled popover in `datanika/ui/`.

## Pipeline Templates

`datanika/data/pipeline_templates.py` defines `LAUNCH_TEMPLATES` — a frozen list of `PipelineTemplate` dataclasses, each with a slug, source/destination type, icon names, and i18n-key references for the display name + description. Three templates ship today:

- **Stripe → Postgres** — Stripe source connector → Postgres destination, pre-seeded with an incremental `orders` upload
- **Postgres → BigQuery** — warehouse replication starter
- **CSV → DuckDB** — zero-credentials template for first-touch users, no external service required

`/pipelines/templates` (`ui/pages/pipeline_templates.py`) renders a grid of `_template_card` components. Clicking a card navigates to `/connections?template=<slug>`; `ConnectionState.load_template_from_query` runs `on_load`, matches the slug against the registry, and prefills the connection form so the user only enters credentials. `data-template-slug` attributes on each card are preserved even in open-source builds so plugin-contributed click listeners (registered via `plugin_registry.register_page_script`) can delegate off a single document-level listener.

## Plugin Extension Points (open-core)

Core stays open-source; SaaS concerns live in `datanika-cloud`. The seam is:

1. **`datanika/hooks.py`** — event bus (see §Hooks System)
2. **`datanika/plugin_registry.py`** — head component + page script registries
3. **Two-phase cloud init in `datanika/datanika.py`** — `bootstrap_cloud()` before `rx.App(...)`, `init_cloud(app)` after

These three seams are used by the cloud plugin to contribute Plausible + Google Ads telemetry, Paddle webhook routes, billing UI, quota enforcement, usage metering, and the signup conversion tracking event — without a single SaaS-specific reference leaking into the open-source core. A regression test on the cloud side (`TestNoAnalyticsLeakIntoCore`) walks the `datanika/` package tree and fails on any future leak, making the separation load-bearing.

Adding a new extension point to the core is intentionally a high-friction operation: it's a plugin API boundary, not general-purpose refactoring surface. Prefer putting new plugin functionality behind existing hooks or the plugin_registry halves before proposing a new seam.

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
- **Test count**: 1,700+ across unit / service / UI / migration / security suites (see `pytest tests/ --collect-only -q` for the live number)
- **Test directories**: `test_models/`, `test_services/`, `test_tasks/`, `test_ui/`, `test_i18n/`, `test_migrations/`, `test_security/`, plus top-level `test_hooks.py`, `test_hooks_integration.py`, `test_plugin_registry.py`, `test_app_plugin_init.py`
- **Security tests**: coverage for injection, path traversal, auth attacks, input validation, tenant isolation
- **E2E tests**: `datanika-examples/tests/` running against real Docker databases (Postgres, MySQL, MSSQL, MongoDB seed scripts + Compose)

## Project Structure

```
datanika/
├── models/            # SQLAlchemy ORM (18 core tables + 3 cloud-plugin tables)
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
│   ├── uploaded_file.py   # File upload references
│   ├── invitation.py  #   Pending org invitations
│   ├── sso_config.py  #   SAML/OIDC SSO config per org (Enterprise)
│   ├── notification_channel.py  # Slack/Telegram/Email/Webhook channel defs
│   └── notification.py  # In-app notification records
├── hooks.py           # Event bus (on/off/emit/collect_events/clear)
├── plugin_registry.py # Plugin head component + page script registry
├── config.py          # Pydantic Settings from .env
├── i18n/              # Translations (en, ru, el, de, fr, es, zh, ar, sr)
├── services/          # Business logic (~50 service modules — see `ls datanika/services`)
│   ├── auth.py        #   JWT + bcrypt + RBAC + email verification tokens
│   ├── user_service.py    # Registration, org provisioning, email_verified
│   ├── connection_service.py  # Encrypted connection CRUD (32 types)
│   ├── upload_service.py      # Upload (dlt) validation + CRUD
│   ├── pipeline_service.py    # Pipeline (dbt) validation + CRUD
│   ├── dlt_runner.py      # dlt source/destination factory (32 connectors)
│   ├── transformation_service.py  # dbt model CRUD
│   ├── dbt_project.py     # Per-tenant dbt project + command execution + clean_target
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
│   ├── email_service.py       # SMTP email sending (verification + invitations)
│   ├── email_routes.py        # Starlette email verification + invite acceptance
│   ├── invitation_service.py  # Org invitation lifecycle (create/accept/cancel)
│   ├── maintenance_service.py # Cleanup orphaned files, old runs, stale artifacts
│   ├── tenant.py              # Tenant provisioning
│   ├── captcha_service.py     # reCAPTCHA v3 verification
│   ├── catalog_service.py     # Data catalog management
│   ├── backup_service.py      # Database backup/restore
│   ├── file_upload_service.py # File upload handling
│   ├── google_sheets_source.py # Google Sheets dlt source
│   ├── mongodb_source.py      # MongoDB dlt source
│   ├── naming.py              # Name/slug generation utilities
│   ├── notification_service.py     # Channel fanout (Slack, Telegram, Email, Webhook)
│   ├── in_app_notification_service.py # In-app bell notifications (per-user)
│   ├── in_app_notification_hooks.py  # Run-completion → notification row bridge
│   ├── onboarding.py               # New-user checklist / getting-started flow
│   ├── transformation_compile.py   # dbt compile + sandboxed preview (agent validation tier)
│   ├── agent_docs.py               # /llms.txt + /api/v1/agent-guide.md renderers
│   ├── agent_tiers.py              # Frozen SoT for 5-tier + 7-capability + golden-path + error codes
│   ├── meta_routes.py              # /api/v1/meta/* discovery endpoints
│   ├── meta_schemas.py             # JSON Schema catalog for connection/dlt/dbt config
│   ├── api_v1_routes.py            # REST API v1 top-level mounting
│   ├── api_middleware.py           # Typed error codes, rate limit, Idempotency-Key handling
│   ├── health_routes.py            # /healthz + /readyz
│   ├── idempotency.py              # Idempotency-Key storage + replay
│   ├── metrics.py                  # Prometheus middleware + /metrics route
│   ├── openapi.py                  # Swagger UI + ReDoc + spec serving
│   ├── openapi_inline.py           # Per-connector config schemas via discriminator
│   ├── sso_service.py              # SAML/OIDC config + SP metadata + ACS
│   ├── sso_routes.py               # Starlette SSO callback routes
│   └── concurrency_service.py      # Run concurrency gate + lock management
├── tasks/             # Celery async tasks (7 task files)
│   ├── celery_app.py          # Celery configuration + Beat schedule
│   ├── upload_tasks.py        # run_upload (dlt extract+load + cleanup)
│   ├── pipeline_tasks.py      # run_pipeline (dbt commands + target cleanup)
│   ├── transformation_tasks.py    # run_transformation
│   ├── dependency_helpers.py  # DAG resolution utilities
│   ├── email_tasks.py         # Async email dispatch (verification, invitations)
│   └── maintenance_tasks.py   # Hourly cleanup (orphaned dirs, old runs, archives)
├── ui/
│   ├── state/         # Reflex state classes (19 active states + base_state)
│   │   ├── base_state.py      # Base state with auth context
│   │   ├── auth_state.py      # Login/signup/session (emits user.signup_completed)
│   │   ├── i18n_state.py      # Language switching + ensure_loaded
│   │   ├── dashboard_state.py # Dashboard stats
│   │   ├── connection_state.py # Connection management (32 types, load_template_from_query)
│   │   ├── upload_state.py    # Upload management + SaaS endpoint picker
│   │   ├── pipeline_state.py  # Pipeline management
│   │   ├── transformation_state.py # Transformation management
│   │   ├── schedule_state.py  # Schedule management
│   │   ├── run_state.py       # Run history
│   │   ├── dag_state.py       # DAG visualization
│   │   ├── settings_state.py  # User/org settings + invitations
│   │   ├── backup_state.py    # Backup management
│   │   ├── api_key_state.py   # API key management
│   │   ├── audit_state.py     # Audit log browsing
│   │   ├── model_state.py     # Data catalog browse
│   │   ├── model_detail_state.py # Data catalog detail
│   │   ├── notification_state.py  # Notification channel CRUD (Settings page)
│   │   ├── notification_center_state.py # In-app bell notifications (unread count, list, mark read)
│   │   └── onboarding_state.py    # Getting-started checklist state
│   ├── pages/         # Route handlers (18 pages)
│   │   ├── login.py           # /login
│   │   ├── signup.py          # /signup
│   │   ├── auth_complete.py   # /auth/complete (OAuth)
│   │   ├── dashboard.py       # /
│   │   ├── connections.py     # /connections (+ plugin page_scripts seam)
│   │   ├── uploads.py         # /uploads
│   │   ├── pipelines.py       # /pipelines
│   │   ├── pipeline_templates.py  # /pipelines/templates (+ plugin page_scripts seam)
│   │   ├── transformations.py # /transformations
│   │   ├── sql_editor.py      # /transformations/sql-editor
│   │   ├── schedules.py       # /schedules
│   │   ├── runs.py            # /runs
│   │   ├── dag.py             # /dag
│   │   ├── settings.py        # /settings
│   │   ├── api_keys.py        # /api-keys
│   │   ├── audit_logs.py      # /audit-log
│   │   ├── models.py          # /models (data catalog)
│   │   └── model_detail.py    # /models/[id] (catalog detail)
│   └── components/    # Reusable UI components (10 files)
│       ├── layout.py              # Sidebar + header layout, extra_sidebar_links plugin hook
│       ├── connection_config_fields.py # Dynamic connection form (32 types)
│       ├── searchable_select.py   # Searchable dropdown for large lists
│       ├── language_switcher.py   # Language selection dropdown
│       ├── captcha.py             # reCAPTCHA v3 widget
│       ├── sql_autocomplete.py    # SQL editor autocomplete
│       ├── notification_bell.py   # Header bell icon + dropdown (rx.popover-based)
│       ├── getting_started_checklist.py # Onboarding checklist widget
│       ├── info_tooltip.py        # Hover tooltip for form field help text
│       └── quota_callout.py       # Plan-limit warning callout (used by cloud quota hooks)
├── migrations/        # Alembic migrations
└── dbt_projects/      # Generated per-tenant dbt projects
```
