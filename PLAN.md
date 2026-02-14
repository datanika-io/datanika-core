# ETL Fabric - Implementation Plan

## Project Overview

**ETL Fabric** is a multi-tenant data pipeline management platform built with:
- **dlt** (data load tool) for Extract + Load
- **dbt-core** for Transform
- **Reflex** for the full-stack Python UI
- **PostgreSQL** for both app metadata and pipeline data
- **APScheduler** for built-in pipeline scheduling
- **Celery + Redis** for async task execution

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Reflex Frontend (Python)                  │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────────────┐  │
│  │ Sources  │ │Pipelines │ │  Transforms│ │  Scheduling   │  │
│  │  Config  │ │  Builder │ │  (dbt)    │ │  & Monitoring │  │
│  └──────────┘ └──────────┘ └──────────┘ └───────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                    Reflex Backend (FastAPI)                  │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────────────┐  │
│  │  Auth &  │ │ Pipeline │ │   Task   │ │  Scheduler    │  │
│  │  Tenancy │ │  Engine  │ │  Queue   │ │  (APScheduler)│  │
│  └──────────┘ └──────────┘ └──────────┘ └───────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                      Data Layer                             │
│  ┌─────────────────────┐  ┌──────────────────────────────┐  │
│  │  PostgreSQL          │  │  Redis                       │  │
│  │  - app schema (meta) │  │  - Celery broker             │  │
│  │  - tenant schemas    │  │  - APScheduler job store     │  │
│  │  - pipeline data     │  │  - session cache             │  │
│  └─────────────────────┘  └──────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
etlfabric/
├── pyproject.toml                  # Project dependencies (uv/poetry)
├── alembic.ini                     # DB migrations config
├── docker-compose.yml              # PostgreSQL + Redis for dev
├── .env.example                    # Environment variables template
│
├── etlfabric/                      # Main Python package
│   ├── __init__.py
│   │
│   ├── models/                     # SQLAlchemy ORM models (app metadata)
│   │   ├── __init__.py
│   │   ├── base.py                 # Base model, tenant mixin
│   │   ├── user.py                 # User, Organization, Membership
│   │   ├── connection.py           # Source/Destination connections
│   │   ├── pipeline.py             # Pipeline definitions
│   │   ├── transformation.py       # dbt project/model configs
│   │   ├── schedule.py             # Schedule definitions
│   │   ├── run.py                  # Pipeline/transform run history
│   │   └── dependency.py           # DAG edges between pipelines & transforms
│   │
│   ├── services/                   # Business logic layer
│   │   ├── __init__.py
│   │   ├── auth.py                 # Authentication & authorization
│   │   ├── tenant.py               # Multi-tenancy (schema-per-tenant)
│   │   ├── connection_service.py   # CRUD for connections, credential encryption
│   │   ├── pipeline_service.py     # Pipeline CRUD, dlt pipeline generation
│   │   ├── transform_service.py    # dbt project management, model CRUD
│   │   ├── scheduler_service.py    # APScheduler job management
│   │   ├── execution_service.py    # Celery task dispatch & monitoring
│   │   ├── dependency_service.py   # DAG construction, topological ordering
│   │   └── dlt_runner.py           # dlt pipeline execution wrapper
│   │
│   ├── tasks/                      # Celery async tasks
│   │   ├── __init__.py
│   │   ├── celery_app.py           # Celery configuration
│   │   ├── pipeline_tasks.py       # Run dlt pipelines
│   │   ├── transform_tasks.py      # Run dbt models
│   │   └── dag_tasks.py            # Execute full DAGs (pipelines + transforms)
│   │
│   ├── dbt_projects/               # Generated dbt projects (per tenant)
│   │   └── .gitkeep
│   │
│   ├── migrations/                 # Alembic migrations
│   │   ├── env.py
│   │   └── versions/
│   │
│   └── ui/                         # Reflex UI pages & components
│       ├── __init__.py
│       ├── etlfabric.py            # Reflex app entry point (rx.App)
│       ├── state/                  # Reflex state classes
│       │   ├── __init__.py
│       │   ├── auth_state.py       # Login/signup/session state
│       │   ├── connection_state.py # Connection management state
│       │   ├── pipeline_state.py   # Pipeline builder state
│       │   ├── transform_state.py  # dbt model editor state
│       │   ├── schedule_state.py   # Scheduling state
│       │   ├── run_state.py        # Run history & monitoring state
│       │   └── dag_state.py        # DAG visualization state
│       │
│       ├── pages/                  # Reflex pages (routes)
│       │   ├── __init__.py
│       │   ├── login.py            # /login
│       │   ├── signup.py           # /signup
│       │   ├── dashboard.py        # / (overview, recent runs, stats)
│       │   ├── connections.py      # /connections (sources & destinations)
│       │   ├── pipelines.py        # /pipelines (list + create/edit)
│       │   ├── transformations.py  # /transformations (dbt models)
│       │   ├── schedules.py        # /schedules
│       │   ├── runs.py             # /runs (execution history + logs)
│       │   ├── dag.py              # /dag (visual dependency graph)
│       │   └── settings.py         # /settings (org, users, roles)
│       │
│       └── components/             # Reusable Reflex components
│           ├── __init__.py
│           ├── layout.py           # Sidebar + header layout
│           ├── connection_form.py  # Dynamic form for connection config
│           ├── pipeline_builder.py # Pipeline configuration UI
│           ├── sql_editor.py       # SQL/dbt model editor
│           ├── cron_picker.py      # Cron expression builder
│           ├── dag_viewer.py       # DAG visualization component
│           ├── run_log_viewer.py   # Streaming log viewer
│           └── data_preview.py     # Table data preview widget
│
├── tests/
│   ├── conftest.py
│   ├── test_models/
│   ├── test_services/
│   ├── test_tasks/
│   └── test_ui/
│
└── rxconfig.py                     # Reflex configuration
```

---

## Phase 1: Foundation (Steps 1-4)

### Step 1: Project Setup & Dependencies

**What:** Initialize the Python project with all dependencies, Docker setup, and basic config.

**Files to create:**
- `pyproject.toml` — with dependencies:
  - Core: `dlt[postgres]`, `dbt-core`, `dbt-postgres`, `reflex`, `sqlalchemy[asyncio]`, `alembic`, `asyncpg`
  - Auth: `passlib[bcrypt]`, `python-jose[cryptography]` (JWT tokens)
  - Task queue: `celery[redis]`, `redis`
  - Scheduler: `apscheduler`
  - Utilities: `pydantic`, `pydantic-settings`, `cryptography` (credential encryption)
- `docker-compose.yml` — PostgreSQL 16 + Redis 7
- `.env.example` — DATABASE_URL, REDIS_URL, SECRET_KEY, etc.
- `rxconfig.py` — Reflex app config

### Step 2: Database Models & Multi-Tenancy

**What:** Define all SQLAlchemy ORM models for app metadata. Use schema-per-tenant isolation in PostgreSQL.

**Key models:**

```
Organization (tenant)
├── User (many-to-many via Membership with roles)
├── Connection (source/destination configs)
│   ├── type: enum (postgres, mysql, api, s3, bigquery, ...)
│   ├── credentials: encrypted JSON
│   └── direction: enum (source, destination, both)
├── Pipeline
│   ├── name, description
│   ├── source_connection_id → Connection
│   ├── destination_connection_id → Connection
│   ├── dlt_config: JSON (resource selection, write disposition, etc.)
│   └── status: enum (draft, active, paused, error)
├── Transformation
│   ├── name, description
│   ├── sql_body: text (the dbt model SQL)
│   ├── materialization: enum (view, table, incremental, ephemeral)
│   ├── schema_name: str (target schema)
│   └── tests: JSON (configured dbt tests)
├── Dependency (DAG edges)
│   ├── upstream_type + upstream_id (pipeline or transformation)
│   └── downstream_type + downstream_id
├── Schedule
│   ├── target_type + target_id (pipeline, transformation, or DAG group)
│   ├── cron_expression: str
│   ├── is_active: bool
│   └── timezone: str
└── Run (execution history)
    ├── target_type + target_id
    ├── status: enum (pending, running, success, failed, cancelled)
    ├── started_at, finished_at
    ├── logs: text
    ├── rows_loaded: int
    └── error_message: text
```

**Multi-tenancy approach:** Schema-per-tenant in PostgreSQL.
- App metadata lives in `public` schema (users, orgs, memberships)
- Each organization gets a schema `tenant_{org_id}` for their pipeline configs
- Pipeline data goes into `tenant_{org_id}_raw`, `tenant_{org_id}_staging`, `tenant_{org_id}_marts`

### Step 3: Authentication & Authorization

**What:** JWT-based auth with role-based access control.

**Roles:**
- `owner` — full access, can manage org settings and members
- `admin` — can create/edit/delete pipelines, transformations, connections
- `editor` — can create/edit pipelines and transformations, cannot delete
- `viewer` — read-only access to dashboards and run history

**Implementation:**
- Password hashing with bcrypt
- JWT access tokens (short-lived, 15min) + refresh tokens (long-lived, 7d)
- Reflex auth state that persists session in browser localStorage
- Middleware that injects tenant context (org_id → schema) on every request

### Step 4: Alembic Migrations

**What:** Set up Alembic for schema migrations with multi-tenant awareness.

- Initial migration creates all tables in `public` schema
- Tenant provisioning creates per-tenant schemas on org creation
- Migration runner applies tenant-specific migrations across all tenant schemas

---

## Phase 2: Connection & Pipeline Management (Steps 5-7)

### Step 5: Connection Management (Sources & Destinations)

**What:** UI and backend for configuring data source/destination connections.

**Supported connection types (initial set):**
- **Databases:** PostgreSQL, MySQL, SQL Server, SQLite
- **APIs:** REST API (generic), GitHub, Stripe (via dlt verified sources)
- **Files:** CSV/JSON/Parquet (local or S3)
- **Warehouses:** BigQuery, Snowflake, Redshift

**UI — `/connections` page:**
- List all connections with status indicators (connected/error)
- "Add Connection" button → modal with:
  - Connection type selector (cards with icons)
  - Dynamic form based on type (host, port, database, credentials)
  - "Test Connection" button (validates credentials in real-time)
  - Direction selector: Source / Destination / Both
- Edit/delete existing connections

**Backend:**
- Credentials encrypted at rest using `cryptography.fernet` with key from env
- Connection testing: attempts a real connection and returns success/error
- dlt destination objects generated dynamically from connection configs

### Step 6: Pipeline Builder (dlt Pipelines)

**What:** UI for creating and configuring dlt extract+load pipelines.

**UI — `/pipelines` page:**
- List all pipelines with status badges (draft/active/paused/error)
- Pipeline creation wizard:
  1. **Select source connection** — dropdown of configured sources
  2. **Configure extraction** — select tables/resources, set incremental cursors, filters
  3. **Select destination connection** — dropdown of configured destinations
  4. **Configure loading** — write disposition (append/replace/merge), dataset name
  5. **Review & save** — summary of configuration
- "Run Now" button on each pipeline → triggers immediate execution
- Pipeline detail page showing recent runs, logs, loaded row counts

**Backend — `pipeline_service.py`:**
- Generates dlt pipeline Python code dynamically from stored config
- Stores config as JSON in the `Pipeline` model
- `dlt_runner.py` — wrapper that:
  1. Creates a dlt pipeline object from config
  2. Builds source/resource from connection type + config
  3. Runs `pipeline.run()` and captures load_info
  4. Stores results in the `Run` model

### Step 7: Manual Pipeline Execution

**What:** Celery tasks for running pipelines asynchronously.

- `pipeline_tasks.py` — Celery task that:
  1. Loads pipeline config from DB
  2. Decrypts source/destination credentials
  3. Creates dlt pipeline + source objects
  4. Runs extraction + loading
  5. Streams logs to the `Run` record
  6. Updates run status on success/failure
- Run detail page shows real-time log output (polling-based via Reflex state)

---

## Phase 3: Transformations (Steps 8-10)

### Step 8: dbt Project Management

**What:** Auto-generate and manage dbt projects per tenant.

**How it works:**
- Each tenant gets a dbt project at `dbt_projects/tenant_{org_id}/`
- Project structure auto-generated:
  ```
  dbt_projects/tenant_{org_id}/
  ├── dbt_project.yml
  ├── profiles.yml          (generated from destination connection)
  ├── models/
  │   ├── staging/
  │   │   ├── sources.yml   (auto-generated from dlt-loaded tables)
  │   │   └── *.sql         (user-defined staging models)
  │   └── marts/
  │       └── *.sql         (user-defined mart models)
  ├── tests/
  │   └── *.sql             (custom singular tests)
  └── macros/
      └── *.sql
  ```
- `profiles.yml` generated dynamically from the tenant's destination connection credentials
- `sources.yml` auto-discovered from dlt pipeline schemas (introspect the DB for tables dlt created)

### Step 9: Transformation Editor

**What:** UI for creating and editing dbt models with SQL.

**UI — `/transformations` page:**
- List all transformations (dbt models) with materialization type
- "Create Transformation" → form with:
  - Name, description
  - SQL editor (with syntax highlighting — use a Reflex-wrapped CodeMirror or Monaco)
  - Materialization selector (view/table/incremental/ephemeral)
  - Target schema selector
  - Available tables sidebar (from dlt-loaded data + other models)
  - `{{ ref('model_name') }}` and `{{ source('schema', 'table') }}` autocomplete hints
- "Preview" button — runs `dbt compile` to show compiled SQL
- "Run" button — runs the single model via `dbt run --select model_name`

**Backend — `transform_service.py`:**
- Writes `.sql` files to the tenant's dbt project directory
- Generates/updates `schema.yml` with configured tests
- Compiles and validates SQL before saving

### Step 10: dbt Tests

**What:** UI for configuring data quality tests on transformations.

**Test types supported:**
- **Generic tests** (built-in): unique, not_null, accepted_values, relationships
- **Custom singular tests**: user-written SQL that should return 0 rows on success

**UI — integrated into transformation editor:**
- Per-column test checkboxes (unique, not_null)
- accepted_values configuration (list input)
- relationships configuration (ref table + column picker)
- Custom test SQL editor
- "Run Tests" button → runs `dbt test --select model_name`
- Test results displayed with pass/fail/warn counts

**Backend:**
- Updates `schema.yml` in the dbt project with test definitions
- `transform_tasks.py` — Celery task wrapping `dbt test`

---

## Phase 4: Scheduling & Dependencies (Steps 11-13)

### Step 11: Dependency Graph (DAG)

**What:** Define and visualize dependencies between pipelines and transformations.

**How dependencies work:**
- A transformation can depend on one or more pipelines (it needs the loaded data)
- A transformation can depend on other transformations (dbt `ref()` chains)
- A pipeline can depend on a transformation (reverse ETL: transform → extract further)
- Dependencies are stored as edges in the `Dependency` table (polymorphic: pipeline or transformation)

**UI — `/dag` page:**
- Visual DAG rendered with a graph library (e.g., `react-flow` wrapped in Reflex, or SVG-based)
- Nodes = pipelines (blue) + transformations (green)
- Edges = dependencies with arrows showing data flow direction
- Click a node → shows detail sidebar
- Drag to add new dependency edges
- Topological sort validation — rejects cycles with clear error messages

**Backend — `dependency_service.py`:**
- Stores DAG edges in `Dependency` table
- Validates DAG is acyclic (topological sort)
- Computes execution order for a given DAG or sub-DAG
- `dag_tasks.py` — executes a full DAG by running nodes in topological order, respecting parallelism where possible

### Step 12: Scheduling

**What:** Cron-based scheduling for pipelines, transformations, and DAG groups.

**UI — `/schedules` page:**
- List all schedules with next run time, last run status
- "Create Schedule" → form with:
  - Target selector (pipeline, transformation, or "run full DAG")
  - Cron expression builder (visual cron picker component)
  - Timezone selector
  - Enable/disable toggle
- Human-readable cron description (e.g., "Every day at 3:00 AM UTC")

**Backend — `scheduler_service.py`:**
- Uses APScheduler with PostgreSQL job store (persistent across restarts)
- On schedule trigger → dispatches Celery task for the target
- For DAG schedules → dispatches `dag_tasks.execute_dag` which runs the full dependency graph
- Handles missed runs (configurable: skip, run immediately, or queue)

### Step 13: Run History & Monitoring

**What:** Comprehensive execution tracking and monitoring dashboard.

**UI — `/runs` page:**
- Filterable list of all runs (by pipeline, transformation, status, date range)
- Run detail view:
  - Status timeline (pending → running → success/failed)
  - Execution duration
  - Rows loaded (for pipelines)
  - Models materialized (for transformations)
  - Full log output (scrollable, auto-updating while running)
  - Error message with stack trace (on failure)
- **Dashboard** (`/` page):
  - Recent runs summary (last 24h: succeeded/failed/running counts)
  - Pipeline health overview (all green / some failing)
  - Next scheduled runs
  - Quick-action buttons (run pipeline, create connection)

---

## Phase 5: Polish & Production Readiness (Steps 14-16)

### Step 14: Dashboard & Overview Page

**What:** The main landing page after login.

- Stat cards: total pipelines, total transformations, runs today, success rate
- Recent runs table (last 10 runs with status)
- Mini DAG visualization showing pipeline health
- Quick actions: create pipeline, add connection, view logs

### Step 15: Settings & Organization Management

**What:** Org management, user invitations, role management.

**UI — `/settings` page:**
- Organization profile (name, slug)
- Members list with roles
- Invite new members (email-based)
- Role management (change roles, remove members)
- Danger zone (delete org, transfer ownership)

### Step 16: Error Handling, Notifications & Retries

**What:** Production hardening.

- **Retry logic:** Configurable retry count and backoff per pipeline/transformation
- **Notifications:** Webhook support for run failures (Slack, email via webhook)
- **Error recovery:** Failed runs can be manually retried from the UI
- **Graceful shutdown:** Celery workers drain tasks on SIGTERM
- **Health checks:** `/health` endpoint for load balancers

---

## Implementation Order Summary

| Step | What | Depends On |
|------|------|------------|
| 1 | Project setup, dependencies, Docker | — |
| 2 | Database models & multi-tenancy | Step 1 |
| 3 | Auth & authorization | Step 2 |
| 4 | Alembic migrations | Step 2 |
| 5 | Connection management UI + backend | Steps 3, 4 |
| 6 | Pipeline builder UI + backend | Step 5 |
| 7 | Manual pipeline execution (Celery) | Step 6 |
| 8 | dbt project management | Step 5 |
| 9 | Transformation editor UI | Step 8 |
| 10 | dbt tests UI | Step 9 |
| 11 | Dependency graph (DAG) | Steps 7, 10 |
| 12 | Scheduling (APScheduler) | Step 11 |
| 13 | Run history & monitoring | Steps 7, 10 |
| 14 | Dashboard | Step 13 |
| 15 | Settings & org management | Step 3 |
| 16 | Error handling & notifications | Steps 12, 13 |

---

## Key Technical Decisions

1. **Reflex for UI** — Pure Python, compiles to React, supports complex multi-page apps with state management. Eliminates the need for a separate frontend codebase.

2. **Schema-per-tenant** — Strong data isolation in PostgreSQL. Each org's pipeline data is in its own schema, preventing cross-tenant data leaks.

3. **Celery + Redis** — Battle-tested async task execution. Pipeline and dbt runs are long-running and must not block the web server.

4. **APScheduler with PostgreSQL job store** — Schedules persist across app restarts. No additional infrastructure beyond the existing PostgreSQL.

5. **dlt pipeline generation from config** — Pipeline configs stored as JSON, dlt pipeline objects created dynamically at runtime. No code generation to disk for dlt (unlike dbt which requires `.sql` files on disk).

6. **dbt projects on disk** — dbt requires a project directory with `.sql` files. Each tenant gets a directory. These can be stored in a volume mount or object storage in production.

7. **Encrypted credentials** — All connection credentials encrypted with Fernet symmetric encryption before storage. Key managed via environment variable.
