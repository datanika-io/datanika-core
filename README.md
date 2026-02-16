# Datanika

A multi-tenant data pipeline management platform that brings **Extract**, **Load**, and **Transform** under one roof. Build end-to-end data pipelines through a web UI — connect to databases, APIs, and files, load data into warehouses, transform it with SQL, schedule everything with cron, and monitor runs in real time.

Built with [dlt](https://dlthub.com/) for extraction and loading, [dbt-core](https://www.getdbt.com/) for transformations, and [Reflex](https://reflex.dev/) for a full-stack Python UI that compiles to React.

## What It Does

**Connect** to 12+ source and destination types — PostgreSQL, MySQL, SQL Server, SQLite, BigQuery, Snowflake, Redshift, REST APIs, S3, CSV, JSON, and Parquet files.

**Extract & Load** with two pipeline modes:
- *Single table* — pull one table with optional incremental loading
- *Full database* — replicate an entire database or a filtered subset of tables

Apply row-level data quality filters (8 operators), choose a write disposition (append, replace, merge), and control schema evolution per entity (evolve, freeze, discard).

**Transform** with dbt SQL models directly from the browser:
- Write SQL, pick a materialization (view, table, incremental, ephemeral)
- Configure column-level tests (not_null, unique, accepted_values, relationships)
- Preview compiled SQL before running
- Create SCD Type 2 snapshots (timestamp or check strategy)
- Manage dbt packages and check source freshness

**Orchestrate** pipelines and transformations as a DAG — define upstream/downstream dependencies, validate for cycles, and schedule execution with 5-field cron expressions and timezone support.

**Secure** with organization-based multi-tenancy, JWT authentication, role-based access (owner/admin/editor/viewer), Google and GitHub SSO, API keys for service accounts, Fernet-encrypted credentials, and a full audit log.

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
    |  (auth, pipeline, |          |  (OAuth callbacks)     |
    |   connections...) |          +-----------+------------+
    +---------+---------+                      |
              |                                |
    +---------v-----------------------------------v---------+
    |                   Services Layer                      |
    |  AuthService, PipelineService, DltRunnerService,      |
    |  DbtProjectService, ScheduleService, AuditService...  |
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

### Multi-Tenancy

All configuration tables live in the `public` schema, isolated by an `org_id` column on every row. Tenant context is extracted from the JWT token. Credentials are Fernet-encrypted at rest and decrypted only at pipeline execution time.

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
| **Transform** | dbt-core with adapters for Postgres, Snowflake, BigQuery, Redshift |
| **Auth** | bcrypt + JWT (python-jose), Google/GitHub OAuth2 |
| **Encryption** | Fernet (cryptography) |
| **Package Manager** | uv |
| **Linting** | Ruff |
| **Testing** | pytest + pytest-asyncio (584 tests) |

## Quick Start

**Prerequisites:** Docker, Python 3.12+, [uv](https://docs.astral.sh/uv/)

```bash
# 1. Start infrastructure
docker-compose up -d   # PostgreSQL 16 + Redis 7

# 2. Create venv and install
uv venv
source .venv/bin/activate   # .venv/Scripts/activate on Windows
uv pip install -e ".[dev]"

# 3. Configure environment
cp .env.example .env
# Generate an encryption key:
#   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Paste it into CREDENTIAL_ENCRYPTION_KEY in .env

# 4. Run database migrations
uv run alembic upgrade head

# 5. Start the app (frontend on :3000, backend on :8000)
uv run reflex run

# 6. Start Celery worker (separate terminal)
uv run celery -A datanika.tasks worker -l info
```

## Configuration

Key environment variables (see `.env.example` for the full list):

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | Async PostgreSQL URL (`postgresql+asyncpg://...`) |
| `DATABASE_URL_SYNC` | Sync PostgreSQL URL (for Celery workers) |
| `REDIS_URL` | Redis broker for Celery |
| `SECRET_KEY` | JWT signing key |
| `CREDENTIAL_ENCRYPTION_KEY` | Fernet key for encrypting connection credentials |
| `DBT_PROJECTS_DIR` | Directory for generated per-tenant dbt projects |
| `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` | Google OAuth2 (optional) |
| `GITHUB_CLIENT_ID` / `GITHUB_CLIENT_SECRET` | GitHub OAuth2 (optional) |

## Supported Sources and Destinations

### Sources

| Type | Mode | Features |
|------|------|----------|
| **PostgreSQL** | single_table, full_database | Incremental loading, table filtering, row filters |
| **MySQL** | single_table, full_database | Incremental loading, table filtering, row filters |
| **SQL Server** | single_table, full_database | Incremental loading, table filtering, row filters |
| **SQLite** | single_table, full_database | Incremental loading, table filtering |
| **REST API** | resources config | Base URL, auth, headers, pagination |
| **S3** | file source | AWS credentials, bucket/path, format selection |
| **CSV / JSON / Parquet** | file source | Local or S3-backed |

### Destinations

| Type | Profile |
|------|---------|
| **PostgreSQL** | Native dlt + dbt-postgres |
| **BigQuery** | dlt[bigquery] + dbt-bigquery |
| **Snowflake** | dlt[snowflake] + dbt-snowflake |
| **Redshift** | dlt + dbt-redshift |
| **MySQL** | dlt MySQL destination |
| **SQL Server** | dlt[mssql] |

## dbt Capabilities

| Feature | Description |
|---------|-------------|
| **Models** | Write SQL, choose materialization (view/table/incremental/ephemeral) |
| **Tests** | Column-level tests: not_null, unique, accepted_values, relationships |
| **SQL Preview** | Compile models and inspect generated SQL before running |
| **Snapshots** | SCD Type 2 with timestamp or check strategies |
| **Source Freshness** | Define freshness thresholds and run `dbt source freshness` |
| **Packages** | Manage packages.yml and install with `dbt deps` |
| **Adapters** | Postgres, MySQL, MSSQL, SQLite, BigQuery, Snowflake, Redshift |

## Project Structure

```
datanika/
├── models/            # SQLAlchemy ORM (11 tables)
│   ├── user.py        #   User, Organization, Membership
│   ├── connection.py  #   Connection (encrypted credentials)
│   ├── pipeline.py    #   Pipeline (dlt config)
│   ├── transformation.py  # Transformation (dbt SQL)
│   ├── dependency.py  #   DAG edges
│   ├── schedule.py    #   Cron schedules
│   ├── run.py         #   Execution history
│   ├── api_key.py     #   Service account keys
│   └── audit_log.py   #   Audit trail
├── services/          # Business logic (18 services)
│   ├── auth.py        #   JWT + bcrypt + RBAC
│   ├── user_service.py    # Registration, org provisioning
│   ├── connection_service.py  # Encrypted connection CRUD
│   ├── pipeline_service.py    # Pipeline validation + CRUD
│   ├── dlt_runner_service.py  # dlt pipeline/source/destination factory
│   ├── transformation_service.py  # dbt model CRUD
│   ├── dbt_project_service.py    # Per-tenant dbt project management
│   ├── schedule_service.py    # Cron validation + CRUD
│   ├── scheduler_integration_service.py  # APScheduler bridge
│   ├── execution_service.py   # Run lifecycle management
│   ├── dependency_service.py  # DAG validation
│   ├── encryption_service.py  # Fernet encrypt/decrypt
│   ├── api_key_service.py     # API key management
│   ├── audit_service.py       # Audit logging
│   └── oauth_service.py       # Google + GitHub OAuth2
├── tasks/             # Celery async tasks
│   ├── pipeline_tasks.py      # run_pipeline
│   └── transformation_tasks.py    # run_transformation
├── ui/
│   ├── state/         # Reflex state classes (11 files)
│   ├── pages/         # Route handlers (12 pages)
│   └── components/    # Reusable UI components
├── migrations/        # Alembic migrations
└── dbt_projects/      # Generated per-tenant dbt projects
```

## Development

```bash
# Run tests
uv run pytest tests/ -v

# Run with coverage
uv run pytest tests/ --cov=datanika --cov-report=html

# Lint and format
uv run ruff check datanika tests
uv run ruff format datanika tests

# Create a migration
uv run alembic revision --autogenerate -m "description"
uv run alembic upgrade head
```

## Security

- **Authentication**: bcrypt password hashing, JWT access (15 min) + refresh (7 day) tokens
- **SSO**: Google and GitHub OAuth2 with automatic account linking
- **Authorization**: 4-tier RBAC (owner > admin > editor > viewer)
- **Credentials**: Fernet-encrypted at rest, decrypted only during pipeline execution
- **API Keys**: `etf_`-prefixed tokens, SHA-256 hashed in DB, scoped with expiry
- **Audit Log**: Tracks create/update/delete/login/logout/run actions with old/new values
- **Soft Delete**: All records are soft-deleted (preserved for audit), never hard-removed
