# SPEC — ELT + IR Architecture

> **Provenance.** Migrated on 2026-08-31 from the local planning directory
> (`plans/engineering/`), which is outside every git repository and therefore has no history,
> no review and no recovery path. A spec is a contract amended across sessions, so it belongs
> in the repository it governs. Content is unchanged apart from link paths; a few links still
> point at internal planning documents that are not part of this repository.


> **Status**: Draft, spec-only (no code yet). Part of the pricing-pivot 6-spec set. See [PRICING_PIVOT.md](https://github.com/datanika-io/datanika-core/blob/master/docs/specs/README.md).
>
> **Author**: Engineering (core). **Date**: 2026-04-15. **Gate**: user review of the full 6-spec set.
>
> **Paired spec**: [SPEC_VOLUME_METERING.md](https://github.com/datanika-io/datanika-cloud/blob/master/docs/specs/SPEC_VOLUME_METERING.md) (cloud). The IR layer is the natural measurement point; these two specs are joined at the hip.

---

## 1. TL;DR

Introduce an **intermediate representation (IR)** — a declarative schema-mapping document — between every source and the destination. A single ingestion path writes **parquet-in-Arrow** straight to destination `raw.*` with no on-disk normalization step. From the IR, we dispatch into one of two modes:

- **ETL mode (today's behaviour, preserved)** — IR + dlt schema hints → dlt normalizes during load → typed tables in user schema. Retained for simple sources and small volumes.
- **ELT mode (new default for new pipelines ≥ 100 MB/run)** — IR → raw parquet landed by our streaming loader → dbt staging models typed via `SELECT … ::type` over `raw.*`. Target cost **$0.01–0.03 / GB processed**.

The IR is the same shape in both modes. Switching a pipeline from ETL to ELT is a mode flag flip; no re-modeling, no source reconfiguration.

## 2. Goals

1. Cut per-GB processing cost from **$0.05–0.15** (current dlt path) to **$0.01–0.03** (streaming-first ELT).
2. Expose a single point where `bytes_processed` can be metered honestly — captures JSON→columnar amplification (see §7). This is the only meter point [SPEC_VOLUME_METERING.md](https://github.com/datanika-io/datanika-cloud/blob/master/docs/specs/SPEC_VOLUME_METERING.md) depends on.
3. Keep **every existing ETL pipeline working unchanged** during and after the pivot. No forced migration, no pricing shock.
4. Keep `TestNoAnalyticsLeakIntoCore` green — IR layer and bytes counter live in core; billing interpretation lives in cloud.
5. Keep dlt and dbt as **implementation details** of modes, not surface area. Users see "ETL vs ELT", not "dlt vs dbt".

## 3. Non-goals

- Replacing dlt or dbt. Both stay. This is a routing layer over them.
- Supporting every source in ELT mode on day one. Phased rollout (§9) — SQL sources first, SaaS last.
- New transformation language. IR reuses dbt's type system (the target warehouse's SQL types via dbt adapter).
- Streaming **transformations**. dbt still runs on batches. "Streaming-first" means ingest — no disk staging pass — not continuous CDC.
- Touching live pricing. That is gated on all 6 specs landing + user sign-off + cloud/infra readiness.

## 4. Background — current state

Anchored to the worktree at `worktrees/datanika-core-engineering/` as of 2026-04-15.

### 4.1 Current ETL flow (the only flow)

```
Source → dlt extractor → dlt normalizer (writes to `pipelines_dir` on disk)
       → dlt loader (reads disk, writes to destination)
       → typed tables in user-configured schema
```

Key pinpoints:
- `datanika/services/dlt_runner.py:795` creates `dlt.pipeline(pipeline_name, destination, pipelines_dir, dataset_name)`.
- `datanika/services/dlt_runner.py:862` runs it: `load_info = pipeline.run(source, **run_kwargs)`.
- dlt writes the normalized form to `pipelines_dir` on local disk, then loads. This is the disk-staging pass price_insights.md §5 calls out as the cost driver.
- We only call `pipeline.run(source)` — **no direct Arrow/Parquet API usage anywhere**. We have not yet touched dlt's streaming internals.
- `Upload.dlt_config` (`datanika/models/upload.py:29`) is a JSON dict carrying mode/table/schema/incremental/batch_size/merge_config. No schema-mapping field today.

### 4.2 Current dbt side

- Per-tenant dbt projects at `datanika/services/dbt_project.py:113` — `tenant_{org_id}/`.
- Models written as SQL files at `tenant_{org_id}/models/{schema_name}/{model_name}.sql` via `write_model()` (`dbt_project.py:164`).
- dbt invoked via `dbtRunner().invoke(["run", "--select", model_name, "--project-dir", …])` at `dbt_project.py:359`.
- No code-level `raw`/`staging`/`dds` convention — default schema is `"staging"`. Users configure destination schema per transformation/upload.

### 4.3 Run shape

- `datanika/models/run.py:19-37`: `Run(id, target_type, target_id, status, started_at, finished_at, rows_loaded, logs, error_message)`. The only "quantity" column today is `rows_loaded` — nullable, no byte accounting.
- Hook emits (the cloud metering surface):
  - `run.upload_completed` @ `datanika/tasks/upload_tasks.py:192` (kwarg `table_count`)
  - `run.models_completed` @ `datanika/tasks/pipeline_tasks.py:257` (kwarg `count`)
  - `run.transformation_completed` @ `datanika/tasks/transformation_tasks.py:171`

## 5. Proposed architecture

### 5.1 The IR document

A JSON document owned by the core, stored on `Upload` and on `Pipeline` (as a new column, additive). Example:

```json
{
  "ir_version": 1,
  "source": {
    "kind": "sql_table",
    "connection_id": 42,
    "schema": "public",
    "table": "orders"
  },
  "columns": [
    {"source_ref": "id",                "target_name": "id",          "type": "bigint",       "nullable": false},
    {"source_ref": "payload.user.id",   "target_name": "user_id",     "type": "bigint",       "nullable": true},
    {"source_ref": "payload.order.total","target_name": "order_total","type": "numeric(18,4)","nullable": true},
    {"source_ref": "created_at",        "target_name": "created_at",  "type": "timestamptz",  "nullable": false}
  ],
  "primary_key": ["id"],
  "incremental": {"mode": "append", "cursor": "created_at"},
  "target": {"connection_id": 17, "raw_schema": "raw", "table": "orders"}
}
```

**Why this shape:**
- `source_ref` uses dotted-path notation identical to the SELECT example in price_insights.md §11 (`payload:user.id::int` in Snowflake dialect → `payload.user.id` in IR → dialect-specific cast at dbt-compile time).
- `columns` + `type` let both dlt (ETL) and dbt (ELT) derive their own schemas deterministically from the same source of truth.
- `target.raw_schema` + `table` is what ELT mode writes parquet into. ETL mode ignores this and uses today's `dataset_name`.
- `ir_version` is explicit — the whole IR is versioned as part of [docs/api_versioning.md](../api_versioning.md) `x-stability` surface (see §11).

### 5.2 Mode dispatch

```
                                        ┌── mode=etl ──► dlt.pipeline.run(source_with_hints)
Upload/Pipeline.run() → build_ir() ─────┤
                                        └── mode=elt ──► stream_to_raw() → dbt.run(--select staging_<name>)
```

**`build_ir()`** is a new service method that:
- For `sql_table` / `sql_database`: introspects live, constructs columns from the information schema (we already have `POST /api/v1/connections/{id}/introspect`).
- For `saas_rest` (Stripe, Salesforce, etc.): derived from the connector's declared schema (dlt's `@dlt.source` already exports this).
- For `file` (CSV/parquet/json): sampled on the first run, persisted on `Upload.ir`, edited via a future UI. First-run sampling stays the only place we touch a file twice.

**Mode decision**: the `mode` field on `Upload` / `Pipeline` (new column, default `"etl"` for all existing rows — §8). New pipelines created for sources ≥ 100 MB/run default to `"elt"`. UI exposes a toggle (Product-owned, see `SPEC_DUAL_MODE_UX.md`).

### 5.3 ELT streaming ingest path

New module `datanika/services/elt_runner.py`. No code yet; interface sketch:

```python
def stream_to_raw(ir: IR, run_id: int) -> StreamStats:
    """
    Stream source → Arrow RecordBatches → parquet files in destination's raw schema.
    No local disk normalization pass. Destination adapters (Postgres, Snowflake,
    BigQuery, etc.) decide whether parquet lands via COPY, EXTERNAL TABLE, or
    native Arrow ingest.
    """
    ...

@dataclass
class StreamStats:
    rows: int
    bytes_in: int     # bytes pulled from source (post-decompression, pre-cast)
    bytes_out: int    # bytes written to destination raw (post-compression)
    batches: int
    duration_ms: int
```

**How it's built, not from scratch:**
- Reuse dlt's source readers. `dlt.sources.sql_database.sql_table(...)` already yields Arrow batches when `backend="pyarrow"` is set — we stop consuming dlt's loader, we keep its extractor.
- Parquet writer = pyarrow (already transitively available via dlt).
- **Arrow-mode env vars must be set globally before `pipeline.run()`** — not via dlt config object. Validated production defaults (see §16):
  ```
  DATA_WRITER__FILE_MAX_ITEMS  = 10_000
  DATA_WRITER__FILE_MAX_BYTES  = 100_000_000    # 100 MB
  LOAD__DELETE_COMPLETED_JOBS  = true
  ```
  **Critical gotcha**: without `DATA_WRITER__FILE_MAX_BYTES`, Arrow batches concatenate into multi-GB single files that time out on HTTP upload to destinations behind a proxy. `NORMALIZE__DATA_WRITER__*` has no effect in Arrow mode — only the global `DATA_WRITER__` prefix applies during extract.
- Per-destination landing strategy:
  - **Postgres**: `COPY FROM STDIN BINARY` with parquet→arrow→COPY chunks; or `aws_s3` fdw if configured. Spec's initial implementation: COPY FROM STDIN.
  - **Snowflake**: `PUT` + `COPY INTO raw.table FROM @stage FILE_FORMAT=parquet`. Stage per-tenant.
  - **BigQuery**: load job with parquet source. Cheapest path BigQuery offers.
  - **ClickHouse**: native parquet insert.
  - **DuckDB** (local dev): direct `COPY … FROM 'file.parquet'`.
- The destination adapter layer is new: `datanika/services/destinations/{postgres,snowflake,bigquery,clickhouse,duckdb}.py`, one `RawLander` class each. Starts with the 5 destinations above; the remaining 6 (from `pyproject.toml`) fall back to dlt (still ELT-shaped — lands to raw only — but uses dlt's loader internally) until a native lander exists.

### 5.4 ELT transformation step

After `stream_to_raw()` succeeds, an auto-generated dbt staging model runs:

```sql
-- dbt_projects/tenant_{org_id}/models/staging/stg_<upload_name>.sql
{{ config(materialized='view') }}
SELECT
    {{ cast('id',                  'bigint',        nullable=false) }}  AS id,
    {{ cast('payload.user.id',     'bigint',        nullable=true)  }}  AS user_id,
    {{ cast('payload.order.total', 'numeric(18,4)', nullable=true)  }}  AS order_total,
    {{ cast('created_at',          'timestamptz',   nullable=false) }}  AS created_at
FROM {{ source('raw', 'orders') }}
```

The `cast()` macro is dialect-aware (Snowflake `VARIANT:path::type`, Postgres `(col->>'path')::type`, BigQuery `JSON_VALUE`). It lives in a new dbt package `datanika/dbt_macros/datanika_ir/` that's auto-included in every tenant project.

Write site: extend `datanika/services/dbt_project.py:164` `write_model()` to call `render_staging_from_ir(ir)`.

### 5.5 Where the byte counter lives

**One and only one meter emission point:**

`datanika/services/elt_runner.py:stream_to_raw()` returns `StreamStats.bytes_out`. The existing `run.upload_completed` hook gets a new kwarg `bytes_processed: int`. Cloud picks it up — see [SPEC_VOLUME_METERING.md](https://github.com/datanika-io/datanika-cloud/blob/master/docs/specs/SPEC_VOLUME_METERING.md) §3.

**ETL mode also reports `bytes_processed`**, derived from dlt's `LoadInfo`. Iterate `load_info.load_packages[*].completed_jobs[*].job_file_info` and sum `file_size`. The extraction pattern is proven — whisk's `log_writer.py:extract_load_info()` does the same walk to count files per table (see §16). Less precise than the ELT path (dlt already normalized; we read the post-normalization size, which is what the customer is actually being processed for). This is the honest "processed GB" — it captures JSON amplification because dlt's normalizer unnested the JSON first.

**Column-injection pattern for `source_run_id` + IR-derived columns** uses `resource.add_map(enrich_fn)` applied *after* source yield, *before* the data writer — same hook whisk uses for Airflow `run_id` injection. The enrich function is polymorphic: `pa.Table.append_column()` for Arrow, dict unpack for dict rows, one `isinstance` dispatch. Proven clean at 54M-row MySQL scale.

Both modes report via the **same** hook kwarg. Cloud code is mode-agnostic.

### 5.6 Proposed module layout

```
datanika/
  services/
    ir/
      __init__.py           # public: build_ir, IR dataclass, ir_version constant
      builder.py            # build_ir(source) — dispatches by connection kind
      validator.py          # validate_ir(ir) — shape + type checks
      introspect.py         # information-schema → columns
    elt_runner.py           # stream_to_raw(ir, run_id) — new
    destinations/
      __init__.py           # RawLander protocol
      postgres.py           # COPY FROM STDIN BINARY
      snowflake.py          # PUT + COPY INTO
      bigquery.py           # parquet load job
      clickhouse.py         # native parquet insert
      duckdb.py             # direct COPY
      _dlt_fallback.py      # wraps dlt for not-yet-implemented destinations
    dlt_runner.py           # unchanged — remains the ETL mode entry
    dbt_project.py          # extended with render_staging_from_ir(ir)
  dbt_macros/
    datanika_ir/
      dbt_project.yml
      macros/
        cast.sql            # dialect-aware cast
        source_ref.sql      # raw.* source resolution
  models/
    upload.py               # + ir: Mapped[dict] (JSON, nullable), mode: Mapped[str]
    pipeline.py             # + ir: Mapped[dict] (JSON, nullable), mode: Mapped[str]
    run.py                  # + bytes_processed: Mapped[int | None]
```

**Zero deletions.** `dlt_runner.py` stays. Everything is additive.

## 6. Migration path for existing ETL pipelines

### 6.1 What happens to live pipelines on deploy day

- New columns `upload.mode`, `upload.ir`, `pipeline.mode`, `pipeline.ir`, `run.bytes_processed` land via Alembic migration. Defaults: `mode="etl"`, `ir=NULL`, `bytes_processed=NULL`.
- **No existing pipeline changes behaviour.** `mode="etl"` + `ir=NULL` → `dlt_runner.py` path, exactly as today.
- First run of an `etl` pipeline after deploy writes `bytes_processed` (from `LoadInfo`). That's the only observable change, and it's only observable to cloud's ledger.

### 6.2 Opt-in migration to ELT

Three entry points, all user-driven:

1. **"Switch to ELT" button** on the pipeline detail page (Product-owned, `SPEC_DUAL_MODE_UX.md`). Calls `POST /api/v1/uploads/{id}/migrate-to-elt`, which: (a) runs `build_ir()`, (b) writes `ir` + sets `mode="elt"`, (c) leaves existing destination tables untouched — next run lands to `raw.*` + auto-generated staging model produces the same-shape typed view. Old table drops are user-initiated only.
2. **New pipelines ≥ 100 MB** default to `mode="elt"` (threshold tunable per-env). Small pipelines default to `mode="etl"` because ELT's per-run overhead (parquet write + dbt compile) is worse than dlt direct for <10 MB.
3. **MCP tool `migrate_to_elt(pipeline_id)`** surfaces the same action to Claude agents.

### 6.3 What never auto-migrates

- Custom `dlt_config` with merge strategies we can't yet express as dbt incremental (merge with `scd2`, specifically). Stay on ETL. Warn in UI. Tracked as follow-up.
- Sources without a `build_ir()` implementation (see §9 phasing). Stay on ETL, silently.

### 6.4 Data-format compatibility

- ETL writes to `{dataset}.{table}` with dlt's chosen types. ELT writes to `raw.{table}` (parquet source) + `staging.stg_{table}` (dbt view). **Different tables.** Migration does not touch existing data; user can drop old tables when confident. This is deliberate — zero risk of silent schema drift.

## 7. Data amplification accounting

This is the honest-metering claim that makes the pricing pivot defensible.

| Source shape | ETL mode `bytes_processed` | ELT mode `bytes_processed` | Notes |
|---|---|---|---|
| SQL table, 100 MB on source | ~100 MB (dlt post-normalize, no schema change) | ~30 MB (parquet compression) | ELT wins on cost AND meter. |
| JSON API, 100 MB payload, 3 nested levels | 300–500 MB (dlt unnests into rows × columns) | 80–120 MB (parquet unnests once, columnar compresses) | Both capture amplification. ELT captures less of it because parquet reuses repeated keys. |
| CSV, 100 MB | ~100 MB | ~25 MB (parquet + snappy) | — |
| Streaming event log, 100 MB jsonl | 200–400 MB | 40–80 MB | — |

**Billing decision** (specified in [SPEC_VOLUME_METERING.md](https://github.com/datanika-io/datanika-cloud/blob/master/docs/specs/SPEC_VOLUME_METERING.md) §4): meter on `run.bytes_processed` as reported, unit-agnostic of mode. This is fair to the customer:

- ELT customer: pays less, processes less (wins both sides).
- ETL customer: pays the true cost of the normalization they're choosing. Motivates ELT migration when volume grows.

**No gaming risk**: the counter is written inside the run wrapper, not by user code. No tenant-supplied input to `bytes_processed`.

## 8. Back-compat guarantee

Explicit contract — breaking any of these is a blocker:

1. Every existing `Upload` and `Pipeline` row continues to run with identical behaviour after the migration.
2. Every existing `Run` record's shape is preserved; `bytes_processed` is nullable and NULL for pre-migration runs.
3. Every existing REST API endpoint returns the same shape (new fields are additive with `x-stability: beta` until the pivot completes).
4. Every existing hook emission keeps its current kwargs. `bytes_processed` is an **added** kwarg — handlers that don't accept it via `**kwargs` break; every cloud handler already accepts `**kwargs` (verified by Explore agent in §4.3 above, paired with `SPEC_VOLUME_METERING.md` §3.2).
5. No live pricing change. Old `model_runs` meter keeps running alongside the new `bytes_processed` meter for one full billing period; see [SPEC_VOLUME_METERING.md](https://github.com/datanika-io/datanika-cloud/blob/master/docs/specs/SPEC_VOLUME_METERING.md) §7.
6. `TestNoAnalyticsLeakIntoCore` stays green. The IR, the ELT runner, and the byte counter all live in core. Billing interpretation (GB → dollars, quota thresholds, Paddle sync) stays in cloud. The only interface across the boundary is the existing hook bus.

## 9. Phased rollout

| Phase | Core work | Paired cloud/infra/product | Guard |
|---|---|---|---|
| **P0 — Spec set (this week)** | All 6 specs PR'd. | — | User sign-off. |
| **P1 — Plumbing** | Alembic migration (new columns, nullable); hook kwarg; IR dataclass + validator; `build_ir()` for SQL sources only. Behind feature flag `DATANIKA_ELT_ENABLED=false`. | Cloud: ledger `bytes_processed` metric column, read-only (no billing effect). Infra: Prometheus counter wired. | All existing tests green; new tests cover IR shape + migration. |
| **P2 — ETL mode byte accounting** | `dlt_runner.py` emits `bytes_processed` from `LoadInfo`. | Cloud: ledger fills with real data, still no billing effect. | Week of observed data matches estimates ±20% on staging. |
| **P3 — ELT for SQL sources** | `stream_to_raw()` for Postgres/Snowflake/BigQuery/ClickHouse/DuckDB destinations. Auto-generate staging models. Feature flag on. | Product: mode toggle + cost estimator UI. Growth: pricing page in branch. | Dual-mode equivalence tests (QA spec) pass. |
| **P4 — ELT for SaaS sources** | Dlt Arrow backend for Stripe/Salesforce/etc. Fallback to dlt loader for non-Arrow-capable sources. | — | Parity of row counts + schema across ETL and ELT for the same source. |
| **P5 — Pricing cutover** | — | Cloud: enable `bytes_processed` billing. Growth: publish pricing page. Infra: Paddle meter live. | All P0–P4 green on staging. No paying subscribers to protect — this is a "flip it on" step, not a migration. |

ELT default-on for new pipelines ≥ 100 MB lands in P3. Existing-pipeline migration buttons land in P3.

## 10. Interactions with current work

- **Hybrid quota contract (cloud#18)**: extends, does not duplicate. Volume is a new dimension of the same predict-and-reject/allow-then-block mechanism. See [SPEC_VOLUME_METERING.md](https://github.com/datanika-io/datanika-cloud/blob/master/docs/specs/SPEC_VOLUME_METERING.md) §5.
- **MCP server (core#153)**: adds two new tools — `estimate_run_cost(pipeline_id)` and `migrate_to_elt(pipeline_id)`. Tool catalog update comes after Product UX spec (see `SPEC_DUAL_MODE_UX.md`).
- **Bulk import (core#150)**: IR document is accepted via the `v2` import format as an optional `ir` key on connection→upload entries. Back-compat — absent = `build_ir()` runs on first execute.
- **Pipeline templates (core#93 funnel)**: template seed data gets an IR block. `/templates/[slug]` landing pages can display the per-template GB estimate (Growth).
- **Open-core boundary**: `TestNoAnalyticsLeakIntoCore` pattern extends to `TestNoBillingInterpretationInCore` — a new test ensures `datanika.services.elt_runner` never imports from `datanika_cloud.*`. Core emits raw bytes; only cloud decides what bytes cost.

## 11. API surface changes

All **additive**. OpenAPI spec gets:

- `Upload.mode: enum(etl, elt) — x-stability: beta`
- `Upload.ir: IRDocument | null — x-stability: beta`
- `Pipeline.mode`, `Pipeline.ir` — same.
- `Run.bytes_processed: integer | null — x-stability: beta`
- `POST /api/v1/uploads/{id}/migrate-to-elt` — new, `x-stability: beta`
- `POST /api/v1/uploads/{id}/estimate-cost` — new, `x-stability: beta`, returns predicted `bytes_processed` + cloud-computed cost (cloud plugin contributes the cost field via hook).

Schemas promoted `beta → stable` after P5.

## 12. Open questions (need user / peer sign-off)

1. **IR storage format — JSON column vs separate `ir_documents` table?** Proposed: JSON column, per-row. Simpler, aligns with `dlt_config`. Separate table would help cross-pipeline IR reuse, but we don't have a use case yet. **Recommendation: JSON column.**
2. ~~**Parquet compression choice — snappy vs zstd?**~~ **Resolved by prior art (§16)**: snappy at 100 MB `FILE_MAX_BYTES` with 10k `FILE_MAX_ITEMS` is proven at 54M-row scale in whisk's production MySQL→ClickHouse pipeline. Decoded faster, writes smaller than the alternative they tried, and the 2 GB single-file upload timeout is a real failure mode we avoid by keeping files ≤100 MB.
3. ~~**Mode threshold — 100 MB/run default?**~~ **Resolved by prior art (§16)**: 100 MB file-size threshold is the proven sweet spot for Arrow-mode benefits to exceed per-pipeline overhead. Expose as `DATANIKA_ELT_DEFAULT_THRESHOLD_MB` env var, default 100.
4. **What happens if IR build fails for a `new Pipeline` call?** Fail the create? Fall back to ETL silently? **Recommendation: fail the create with a clear error code `IR_BUILD_FAILED`, linking to docs. Explicit is safer than implicit fallback.**
5. **Do we version raw schema?** i.e. if IR changes from v1 to v2 and `raw.orders` already exists with v1 columns — do we alter, drop-recreate, or version-suffix (`raw.orders_v2`)? **Recommendation: version-suffix, old table drop is user-initiated.** This matches §6.4 and keeps the invariant "we never mutate customer raw data without explicit user action."
6. ~~**Dbt staging models — disk files vs ephemeral?**~~ **Resolved by prior art (§16)**: whisk runs the equivalent transformation in dbt SQL over raw-landed data as disk models, inspectable and user-editable. Consistent with how Datanika tenants edit dbt models today. **Disk files.**
7. **Per-resource `mode` override inside a single Upload?** whisk exposes `use_arrow: false` per-table in YAML as a production escape hatch. Our Upload-level `mode` is coarser. **Recommendation: not for P3. Add as P4 follow-up if a real source triggers Arrow misbehavior.**

## 13. Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Parquet landing bug corrupts a customer's `raw.*` data | Low, High impact | Destination adapters are per-warehouse; each has a full unit-test matrix (QA spec). Feature-flagged per-org during P3. Alembic migration is additive only — no schema rewrites. |
| dlt's Arrow backend missing features for some sources | Medium, Medium impact | Explicit `_dlt_fallback.py` for any source without Arrow support; still ELT-shaped (lands to raw) but uses dlt's loader. P4 can be long-tailed without blocking P3. |
| `bytes_processed` under-reports for warehouses that ingest via external stage (BigQuery) | Medium, Medium impact | Measure on the write side (our bytes to stage), not on BigQuery's LoadJob stats. Counter is always what **we** wrote, not what the warehouse counted. Consistent across destinations. |
| Per-destination raw landing adapters multiply code | Medium, Low impact | 5 adapters for P3, dlt fallback for the rest. Adapter interface is small (`RawLander.land(parquet_iter) -> bytes_written`). |

## 14. Test strategy (handshake with QA spec)

QA's `SPEC_VOLUME_METERING_TESTS.md` owns the full list. Core spec's requirements to QA:

1. **Dual-mode equivalence** — for each supported source, running the same Upload twice (once `etl`, once `elt`) produces datasets with identical row counts, identical column sets post-staging, and identical primary key values. Tolerance 0 for row count, tolerance 0 for PK, tolerance ±1 ULP for floating-point columns (dlt and Arrow agree on double casts, except for floats serialized as JSON string decimals).
2. **Byte counter sanity** — `bytes_processed > 0` after every successful run. `bytes_processed == 0` never occurs except for runs producing zero rows.
3. **IR round-trip** — `build_ir(source)` then use that IR to run; use the Run's resulting staging view schema; re-introspect via `/api/v1/connections/{id}/introspect`; the schemas match.
4. **Back-compat invariant** — on a DB seeded with pre-migration rows (`mode=NULL`, `ir=NULL`), every Upload.run() uses dlt path and produces identical table output to pre-migration code. Seed fixture stored in `tests/fixtures/pre_pivot_seed.sql`.

## 15. What we explicitly do NOT commit to in this spec

- Performance SLA beyond the per-GB cost target. Latency is spec'd only loosely ("≤ existing ETL latency on sources ≤ 1 GB").
- UI copy, pricing numbers, or any customer-facing messaging (Growth / Product own those specs).
- Paddle meter schema (cloud owns — see [SPEC_VOLUME_METERING.md](https://github.com/datanika-io/datanika-cloud/blob/master/docs/specs/SPEC_VOLUME_METERING.md)).
- Grafana dashboards or alert thresholds (Infra owns — see `SPEC_GB_THROUGHPUT_METRICS.md`).

## 16. Prior-art validation — whisk's production dlt+Arrow pipeline

A sibling Python data-pipeline project ("whisk") runs the same stack this spec proposes — dlt + PyArrow + parquet + raw-land → dbt-transform — in production against MySQL/Postgres/MongoDB/REST-API sources landing to ClickHouse. They hit the exact problems we're about to hit; this section pins their solutions so we don't re-discover them. Source: `D:/Projects/whisk/data_repos/DB data transfer architecture.md` and `D:/Projects/whisk/data_repos/whisk-data-pipelines/operators/dlt_operator/`.

### 16.1 What transfers directly

| Concern | Whisk pattern | Where we use it |
|---|---|---|
| Arrow env-var defaults | `DATA_WRITER__FILE_MAX_ITEMS=10000`, `DATA_WRITER__FILE_MAX_BYTES=100_000_000`, `LOAD__DELETE_COMPLETED_JOBS=true` — set globally via `os.environ` before `pipeline.run()` | §5.3 `stream_to_raw()` setup |
| Polymorphic enrich function | `_enrich(item: dict \| pa.Table)` with `isinstance(item, (pa.Table, pa.RecordBatch))` dispatch; attached via `resource.add_map(_enrich)` | §5.5 `source_run_id` + IR-derived column injection |
| `LoadInfo` walk for file metadata | Iterate `load_info.load_packages[*].completed_jobs[*].job_file_info` to count files (and for us, sum `file_size` for `bytes_processed`) | §5.5 ETL-mode byte counter |
| Per-destination adapter pattern | They monkey-patch `ClickHouseSqlClient.execute_query()` to intercept DDL; we subclass into `RawLander` protocol per destination. **Same principle — dlt doesn't natively do X for some destinations, so we intercept.** | §5.3 `datanika/services/destinations/` |
| `@dlt.source` / `@dlt.resource` reuse | They keep dlt's source library entirely, replace only the loader. Works. | §5.3 "we stop consuming dlt's loader, we keep its extractor" |
| `dlt_pipeline_group` batching | Small tables share a single dlt pipeline instance to amortize per-pipeline overhead. Config-driven. | Optional IR field (P4): `ir.batch_group` hint for multi-table Uploads |

### 16.2 Performance data to cite in Growth's pricing-page messaging

Real production numbers (not projections) from whisk's workloads:

| Workload | Legacy batch-INSERT | dlt + JSON (default) | dlt + Arrow (what we're proposing) | Speedup vs legacy |
|---|---|---|---|---|
| **MySQL, 54M rows** (`user_recipe_rel`) | 99 min | 174 min (**slower**) | **17 min** | 5.8× |
| **MongoDB, 8.6M docs** (`hostedImages`) | ~49 min (prod, 29.7M) | 23.5 min (dev) | **4 min** (dev) | ~10× |
| **MongoDB, extrapolated prod 29.7M docs** | 49 min | ~81 min | **~14 min** | 3.5× |

Two insights worth flagging to Growth (`SPEC_PRICING_V2.md`):
1. **dlt's default (JSON) mode is 2× slower than the legacy batch-INSERT operator it replaced.** Our current "just wrap dlt" product is in this regime. This is a real cost story, not a hypothetical.
2. **dlt + Arrow beats legacy by 3.5–10× across all tested workloads.** The pivot's cost-reduction claim (5-15× from $0.05-0.15/GB → $0.01-0.03/GB) is grounded in observed, not estimated, data.

### 16.3 What we diverge on (deliberately)

| Whisk does | We do | Why |
|---|---|---|
| Airflow `BaseOperator` wrapping | Celery task (existing) | We're not Airflow. |
| Monkey-patch `ClickHouseSqlClient.execute_query()` for ON CLUSTER | Protocol-based `RawLander` subclass per destination | Cleaner, no fragility with dlt internal churn. We control the interface. |
| MySQL-specific `SSDictCursor` + `SET net_write_timeout` | Per-destination adapter handles its own cursor + timeout | We support 5+ destinations, not MySQL alone. |
| ClickHouse dual-port (9000 native + 8123 HTTP) | Destination-specific inside each `RawLander` | Generalized; Postgres/Snowflake/BQ have their own transport paths. |
| `_dlt_execution_log` in ClickHouse, per-table-per-run | Our `Run` model + `UsageLedger`, also per-target | We already have a richer Run model with tenant scoping and billing-linked metrics. We don't need a new log table — we extend the existing `Run.bytes_processed`. |
| YAML-driven pipeline config | IR JSON document on `Upload`/`Pipeline` rows | We're multi-tenant SaaS with API/UI, not engineer-edited YAML. Same semantic content, different storage. |

### 16.4 What whisk does NOT do (where we innovate)

- **No `bytes_processed` tracking.** Their `_dlt_execution_log` schema records `rows_loaded`, `files_loaded`, timing, status, errors — but no byte accounting. This is the dimension that makes usage-based billing possible; it's the core of our pricing pivot and not in their system.
- **No mode-level dispatch.** Whisk is 100% ELT by default (all sources → raw ClickHouse → downstream transforms). They don't keep an ETL path for small workloads because their use case is big-batch only. We keep both because our multi-tenant mix includes tiny pipelines where Arrow's per-pipeline overhead exceeds its benefit.
- **No cost estimation before run.** They don't predict bytes or dollars. We do (see [SPEC_VOLUME_METERING.md](https://github.com/datanika-io/datanika-cloud/blob/master/docs/specs/SPEC_VOLUME_METERING.md) §5.4), because it feeds quota enforcement and customer-facing cost display.

### 16.5 Risk-reducing lift summary

Lift from whisk directly, no adaptation needed: (1) the env-var trio and the file-size gotcha it prevents; (2) the polymorphic `_enrich` function shape; (3) the `LoadInfo` walk for file metadata; (4) the 100 MB / 10k-row Arrow defaults. Four concrete pins in this spec moved from "engineering best-guess" to "production-validated" — the pivot's implementation risk is materially lower.

---

**Review checklist before implementation begins (after user sign-off on all 6 specs):**

- [ ] This spec signed off by user.
- [ ] [SPEC_VOLUME_METERING.md](https://github.com/datanika-io/datanika-cloud/blob/master/docs/specs/SPEC_VOLUME_METERING.md) signed off — interfaces align on `run.bytes_processed` hook kwarg and `StreamStats` shape.
- [ ] `SPEC_GB_THROUGHPUT_METRICS.md` (Infra) signed off — Prometheus counter contract matches.
- [ ] `SPEC_VOLUME_METERING_TESTS.md` (QA) signed off — §14 tests land in the implementation PRs.
- [ ] `SPEC_DUAL_MODE_UX.md` (Product) signed off — UI surfaces match the `mode` column semantics.
- [ ] `SPEC_PRICING_V2.md` (Growth) signed off — Free/Pro/Enterprise GB tiers match what cloud's `Plan` fields will hold.
