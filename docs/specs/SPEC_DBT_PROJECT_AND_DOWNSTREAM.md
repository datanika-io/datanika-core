# SPEC — Bring an existing dbt project, and start each downstream job from one cause

> **Status:** contract. This pull request does not implement it.
> **Owner:** Product. **Engineering is who it binds.**
> **Issue:** [core#1669](https://github.com/datanika-io/datanika-core/issues/1669).
> **Measured on** core `origin/dev` `05fe308e3410cd1cfa7bfba231819251ad0bf2aa` (2026-11-12). Both claims in the issue still hold on that commit. The decisions below are made against that tree.

---

## What that tree does

### A client cannot bring an existing dbt project

`DbtProjectService.get_project_path` returns `{projects_dir}/tenant_{org_id}/`. `ensure_project` creates `models/`, `macros/`, `tests/`, and `snapshots/`, writes `dbt_project.yml` (name `tenant_{org_id}`, those four path keys, profile of the same name) when the file is absent, and writes `macros/generate_schema_name.sql` when that file is absent. It does not create a `seeds/` directory and the project file has no seed path.

`write_model` writes one model: `models/{schema}/{name}.sql` from `sql_body`, plus a per-model `.yml` (`materialized`, `schema`, and the incremental keys `unique_key`, `strategy`, `updated_at`, `on_schema_change`). An existing `.yml` keeps its `columns`. `write_tests_config` writes column tests into that `.yml` from a transformation's `tests_config`. The accepted generic tests are `not_null`, `unique`, `accepted_values`, and `relationships`.

`write_packages_yml` writes `packages.yml`. `install_packages` runs `dbt deps`. On this commit both are called from `tests/test_services/test_dbt_project.py` and from no route. The API does not call them. `write_snapshot` writes one snapshot file from `snapshot_name`, `sql_body`, `unique_key`, `strategy` (`timestamp` or `check`), and the strategy's columns. Nothing in `datanika/` calls it. There is no macro writer and no singular-test writer.

`POST /api/v1/import` and `POST /api/v1/pipelines/yaml` require `version` 2. The sections they create are `connections`, `uploads`, `pipelines`, and `transformations` (`_IMPORT_SECTIONS`). One call may create at most 1000 objects, summed across those sections. A transformation in that document is `name` plus `sql_body`, with the materialization, schema, incremental config, and `tests_config` the transformation API already accepts. The route table has no project-archive route and no git-remote route.

### A finished extract-load does not start dbt

A `schedules` row has one `target_type`, one `target_id`, and one `cron_expression`. The scheduler's job id is `schedule_{id}`, and `_dispatch_target` starts that one target.

`check_upstream_dependencies` reads `get_upstream`. An edge with no `check_timeframe_value` is skipped. An edge with a timeframe requires a `Run` of status `SUCCESS` whose `finished_at` is inside the window. `check_deps_or_retry` calls that check on a run that has already started (upload, transformation, and pipeline tasks). Unsatisfied deps retry every 60 seconds, five times, then fail the run. The check looks backward. It does not enqueue the next node.

`get_downstream` is defined on `DependencyService` and called from `tests/test_services/test_dependency_service.py` only. `api_v1_routes.py` has no dependency route. Edges are added and removed on the DAG page (`datanika/ui/pages/dag.py`).

`NON_TERMINAL_RUN_STATUSES` is `pending`, `running`, and `cancelling`. `TERMINAL_RUN_STATUSES` is `success`, `failed`, and `cancelled`. `datanika/services/agent_docs.py` tells a caller that cancel stops a run only when its work has not started, and that a run already working continues to its end.

---

## Decision — what bringing a project includes

Bringing a project is one version-2 import document, JSON on `POST /api/v1/import` or YAML on `POST /api/v1/pipelines/yaml`. The document keeps the four sections above and adds the four sections in this decision. Models are the existing `transformations` section. There is no second models key.

The files land in the org's existing `tenant_{org_id}/`. The call is atomic with the import that exists today: a refusal creates nothing, and a failure leaves the project files the org already had.

### Included

| Piece | Document | Where it is written | Record shape |
|---|---|---|---|
| Models | existing `transformations` | `write_model`, column tests via `write_tests_config` | `name`, `sql_body`, and the materialization, `schema_name`, `incremental_config`, `tests_config`, description, tags, and destination the transformation API already takes. Column tests stay `tests_config`. The four generic tests stay the accepted set. |
| Macros | new `macros` | a writer Engineering adds, one file `macros/{name}.sql` | `name`, `sql_body`. Identifier rules match `write_model`. |
| Packages | new `packages` | `write_packages_yml`, then `install_packages` | Hub `package` and `version` only. The same shape `tests/test_services/test_dbt_project.py` writes for `dbt-labs/dbt_utils`. |
| Snapshots | new `snapshots` | `write_snapshot` | `snapshot_name`, `sql_body`, `unique_key`, `strategy` (`timestamp` or `check`), `updated_at` for `timestamp`, `check_cols` for `check`, `target_schema`. |
| Singular tests | new `tests` | a writer Engineering adds, one file `tests/{name}.sql` | `name`, `sql_body`. |

`macros`, `packages`, `snapshots`, and `tests` count toward the same 1000-object cap as the four existing sections.

A key that sends `macros`, `packages`, `snapshots`, or `tests` needs `transformations:write`. The refusal is the existing `insufficient_scope` response, and nothing is created.

Packages are a full set. When the key is present, `packages.yml` is replaced and `dbt deps` runs. When the key is absent, `packages.yml` stays as it is. An empty list writes an empty package list and runs `dbt deps`.

A snapshot is not a transformation and not a schedule target. A transformation whose materialization is `snapshot` stays that transformation. A singular test is not a column test and not a schedule target.

### Refused

The whole call returns 400 and creates nothing when the document contains any of these:

- `project_archive`, `git_remote`, `git_url`, or `files`. Those keys are a refusal. They are not ignored unknown keys.
- `seeds`. `ensure_project` has no seeds directory. A CSV is an upload.
- A package entry that is anything other than Hub `package` plus `version`. A `git` or `revision` entry is a refusal on this boundary. The service method can persist that dict in its own tests; the import does not send it.
- A model, macro, or singular test whose name ends in `.py`, and a declared Python body.
- A macro named `generate_schema_name`.
- A name or schema that resolves outside `tenant_{org_id}/`, or outside the directory that writer owns. The identifier check `write_model` and `write_snapshot` already apply is the floor.

The client does not send `dbt_project.yml`, `profiles.yml`, or `macros/generate_schema_name.sql`. The project name stays `tenant_{org_id}`. Profiles stay the ones `generate_profiles_yml` writes from the destination connection. `dbt_packages/`, `target/`, `logs/`, analyses, exposures, and selectors are not part of this document.

This document does not create dependency edges, and it does not parse `ref()`. Edges remain the ones the DAG page writes. This spec adds no dependency route.

---

## Decision — one starter, so the model runs once

A run that reaches `SUCCESS` considers each **direct** downstream from `get_downstream`. A self-edge starts nothing. A later hop starts when that downstream itself succeeds.

**A node with an active schedule starts only from that schedule.** Upstream success enqueues nothing for it and arms no follow-up. The cron remains the start. `check_upstream_dependencies` still gates that cron run: every upstream edge that has a timeframe needs a `SUCCESS` inside the window. An edge with no timeframe stays metadata-only and still does not block the cron run. The same edge does count as a downstream when the node has no active schedule.

An active schedule is a `schedules` row for that org, `target_type`, and `target_id` with `is_active` true and `deleted_at` null. One such row is enough. Two active schedules on one node already fire twice; this spec leaves that pair alone, and upstream success still enqueues nothing.

A cron run that exhausts the current wait (60 seconds, five retries) fails. The model does not start early. The next cron fire is the next attempt. Upstream success is not a substitute attempt.

**A node with no active schedule starts from upstream success.** `FAILED` and `CANCELLED` start nothing. A run that is still working starts nothing.

- Two successes that land together start one run. The node has at most one run in `NON_TERMINAL_RUN_STATUSES`.
- A success that lands while that run is non-terminal does not start a second concurrent run.
- When the in-progress run reaches a terminal status, exactly one follow-up starts if a direct upstream succeeded after that run started and that success was not the cause of the in-progress run. Several of those successes collapse into the one follow-up.
- If an active schedule exists when the in-progress run ends, the follow-up does not start. The schedule is the only starter from then on.

The in-run wait stays 60 seconds and five retries. This spec does not lengthen it. Work starts only after `check_upstream_dependencies` is satisfied. A wait that expires fails the run.

A run that is already working is left to finish. Cancel stays what `agent_docs.py` already says: it stops a run whose work has not started, and a run already working reads `cancelling` and runs to its end. Upstream success does not cancel that run, restart it, or replace it.

A manual success, an API success, and a cron success of the upstream all follow this rule. The nodes are uploads, transformations, and pipelines.

The two starters are exclusive. A scheduled model runs from its cron. An unscheduled model runs from upstream success, one in flight, with at most one follow-up collapsed from successes that arrived during the flight. The same model is not started by both.

---

## Out of scope

- Any implementation in this pull request. No import section, no enqueue, no test that performs either behavior.
- Homepage, `/privacy`, `/trust`, `/dpa`.
- Run-cancellation behavior. The sentences in `agent_docs.py` stay.
- Dependency routes, and creating edges by reading `ref()`.
- Collapsing two active crons on one node into one fire.
- A snapshot or a singular test as a schedule target.
- The DuckDB volume. That contract is `docs/specs/SPEC_DUCKDB_SHARED_VOLUME.md`.

---

## Done, from the user's side

A client sends one version-2 document and the org's `tenant_{org_id}/` gains those transformations, macros, Hub packages, snapshots, and singular SQL tests. A document that carries an archive, a git remote, a seed, or a git package creates none of them, and the project files already on disk stay as they were.

A finished upload starts a downstream model that has no active schedule, once. It does not start a downstream model that has an active schedule. That scheduled model runs from its cron, and the backward check still requires each timed upstream success to be inside its window before the model runs.
