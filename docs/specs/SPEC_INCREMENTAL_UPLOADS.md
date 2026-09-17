# SPEC — Incremental uploads resume from where the last run stopped

**Author:** Product · **Status:** contract, ready for Engineering
**Written:** 2026-09-17 · **Binds:** Engineering
**Source of truth for:** [core#1404] acceptance criterion 2
**Verified against:** `origin/dev` @ `321b684`, fetched 2026-09-17. Line numbers below are on that tree
and will drift; the symbol beside each one is the durable half.

---

## §0 — The measurement this decision is taken on

QA measured it on [core#1404] (2026-09-17, `origin/dev` `3a2d414`), through the product's own
`run_upload`, a real SQLite source and a real DuckDB destination, with the `dlt_config` produced by the
upload form (**Single table**, **Enable incremental**, cursor `updated_at`, no initial value). Five rows,
run 1, three rows added, run 2:

| arm | disposition | run 2's `SELECT` | run 2 `rows_loaded` | destination after run 2 |
|---|---|---|---|---|
| as shipped | `append` | no `WHERE` | 8 | **13 rows, 8 ids** |
| as shipped | `merge` | no `WHERE` | 8 | 8 rows, 8 ids |
| control: one stable pipeline name | `append` or `merge` | `WHERE updated_at >= 50` | 3 | 8 rows, 8 ids |

**Every run of an incremental upload starts again from its initial value.** Under `append`, the form's
default for SQL sources, every run lands the whole table again. Under `merge` the table stays correct and
every run still extracts, loads and is metered for the whole table.

The mechanism, read in code: `run_upload` passes `pipeline_id=run_id` (`upload_tasks.py:265`),
`DltRunnerService.build_pipeline` names the pipeline `pipeline_{pipeline_id}_run_{run_id}`
(`dlt_runner.py:2781-2783`), and the working directory is deleted after every run
(`upload_tasks.py:472-474`). dlt restores a cursor from the destination **by pipeline name**, and no two
runs share one. QA's control shows the rest: with one name across runs, dlt restored the cursor from the
destination even though the working directory had been deleted.

🔑 **Nobody decided that uploads should not resume.** The per-run name arrived in `af9fe78`
(2026-03-23, *"Phase 18A — dlt pipeline cleanup"*) to make a run's working directory safe to delete.
Stopping the cursor was a side effect of a clean-up design, not a product choice.

---

## §1 — Decision: fix the resume. Do not rename the mode.

[core#1404] AC2 offered two answers: a second run extracts only rows past the first run's cursor, or the
product stops calling the mode incremental. **The first.** Five reasons, in the order they weigh:

1. **Every surface already promises the resume, including the ones inside the product.** The cursor's
   tooltip reads *"Datanika uses it to fetch only new or changed rows since the last run, instead of
   scanning the full source table"* (`tooltip.incremental_cursor` in English; the other eight locales
   promise only new or changed rows). The API and MCP
   schema says single-table mode *"supports incremental"* and that `initial_value` is the *"starting
   cursor value for the first run"* (`meta_schemas.py:38`, `:84`). Withdrawing the mode means rewriting
   every one of them, plus the landing claims [core#1404] lists, to describe a feature nobody wants.
2. **What the renamed mode would be is not useful.** Without a resume, a cursor is a fixed `WHERE` that
   re-reads everything past one value on every run, forever. That is not a product anyone schedules.
3. **The byte meter makes the defect a cost, not only a slowness.** `bytes_processed` is read from the
   run's `LoadInfo` (`upload_tasks.py:276-277`), so an upload that re-extracts its whole table on every
   run is metered for the whole table on every run. A scheduled load of a growing table stays affordable
   only if runs carry deltas.
4. **It is the core of the primary persona's job.** A solo data engineer scheduling a load of a large,
   growing table needs incremental extraction. A full reload on every run is the thing they are
   replacing.
5. **The fix is demonstrated, not hoped for.** QA's control arm resumed with a stable name, and QA's
   temporary mutation of `run_upload` to a stable per-upload name flipped both strict xfails to
   `XPASS(strict)` while the control stayed green.

⚠️ **Until the fix reaches production, the product still restarts the cursor**, and every published
claim must describe that. Growth's [core#1404] AC3 correction stands until then (§6).

---

## §2 — The contract

Each criterion names the outcome, the entry point that is driven, and the witness that is read
(`PRODUCT_RULES` §16). **"A run" means `run_upload`, the function every trigger reaches**: Run on
`/uploads` (`upload_state.py:767`), the REST API (`api_v1_routes.py:677`) and a schedule
(`scheduler_integration.py:229`) all enqueue `run_upload_task`, which calls it. "The destination" means
rows read back from the destination, never `rows_loaded` alone.

### 2.1 · AC2a — the next run resumes

**Outcome.** A run of an incremental upload extracts only rows at or past the cursor the previous
successful run reached, and loads only the rows it has not loaded before.
**Entry point.** Two runs of one upload with rows added to the source between them, into a real
destination, under `append` and under `merge`.
**Witness.** Run 2's `SELECT` carries the cursor predicate with run 1's last value; run 2's
`rows_loaded` equals the rows added (3 in QA's sequence, where the boundary row the `>=` returns again
is not loaded twice); the destination holds each source row once.
**Instrument.** `tests/test_tasks/test_incremental_upload_resumes_between_runs.py` (PR #1415). Its two
`xfail(strict=True)` arms go `XPASS(strict)` on the fix, so **the fix PR removes both markers**.

### 2.2 · AC2b — a failed run does not move the cursor

**Outcome.** A run that fails after extracting leaves the cursor where the last successful run left it,
so no row is skipped.
**Entry point.** Run 1 succeeds; rows are added; run 2 fails after extraction (at load); run 3 succeeds.
**Witness.** Run 3's `SELECT` carries run 1's last value, and the destination holds every added row
exactly once.
🚨 **This is the criterion that matters most.** A cursor that advances past rows that never landed is a
green run that silently loses data on every later run, which is worse than today's defect: today's
defect over-loads and can be seen.

### 2.3 · AC2c — the cursor belongs to one source table and one destination

**Outcome.** A cursor is valid only for the source connection, source schema, table, cursor column and
initial value it was recorded under, and for the destination connection and destination schema the rows
went to. **Changing any of them makes the next run start from the initial value.**
**Entry point.** A successful run, then an edit to one of those fields, then a run. One arm per field is
not required; the destination arm and the source-connection arm are.
**Witness.** After a destination change, the new destination holds **every** source row past the
initial value, not only rows past the old cursor. After a source-connection change, the run extracts
from the initial value. Neither run fails because of state recorded under the earlier configuration.

Why each field is in the key, because each one is a way to lose rows silently:
- **destination connection or schema** — a cursor carried into a new destination omits every row loaded
  before the change;
- **source connection, schema or table** — a cursor from one database applied to another skips that
  database's older rows;
- **cursor column** — a value recorded for `id` means nothing for `updated_at`;
- **initial value** — changing it is the user saying where to start. This is also the reset: to
  re-extract everything, change or clear the initial value. No new button is needed.

⚠️ `write_disposition`, `row_order`, batch size, the schema contract and the description are **not** in
the key. Changing them applies to the rows the next run extracts; it does not restart the cursor. **The
negative control is one of these edits followed by a run that still resumes**, or a key that restarts on
every save passes every arm above. Compare values, not their stored spelling: re-saving the form must
not restart a cursor whose fields did not change. (Renaming the upload can restart it, because for a
destination with no schema field the schema is named after the upload, `upload_tasks.py:261-263`.)

### 2.4 · AC2d — overlapping runs of one upload do not both load the same rows

**Outcome.** While a run of an incremental upload is pending or running, a second run of that upload
does not extract against the same cursor at the same time. It either waits for the first to finish, or
it is refused, and a refusal says which run is in progress.
**Entry point.** Two runs of the same upload started together.
**Witness.** The destination holds each row once; a refused run's status and message say why.
**Why it is new.** Today each run has its own name, so overlapping runs cannot share a cursor.
`run_upload_task` takes a **per-organization** slot (`concurrency_service.acquire(org_id)`, 5 by default
in the core edition), not a per-upload lock, so two runs of one upload can overlap today and will share
a cursor after the fix.

### 2.5 · AC2e — every surface that accepts a cursor resumes, or refuses it at save

The product never calls something incremental that does not resume. Engineering enumerates the surfaces
and reports the count in the PR body, **including zero** (`SPEC_EARNED_VERDICTS` §5.4). Known today:

| surface | today, by code | required |
|---|---|---|
| upload form, **Single table** mode | restarts (§0) | resumes (§2.1–2.4) |
| `dlt_config.incremental` through the REST API and MCP (`meta_schemas.py:75-93`) | the same path | the same |
| MongoDB `incremental` (`dlt_runner.py` `_build_mongodb_source`) | a fixed filter that never advances (QA, [core#1404], read in code) | resumes, or is refused at save with a message |
| ELT mode's `IRIncremental` (`ir/builder.py`) | only `ir/validator.py` reads it, to check the column exists; `elt_runner.py` never mentions it | honoured, or refused at save |

**Not affected:** Kafka, which by code resumes through offsets committed to the broker, not through dlt
state (QA, [core#1404]).

### 2.6 · AC2f — the change says what it does to an upload that already exists

The first run after the fix finds no cursor recorded under the new identity, so it extracts from the
initial value once more. Under `append` that lands one more full copy. **The PR body states this**, so the
promotion carries it. Nobody has established whether a production upload uses a cursor today, and at 0
paying users that does not change the decision.

---

## §3 — What must not change

- **An upload with no cursor behaves exactly as today.** SaaS, OpenAPI, file and non-incremental SQL
  uploads keep one pipeline name per run: [core#1336] designed those paths around no dlt state crossing
  runs. `tests/test_services/test_rerun_lands_each_record_once.py` stays green unmodified.
- **A run's working directory is still removed after the run, success or failure** (`upload_tasks.py:472-474`).
- **The hourly sweep still removes an orphaned working directory and still never removes an active
  run's.** `maintenance_service._extract_run_id` finds the run by parsing `_run_<id>` out of the directory
  name (`maintenance_service.py:73-93`). A directory named without a run id is never recognised as active
  and falls back to its age alone.

---

## §4 — Mutations that must go red (`SPEC_EARNED_VERDICTS` §5.2)

| AC | mutation | why this one |
|---|---|---|
| 2.1 | name the pipeline per run again | today's code |
| 2.2 | advance the cursor before the load commits | the silent data loss 2.2 exists for |
| 2.3 | leave the destination out of the cursor's key | a new destination receives only rows past the old cursor |
| 2.3 | leave the source connection out of the key | a second database's older rows are skipped |
| 2.4 | start both runs with nothing serialising them | overlapping runs extract the same rows |
| §3 | give a non-incremental upload the stable identity | `test_rerun_lands_each_record_once.py` goes red |

---

## §5 — Out of scope

- **A "reset cursor" or "full refresh" button.** §2.3 makes the initial value the reset, which needs no
  new control. The signal that would un-defer a button: a user who needs to re-extract without changing
  the initial value.
- **Showing the current cursor value in the UI.** Useful, and not needed for the resume to be correct.
- **Deduplicating rows that earlier `append` runs already duplicated.** Those rows are the user's data
  now, and rewriting them is not the loader's decision.
- **[core#1414]**, an initial value typed in the form stored as text. It fails the run on a non-text
  cursor today, which is a separate defect with its own issue. The §2.3 initial-value arm uses a cursor
  type that defect does not affect until it ships.

---

## §6 — What waits on this fix reaching production

Each item is **Blocked by: this fix on core `master`**, and none is a task before then.

- **The landing site's incremental claims** go back to describing a resume. Until then they describe
  today's restart, which is what [core#1404] AC3 is correcting now (Growth).
- **The published incremental benchmark** measured dlt with a fixed pipeline name, not the product
  ([core#1404], `scripts/benchmark/benchmark.py`). A product figure needs a run through the product.
- **The connector guides' incremental text** says what the cursor's key is (§2.3), that a failed run does
  not move it (§2.2), and how to re-extract (change or clear the initial value) (Product).

[core#1336]: https://github.com/datanika-io/datanika-core/issues/1336
[core#1404]: https://github.com/datanika-io/datanika-core/issues/1404
[core#1414]: https://github.com/datanika-io/datanika-core/issues/1414
