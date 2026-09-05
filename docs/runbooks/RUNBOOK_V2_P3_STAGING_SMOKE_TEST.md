# V2 P3 ELT Engine — Staging smoke-test runbook

**Purpose**: Verify the V2 P3 ELT engine (IR builders, `bytes_processed` emission, dual-mode dispatch) works end-to-end on staging before proceeding to the V2 P4 production flag flip.

**When to run**: After Engineering replaces the 4 `NotImplementedError` stubs in `datanika/services/ir/{builder,validator,introspect}.py` + `datanika/services/elt_runner.py` with real implementations, and the commits are promoted to staging.

**Blast radius**: Staging only (`staging-app.datanika.io`). Do NOT proceed to V2 P4 prod flip until this runbook passes.

**Owner**: Product executes; QA verifies independently. Engineering on-call paged only if IR failures suggest a code bug (not a config issue).

---

## 0. Pre-flight

- [ ] Engineering V2 P3 IR builder PRs **running on staging** — compare the staging container's build SHA against the merge commit, rather than reading the branch.
- [ ] Cloud `bytes_processed` handlers are **subscribed in the worker** — `docker exec datanika-staging-celery /app/.venv/bin/python -c "import datanika.tasks.celery_app; from datanika import hooks; print(len(hooks._handlers))"` returns a non-zero count including `run.before_execute`. 🚨 **"Already on `master`" is not evidence of this.** [core#772]: the code was on `master`, in the image, and `bootstrap_cloud()` ran only in the web process — the worker dispatched into an **empty handler dict**, so run-quota, volume-quota and every run/byte metering hook had *never once* executed in production. Ask the process, and import the entrypoint **it** imports; a bare interpreter registers nothing.
- [ ] `docker exec datanika-staging-app /app/.venv/bin/python -c "from datanika.config import settings; print(settings.datanika_dual_mode_ux_enabled)"` prints `True` (set by the P1 flip). Reading `.env.docker` confirms the file, not the process ([core#646]).
- [ ] `docker exec datanika-staging-celery /app/.venv/bin/python -c "from datanika_cloud.billing.config import cloud_settings as c; print(c.bytes_quota_enforce)"` prints `False` on staging (dry-run mode — NOT enforcing yet). ⚠️ Check the **worker**, not only the web process: the quota hooks fire inside the worker.
- [ ] Staging has a PostgreSQL source connection available (or create one via the e2e seed)
- [ ] You have SSH access to the staging box
- [ ] Grafana accessible at `staging-app.datanika.io:3001` (or port-forwarded)
- [ ] CF Access credentials set if staging is behind Cloudflare Zero Trust

## 1. Seed a test tenant with source data

Create a Free-tier org with a PostgreSQL source and a PostgreSQL destination (PG-to-PG is the simplest dlt-compatible path).

```bash
ssh root@<staging-ip>
cd /opt/datanika/datanika
docker compose exec app bash

# Inside container
uv run python <<'PY'
import asyncio
from datanika.database import get_session
from datanika.models.organization import Organization
from datanika.services.auth import create_user

async def seed():
    async with get_session() as session:
        org = Organization(name="V2P3 Smoke", slug="v2p3-smoke")
        session.add(org)
        await session.commit()
        await session.refresh(org)
        user = await create_user(
            session,
            email="v2p3-smoke@datanika.io",
            password="SmokeTest!2026",
            org_id=org.id,
        )
        print(f"org_id={org.id}, user_id={user.id}")

asyncio.run(seed())
PY
```

- [ ] Test org created
- [ ] Source PostgreSQL connection added (pointing at staging PG or an external test DB)
- [ ] Destination connection added (can be the same PG instance, different schema)

## 2. Run an ETL pipeline and verify `bytes_processed` emission

Create a pipeline in **ETL mode** (the default) pointing at a source table with known row count.

1. Navigate to `/pipelines` as the seeded user
2. Create a new pipeline: source connection → destination connection, pick 1-2 small tables
3. Mode selector should show "auto" (resolved to ETL for this source)
4. Click "Create" → "Run now"
5. Wait for run completion in the Runs tab

Verification:

- [ ] Run completes successfully (status = `success`)
- [ ] Run detail shows rows loaded
- [ ] Check `bytes_processed` emission:
  ```sql
  -- On staging DB
  SELECT * FROM usage_ledger
  WHERE org_id = <org_id> AND metric = 'bytes_processed'
  ORDER BY created_at DESC LIMIT 5;
  ```
- [ ] At least one `usage_ledger` row exists with `metric = 'bytes_processed'` and `amount > 0`
- [ ] The `source_run_id` column is populated (dedup key from cloud#33)

## 3. Run an ELT pipeline and verify IR compiler handoff

Create a second pipeline in **ELT mode** (explicit selection) pointing at the same source.

1. Create a new pipeline
2. In the mode selector, pick **ELT** explicitly
3. Configure tables, save, run

Verification:

- [ ] Run completes successfully
- [ ] IR builder was invoked (check app container logs for `ir.builder` or `elt_runner` log lines)
- [ ] dbt was invoked for the transformation phase (check logs for `dbt run` execution)
- [ ] No dlt extract phase for the ELT path (the streaming ingest path should bypass dlt's extract)
- [ ] `bytes_processed` emission in `usage_ledger` — **ELT amount should be smaller** than the ETL run for the same source data (3-5x fewer billable bytes is the spec target)

## 4. Verify Grafana volume dashboard

Open Grafana via an SSH tunnel — it binds `127.0.0.1` only, so `staging-app.datanika.io:3001` is not
reachable ([core#907]):

```bash
ssh -N -L 3001:127.0.0.1:3001 root@185.25.22.188    # then browse http://localhost:3001
```

⚠️ **There is no "Datanika Volume Metrics" dashboard.** The only provisioned dashboards are
`Database Performance` and `Datanika Server Overview`, so the two panel checks that used to sit here
could never have been performed. Verify the same facts from `usage_ledger` (step 2) and from the
metrics surface below.

- [ ] Volume Alerts group present, rules in `OK`/`Normal`
- [ ] `/metrics` on staging asserts a **sample line**, not a metric name — run it on the box:

  ```bash
  ssh root@185.25.22.188
  curl -sf http://127.0.0.1:8100/metrics > m.txt && wc -l < m.txt
  grep -E '^datanika_cloud_bytes_ledger_scrape_ok [0-9]' m.txt
  grep -E '^datanika_cloud_bytes_processed_total\{org_id="[0-9]+"\} [0-9]' m.txt
  ```

  🚨 `prometheus_client` emits `# HELP`/`# TYPE` for a labelled metric with zero children, so a
  `grep` for a bare metric name passes on a counter that has never recorded anything. `/metrics` is
  also **not** routed through Apache on purpose (it carries per-tenant byte volumes unauthenticated),
  so a `curl` to the public hostname returns the SPA. See `docs/ENGINEERING_RULES.md` §5.

- [ ] `datanika_cloud_bytes_ledger_scrape_ok` is `1`
- [ ] `datanika_cloud_bytes_processed_total{org_id="…"}` has at least one sample line, value > 0

## 5. Verify `check_bytes_quota` dry-run logging

Since `DATANIKA_BYTES_QUOTA_ENFORCE=false` (dry-run mode), quota violations should be logged but not enforced.

1. Seed the test org to near its Free cap (10 GB):
   ```sql
   -- Bump bytes_processed to 9.5 GB so the next run crosses the threshold
   INSERT INTO usage_ledger (org_id, metric, amount, period_start, created_at)
   VALUES (<org_id>, 'bytes_processed', 9500000000,
           date_trunc('month', now()), now());
   ```
2. Run another pipeline

Verification:

- [ ] App container logs contain `"bytes quota dry-run"` (cloud#33's dry-run log line)
- [ ] The run is NOT blocked (dry-run mode allows it through)
- [ ] The log line includes the predicted bytes and the plan cap
- [ ] `datanika_cloud_bytes_quota_rejected_total` — ⚠️ **cannot be asserted from `/metrics`**: it is labelled, so a healthy zero state emits no sample line, identical to the collector being broken. Confirm dry-run rejections from the app log line and `usage_ledger` instead.

## 6. Verify V2 P3 UI surfaces (from VA2)

With `DATANIKA_DUAL_MODE_UX_ENABLED=true` already set:

- [ ] **Cost estimator card** visible on pipeline create/edit form (below mode selector)
- [ ] **ELT nudge card** visible on runs page (if test pipeline is in ETL mode — may need >=5 runs + 20 GB to trigger; skip if threshold not met, note as expected)
- [ ] **Billing preview card** visible on settings page (after org profile card)
- [ ] All three use translated i18n keys (test with `?locale=de`)

## 7. Verify dual-dim dashboard usage bar

Navigate to `/dashboard`.

- [ ] Runs usage bar visible (existing dimension)
- [ ] Volume usage bar visible (new dimension from P1)
- [ ] Volume bar reflects the seeded + run-generated bytes
- [ ] Bar freshness: value updates within 5 minutes of a completed run (per SPEC §13.2 resolution)

## 8. Back-out plan

If any of steps 2-7 fail:

```bash
# Do NOT touch DATANIKA_DUAL_MODE_UX_ENABLED — the UI is separate from the engine
# The engine itself has no flag; if IR builder crashes, the run fails gracefully
# and falls back to the ETL path (spec §12.3 degradation guarantee)

# If the failure is data corruption or ledger inconsistency:
# 1. Stop celery to prevent further runs
docker compose stop celery

# 2. File a P0 bug against Engineering with the specific failure
# 3. Delete the test org's usage_ledger rows to clean up staging
DELETE FROM usage_ledger WHERE org_id = <org_id>;

# 4. Restart celery once the bug is diagnosed
docker compose start celery
```

- [ ] Issue filed with reproduction steps and log excerpt
- [ ] Do NOT proceed to V2 P4 flag flip

## 9. Sign-off before V2 P4

- [ ] All checkboxes in steps 2-7 green
- [ ] QA has independently walked through steps 2-7 on staging
- [ ] Engineering confirms IR builder test coverage is `strict=True` (cloud#32 xfails flipped)
- [ ] Dry-run log sweep over 48h shows zero unexpected quota rejections
- [ ] Product lead approves proceeding to V2 P4 (production flag flip)
- [ ] Infra on-call aware of the upcoming V2 P4 flip window

Proceed to [RUNBOOK_V2_P4_FLAG_FLIP.md](RUNBOOK_V2_P4_FLAG_FLIP.md).

---

## Related artifacts

- [SPEC_ELT_IR_ARCHITECTURE.md](../specs/SPEC_ELT_IR_ARCHITECTURE.md) — IR builder design
- [SPEC_VOLUME_METERING.md](https://github.com/datanika-io/datanika-cloud/blob/dev/docs/specs/SPEC_VOLUME_METERING.md) — bytes_processed metering
- [SPEC_DUAL_MODE_UX.md](../specs/SPEC_DUAL_MODE_UX.md) — UX spec with §13.2 resolved questions
- [SPEC_GB_THROUGHPUT_METRICS.md](https://github.com/datanika-io/datanika-cloud/blob/dev/docs/specs/SPEC_GB_THROUGHPUT_METRICS.md) — Grafana dashboards + alerts
- [RUNBOOK_V2_P1_STAGING_SMOKE_TEST.md](RUNBOOK_V2_P1_STAGING_SMOKE_TEST.md) — P1 predecessor runbook
- [core#190](https://github.com/datanika-io/datanika-core/pull/190) — VA2 follow-up UI surfaces

[core#646]: https://github.com/datanika-io/datanika-core/issues/646
[core#772]: https://github.com/datanika-io/datanika-core/issues/772
