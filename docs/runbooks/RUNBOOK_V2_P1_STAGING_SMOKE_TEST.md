# V2 P1 Dual-Mode UX — Staging flag-flip smoke-test runbook

**Purpose**: Verify the flag-gated V2 P1 UI (shipped in [core#165](https://github.com/datanika-io/datanika-core/pull/165)) renders correctly on staging before flipping `DATANIKA_DUAL_MODE_UX_ENABLED=true` in production.

**When to run**: After (a) Engineering's P1 plumbing lands (bytes quota hook, IR mode resolver, `pipeline.mode` column) and (b) `datanika-cloud` plugin extends `usage.get_summary` to populate `bytes_used`/`bytes_limit` and sets `QuotaExceededError.metric = "bytes_processed"`.

**Blast radius**: Staging only (`staging-app.datanika.io`, on the pointer.gr box `185.25.22.188` — the Hetzner box was terminated 2026-07-14). Do NOT flip the flag on `app.datanika.io` until this runbook passes.

**Owner**: Product (runbook author) executes; QA verifies. Infra on-call paged only if the flip itself triggers a prod alert.

---

## 0. Pre-flight

- [ ] Engineering PR for P1 plumbing merged to `dev` and promoted to `master`
- [ ] Cloud PR for `usage.get_summary` bytes + `QuotaExceededError.metric` merged and promoted
- [ ] Staging has been re-deployed with the above commits
- [ ] You have SSH access to the staging box (or a staging `psql` equivalent)
- [ ] You know the current staging `.env` location (typically `/opt/datanika/datanika/.env.docker`)

## 1. Seed the test tenant

Create a Free-tier org at 9.8 GB of bytes-processed usage (i.e. 98% of the 10 GB cap) so the Path A modal fires on the first pipeline run that would tip over.

```bash
# SSH to staging
ssh root@<staging-ip>

# Shell into the app container
cd /opt/datanika/datanika
docker compose exec app bash

# Inside container — seed via Python
uv run python <<'PY'
import asyncio
from datanika.database import get_session
from datanika.models.organization import Organization
from datanika.services.auth import create_user
from datetime import datetime, timezone

async def seed():
    async with get_session() as session:
        # Create org on Free plan
        org = Organization(name="V2P1 Smoke Test", slug="v2p1-smoke")
        session.add(org)
        await session.commit()
        await session.refresh(org)

        # Create owner user
        user = await create_user(
            session,
            email="v2p1-smoke@datanika.io",
            password="SmokeTest!2026",
            org_id=org.id,
        )
        print(f"org_id={org.id}, user_id={user.id}")

asyncio.run(seed())
PY
```

Then seed usage rows in `UsageLedger` so the org shows 9.8 GB consumed this month. Exact column names / aggregation logic depend on where Cloud stores the running total — confirm with the Cloud owner before running:

```bash
# Inside container — adjust column names to match cloud model
uv run python <<'PY'
import asyncio
from datanika.database import get_session
from datanika_cloud.models.usage_ledger import UsageLedger
from datetime import datetime, timezone

ORG_ID = <fill_in>  # from previous step
BYTES_TO_SEED = int(9.8 * 1024**3)  # 9.8 GB

async def seed():
    async with get_session() as session:
        row = UsageLedger(
            org_id=ORG_ID,
            metric="bytes_processed",
            amount=BYTES_TO_SEED,
            period_start=datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0),
        )
        session.add(row)
        await session.commit()

asyncio.run(seed())
PY
```

- [ ] Tenant created at 9.8 GB / 10 GB (98%)
- [ ] Confirmed via `SELECT sum(amount) FROM usage_ledger WHERE org_id = ? AND metric = 'bytes_processed'`

## 2. Flip the flag on staging

```bash
# On staging, edit the .env
vim /opt/datanika/datanika/.env.docker
# Set: DATANIKA_DUAL_MODE_UX_ENABLED=true

# Restart the app container to pick up the new env
cd /opt/datanika/datanika
set -a && source .env.docker && set +a
docker compose up -d app
```

- [ ] `docker exec datanika-staging-app /app/.venv/bin/python -c "from datanika.config import settings; print(settings.datanika_dual_mode_ux_enabled)"` prints `True` — the process, not `.env.docker`. A variable present in that file is not a setting the process read ([core#646]); it is why 48% of production reconnects once served a stale session.
- [ ] App container restarted and healthy (`docker compose ps` shows `datanika-app` up)

## 3. Verify dashboard dual-dim usage bar

Log in as the seeded user, navigate to `/dashboard`.

- [ ] Runs usage bar visible (existing dimension)
- [ ] **New: Volume usage bar visible below runs bar**, showing "9.8 GB / 10 GB (98%)"
- [ ] Volume bar fill color is **red** (≥80% threshold)
- [ ] Bar copy uses the quota.volume_* i18n keys (not raw strings), verify with `?locale=ru` — Russian translations render
- [ ] Divider visible between runs and volume dimensions

## 4. Verify pipeline mode selector

Navigate to `/pipelines` and click "New pipeline" (or edit an existing one).

- [ ] Mode selector renders below "Custom selector" field
- [ ] Three radio options visible: auto / ETL / ELT
- [ ] Default selected = "auto"
- [ ] Clicking an option updates `PipelineState.form_mode` (no console errors)
- [ ] "Advanced: volume estimate" disclosure expands on click
- [ ] Volume estimate input accepts numeric input
- [ ] All mode selector copy uses the pipelines.mode_* i18n keys (verify with `?locale=de`)

## 5. Verify Path A modal (Free hard-block branch)

While on the pipeline form, fill in a valid config pointing at a connection that will actually process data. Click "Create" → "Run".

Expected behavior: Cloud's `check_run_quota` hook raises `QuotaExceededError(metric="bytes_processed")` because the seeded usage (9.8 GB) + any tiny projection tips over 10 GB.

- [ ] Pipeline save succeeds
- [ ] First run attempt triggers modal (NOT a generic error callout)
- [ ] Modal title: "Volume quota reached" (or locale equivalent)
- [ ] Modal body references the 10 GB Free cap
- [ ] **Free branch**: only "Upgrade to Pro" button visible (NOT "Allow as overage")
- [ ] Clicking "Upgrade" routes to `/settings/billing` (or wherever the upgrade surface lives)
- [ ] Modal dismisses cleanly via ESC / close button
- [ ] Pipeline run status reflects the blocked attempt in the runs table

## 6. Verify Path A modal (Pro soft-overage branch)

Upgrade the seed org to Pro in the DB (`UPDATE subscription SET plan_id = (SELECT id FROM plan WHERE name = 'Pro') WHERE org_id = ?`) and re-trigger a run.

- [ ] Modal fires again
- [ ] **Pro branch**: "Allow as overage" button visible (NOT "Upgrade to next tier")
- [ ] Clicking "Allow as overage" dismisses the modal and proceeds with the run
- [ ] Run executes to completion (or errors for unrelated reasons — note them separately)
- [ ] Per Q3 SILENT: NO Enterprise upsell copy anywhere in the modal

## 7. Regression checks on non-volume errors

Trigger a non-quota error (e.g. malformed connection config) and confirm the modal does NOT fire for unrelated errors.

- [ ] Non-quota errors still show the normal error callout, not the Path A modal
- [ ] `is_quota_error` is True only when `metric == "bytes_processed"`

## 8. Back-out plan

If any of steps 3–7 fail and the issue is not a quick fix:

```bash
# On staging — un-flip the flag
vim /opt/datanika/datanika/.env.docker
# Set: DATANIKA_DUAL_MODE_UX_ENABLED=false

cd /opt/datanika/datanika
set -a && source .env.docker && set +a
docker compose up -d app
```

- [ ] Flag reverted
- [ ] Dashboard shows only runs bar (no volume bar)
- [ ] Pipeline form shows no mode selector
- [ ] File a bug against the failing area, do NOT proceed to prod flip

## 9. Sign-off before prod flip

- [ ] All checkboxes in §3–§7 green
- [ ] QA has independently walked through §3–§7 on staging
- [ ] Product lead approves prod flip
- [ ] Infra on-call aware of the flip window (for monitoring/alerting)

Prod flip follows the same `vim .env.docker` + `docker compose up -d app` pattern on `app.datanika.io`. Keep a terminal open on Grafana (`datanika-grafana:3001`) for the first 15 minutes post-flip to catch any unexpected error-rate spike.

---

## Open questions to close before running this runbook

From SPEC_DUAL_MODE_UX.md §13.2 — the 3 open questions carried on current_state.md:

1. **Bar freshness → Infra**: How fresh is the `bytes_used` value shown in the usage bar? If it's minutely-aggregated, step §3's "9.8 GB" verification needs to account for up-to-60s lag. Flag this to Infra before seeding.
2. **$/GB rates → Growth**: The seeded illustrative $/GB rates in the modal copy need Growth's sign-off before the prod flip — staging can use placeholder rates.
3. **Mid-run counter → Infra**: Cloud VOLUME §3 already answered "no" — a running pipeline cannot show a live GB counter. The modal fires post-run-projection, not mid-run. This runbook assumes the post-projection flow; flag to Infra only if the semantics have changed since spec v3.

## Related artifacts

- [SPEC_DUAL_MODE_UX.md](../specs/SPEC_DUAL_MODE_UX.md) v3
- [core#165](https://github.com/datanika-io/datanika-core/pull/165) — V2 P1 UI implementation
- [plans/product/PLAN_PRODUCT.md](https://github.com/datanika-io/datanika-core/issues/734) — V2 P1 row
- plans/product/current_state.md (`plans/product/current_state.md`) — current standing

[core#646]: https://github.com/datanika-io/datanika-core/issues/646
