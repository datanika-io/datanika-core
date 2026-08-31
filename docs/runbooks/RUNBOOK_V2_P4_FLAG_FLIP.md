# V2 P4 Production Flag Flip — Runbook

**Purpose**: Step-by-step procedure for flipping `DATANIKA_BYTES_QUOTA_ENFORCE=true` on production (`app.datanika.io`), activating volume-based quota enforcement for all orgs. This is the hardest single operational event in the V2 rollout — once this flag flips, Free-tier orgs are hard-blocked at 10 GB and Pro/Enterprise orgs start accruing metered overage.

**When to run**: After [RUNBOOK_V2_P3_STAGING_SMOKE_TEST.md](RUNBOOK_V2_P3_STAGING_SMOKE_TEST.md) passes all 9 steps, including the 48h dry-run log sweep.

**Blast radius**: Production (`app.datanika.io`). This is irreversible in the sense that any run blocked by quota enforcement is a user-visible event. The flag itself is reversible within ~45s.

**Owner**: Infra executes the flag flip + deploy. Product owns the verification checklist. QA on standby for regression. Growth on standby for comms.

---

## 1. Pre-flight (T-24h)

All items must be green before scheduling the flip window.

### 1.1 Staging sign-off

- [ ] [RUNBOOK_V2_P3_STAGING_SMOKE_TEST.md](RUNBOOK_V2_P3_STAGING_SMOKE_TEST.md) §9 sign-off complete
- [ ] All V2 P3 Engineering tests green in CI (`strict=True` on cloud#32 xfails)
- [ ] Dry-run log sweep on staging: zero unexpected `"bytes quota dry-run"` rejections over 48h
- [ ] `DATANIKA_DUAL_MODE_UX_ENABLED=true` already live on prod (from P1 — confirmed via UI check)

### 1.2 Code readiness

- [ ] All V2 PRs merged to `dev` and promoted to `master` on all 3 repos
- [ ] `DATANIKA_BYTES_QUOTA_ENFORCE` env var exists in `.env.docker` on Hetzner (currently `=false`)
- [ ] Plan seed rows updated with V2 values: Free = 10 GB, Pro = 100 GB / $0.50/GB, Enterprise = 1 TB / $0.25/GB
- [ ] Paddle `datanika_bytes_processed` meter created (human locker — confirm with founder)

### 1.3 Monitoring readiness

- [ ] Grafana Volume Alerts group active (`volume-tenant-spike`, `volume-plan-cap-exceeded`)
- [ ] Volume dashboard ("Datanika Volume Metrics") bind-mounted and loading
- [ ] Revenue alerts active (`revenue-webhook-silence`, `revenue-stuck-past-due`, `revenue-mrr-drop-wow`)
- [ ] Telegram alert channel verified (send a test message)

### 1.4 Rollback readiness

- [ ] Infra has SSH session open to `app.datanika.io` (or ready to open within 60s)
- [ ] `.env.docker` backup taken: `cp .env.docker .env.docker.bak-$(date +%Y%m%d-%H%M)`
- [ ] Rollback commands tested in dry-run (see §6)

## 2. Comms (T-24h to T-0)

- [ ] **Internal Telegram**: Post to team channel:
  > V2 volume-based quota enforcement going live on app.datanika.io at [TIME UTC] today. Free tier: 10 GB hard cap. Pro/Enterprise: metered overage. Rollback window: 2h. Ping [Infra on-call] if you see issues.
- [ ] **No external comms** — zero paying customers at pivot time, per PRICING_PIVOT_DECISIONS.md Q6 (hard sunset). No migration email, no in-app banner, no blog post yet (Growth owns cutover-day publish).

## 3. Flag flip (T-0)

Execute during a low-traffic window (check Plausible for the quietest 2h block — typically 02:00-04:00 UTC weekdays).

```bash
# SSH to Hetzner
ssh -i ~/.ssh/id_ed25519 root@185.25.22.188

# Backup current env
cd /opt/datanika/datanika
cp .env.docker .env.docker.bak-$(date +%Y%m%d-%H%M)

# Flip the quota enforcement flag
# Edit .env.docker:
#   DATANIKA_BYTES_QUOTA_ENFORCE=true    (was false)
vim .env.docker

# Verify the edit
grep BYTES_QUOTA .env.docker
# Expected: DATANIKA_BYTES_QUOTA_ENFORCE=true

# Rebuild and restart app + celery (zero-downtime: new containers start
# before old ones stop because docker compose uses rolling restart)
set -a && source .env.docker && set +a
docker compose up -d --build app celery
```

- [ ] `.env.docker` shows `DATANIKA_BYTES_QUOTA_ENFORCE=true`
- [ ] `docker compose ps` shows `datanika-app` and `datanika-celery` healthy
- [ ] No crash loops in first 60s (`docker logs datanika-app --tail 50`)

## 4. Post-flip verification (T+0 to T+15m)

### 4.1 Smoke endpoints

```bash
# Health check
curl -sf https://app.datanika.io/api/v1/meta/agent-tiers | python -c "
import json,sys; d=json.load(sys.stdin)
print(f'tiers={d[\"tier_count\"]}, caps={d[\"capability_count\"]}')
"
# Expected: tiers=5, caps=8
```

- [ ] Agent-tiers endpoint returns `tier_count=5, capability_count=8`
- [ ] `/` loads the SPA shell
- [ ] `/login` renders the login form

### 4.2 Grafana verification

Open `app.datanika.io:3001` (Grafana).

- [ ] Volume Alerts group: all rules in `OK` or `Normal` state (no false-fires)
- [ ] Volume dashboard: panels rendering (may show zero data if no runs have fired post-flip — that's expected)
- [ ] Revenue Alerts group: all rules in `OK`
- [ ] No new `alerting` state alerts triggered by the deploy itself

### 4.3 `/metrics` endpoint

```bash
curl -sf https://app.datanika.io/metrics | grep -E "bytes_processed|bytes_quota"
```

- [ ] `datanika_bytes_processed_by_run` histogram registered
- [ ] `datanika_cloud_bytes_processed_total` counter registered
- [ ] `datanika_cloud_bytes_quota_rejected_total` counter registered (value = 0 post-flip, expected)

### 4.4 Dry-run → live transition

- [ ] Run a test pipeline on a non-production org (if one exists) and verify:
  - Run completes if within quota
  - `usage_ledger` row created with `bytes_processed`
  - Cloud billing counter increments

## 5. Watch window (T+15m to T+2h)

Keep a terminal open on:
- Grafana alerting page (volume + revenue groups)
- `docker logs -f datanika-app` (watch for `QuotaExceededError` or unexpected 500s)
- `docker logs -f datanika-celery` (watch for task failures)

### What to watch for

| Signal | Action |
|--------|--------|
| `403 quota exceeded` on a legitimate org run | Expected for Free orgs over 10 GB. Verify the modal shows correctly via browser. |
| `403 quota exceeded` spike (>5 in 10 minutes) | Unexpected — may indicate a misconfigured plan seed. Check `SELECT * FROM plan` for correct `bytes_included` values. |
| `500 Internal Server Error` on pipeline runs | Bug in IR builder or quota hook. Capture stack trace, proceed to §6 rollback. |
| Grafana `volume-tenant-spike` fires | A single org processed >100 GB in 24h or >10× their 7-day mean. Informational — not a rollback trigger unless it's our test org. |
| Revenue alert fires | Unrelated to V2 flip unless the flip somehow corrupted subscription state. Investigate separately. |
| Zero `bytes_processed` rows after 2h | No runs have fired, OR the emission hook isn't wired. Check celery logs. |

- [ ] 2h watch window complete
- [ ] No unexpected 500s
- [ ] No false-positive quota blocks on orgs within their cap
- [ ] Grafana alerts stable

## 6. Emergency rollback

If the flip causes production issues that cannot be quickly diagnosed:

```bash
ssh -i ~/.ssh/id_ed25519 root@185.25.22.188
cd /opt/datanika/datanika

# Restore the backed-up env
cp .env.docker.bak-* .env.docker
# OR manually set:
# vim .env.docker → DATANIKA_BYTES_QUOTA_ENFORCE=false

# Rebuild and restart
set -a && source .env.docker && set +a
docker compose up -d --build app celery
```

**Expected rollback time**: ~45s (build is cached from the flip 2h ago).

**What rollback does NOT undo**:
- `usage_ledger` rows written during the enforcement window stay. They're accurate data.
- Any org that was blocked during the window experienced a real quota rejection. No automated re-run.
- `DATANIKA_DUAL_MODE_UX_ENABLED` stays `true` — the UI surfaces are independent of enforcement.

**What rollback does**:
- All quota checks return to dry-run mode (log-only, no blocking)
- All pipeline runs proceed regardless of usage
- Cloud billing counters still increment (accurate metering continues)

- [ ] Rollback executed (if needed)
- [ ] App + celery healthy post-rollback
- [ ] File a P0 bug with reproduction steps

## 7. Sign-off and next steps

- [ ] All §4 verification items green
- [ ] 2h watch window clean
- [ ] Product lead signs off
- [ ] QA lead signs off
- [ ] Infra confirms monitoring is stable

### Post-flip actions (next 24h)

- [ ] **Growth**: publish V2 cutover blog post (`pricing-v2-math-and-why.md` from held branch)
- [ ] **Growth**: merge held `182-pricing-v2-copy-draft` branch (pricing page + `/why-cheaper/` rewrite + comparison tables)
- [ ] **Growth**: un-draft landing#173 (benchmark manifesto) and merge
- [ ] **Growth**: execute Launch Week (HN + PH + Reddit drop) per LAUNCH_WEEK_2026-04-28.md Option B
- [ ] **Infra**: flip `volume-system-throughput-ceiling` alert from commented-out to active (after 30d baseline)
- [ ] **Product**: close the `DATANIKA_DUAL_MODE_UX_ENABLED` flag — remove the `if settings.x else rx.fragment()` guards and make the V2 surfaces unconditional (cleanup PR)

### Paging escalation matrix

| Severity | Who to page | Channel |
|----------|-------------|---------|
| App down (5xx on `/`) | Infra on-call | Telegram |
| False quota blocks on known-good orgs | Engineering + Product | Telegram |
| Paddle webhook failures post-flip | Engineering (cloud) | Telegram |
| Revenue alert fires | Infra + Engineering | Telegram |
| UI rendering issue (modal not showing) | Product | Telegram |

---

## Related artifacts

- [RUNBOOK_V2_P3_STAGING_SMOKE_TEST.md](RUNBOOK_V2_P3_STAGING_SMOKE_TEST.md) — prerequisite staging runbook
- [RUNBOOK_V2_P1_STAGING_SMOKE_TEST.md](RUNBOOK_V2_P1_STAGING_SMOKE_TEST.md) — P1 predecessor runbook
- [SPEC_DUAL_MODE_UX.md](../specs/SPEC_DUAL_MODE_UX.md) — UX spec with acceptance criteria
- [SPEC_VOLUME_METERING.md](https://github.com/datanika-io/datanika-cloud/blob/dev/docs/specs/SPEC_VOLUME_METERING.md) — metering spec
- [SPEC_GB_THROUGHPUT_METRICS.md](https://github.com/datanika-io/datanika-cloud/blob/dev/docs/specs/SPEC_GB_THROUGHPUT_METRICS.md) — monitoring spec
- PRICING_PIVOT_DECISIONS.md (`plans/PRICING_PIVOT_DECISIONS.md`) — Q6 hard-sunset decision
- LAUNCH_WEEK_2026-04-28.md (`plans/growth/LAUNCH_WEEK_2026-04-28.md`) — coordinated launch plan
- PLAN_HUMAN_LOCKERS.md (`plans/PLAN_HUMAN_LOCKERS.md`) — Paddle meter creation locker
