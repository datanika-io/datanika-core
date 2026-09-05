# V2 P4 Production Flag Flip — Runbook

**Purpose**: Step-by-step procedure for flipping `DATANIKA_BYTES_QUOTA_ENFORCE=true` on production (`app.datanika.io`), activating volume-based quota enforcement for all orgs. This is the hardest single operational event in the V2 rollout — once this flag flips, Free-tier orgs are hard-blocked at 10 GB and Pro/Enterprise orgs start accruing metered overage.

**When to run**: After [RUNBOOK_V2_P3_STAGING_SMOKE_TEST.md](RUNBOOK_V2_P3_STAGING_SMOKE_TEST.md) passes all 9 steps, including the 48h dry-run log sweep.

**Blast radius**: Production (`app.datanika.io`). This is irreversible in the sense that any run blocked by quota enforcement is a user-visible event. The flag itself is reversible within ~45s.

**Owner**: Infra executes the flag flip + deploy. Product owns the verification checklist. QA on standby for regression. Growth on standby for comms.

---

> ✅ **POST-CONDITION ALREADY IN FORCE — re-derive before running any of this.**
> `DATANIKA_BYTES_QUOTA_ENFORCE` was measured **`True` on `datanika-app-b`, `datanika-celery`
> and `datanika-beat`** on **2026-09-01** ([cloud#117]), read with each container's own
> interpreter. `.env.docker`'s mtime is `2026-07-24 11:58:49 +0300`, so the flag has been on
> for six weeks. **Nothing here flipped it and no promotion could have shown it** — the deploy
> *preserves* `.env.docker` rather than shipping it, so the change appears in no diff.
> **This runbook is therefore a `false → true` procedure for a value that is already `true`.**
> Re-measure before acting; do not treat the pre-flight list below as a description of today.
>
> Its P5 sibling (`datanika-cloud/docs/runbooks/RUNBOOK_V2_P5_CUTOVER.md`) carries the same
> banner for `DATANIKA_OVERAGE_CHARGE_ENABLE`, flipped by the same edit at the same timestamp.

## 1. Pre-flight (T-24h)

All items must be green before scheduling the flip window.

### 1.1 Staging sign-off

- [ ] [RUNBOOK_V2_P3_STAGING_SMOKE_TEST.md](RUNBOOK_V2_P3_STAGING_SMOKE_TEST.md) §9 sign-off complete
- [ ] All V2 P3 Engineering tests green in CI (`strict=True` on cloud#32 xfails)
- [ ] Dry-run log sweep on staging: zero unexpected `"bytes quota dry-run"` rejections over 48h
- [ ] `docker exec datanika-app /app/.venv/bin/python -c "from datanika.config import settings; print(settings.datanika_dual_mode_ux_enabled)"` prints `True` on the **serving** colour (`cat /etc/apache2/conf-enabled/datanika-prod-active.conf` names it). A UI render is a proxy for the flag, not a read of it.

### 1.2 Code readiness

- [ ] All V2 PRs **deployed**, not merely promoted — `git log -1` on each `master` gives the intended SHA, and the **serving container** is running it (`docker exec datanika-app cat /app/.deployed-sha` / the running image's build label). A promoted cloud tree reaches production only on the next **core** `master` push, so a cloud promotion with no core promotion behind it has not shipped.
- [ ] `docker exec datanika-celery /app/.venv/bin/python -c "from datanika_cloud.billing.config import cloud_settings as c; print(c.bytes_quota_enforce)"` prints the flag's **current** value on all three of `datanika-app`/`datanika-app-b` (serving), `datanika-celery`, `datanika-beat`. ⚠️ Run a bogus-attribute control beside it (`print(c.no_such_setting)` must raise) — a probe that returns a constant reads like a resolution. **Not** `.env.docker`: a variable present in that file is not a setting the process read ([core#646]).
- [ ] Plan rows read **off the `plans` table on the serving box** — `docker exec datanika-postgres psql -U datanika -d datanika -c "SELECT slug, bytes_included, hard_cap_bytes, overage_bytes_price_cents_per_gb FROM plans ORDER BY id;"` — and compared against the published values: Free 10 GiB hard-capped, Pro 100 GiB @ 50c/GB, Enterprise 1 TiB @ 25c/GB. 🚨 **The repository cannot answer this.** No migration creates the paid rows; they are made out of band, and Infra's rebuild-parity drill measures a from-scratch build disagreeing with production on 26 columns across 4 slugs. Every repo source (`seed_paid_plans.py`, `pricing-tiers.ts`, `SPEC_PRICING_V2`) agreeing is the state in which [cloud#177] shipped a wrong value. ⚠️ The biller uses `1024**3` — these are **GiB**, not GB.
- [ ] Paddle `datanika_bytes_processed` meter exists **in the `production` environment** — read it from the Paddle API with the production key, not from a founder's recollection. ⚠️ `paddle_environment` resolves to `production` on all three containers ([cloud#117]), so a sandbox-only meter would satisfy a hand-check and not exist where the charge is issued.

### 1.3 Monitoring readiness

- [ ] Grafana Volume Alerts group active — read **Grafana's own provisioning API** for both rule *count* and rule *health* (`curl -s 127.0.0.1:3001/api/v1/provisioning/alert-rules`), the way the deploy's own verification step does. 🚨 A rule file on disk is not a rule in effect: `postgres-exporter`/`cadvisor`/`node-exporter` sat in **no** deploy step for six weeks with a correct config beside them ([core#616]), and three rules were green while structurally unable to fire ([core#615], [core#616], [core#622]).
- [ ] Volume dashboard ("Datanika Volume Metrics") returns from the Grafana API on the box (`/api/search?query=Datanika%20Volume%20Metrics`) **and** its panels return series — a bind-mount present in `docker-compose.yml` says nothing about what the running container has open (single-**file** bind mounts pin the inode).
- [ ] Revenue alerts active — same reading, from the provisioning API, asserting `health: ok` on each of the three by name. ⚠️ All rules are `noDataState: OK` filtering expressions, so *healthy ≡ NoData* and "not firing" is not evidence the rule can fire.
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
# SSH to the prod box (pointer.gr, 185.25.22.188 — Hetzner was terminated 2026-07-14)
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

- [ ] `docker exec datanika-celery /app/.venv/bin/python -c "from datanika_cloud.billing.config import cloud_settings as c; print(c.bytes_quota_enforce)"` prints `True` on **every** container that reads it — `datanika-app`/`datanika-app-b`, `datanika-celery`, `datanika-beat`. 🚨 This is the post-flip verification: `grep`ping `.env.docker` (step above) confirms the *edit*, never that any process picked it up. The worker and beat are recreated separately from the blue/green swap, so one of them silently running the old value is the expected failure, not an exotic one.
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

⚠️ **Grafana binds `127.0.0.1` only — `app.datanika.io:3001` is not reachable and never was.**
Measured: `curl -m 8 https://app.datanika.io:3001/` → exit 28 (timeout), `http_code 000` ([core#907]).
Open a tunnel from your own machine first:

```bash
ssh -N -L 3001:127.0.0.1:3001 root@185.25.22.188    # then browse http://localhost:3001
```

- [ ] Volume Alerts group: all rules in `OK` or `Normal` state (no false-fires)
- [ ] Revenue Alerts group: all rules in `OK`
- [ ] No new `alerting` state alerts triggered by the deploy itself

⚠️ **There is no "Volume dashboard" to check.** The only provisioned dashboards are
`Database Performance` and `Datanika Server Overview`. Do not tick a panel check against a dashboard
that does not exist — verify the numbers in §4.3 instead.

### 4.3 `/metrics` — run it ON THE BOX, and assert a sample line

🚨 **`/metrics` is deliberately NOT routed through Apache, and must stay that way.** It carries
`org_id`-labelled per-tenant byte volumes — customer usage data — on an endpoint with no
authentication. Prometheus already scrapes it inside the Docker network (`job="datanika-app"`), so a
public route would add **no** monitoring capability and would publish per-customer volumes to the
internet. `curl https://app.datanika.io/metrics` returns **the SPA** (`200`, `text/html`), so any
check written against the public hostname measures nothing.

🚨 **Assert a sample line, never a metric name.** `prometheus_client` emits `# HELP` and `# TYPE`
for a labelled metric with **zero children**, so `grep -E "bytes_processed|bytes_quota"` returns
non-empty for a counter that has never recorded anything — the check cannot fail. Measured in
production on 2026-09-01: all three of these metrics were present as headers with **0 sample lines**.
Full rule: `docs/ENGINEERING_RULES.md` §5.

```bash
ssh root@185.25.22.188
# Backend port follows the live colour — never hardcode it (core#622).
BE=$(sed -n 's/^Define DATANIKA_BE \([0-9]*\).*/\1/p' /etc/apache2/conf-enabled/datanika-prod-active.conf)
curl -sf "http://127.0.0.1:${BE}/metrics" > m.txt && wc -l < m.txt

# 1. Collector health. UNLABELLED, so it ALWAYS emits a value — this is the only
#    one of these that tells "broken" apart from "no tenants yet".
grep -E '^datanika_cloud_bytes_ledger_scrape_ok [0-9]' m.txt

# 2. Per-tenant bytes: a real sample line, not the HELP/TYPE header.
grep -E '^datanika_cloud_bytes_processed_total\{org_id="[0-9]+"\} [0-9]' m.txt
```

- [ ] `datanika_cloud_bytes_ledger_scrape_ok` emits **`1`**. A `0` means the collector could not read
      `usage_ledger`, in which case every per-org series below is missing for *that* reason rather
      than for lack of traffic — do not read its absence as "no runs yet"
- [ ] `datanika_cloud_bytes_processed_total{org_id="…"}` emits **at least one sample line**, value > 0

⚠️ **`datanika_cloud_bytes_quota_rejected_total` cannot be verified here — do not tick it.** It is
labelled, so a healthy zero-rejection state emits **no sample line at all**, which is byte-identical
to the collector being broken. Verify post-flip rejections from `usage_ledger` in §4.4.

⚠️ **`datanika_bytes_processed_by_run` (core histogram) is not observable on this surface**, and its
absence here is expected rather than a finding. It is incremented in the **Celery worker** while
`/metrics` is served by the **app** process, so the app's registry never sees it — the same shape as
[core#704]'s `celery_tasks_total`. Tracked in [core#907] §2 / [core#895]; verify volume from
`usage_ledger`, which is what the cloud collector reads and therefore cannot drift from the billing
record.

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
| Zero `bytes_processed` rows after 2h | Check `datanika_cloud_bytes_ledger_scrape_ok` FIRST (§4.3). `0` = the collector cannot read `usage_ledger`, so absence proves nothing about traffic. Only if it is `1`: no runs have fired, OR the emission hook isn't wired — then check celery logs. |

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

[core#615]: https://github.com/datanika-io/datanika-core/issues/615
[core#616]: https://github.com/datanika-io/datanika-core/issues/616
[core#622]: https://github.com/datanika-io/datanika-core/issues/622
[core#646]: https://github.com/datanika-io/datanika-core/issues/646
[cloud#117]: https://github.com/datanika-io/datanika-cloud/issues/117
[cloud#177]: https://github.com/datanika-io/datanika-cloud/issues/177
