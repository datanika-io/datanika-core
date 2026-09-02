# SLO Baseline — 2026-09-01

First measurement of `docs/slo_targets.md` against production, ever. The document
shipped **2026-04-14** and until today no instrument read any of it ([core#721]).

Reproduce with `scripts/slo_report.py`; the instrument decisions are in
`docs/slo_instruments.yml`; `tests/test_slo/test_slo_coverage.py` keeps the two in
sync and fails the build if the document gains a commitment nothing measures.

## Topology — every number below is bound to this

| | |
|---|---|
| host | pointer.gr, Athens (`185.25.22.188`), 4 vCPU / 8 GB. **Not the Hetzner box the document still names** |
| serving colour | **blue** (`datanika-app`) — `datanika-prod-active.conf` = 8000/3000 |
| prod branch | `master d1e41060` |
| `GRANIAN_WORKERS` | **4** (staging runs 2; the document's numbers were derived on neither) |
| Prometheus retention | 30 d, and it holds a full 30 d — 172,787 of a possible 172,800 `up` samples |
| Celery | single worker, `-E`, scraped via `celery-exporter` 0.12.2 as `job=celery` |

## Result

```
Commitments in document: 33   PASS 8   FAIL 2   NO_VERDICT 23
```

33, not 26: seven SLIs commit to **both** a p95 and a p99, and each is a separate
promise. **NO_VERDICT is not a pass** — 23 of 33 commitments cannot presently be
violated or met by anything production measures.

### FAIL — 2

| commitment | target | measured | |
|---|---|---|---|
| Health probes p95 | 50 ms | **139.2 ms** | **2.8x over** |
| Health probes p99 | 150 ms | **416.5 ms** | **2.8x over** |

40,301 samples over 7 days from a single-process exporter at a 15 s interval —
the most trustworthy instrument in the file. This SLO has been missed every day
since it was written, and nothing has ever said so.

🚨 **Do not close this by raising the number to 140 ms**, and note that this is
not a case of production being slow. Decomposing the probe settles it.

The obvious alternative explanation was tested first and **refuted**: blackbox
re-resolves DNS on every probe where a real client would cache it, but DNS is
**1.8 %** of the p95.

| phase | p50 | p95 |
|---|---|---|
| resolve | 1.5 ms | 2.5 ms |
| connect | 7.4 ms | 18.1 ms |
| tls | 18.8 ms | 30.9 ms |
| **processing** | 28.6 ms | **86.8 ms** |
| transfer | 0.3 ms | 0.4 ms |
| sum of phases | | 138.4 ms (reconciles with the 139.2 ms total) |

**`connect` + `tls` alone is 49.0 ms at p95 — the entire budget, spent before the
request is written and before the app is reached.** blackbox opens a fresh
connection every 15 s, so it pays full TCP + TLS setup to a Cloudflare edge each
time. **No value of application performance brings this SLI under 50 ms.** The
target is not missed; it is unachievable on the path its own `Measurement` column
names.

The same path at three layers, measured directly on the box:

| measured at | latency |
|---|---|
| app directly, `127.0.0.1:8000/healthz`, 10 samples | **1.9 – 2.6 ms** |
| through Apache + TLS on the box, no Cloudflare | **11.8 – 16.1 ms**, plus one 33.6 ms first sample (cold TLS session cache) |
| through Cloudflare — what blackbox scores | **60.4 ms p50 / 139.2 ms p95** |

The document's derivation — *"`/healthz` in ~5 ms"* — matches the first row (2 ms
today), so 50 ms was a sane ~10x headroom **on the local path**. The
`Measurement` column then named an instrument on a different one. The two halves
of that row have never described the same thing, which is why **editing only the
number would hide the defect rather than fix it — the `Measurement` column has to
change too.** Options are on [core#897].

### PASS — 8

| commitment | target | measured | n |
|---|---|---|---|
| Landing p95 TTFB | 300 ms | 105.9 ms | 40,302 / 7 d |
| Landing p99 TTFB | 800 ms | 246.2 ms | 40,302 / 7 d |
| Celery task failure rate | < 1 % | 0.000 % | 39 tasks / 7 d |
| Landing probe success | ≥ 99.5 % | 100.000 % | 5,755 / 24 h |
| App container CPU | < 70 % | 0.921 % | — |
| Celery worker CPU | < 80 % | 0.222 % | — |
| Postgres CPU | < 60 % | 1.795 % | — |
| Disk free on `/` | > 20 % | 61.502 % | — |

⚠️ Read the CPU and disk rows as *"passes at zero load"*. The saturation targets
say "sustained 5 min" without saying of what; nothing has ever applied sustained
load to this box. They are true and they are close to uninformative.

⚠️ The landing figures use `probe_duration_seconds`, which is **total** probe time
and therefore an over-estimate of TTFB. A pass is sound; a fail would need
re-checking against the phase breakdown before being believed.

### NO_VERDICT — 23

🚨 **Corrected 2026-09-02. This table previously read 14 / 6 / 3 and every one of
the three cells was wrong.** The numbers had been tallied from *registry entries*
— 12 with `status: unmeasured`, 6 carrying `blocked_by` — and then published as a
breakdown of the 33 *commitments*, which is a different population. Nothing in
the report ever said 14, 6 or 3; the figures were arithmetic performed on a count
of the wrong objects, and they read exactly like a measurement.

`scripts/slo_report.py` now classifies its own verdicts and prints the breakdown,
so this table is **copied from output** rather than counted by hand:

```
--- why the 23 NO_VERDICTs have no verdict ---
   13  no instrument exists at all
    4  wired to an instrument measured to be defective
    6  instrument exists, not enough samples yet
```

| why | count | was published as |
|---|---|---|
| no instrument exists at all | **13** | 14 |
| wired to an instrument measured to be **defective** ([core#895]) | **4** | 6 |
| instrument exists, **insufficient samples** | **6** | 3 |

Cross-checked independently by counting the report's own detail strings
(`declared unmeasured` 13 · `instrument known defective` 4 · `only N samples` 6).

**Why the two errors happened, since the mechanism is the reusable part:**

- **13, not 14.** Twelve registry entries are `unmeasured`, but the WebSocket SLI
  is one entry carrying **two** commitments (p95 and p99). Rows are not
  commitments. The document has 26 rows and 33 commitments, and this table is
  about the 33.
- **4, not 6.** Six entries carry `blocked_by`, but `scripts/slo_report.py`
  checks **sample sufficiency first** — so four of the six (REST-API write p95
  and p99, Auth p95 and p99, plus the any-5xx and Paddle-webhook error-rate SLOs)
  never reach the blocked branch at all. They have zero samples. Only REST-API
  read and Agent API have enough traffic to produce a number worth refusing to
  score.

🔑 **The buckets are not interchangeable, which is why this mattered rather than
being a rounding quibble.** "No instrument" is engineering work. "Defective
instrument" is unblocked by fixing one named defect ([core#895]). "Not enough
samples" is a **traffic** problem that no amount of engineering resolves — at 0
paying users it resolves itself on the first real customer and not before. Being
told that six were blocked on core#895 when four are, and that three were waiting
on traffic when six are, sends the work to the wrong place. The waiting-on-traffic
bucket is **twice the size** it was reported as.

⚠️ And two of those six are not really waiting for traffic either. The Auth SLI
(`/api/v1/auth/signup`, `/api/v1/auth/login`) is **structurally unreachable**: the
product's own signup and login run over the Reflex `/_event` WebSocket, and the
ASGI metrics middleware returns early for non-HTTP scopes. No amount of real
customers puts a sample in that bucket. It is a [core#897] document defect wearing
an insufficient-samples costume.

The 13 with no instrument: the Reflex event round-trip (p95 **and** p99 — two
commitments, one missing instrument), all four pipeline-level SLOs, all five
throughput SLOs, Postgres pool occupancy, and Redis memory. Each now carries a
`needs:` in `docs/slo_instruments.yml` naming the specific missing thing, and
`tests/test_slo/test_slo_coverage.py` fails if one is added without it.

**None of the 13 is instrumentable from what production exports today** — checked
against the full metric inventory on 2026-09-02, 1,048 names, not assumed:

| SLO | what it actually needs |
|---|---|
| WebSocket round-trip (×2), signup→first event | a duration observed **inside the Reflex event path**. No exporter can reach it; the round-trip lives entirely in a WebSocket frame |
| REST sustained throughput, both Celery throughput SLOs | **load**, not an instrument. These are capacity claims; scoring them against organic traffic yields ~0 and a confident FAIL meaning *"nobody used the product"* |
| Scheduler dispatch latency | a metric **and** an armed schedule — prod has `schedules_armed=0` ([core#648]) |
| Pipeline trigger→enqueue, enqueue→extract, extract→transform | a **schema change** first. `Run` carries `created_at`, `started_at`, `finished_at` — no enqueue timestamp and no parent-run link, so three of these four intervals are not derivable at any layer |
| Pipeline end-to-end | the cheapest of the four: `Run.created_at → finished_at` already suffices. Needs an exported series and traffic (17 runs, ever) |
| Postgres pool occupancy | the app to export its own pool, or a [core#897] decision to restate against `pg_settings_max_connections` (present, reads 100) |
| Redis memory | **a denominator, which does not exist anywhere.** See below |

🚨 **Redis memory is undefined three independent ways, one of them new.**
(1) No redis exporter — zero of the 1,048 metric names match `redis`.
(2) Prod runs `maxmemory 0`, deliberately, and 60 % of unlimited is not a number.
(3) **The container has no memory limit either**:
`container_spec_memory_limit_bytes{name="datanika-redis"}` = **0**. cadvisor does
export the numerator (`container_memory_usage_bytes{name="datanika-redis"}` =
5.35 MB), so this is not a missing measurement — there is nothing to divide by.
Per [core#897], a commitment that cannot be defined should be **removed** from the
document, not wired to the nearest available number.

**One registry reason was also wrong and is corrected.** The upload-throughput SLO
said *"no run-kind label"*; celery-exporter labels by task **name**, and
`celery_task_runtime{name="datanika.run_upload"}` carries 9 observations over 30
days. The kind dimension exists for that SLO — sustained load is what is missing.
It does not exist for the *pipeline* SLO, and for a stronger reason than a missing
label: `celery_task_received_total` names exactly four tasks in production —
`datanika.run_maintenance` (51), `datanika.billing_tick` (45),
`datanika.run_upload` (9), `datanika.send_quota_warning_email` (1). There is no
`datanika.run_pipeline` series at all.

[core#648]: https://github.com/datanika-io/datanika-core/issues/648

## What the traffic actually is

The reason most latency SLIs have no verdict is not a missing metric. It is that
**production has almost no traffic, and most of what it has is hostile.** Over 30
days, `http_requests_total`:

| | |
|---|---|
| total requests (nominal — see the caveat below) | 1,836 |
| `status="200"` | **423** |
| `status="404"` | 663 |
| `status="401"` | 289 |
| distinct `path` label values ever seen | **298** |
| of those, seen only as 404 | **290** |

The 290 are vulnerability scanners: `/api/.env/wp-includes/wso112233`,
`/api/vendor/phpunit/phpunit/src/Util/PHP/eval-stdin.php`,
`/api/config/.env.sendgrid.backup`, and 287 more. Filed as [core#896] — the
`path` label is unbounded and driven by unauthenticated internet input; those
paths account for 3,520 histogram series, 31 % of the TSDB.

The SLI queries filter to `status=~"2.."` **because of this**. Unfiltered, the
quantiles are dominated by fast 404s and *flatter* every number.

Of the named SLI endpoints: `/api/v1/connections` and `/api/v1/pipelines` have
received no authenticated GET at all, `/api/v1/auth/*` has received nothing, and
no Paddle webhook has ever been delivered successfully.

## The instrument defect, measured

Six reads of the same counter on the serving container over **fresh TCP
connections**, with no other traffic arriving and `/metrics` excluded from its own
metering:

```
read 1: sum(http_requests_total)=3   distinct_series=5
read 2: sum(http_requests_total)=7   distinct_series=5
read 3: sum(http_requests_total)=6   distinct_series=5
read 4: sum(http_requests_total)=7   distinct_series=5
read 5: sum(http_requests_total)=7   distinct_series=5
read 6: sum(http_requests_total)=6   distinct_series=3
```

`prometheus_client`'s default registry is process-local and `GRANIAN_WORKERS=4`,
with `PROMETHEUS_MULTIPROC_DIR` unset. The discriminating control, over the same
30 days:

| counter | `resets()` / 30 d | processes |
|---|---|---|
| `http_requests_total` | **121** | 4 |
| `celery_task_received_total` | **0** | 1 |

121 counter decreases on a process that never restarted. Every windowed figure
from these metrics is inflated by phantom resets; every instantaneous one is
roughly one worker's share. Six commitments are marked `blocked_by` for this
reason — their numbers are printed and **deliberately not scored**, because a
broken meter emitting confident greens is worse than no meter. [core#895].

## Revision policy — one trigger has already fired

The document says to revise when *"prod hardware changes (we are on a single 8 GB
Hetzner box today)"*. The Hetzner box was terminated **2026-07-14** and production
moved to pointer.gr on **2026-07-17** — six and a half weeks before this baseline.
The trigger fired and nothing revised, because nothing was watching. That is the
same defect as the targets themselves, one level up.

🚨 **When these numbers are next revised, revise them against a measurement, not
against this table.** A target quietly moved to match observed behaviour is the
same defect as a guard that passes because it looks at nothing.

[core#721]: https://github.com/datanika-io/datanika-core/issues/721
[core#895]: https://github.com/datanika-io/datanika-core/issues/895
[core#896]: https://github.com/datanika-io/datanika-core/issues/896
[core#897]: https://github.com/datanika-io/datanika-core/issues/897
