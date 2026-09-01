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

| why | count |
|---|---|
| no instrument exists at all | 14 |
| wired to an instrument measured to be **defective** ([core#895]) | 6 |
| instrument exists, **insufficient samples** | 3 |

The 14 with no instrument: the Reflex event round-trip (p95 + p99), all four
pipeline-level SLOs, all four throughput SLOs plus signup-to-first-event,
Postgres pool occupancy, and Redis memory. Reasons per SLO are in
`docs/slo_instruments.yml` — each was checked against production, not assumed.

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
