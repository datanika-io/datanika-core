# Load-test harness (`core#778`)

**The published floor is `>= 60 authed req/s` on the current hardware.** This directory is the
instrument that produced it. It exists because the instrument that produced it the *first* time
did not: Run 9 (2026-09-17) lived in `.scratch/`, which is swept without warning, and by
2026-09-20 the number was being cited while nothing could re-run it.

> 🔑 **A floor whose instrument cannot be re-run is not a measurement, it is a memory.**
> Coordinator rule 9: if a measurement will become a floor, the instrument ships in the same PR
> — not the number.

## Run it

```bash
# ON THE PRODUCTION BOX. Targets staging. Never run this against production.
bash scripts/loadtest/run.sh --keys 161 --stages "5:120s,10:120s,20:120s,30:120s,40:120s,60:120s"
```

Results land in a fresh `mktemp -d` (override with `--out`): `summary.json`, `raw.csv`,
`k6.out`, `run.log`.

## The founder's label, which governs how any result may be written up

> **A floor under neighbour load, not a capacity figure.**

Prod and staging share one 4 vCPU box with a co-tenant, and the load generator runs on it too.
Every number this produces is therefore a lower bound obtained while the machine was doing
other things. **It must never be published as "Datanika handles N req/s".**

## What decides how high you can go: keys, not hardware

`rate_limit_rpm` resolves per **org** (from the plan row) and buckets per **key**. Free is
30 rpm. So:

```
req/s_ceiling = keys * rate_limit_rpm / 60
```

| target | keys needed at 30 rpm |
|---|---|
| 50 req/s sustained (`docs/slo_targets.md`) | >= 100 |
| 60 req/s (Run 9's top stage) | >= 120 — it used **161** |
| 80 req/s | >= 160 |
| 100 req/s | >= 200 |
| 120 req/s | >= 240 |

⚠️ **The shortcut of raising the plan's rpm and using one key is not equivalent.** 161 keys is
161 Redis buckets; one key is one. It moves where the limiter's three Redis round trips land
and makes the result non-comparable to Run 9 in the one dimension this issue is about. If you
do it, it is a **different run** and must be labelled one.

## Abort criteria — fixed before the run, in `k6_baseline.js`

They live in the k6 `thresholds` block, not in prose and not in a reviewer's head, so the pass
bar cannot be chosen after seeing the numbers:

| threshold | why |
|---|---|
| `http_req_failed < 1%` (abort) | a run past this is measuring failure, not throughput |
| `http_req_duration p(95) < 1s` (abort, **not before the first stage ends**) | the knee. The bar and the population are unchanged; only *when* the abort may fire is (`delayAbortEval`, core#778 gap 1) — see below |
| `datanika_neighbour_non_200 < 1` (abort) | **the founder's condition.** Production must not be harmed |
| `datanika_neighbour_healthz_ms p(99) < 250` | the neighbour's latency, not just its status code |

A non-zero k6 exit means a threshold aborted the run. **That is the abort criteria working**,
not a broken harness.

### Why the latency abort waits for the first stage — and how that is proven, not asserted

k6 evaluates a threshold over the **cumulative** metric, continuously, from t=0. In the opening
seconds of an open-model ladder that is a percentile over a handful of samples: at the opening
rate r0, after t seconds p95 is the ⌈0.05 · r0 · t⌉-th slowest request — the 6th of 120 at
5 req/s and 24 s. So a few slow requests decided the whole run, and on 2026-09-21 the ladder
aborted in stage 1 and never reached the stages it exists to measure. That follows from the
sample size alone; it needed no run's numbers to see.

The latency abort therefore may not fire until the first stage is complete (`delayAbortEval:
first.duration` — derived from `STAGES`, never a literal). At the default `5:120s` that is 600
samples, so p95 is the 30th slowest. A target that is genuinely failing still aborts, one stage
later. The **failure-rate** abort stays immediate: an error is not sampling noise.

A delay that has never been seen doing anything is not evidence, so **run the rehearsal after
any change to the thresholds.** It drives the real `k6_baseline.js` against a synthetic target
on a private docker network and requires three different answers:

```bash
bash scripts/loadtest/abort_rehearsal.sh      # anywhere with docker; ~2 minutes
```

| case | target | must |
|---|---|---|
| A | every request 1.5 s | **abort** (k6 exit 99), and not before the first stage ends |
| B | an opening tail of 3 slow requests | **not** abort — the ladder runs to completion |
| C | B, with the delay removed | **abort early** — the pre-fix behaviour, i.e. gap 1 |

C is what makes B mean anything: without it, "B did not abort" is also what a threshold that can
never fire would print. First run, 2026-09-22 on Docker Desktop: A exit 99 after 26 s, B exit 0
after 44 s, C exit 99 after 5 s — **all six verdicts PASS**.

## What the driver refuses to do, and why each refusal exists

1. **Targets staging, never production.** April's runs went at production and left its database
   unusable for the better part of an hour — `max_connections` pegged, 99 connections idle in
   `wait_event=ClientRead`. `seed_loadtest_org.py` additionally refuses to seed anywhere whose
   `database_url`/`app_env` does not positively say `staging`.
2. **Refuses while `e2e-staging` could be running.** That job fires on every push to `dev`;
   added load would produce a gating red indistinguishable from a real regression.
3. **Refuses while a build is running and load is already high** (`core#1476`) — a staging image
   build starves Grafana's SQLite, and a run on top of one measures contention, not the app.
4. **Cleanup is verified by effect.** Keys revoked, remaining-active asserted **0** against the
   minted total by re-reading, keys file asserted gone, generator image removed.
5. **Records the drain.** Run 8's failure mode was connections that stayed pegged *after* the
   run; `pg_stat_activity` is sampled for two minutes past the finish. A run that looks clean
   and leaves the database saturated has not been measured, only survived.

## Endpoints

Recovered from Run 9's own raw sample stream so a re-run is comparable to it rather than
re-invented: `notifications/unread-count` ~40%, `connections` ~28%, `runs?limit=50` ~20%,
`notifications?limit=20` ~15%. Staging's backend is `127.0.0.1:8100`.

## Prerequisite, one-time and deliberate

The org and its admin are **not** created by this script. `seed_loadtest_org.py` refuses if they
are missing, and names what to create: an organisation `loadtest-core778` with an **ADMIN**
member `loadtest-core778@invalid.local`. A load-test script that mints organisations as a side
effect is how a test fixture ends up in a production-shaped database.

## Status of this harness — read before quoting a number from it

🔴 **Executed end to end on 2026-09-21, and NOT yet executed to a result.** Its structure is
guarded by `tests/test_deploy/test_loadtest_harness.py`. The first execution did what a first
run should: it found **eight defects in the harness itself** (core#1492, core#1503), and both runs
it produced aborted in stage 1 at 5 req/s on the latency threshold — gap 1 above. Since
2026-09-22 that abort waits for the first full stage, and the rehearsal shows it still aborts a
failing target. **Until a run completes the ladder with this code, no number from it validates
the ≥ 60 req/s floor.**

**Do not attach Run 9's numbers to this code.** The five gaps on `core#778` — the knee above 60,
a real sustain at 50 req/s, `/meta` at >= 100, production's 4-worker shape, and query cost on
real data volume — remain open, and the first credible run against this harness is what starts
closing them.
