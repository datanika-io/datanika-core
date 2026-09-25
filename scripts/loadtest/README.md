# Load-test harness (`core#778`)

## 📌 The published figure — quote this sentence, and no other

> **A floor of ~50 req/s under neighbour load, measured 2026-09-24, with no knee observed up to
> 60 instantaneous.**

**Founder decision, 2026-09-25: publish the measured ~50 and stop citing 60.** The `>= 60 req/s`
figure was carried for weeks and is **retired from every citation**.

🔑 **60 is not *wrong* — it is UNMEASURED, and that is a different claim.** Run 10 found no knee
anywhere below 60 instantaneous, so nothing here says the box cannot do 60. What run 10 showed is
that the ladder which produced the 60 figure never *held* any rate, so the number was never
measured in the form it was quoted ([core#1560]). **Unmeasured is the only claim we may publish.**

🚨 **"A floor under neighbour load" is the whole label and it is not a capacity figure.** Production,
staging, the co-tenants **and the generator** share one 4 vCPU box, so this number is a lower bound
observed while the machine was also doing everything else it normally does. **Never publish it as
*"Datanika handles N req/s."*** That is the founder's original label and it still governs.

⚠️ **A third number is in circulation and is not this one:** `docs/slo_targets.md`'s callout cites
**~48 req/s** from Run 5/6 (2026-04-18) as its newest conclusive datum. That was measured on the
**terminated** Hetzner CPX31, not on `pointer.gr`. The ~50 above is the current-host figure.

This directory is the instrument. It exists because the instrument that produced the original
number did not: Run 9 (2026-09-17) lived in `.scratch/`, which is swept without warning, and by
2026-09-20 the number was being cited while nothing could re-run it.

> 🔑 **A floor whose instrument cannot be re-run is not a measurement, it is a memory.**
> Coordinator rule 9: if a measurement will become a floor, the instrument ships in the same PR
> — not the number.

## Run it — two machines, in this order

**1. On the dev machine, gate the invocation.** `preflight.sh` reads the Actions API, the merge
queue and the open-PR list — none of which the box can see — and exits non-zero if anything is
positioned to rebuild staging during the ladder.

```bash
bash scripts/loadtest/preflight.sh --duration-min 25     # exit 0 = go; anything else = do not start
```

**2. Only then, on the production box.** Targets staging. Never run this against production.

```bash
bash scripts/loadtest/run.sh          # defaults: --keys 300, a ladder to 100 req/s, --ramp 30s
```

🔑 **A rate is measured only where it is HELD.** `ramping-arrival-rate` *interpolates*, so a
segment whose target differs from the current rate is travel, not a measurement — its achieved
rate is the mean of the ramp. Run 10's `60:120s` therefore delivered **49.92 = (40+60)/2** while
k6's console printed `60.00 iters/s` beside it ([core#1560]).

`run.sh` now expands each requested rung into **ramp, then hold**, and prints the effective spec:

```
--stages "5:120s,10:120s"   ->   5:120s,10:30s,10:120s
                                 ^^^^^^ flat: startRate == the first target
                                        ^^^^^^^ ramp    ^^^^^^^^ HOLD — the measured window
```

The expansion is idempotent, so a spec you wrote out with holds yourself is passed through
untouched. **Attribute every figure to a hold**: each held rung now carries its own
`http_reqs{rung:N}` threshold, so the summary states the *achieved* count against the count the
requested rate implies — the gap can no longer hide behind the label.

🚨 **`--keys` is a gate, not a suggestion.** The top stage must be strictly below the limiter
ceiling (`keys x rpm / 60`) or the upper rungs measure the rate limiter and report it as
throughput. `run.sh` **refuses** and prints the key count that would satisfy it ([core#1556]).
There is deliberately no override: mint more keys, or lower the top stage.

> 🔑 **Why the gate is on the other machine, and why `run.sh`'s own preflight is not enough.**
> `run.sh` asks *"is an E2E suite running right now"*. A 25-minute ladder needs *"will one
> START during my run"*. Those differ, and they differ **worst** in the gap between a `dev`
> run's unit jobs finishing and its staging jobs starting: in that window the container census
> reads a confident zero and a staging deploy is minutes away.
>
> Measured twice, on two sessions: E2E containers **0**, staging `Up 3 hours`, `load1 0.46` —
> every box-side check clean — while the Actions API showed an in-flight `dev` run whose
> staging jobs did not exist yet. **All four box-side checks would have passed and staging
> would have been rebuilt underneath the run.**
>
> ⚠️ A `PASS` lowers the probability of a collision; it does not remove it. Nothing can see a
> merge that has not happened. Re-read the preflight before the ladder's top stage, and stop
> the run rather than reasoning afterwards about a staging deploy that started mid-ladder.
>
> `bash scripts/loadtest/preflight.sh --self-check` drives the decision against ten synthetic
> populations — a clear board and each gate failing on its own — and is what proves the guard
> **discriminates** rather than merely refuses. Guarded by
> `tests/test_deploy/test_loadtest_preflight.py`, which mutates each gate and requires the
> self-check to go red.

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

**Execution status:** executed end to end — run 10, 2026-09-24. ⚠️ **That run used the PRE-FIX
stage spec**, so it validates the harness's safety and plumbing but not a sustained figure above
~50 req/s; the hold stages below have not yet been exercised against staging.

🟢 **Run 10 (2026-09-24) completed the ladder with this code — the first run that did.** k6 exit 0;
**16,980 requests, `http_req_failed` 0.00%**, `http_req_duration` p95 **33.12 ms**, production
sampled throughout with **`datanika_neighbour_non_200` = 0**. Both validity controls clean:
`dropped_iterations` **0** (17 of 202 VUs used, so the generator never bottlenecked) and
`datanika_rate_limited` **0** with every response a `200` — so the result describes the application
and not the fixture. Structure still guarded by `tests/test_deploy/test_loadtest_harness.py`.
*(The 2026-09-21 first execution found eight defects in the harness itself — core#1492, core#1503 —
and both runs it produced aborted in stage 1 on the latency threshold, which is gap 1 above.)*

🔴 **Run 10 did not validate a 60 req/s floor, and the reason was the stage spec, not the box —
[core#1560].** `ramping-arrival-rate` **interpolates**, and no stage in either invocation of the
day repeated a target, so **that ladder never held any rate.** Every per-stage achieved rate was
the *mean of the ramp*: the `60:120s` stage delivered **49.92 req/s**, i.e. `(40+60)/2`, and the
run touched 60 only at the final instant of the last ramp.

✅ **The spec defect is fixed** (hold stages, per-rung achieved thresholds, and the limiter-ceiling
gate of [core#1556]). ⚠️ **A fix is not a measurement** — it means the *next* run can measure a
held rate, not that one has been measured.

➡️ **And as of 2026-09-25 we are no longer chasing that number.** The founder ruled: publish the
measured ~50, stop citing 60. So the sentence at the top of this file is the published figure, and
a rung above it is a *knee hunt*, not a floor to be validated. The ladder still climbs past 50
deliberately — **stopping the citation is not the same as stopping the probe**, and the knee is
still unlocated.

✅ **What Run 10 establishes, and what may be quoted:**

- a **real ~50 req/s sustain** — 5,991 requests in 120 s at a mean 49.92/s, **zero errors, zero
  429s**, p95 **26.57 ms**, p99 85.21 ms. That closes the *"a real sustain at 50 req/s"* gap.
- **no knee anywhere up to 60 instantaneous**: p95 *improved* 126.75 → 26.57 ms and p99 727 → 85 ms
  as load rose. (Early-stage figures are cold-start over few samples — a statement about warm-up,
  not about load.)
- **production was not harmed**: 0 non-200s across 481 neighbour samples, healthz p99 2.55 ms.

⚠️ **The top stage's NAME and the load it applies are different numbers, and the name is the one
that gets published.** k6's console prints the stage *target* (`60.00 iters/s`) beside the
scenario's completion, which is exactly what made the gap invisible in every artefact run 10
produced. The per-rung `http_reqs{rung:N}` thresholds exist to close that: they put the
**achieved** count for each held rung into the summary, graded against what the requested rate
implies. A red rung line means **that rung was not delivered** — a finding about the target or the
fixture, not a broken harness — so it is deliberately not `abortOnFail`.

**Do not attach Run 9's numbers to this code.** Remaining `core#778` gaps, now framed as a knee
hunt rather than as validating a retired figure: a run on the fixed spec that actually **holds** a
rung above 50, `/meta` at >= 100, production's 4-worker shape, and query cost on real data volume.
⚠️ **None of these is a precondition for the published ~50** — that figure is measured and stands
on run 10 alone.

⚠️ **Operational, learned in Run 10: do not drive the ladder from a foreground SSH session.** The
control channel dropped ~10 minutes in. The remote processes survived and the run finished
correctly — but `say()` pipes through `tee` to stdout, so **every later log line died of SIGPIPE
and `run.log` freezes at `generator start`, missing the drain and cleanup sections, while the run
itself completed.** A truncated log there means a dead console, not a dead run; and the wrapper
reported **exit 0** describing its own last call rather than the ladder. Verify cleanup **by
effect** — keys revoked, keys file gone, image removed — which is what this harness asks for
anyway.
