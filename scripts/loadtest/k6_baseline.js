// Datanika load-test generator (core#778).
//
// ── THE PUBLISHED FIGURE ──────────────────────────────────────────────────────────────────
// **A floor of ~50 req/s under neighbour load, measured 2026-09-24 (run 10), with no knee
// observed up to 60 instantaneous.** That is the whole claim, and "under neighbour load" is part
// of it: prod, staging, the co-tenants and this generator share one 4 vCPU box, so it is a lower
// bound observed on a busy machine and NOT a capacity figure. Never quote it as "Datanika handles
// N req/s".
//
// 🔴 `>= 60 authed req/s` is RETIRED as a citation (founder, 2026-09-25). It is not *wrong* —
// run 10 found no knee below 60 — it is **unmeasured**, because the ladder that produced it never
// held a rate (core#1560). Unmeasured is the only claim we may publish. The stage defaults below
// still climb past 50 on purpose: retiring the citation does not retire the knee hunt.
//
// ── WHY THIS FILE EXISTS IN THE REPOSITORY ────────────────────────────────────────────────
// Run 9 (2026-09-17) produced the figure that was published for weeks. Its harness lived in
// `.scratch/`, which is swept without warning, so by 2026-09-20 the number was being cited and
// the instrument that produced it was gone. A floor whose instrument cannot be re-run is not a
// measurement, it is a memory. This is the coordinator's rule 9: if a measurement will become a
// floor, the instrument ships in the same PR. ⚠️ Rule 34 is the other half, learned here: the
// instrument shipping is necessary and not sufficient — it must measure the thing the number
// claims, and this one did not.
//
// ── THE MODEL, AND WHY IT IS OPEN ─────────────────────────────────────────────────────────
// `constant-arrival-rate`, NOT a VU loop. A closed-loop generator slows down when the target
// slows down, so it measures the target's pace back to itself and can never find a knee. An
// open model keeps offering the configured rate and records what is dropped — `dropped_iterations`
// is therefore a PRIMARY result here, not an error.
//
// ── THE KEY-COUNT CONSTRAINT, measured 2026-09-20 ─────────────────────────────────────────
// `rate_limit_rpm` resolves per ORG (from the plan row) and buckets per KEY
// (`datanika/services/api_key_service.py`, migration `e5f6a7b8c9d0`). Free is 30 rpm. So:
//
//     req/s_ceiling = keys * rate_limit_rpm / 60
//
// Run 9 used **161 keys** on a free-plan org: 161 * 30 / 60 = 80.5 req/s, comfortably above the
// top rung it requested. (The `/ 60` there is seconds per minute, not the retired floor — they
// are unrelated 60s that sit two lines apart, which is worth knowing before grepping this file.)
// To go higher you need more keys, not a bigger machine:
//
//     50 req/s sustain  -> >= 100 keys        80 req/s -> >= 160        120 req/s -> >= 240
//
// ⚠️ The shortcut of raising the plan's rpm and using ONE key is not equivalent. 161 keys is 161
// Redis buckets; one key is one. It changes where the limiter's three Redis round trips land and
// makes the result non-comparable to Run 9 in the one dimension this issue is about. If you do
// it, it is a DIFFERENT run and must be labelled one.
//
// ── ENDPOINTS ─────────────────────────────────────────────────────────────────────────────
// Recovered from Run 9's own raw sample stream so this run is comparable to it, not re-invented:
// unread-count ~40%, connections ~28%, runs ~20%, notifications ~15%.

import http from 'k6/http';
import exec from 'k6/execution';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { SharedArray } from 'k6/data';

const BASE = __ENV.TARGET_BASE || 'http://127.0.0.1:8100';
// The neighbour. Read-only, low rate, and deliberately a DIFFERENT origin: the founder's
// condition on this test is that production must not be harmed, and the only honest way to
// show that is to sample it throughout rather than to assert it afterwards.
// 🔴 No default. NEIGHBOUR_BASE points at the PRODUCTION backend, whose port alternates with
// the blue/green colour (8000/8010), so any literal here is right half the time and silently
// wrong the other half — and a neighbour sampled on the colour that serves no traffic reports
// a reassuring number that means nothing. run.sh resolves it from the active vhost and passes
// it in; if it is missing, that is a driver bug and this must say so rather than guess.
const NEIGHBOUR = __ENV.NEIGHBOUR_BASE;
if (!NEIGHBOUR) {
  throw new Error(
    'NEIGHBOUR_BASE is unset. It must be the CURRENTLY SERVING production backend, which ' +
      'alternates 8000/8010 per deploy — run this through scripts/loadtest/run.sh, which ' +
      'reads the colour from the active vhost, rather than invoking k6 directly.',
  );
}

// ── STAGES: A RATE IS "HELD" ONLY WHERE TWO CONSECUTIVE SEGMENTS SHARE A TARGET ───────────
// 🔴 core#1560, found by running this harness (run 10, 2026-09-24) and reconciling the achieved
// rate against the stage label. `ramping-arrival-rate` LINEARLY INTERPOLATES from the current
// rate to each stage's target over that stage's duration. A spec whose targets never repeat
// therefore never sustains anything — every achieved rate is the **mean of its ramp**:
//
//     stage `60:120s`, entered at 40  ->  delivered 49.92 req/s  =  (40 + 60) / 2
//
// 🔑 And k6's console prints the stage TARGET (`60.00 iters/s`), so the gap was invisible in
// every artefact the run produced. The number that gets published was never measured.
//
// So the unit of measurement here is a RUNG: a ramp segment followed by a HOLD segment at the
// same target. **Only the hold is a measurement; the ramp is travel.** `run.sh` expands a plain
// "rate:duration" ladder into rungs, and passes an already-expanded spec through untouched. The
// default below is written out already expanded, so this file is honest when read on its own.
//
//     5:120s            rung 1 is flat -- startRate == the first target, so this IS a hold
//     10:30s,10:120s    ramp to 10 over 30s, then HOLD 10 for 120s   <- the measured window
const STAGE_SPEC =
  __ENV.STAGES ||
  '5:120s,10:30s,10:120s,20:30s,20:120s,30:30s,30:120s,40:30s,40:120s,' +
    '60:30s,60:120s,80:30s,80:120s,100:30s,100:120s';

const keys = new SharedArray('keys', function () {
  // One key per line. `run.sh` writes this file from the seeder and deletes it afterwards.
  const raw = open(__ENV.KEYS_FILE || '/keys/loadtest-keys.txt');
  return raw.split('\n').map((s) => s.trim()).filter((s) => s.length > 0);
});

const rateLimited = new Counter('datanika_rate_limited');
const neighbourLatency = new Trend('datanika_neighbour_healthz_ms', true);
const neighbourBad = new Counter('datanika_neighbour_non_200');

function stages() {
  return STAGE_SPEC.split(',').map((part) => {
    const [target, duration] = part.split(':');
    return { target: parseInt(target, 10), duration };
  });
}

function durationSeconds(d) {
  const m = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/.exec(String(d).trim());
  if (!m) {
    throw new Error(
      `unparseable stage duration ${JSON.stringify(d)} in STAGES. Use 30s / 2m / 1h. Guessing ` +
        'one would silently mis-attribute which samples belong to a hold, which is the whole ' +
        'defect core#1560 is about.',
    );
  }
  return parseFloat(m[1]) * { ms: 0.001, s: 1, m: 60, h: 3600 }[m[2]];
}

// The schedule, with every segment classified. A segment is a HOLD iff its target equals the
// rate we were already at when it began — derived from the spec, never from how the spec was
// written down. `startRate` is the first target, so segment 1 is always flat and always a hold.
const SCHEDULE = (function () {
  const out = [];
  let t = 0;
  let prev = null;
  for (const s of stages()) {
    const secs = durationSeconds(s.duration);
    out.push({
      target: s.target,
      phase: prev === null || s.target === prev ? 'hold' : 'ramp',
      from: t,
      to: t + secs,
    });
    t += secs;
    prev = s.target;
  }
  return out;
})();

const LADDER_SECONDS = SCHEDULE.length ? SCHEDULE[SCHEDULE.length - 1].to : 0;

// Held seconds per target — the denominator of every achieved-rate figure this run reports.
const HELD = {};
for (const seg of SCHEDULE) {
  if (seg.phase === 'hold') HELD[seg.target] = (HELD[seg.target] || 0) + (seg.to - seg.from);
}

function segmentAt(elapsedSeconds) {
  for (const seg of SCHEDULE) {
    if (elapsedSeconds >= seg.from && elapsedSeconds < seg.to) return seg;
  }
  return null;
}

// 🔑 THE OTHER HALF OF core#1560: put the ACHIEVED rate of every held rung into the run's own
// artefact. k6 prints a sub-metric in the end-of-test summary when that sub-metric carries a
// threshold — so grading `http_reqs{rung:N}` against the count N implies over its own hold
// makes "did the ladder actually hold this rate" a CHECKED property rather than a stated one.
//
// A red line here means THAT RUNG WAS NOT DELIVERED. That is a finding about the target or the
// fixture, not a broken harness, so it is deliberately **not** `abortOnFail`: the ladder should
// finish and report every rung rather than stop at the first one the box cannot serve.
const HOLD_TOLERANCE = parseFloat(__ENV.HOLD_TOLERANCE || '0.95');
const rungThresholds = {};
for (const target of Object.keys(HELD)) {
  const expected = Math.floor(target * HELD[target] * HOLD_TOLERANCE);
  rungThresholds[`http_reqs{rung:${target}}`] = [`count>=${expected}`];
}

const first = stages()[0];

export const options = {
  discardResponseBodies: true,
  scenarios: {
    api: {
      executor: 'ramping-arrival-rate',
      startRate: first.target,
      timeUnit: '1s',
      // Headroom over the top stage so the executor is never the bottleneck. If these are
      // exhausted k6 reports `dropped_iterations` and the run is invalid as a floor.
      preAllocatedVUs: parseInt(__ENV.PRE_VUS || '200', 10),
      maxVUs: parseInt(__ENV.MAX_VUS || '600', 10),
      stages: stages(),
      exec: 'api',
    },
    neighbour: {
      executor: 'constant-arrival-rate',
      rate: 1,
      timeUnit: '2s',
      // 🔴 DERIVED, never a literal. This was `'16m'`, which happened to equal the old ladder's
      // 8 x 120s exactly — so it was correct by coincidence, and any change to STAGES silently
      // stopped sampling production before the ladder finished. The founder's condition is that
      // production is watched THROUGHOUT; a neighbour that stops early reports a reassuring
      // number about the part of the run that mattered least.
      duration: __ENV.NEIGHBOUR_DURATION || `${Math.ceil(LADDER_SECONDS + 60)}s`,
      preAllocatedVUs: 2,
      maxVUs: 4,
      exec: 'neighbour',
    },
  },
  // 🚨 THE ABORT CRITERIA. Fixed here, before the run, so the pass bar cannot be chosen after
  // seeing the numbers. `abortOnFail` stops the run rather than letting it keep loading a box
  // that is already failing — this runs beside production.
  //
  // ⏱️ `delayAbortEval` on the LATENCY abort (core#778 gap 1, decided 2026-09-22). k6 evaluates
  // a threshold continuously over the cumulative metric from t=0, so in the opening seconds of
  // an open-model ladder the percentile is taken over a handful of samples: at the opening
  // rate r0, after t seconds p95 is the ceil(0.05 * r0 * t)-th slowest request — the 6th of
  // 120 at 5 req/s and 24 s. A few slow requests then decide the whole run, and the ladder can
  // never reach the stages it exists to measure. That follows from the sample size alone, not
  // from any run's numbers.
  //
  // What changes is WHEN the abort may fire — not the bar (p95 < 1 s) and not the population
  // (every `api` sample from t=0, the opening included). It may not fire until the first full
  // stage is in: at 5:120s that is 600 samples, so p95 is the 30th slowest. A target that is
  // genuinely failing — 5% of a full stage over 1 s — still aborts, one stage later.
  // `scripts/loadtest/abort_rehearsal.sh` drives both halves against a synthetic target and
  // must be run after any change here: a sustained-slow target MUST abort, and an opening tail
  // of a few slow requests must NOT decide the run.
  //
  // The FAILURE-rate abort is deliberately left immediate: an error is not sampling noise, and
  // a run whose keys are being refused should stop at once rather than a stage later.
  //
  // `rungThresholds` is merged in first so every held rung's achieved count is graded and
  // printed; the abort criteria below are the ones that can stop the run.
  thresholds: Object.assign(rungThresholds, {
    'http_req_failed{scenario:api}': [{ threshold: 'rate<0.01', abortOnFail: true }],
    'http_req_duration{scenario:api}': [
      { threshold: 'p(95)<1000', abortOnFail: true, delayAbortEval: first.duration },
    ],
    // The neighbour is the founder's condition and is therefore the hardest line here.
    datanika_neighbour_non_200: [{ threshold: 'count<1', abortOnFail: true }],
    datanika_neighbour_healthz_ms: ['p(99)<250'],
  }),
};

function nextKey() {
  // Spread across keys so no single per-key bucket is the limiter. Deterministic per VU+iter
  // rather than random, so a re-run offers the same distribution.
  return keys[(__VU + __ITER) % keys.length];
}

export function api() {
  const key = nextKey();
  // Which rung is this sample part of? Only a HOLD is a measurement, so ramp samples are tagged
  // as travel and cannot be averaged into a rung's achieved rate — which is exactly how the old
  // ladder reported (prev + target) / 2 under the name `target`.
  const seg = segmentAt(exec.instance.currentTestRunDuration / 1000);
  const rung = seg === null ? 'outside' : seg.phase === 'hold' ? String(seg.target) : 'ramp';
  const params = {
    headers: { Authorization: `Bearer ${key}` },
    tags: { scenario: 'api', rung: rung },
  };

  // Weighted by Run 9's observed mix.
  const r = Math.random();
  let url;
  if (r < 0.40) url = `${BASE}/api/v1/notifications/unread-count`;
  else if (r < 0.68) url = `${BASE}/api/v1/connections`;
  else if (r < 0.88) url = `${BASE}/api/v1/runs?limit=50`;
  else url = `${BASE}/api/v1/notifications?limit=20`;

  const res = http.get(url, params);
  // 429 is a CORRECT answer, not a failure of the target — but a run whose throughput is
  // limiter-bound is measuring the limiter, not the app. Counted so that is visible instead
  // of being read as capacity.
  if (res.status === 429) rateLimited.add(1);
  check(res, { 'status is 200 or 429': (x) => x.status === 200 || x.status === 429 });
}

export function neighbour() {
  const res = http.get(`${NEIGHBOUR}/healthz`, { tags: { scenario: 'neighbour' } });
  neighbourLatency.add(res.timings.duration);
  if (res.status !== 200) neighbourBad.add(1);
}
