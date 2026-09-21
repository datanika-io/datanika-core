// Datanika load-test generator (core#778).
//
// ── WHY THIS FILE EXISTS IN THE REPOSITORY ────────────────────────────────────────────────
// Run 9 (2026-09-17) established the published floor of **>= 60 authed req/s** on the current
// hardware. Its harness lived in `.scratch/`, which is swept without warning, so by 2026-09-20
// the number was being cited and the instrument that produced it was gone. A floor whose
// instrument cannot be re-run is not a measurement, it is a memory. This is the coordinator's
// rule 9: if a measurement will become a floor, the instrument ships in the same PR.
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
// Run 9 used **161 keys** on a free-plan org: 161 * 30 / 60 = 80.5 req/s, comfortably above its
// 60 req/s top stage. To go higher you need more keys, not a bigger machine:
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

// Stages: "rate:duration,rate:duration,...". The default reproduces Run 9 exactly and then
// continues past its top stage, which is gap 1 on the issue (Run 9 found no knee because 60
// was where it stopped, not where it broke).
const STAGE_SPEC = __ENV.STAGES || '5:120s,10:120s,20:120s,30:120s,40:120s,60:120s,80:120s,100:120s';

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
      duration: __ENV.NEIGHBOUR_DURATION || '16m',
      preAllocatedVUs: 2,
      maxVUs: 4,
      exec: 'neighbour',
    },
  },
  // 🚨 THE ABORT CRITERIA. Fixed here, before the run, so the pass bar cannot be chosen after
  // seeing the numbers. `abortOnFail` stops the run rather than letting it keep loading a box
  // that is already failing — this runs beside production.
  thresholds: {
    'http_req_failed{scenario:api}': [{ threshold: 'rate<0.01', abortOnFail: true }],
    'http_req_duration{scenario:api}': [{ threshold: 'p(95)<1000', abortOnFail: true }],
    // The neighbour is the founder's condition and is therefore the hardest line here.
    datanika_neighbour_non_200: [{ threshold: 'count<1', abortOnFail: true }],
    datanika_neighbour_healthz_ms: ['p(99)<250'],
  },
};

function nextKey() {
  // Spread across keys so no single per-key bucket is the limiter. Deterministic per VU+iter
  // rather than random, so a re-run offers the same distribution.
  return keys[(__VU + __ITER) % keys.length];
}

export function api() {
  const key = nextKey();
  const params = { headers: { Authorization: `Bearer ${key}` }, tags: { scenario: 'api' } };

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
