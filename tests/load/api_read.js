// Tracked load profile for the REST read SLO. core#778.
//
// WHY THIS FILE EXISTS AT ALL
// ---------------------------
// `plans/infra/LOAD_TEST_BASELINE_2026-04-21.md` records k6 results whose script was
// never committed, and k6 is not installed on the box. So the only load baseline this
// project has is **not reproducible even in principle** — independently of the fact
// that the machine it ran on was terminated on 2026-07-14.
//
// That makes the profile a deliverable on its own. A number without the script that
// produced it is a claim, not a measurement, and the next person cannot tell whether
// their run differs because the system changed or because they asked a different
// question.
//
// WHAT A NUMBER FROM THIS MEANS — read before quoting one
// ------------------------------------------------------
// Staging and production are THE SAME MACHINE (`s538673`, 4 vCPU, ~24 containers,
// load average ~3, ~2 GiB free, shared with co-tenant services). Load applied here
// is load taken from production.
//
// 🚨 Therefore any figure obtained on the live box is a **FLOOR UNDER CONTENTION**,
// not a capacity ceiling. "We sustained N rps with everything else running" is a
// real and useful answer to "will a launch spike kill us". It is NOT an answer to
// "what can this hardware do". Record it in those words or it will be quoted as the
// second thing.
//
// SAFETY
// ------
// Read-only endpoints only, and unauthenticated ones at that. This profile creates
// nothing, mutates nothing, and needs no credentials — so it cannot leave residue in
// a database shared with production, and a mistake costs latency rather than data.
//
//   k6 run -e BASE=http://127.0.0.1:8100 -e RATE=20 tests/load/api_read.js
//
// RATE defaults low on purpose. Raise it deliberately, watching the box, rather than
// starting at the SLO figure — the SLO figure came from hardware we no longer own.

import http from "k6/http";
import { check } from "k6";
import { Rate, Trend } from "k6/metrics";

const BASE = __ENV.BASE || "http://127.0.0.1:8100";
const RATE = parseInt(__ENV.RATE || "20", 10);
const DURATION = __ENV.DURATION || "60s";

// Endpoints named by the "REST API — read" and "Agent API" rows of docs/slo_targets.md.
// Kept in one array so the coverage test can compare this list against that document.
export const ENDPOINTS = [
  "/api/v1/meta/agent-tiers",
  "/api/v1/agent-guide.md",
  "/llms.txt",
  "/healthz",
];

const failures = new Rate("datanika_failed_reads");
const latency = new Trend("datanika_read_latency_ms", true);

export const options = {
  scenarios: {
    steady_read: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: DURATION,
      // Generous pre-allocation: under contention an under-allocated pool silently
      // lowers the achieved rate, and a profile that quietly delivers less load than
      // it claims reports a better latency than the run deserved.
      preAllocatedVUs: Math.max(10, RATE * 2),
      maxVUs: Math.max(50, RATE * 10),
    },
  },
  thresholds: {
    // From docs/slo_targets.md. Deliberately NOT restated as prose anywhere else —
    // the targets live in that document and the coverage test asserts these match.
    "http_req_duration{kind:read}": ["p(95)<200", "p(99)<500"],
    "http_req_duration{kind:agent}": ["p(95)<150", "p(99)<400"],
    "http_req_duration{kind:health}": ["p(95)<50", "p(99)<150"],
    datanika_failed_reads: ["rate<0.01"],
    // A floor on work actually done. Without this, a run where almost every request
    // failed fast would post excellent latency and pass — the failure mode this
    // project keeps finding, in a load profile's costume.
    http_reqs: [`count>${Math.floor(RATE * 0.5)}`],
  },
};

function kindOf(path) {
  if (path === "/healthz") return "health";
  if (path === "/llms.txt" || path.endsWith("agent-guide.md") || path.includes("agent-tiers")) {
    return "agent";
  }
  return "read";
}

export default function () {
  const path = ENDPOINTS[Math.floor(Math.random() * ENDPOINTS.length)];
  const res = http.get(`${BASE}${path}`, {
    tags: { kind: kindOf(path), endpoint: path },
    timeout: "10s",
  });
  const ok = check(res, {
    "status is 200": (r) => r.status === 200,
    "body is not empty": (r) => r.body && r.body.length > 0,
  });
  failures.add(!ok);
  latency.add(res.timings.duration);
}

export function handleSummary(data) {
  const reqs = data.metrics.http_reqs ? data.metrics.http_reqs.values.count : 0;
  const note =
    reqs === 0
      ? "\n*** NO REQUESTS WERE MADE. This is not a pass: nothing was measured. ***\n"
      : "\nThis figure is a FLOOR UNDER CONTENTION on a box shared with production,\n" +
        "not a capacity ceiling. Quote it with that sentence attached.\n";
  return {
    stdout: `\nrequests: ${reqs}${note}`,
    "load-summary.json": JSON.stringify(data, null, 2),
  };
}
