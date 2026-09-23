/**
 * The API budget, driven against a model of the server's own limiter (core#1296).
 *
 *   node --experimental-strip-types --test e2e/fixtures/api-budget.test.ts
 *
 * CI and the pre-push hook run it through `tests/test_deploy/test_api_budget_model.py`. It
 * lives beside the fixture, outside `testDir`, so Playwright never collects it and it is in
 * neither E2E tier: it runs no browser and touches no stack.
 *
 * ## What it pins
 *
 * core#1296 is an alert that has fired three times. Two of those were the same gating probe,
 * `POST /api/v1/runs/{id}/cancel`, failing with `ApiRateLimitExceeded` at **30/30** and passing
 * on Playwright's retry. The harness said, correctly, that no tenant check had run. Why it had
 * reached 30/30 with the server already over the limit is the part this file is about.
 *
 * The reset lagged. `reserve()` rolls its window `BOUNDARY_SKEW_MS` AFTER our clock's minute
 * (core#1209, so it cannot reset early) and set `spent = 0`. A request sent inside that lag
 * was therefore counted against the OLD window and then forgotten, while the server counted
 * it in the NEW one. With exactly one such request, the harness spends its 30 while the
 * server sees 31. Both logs fit that timeline, on the runner's clock:
 *
 *   2026-09-11  the run's first org-A test started 19:31:00.01, ended 19:31:01.61
 *   2026-09-15  the run's first org-A test started 17:07:01.48, ended 17:07:02.09
 *
 * That test's first act is the budgeted request. Both started less than 1.5 s after a minute
 * boundary, the second with 20 ms to spare. Both runs then failed on the 31st org-A request,
 * which is 10 in `tenant-isolation` plus the 21st route of `tenant-jwt-boundary`. The odds fit
 * too: a boundary lands in that lag about 1.5 s in 60, or 2.5% of runs.
 *
 * ## The model, and what it deliberately leaves out
 *
 * `datanika/services/rate_limit_service.py::check_window`, reduced to what the harness meets:
 * a fixed window keyed on `int(time.time()) // 60`, every request counted (a rejected one
 * too), `count <= limit` allowed, `X-RateLimit-*` on every response, and the advisory
 * `mark_refused` shed after a rejection, which refuses without counting until the window
 * resets. **The per-second burst limit is left out.** At the measured cadence of about 155 ms
 * per request the suite never approaches 10 per second, and nothing in doubt here involves it.
 *
 * The server counts a request at our clock plus `skewMs`. That stands for clock skew AND for
 * the request's travel time to the origin. The first request of a file opens a new
 * connection, and on 2026-09-15 that test took 460 ms longer than its siblings.
 * `BOUNDARY_SKEW_MS` is a claim that the sum stays within 1.5 s either way, and every sweep
 * below runs inside that claim. The controls at the bottom show the sweep can find a
 * rejection at all. Without them, "zero rejections" would look the same from a model that
 * cannot reject.
 */
import { test } from "node:test";
import assert from "node:assert/strict";

import { ApiBudget, ApiRateLimitExceeded, ORG_A_KEY } from "./api-budget.ts";

const LIMIT = 30;
/** Measured: the tenant-jwt-boundary probes ran about 155 ms apart on both flaking runs. */
const LATENCY_MS = 155;
/** 30 would fit a window exactly; the suite needs 31 on org A's key before it can wait. */
const BURST = LIMIT + 1;
/** A minute boundary, as the server sees it. Any will do; this one is the 2026-09-11 flake. */
const BOUNDARY_MS = Date.UTC(2026, 8, 11, 19, 31, 0);
/** Inside `BOUNDARY_SKEW_MS` (1.5 s) in both directions, and at zero. */
const SKEWS_WITHIN_THE_BOUND = [-1400, -700, 0, 700, 1400];

type FakeResponse = {
  status(): number;
  headers(): Record<string, string>;
  text(): Promise<string>;
};

function respond(status: number, headers: Record<string, string>, body: string): FakeResponse {
  return {
    status: () => status,
    headers: () => headers,
    text: async () => body,
  };
}

/** `check_window` + `mark_refused`, on a clock `skewMs` away from ours. */
class FixedWindowServer {
  readonly skewMs: number;
  readonly counts = new Map<number, number>();
  /** Requests the authoritative window REJECTED — what a flake is made of. */
  rejected = 0;
  shedUntilS = 0;

  constructor(skewMs: number) {
    this.skewMs = skewMs;
  }

  handle(): FakeResponse {
    const nowS = Math.floor((Date.now() + this.skewMs) / 1000);
    if (this.shedUntilS > nowS) {
      // preauth_check: refused from Redis alone, NOT counted, no X-RateLimit-* headers.
      return respond(429, { "retry-after": String(this.shedUntilS - nowS) }, '{"error":"shed"}');
    }
    const window = Math.floor(nowS / 60);
    const count = (this.counts.get(window) ?? 0) + 1;
    this.counts.set(window, count);
    const resetAt = (window + 1) * 60;
    const headers: Record<string, string> = {
      "x-ratelimit-limit": String(LIMIT),
      "x-ratelimit-remaining": String(Math.max(0, LIMIT - count)),
      "x-ratelimit-reset": String(resetAt),
    };
    if (count > LIMIT) {
      this.rejected += 1;
      const retryAfter = Math.max(resetAt - nowS, 1);
      this.shedUntilS = nowS + retryAfter;
      headers["retry-after"] = String(retryAfter);
      return respond(429, headers, `{"error":"Rate limit exceeded (${LIMIT} requests/minute)."}`);
    }
    // A cross-tenant probe's answer. The status is irrelevant to the budget; the count is not.
    return respond(404, headers, '{"error":"not found"}');
  }
}

type Clock = { now(): number; advance(ms: number): void };

/**
 * Run `fn` on a virtual clock: `Date.now()` reads it and `setTimeout` advances it, so the
 * budget's one-minute waits cost nothing and happen in a deterministic order. Restored after,
 * whatever `fn` does.
 */
async function onVirtualTime<T>(startMs: number, fn: (clock: Clock) => Promise<T>): Promise<T> {
  const realNow = Date.now;
  const realSetTimeout = globalThis.setTimeout;
  const realLog = console.log;
  let t = startMs;
  Date.now = () => t;
  globalThis.setTimeout = ((cb: () => void, ms?: number) => {
    t += Math.max(0, Number(ms) || 0);
    queueMicrotask(cb);
    return 0;
  }) as unknown as typeof setTimeout;
  console.log = () => {};
  try {
    return await fn({ now: () => t, advance: (ms) => (t += ms) });
  } finally {
    Date.now = realNow;
    globalThis.setTimeout = realSetTimeout;
    console.log = realLog;
  }
}

/** A request context whose answer comes from `server`, `LATENCY_MS` after it is asked. */
function contextFor(server: FixedWindowServer, clock: Clock) {
  return {
    fetch: async () => {
      const response = server.handle();
      clock.advance(LATENCY_MS);
      return response;
    },
  } as unknown as Parameters<ApiBudget["fetch"]>[0];
}

type BurstResult = { server: FixedWindowServer; harness429: number; elapsedMs: number };

/** `requests` sequential budgeted requests on one key, the first sent at `firstAtMs`. */
async function burst(skewMs: number, firstAtMs: number, requests = BURST): Promise<BurstResult> {
  return onVirtualTime(firstAtMs, async (clock) => {
    const server = new FixedWindowServer(skewMs);
    const budget = new ApiBudget();
    const request = contextFor(server, clock);
    let harness429 = 0;
    for (let i = 0; i < requests; i++) {
      try {
        await budget.fetch(request, ORG_A_KEY, "/api/v1/runs/999999/cancel", { method: "POST" });
      } catch (error) {
        if (!(error instanceof ApiRateLimitExceeded)) throw error;
        harness429 += 1;
      }
    }
    return { server, harness429, elapsedMs: clock.now() - firstAtMs };
  });
}

/** The same shape with NO budget at all, for the controls. */
async function unbudgeted(skewMs: number, firstAtMs: number, requests: number): Promise<number> {
  return onVirtualTime(firstAtMs, async (clock) => {
    const server = new FixedWindowServer(skewMs);
    const request = contextFor(server, clock);
    for (let i = 0; i < requests; i++) await request.fetch("/api/v1/x", {});
    return server.rejected;
  });
}

/** Every 250 ms of one minute, for one skew: the offsets whose burst the server rejected. */
async function sweep(skewMs: number): Promise<string[]> {
  const bad: string[] = [];
  for (let offset = 0; offset < 60_000; offset += 250) {
    const r = await burst(skewMs, BOUNDARY_MS + offset);
    if (r.server.rejected > 0 || r.harness429 > 0) {
      bad.push(`skew ${skewMs} ms, first request +${offset} ms: server rejected ${r.server.rejected}`);
    }
  }
  return bad;
}

test("replays the 2026-09-11 flake: first request 1.45 s after a boundary, 31 on one key", async () => {
  const r = await burst(0, BOUNDARY_MS + 1_450);
  assert.equal(
    r.server.rejected,
    0,
    `the server rejected ${r.server.rejected} request(s) the budget sent — the budget forgot the ` +
      "one it sent inside its own reset lag, so it spent its 30 while the server counted 31 " +
      "(core#1296). The tenant check never ran on the rejected probe.",
  );
  assert.equal(r.harness429, 0, "the budget raised ApiRateLimitExceeded on a burst it had budgeted");
});

test("no burst start in the minute, at any skew within BOUNDARY_SKEW_MS, is ever rejected", async () => {
  const bad: string[] = [];
  for (const skew of SKEWS_WITHIN_THE_BOUND) bad.push(...(await sweep(skew)));
  assert.deepEqual(
    bad.slice(0, 12),
    [],
    `${bad.length} of ${SKEWS_WITHIN_THE_BOUND.length * 240} burst starts were rejected by the ` +
      "server. The first are listed. A fixed-window limiter decides by where a burst lands, " +
      "so a budget that is right for one start and wrong for another is a coin flip with extra steps.",
  );
});

test("the budget waits at most once for a 31-request burst, and never longer than a window", async () => {
  // Over-waiting is the obvious way to "fix" rejections, and it is a regression too: every
  // extra minute is a minute the gating suite spends holding a promotion.
  for (const skew of SKEWS_WITHIN_THE_BOUND) {
    for (let offset = 0; offset < 60_000; offset += 250) {
      const r = await burst(skew, BOUNDARY_MS + offset);
      const ceiling = BURST * LATENCY_MS + 60_000 + 2 * 1_500 + LATENCY_MS;
      assert.ok(
        r.elapsedMs <= ceiling,
        `skew ${skew} ms, start +${offset} ms: the burst took ${r.elapsedMs} ms, more than one window`,
      );
    }
  }
});

test("control: the server model rejects an unbudgeted 31st request in one window", async () => {
  // Without this, the sweeps above could pass against a model that never rejects anything.
  assert.equal(await unbudgeted(0, BOUNDARY_MS + 5_000, BURST), 1);
});

test("control: the server model does not over-reject — 30 unbudgeted requests in one window pass", async () => {
  // The other direction. A model that rejects too eagerly would make a correct budget look broken.
  assert.equal(await unbudgeted(0, BOUNDARY_MS + 5_000, LIMIT), 0);
});

test("control: the sweep finds rejections when nothing budgets the traffic", async () => {
  // The sweep's own discrimination, independent of how good the budget is: an unbudgeted
  // burst straddling nothing must be caught at every start where all 31 share a window.
  let caught = 0;
  for (let offset = 0; offset < 60_000; offset += 250) {
    if ((await unbudgeted(0, BOUNDARY_MS + offset, BURST)) > 0) caught += 1;
  }
  assert.ok(caught > 200, `the sweep caught only ${caught} of 240 unbudgeted bursts`);
});
