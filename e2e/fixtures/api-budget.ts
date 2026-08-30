import type { APIRequestContext, APIResponse } from "@playwright/test";

/**
 * A request budget for the seeded API keys — core#699.
 *
 * ## Why this exists
 *
 * The `/api/v1/*` surface is rate limited, and since cloud#107 an org with no
 * subscription resolves to the **Free** plan instead of being treated as
 * unlimited. Free is **30 requests/minute** (`plans.rate_limit_rpm`, and the
 * number published at datanika.io). The gating E2E suite issues **36** requests
 * on org A's key, so it is over that allowance by construction.
 *
 * Run 33334823581 is what that looked like: `tenant-jwt-boundary.spec.ts`
 * failed on `POST /api/v1/runs/{id}/cancel` with a **429**, and the assertion
 * that caught it was `expect([400, 403, 404]).toContain(status)` — which reads
 * exactly like a tenant-isolation regression and is nothing of the kind.
 *
 * ## The two things this fixes, in order of importance
 *
 * 1. **A 429 is not a boundary result.** It is a rejection by the rate limiter
 *    *before the tenant check runs*. Widening the accepted-status list to
 *    include it would make a tenant-isolation suite pass on requests that never
 *    reached tenant isolation, and it would keep passing if the boundary itself
 *    broke. So a 429 is raised here as an explicit **harness** failure that
 *    names itself, and can never be mistaken for a verdict about the product.
 *
 * 2. **The suite must not reach the limit at all.** The server's window is a
 *    *fixed* wall-clock minute (`rl:{bucket}:{floor(now/60)}`), not a sliding
 *    one, so whether the suite went red — and which route it went red on —
 *    depended entirely on where the run happened to sit relative to a minute
 *    boundary. Measured against the real limiter: the same 36 requests are
 *    rejected when they fall inside one minute and pass cleanly when a boundary
 *    splits them. A green obtained that way proves nothing. This budget makes
 *    the outcome deterministic by waiting for the window to roll instead of
 *    letting the wall clock decide.
 *
 * ## What this models, and what breaks if that changes
 *
 * The server buckets on `api_key.id` (`api_middleware.py` passes
 * `bucket=f"{api_key.id}"`), while the *limit* is resolved per org. So each
 * seeded key gets its own budget here. **If bucketing ever moves to per-org,
 * this guard will under-count and the run will hit a real 429** — which is
 * caught loudly by (1) above and names itself, rather than surfacing as a
 * mystery failure on a random route. That is the intended failure mode.
 *
 * The budget **self-calibrates**: every response that carries
 * `X-RateLimit-Limit` updates the tracked allowance, so raising the seeded
 * org's plan removes the wait automatically with no change here.
 *
 * ## Who spends the seeded keys
 *
 * | spec | key | requests |
 * |---|---|---|
 * | `tenant-isolation.spec.ts`   | org A            | 10 |
 * | `tenant-jwt-boundary.spec.ts`| org A            | 26 |
 * | `tenant-jwt-boundary.spec.ts`| org B            |  3 |
 * | `tenant-isolation.spec.ts`   | org B            |  1 |
 * | `rbac.spec.ts`               | org A read-only  |  6 (3 reach the limiter) |
 * | `sso-oidc.spec.ts`           | org A            |  1 — **NOT budgeted** |
 *
 * `sso-oidc.spec.ts` imports `test` from `@playwright/test` rather than from
 * `fixtures/auth`, so it cannot see this fixture. It is gated behind
 * `DATANIKA_E2E_SSO_AUTHENTIK=1`, which no CI job sets, so it spends nothing
 * today. **If that gate is ever turned on in `e2e-staging`, switch its import
 * to `../fixtures/auth` and budget its `/api/v1/members` call** — otherwise
 * org A's real spend is 37 against a budget that believes it is 36.
 *
 * Nothing else authenticates with an API key: `smoke-staging` runs in parallel
 * but only touches unauthenticated endpoints, and the seed recreates the keys
 * on every run, so each run starts from an empty Redis counter.
 */

/**
 * The rate-limit subjects the harness spends against — one per seeded API key,
 * because that is what the server buckets on. Named constants rather than bare
 * strings so a typo is a compile error and not a second, silently-unbudgeted
 * subject that reads as "plenty of allowance left".
 */
export const ORG_A_KEY = "org-a-api-key";
export const ORG_B_KEY = "org-b-api-key";
export const ORG_A_READONLY_KEY = "org-a-readonly-api-key";

/** Free-tier allowance. Only the starting guess — the server's own header wins. */
const DEFAULT_RPM = Number(process.env.DATANIKA_E2E_API_RPM ?? "30");

/**
 * Extra pause past the boundary before resuming.
 *
 * The window index is computed from the *server's* clock; ours can differ by a
 * fraction of a second. Resuming a moment early would put the first request of
 * the new window back into the exhausted one.
 */
const BOUNDARY_SKEW_MS = 1_500;

type SubjectState = {
  /** `floor(epochSeconds / 60)` — the same key the server derives. */
  window: number;
  /** Requests issued in `window`. Rejected ones count: the server counts them too. */
  spent: number;
  /** Allowance, seeded from DEFAULT_RPM and replaced by the server's own header. */
  limit: number;
  /** Set once the server has told us the real number, for the log line. */
  calibrated: boolean;
};

function currentWindow(): number {
  return Math.floor(Date.now() / 1000 / 60);
}

export class ApiRateLimitExceeded extends Error {
  constructor(subject: string, detail: string, serverSaid: string) {
    super(
      `HARNESS BUDGET EXCEEDED — this is NOT a tenant-boundary failure.\n` +
        `  ${detail}\n` +
        `  The request was rejected with 429 by the rate limiter, before the ` +
        `handler (and therefore before any tenant check) ran, so it proves ` +
        `nothing about the boundary either way.\n` +
        `  Subject: ${subject}. Server said: ${serverSaid}\n` +
        `  Fix the harness budget or raise the seeded org's allowance — do NOT ` +
        `add 429 to an accepted-status list. See core#699.`,
    );
    this.name = "ApiRateLimitExceeded";
  }
}

export class ApiBudget {
  private readonly subjects = new Map<string, SubjectState>();
  private readonly log: string[] = [];

  private state(subject: string): SubjectState {
    let s = this.subjects.get(subject);
    if (!s) {
      s = { window: currentWindow(), spent: 0, limit: DEFAULT_RPM, calibrated: false };
      this.subjects.set(subject, s);
    }
    return s;
  }

  /** Block until the server's fixed window rolls over, then reset the counter. */
  private async waitForWindow(subject: string, s: SubjectState): Promise<void> {
    const resumeAt = (s.window + 1) * 60_000 + BOUNDARY_SKEW_MS;
    const waitMs = Math.max(0, resumeAt - Date.now());
    this.log.push(
      `[api-budget] ${subject}: spent ${s.spent}/${s.limit} in this minute — ` +
        `waiting ${(waitMs / 1000).toFixed(1)}s for the server's window to roll.`,
    );
    // eslint-disable-next-line no-console
    console.log(this.log[this.log.length - 1]);
    await new Promise((resolve) => setTimeout(resolve, waitMs));
    s.window = currentWindow();
    s.spent = 0;
  }

  /** Reserve one request against `subject`, waiting for the window if needed. */
  private async reserve(subject: string): Promise<void> {
    const s = this.state(subject);
    const now = currentWindow();
    if (now !== s.window) {
      s.window = now;
      s.spent = 0;
    }
    if (s.spent >= s.limit) {
      await this.waitForWindow(subject, s);
    }
    s.spent += 1;
  }

  /** Adopt the server's own allowance whenever it tells us one. */
  private calibrate(subject: string, response: APIResponse): void {
    const raw = response.headers()["x-ratelimit-limit"];
    if (!raw) return;
    const limit = Number(raw);
    if (!Number.isFinite(limit) || limit <= 0) return;
    const s = this.state(subject);
    if (s.limit !== limit) {
      // eslint-disable-next-line no-console
      console.log(`[api-budget] ${subject}: allowance ${s.limit} -> ${limit} (from the server).`);
      s.limit = limit;
    }
    s.calibrated = true;
  }

  /**
   * Issue one budgeted API request.
   *
   * `subject` must name the API key being spent (the server buckets on the key,
   * see the header comment). Throws {@link ApiRateLimitExceeded} on a 429 —
   * never returns one to the caller, so no assertion downstream can silently
   * accept it as a product verdict.
   */
  async fetch(
    request: APIRequestContext,
    subject: string,
    url: string,
    options: Parameters<APIRequestContext["fetch"]>[1] & { label?: string } = {},
  ): Promise<APIResponse> {
    const { label, ...fetchOptions } = options;
    await this.reserve(subject);
    const response = await request.fetch(url, fetchOptions);
    this.calibrate(subject, response);
    if (response.status() === 429) {
      const s = this.state(subject);
      throw new ApiRateLimitExceeded(
        subject,
        `${label ?? `${fetchOptions.method ?? "GET"} ${url}`} — the harness ` +
          `believed it had spent ${s.spent}/${s.limit} requests this minute.`,
        (await response.text()).slice(0, 300),
      );
    }
    return response;
  }

  /** Per-subject accounting, for a spec that wants to assert its own footprint. */
  spentOn(subject: string): number {
    return this.state(subject).spent;
  }
}
