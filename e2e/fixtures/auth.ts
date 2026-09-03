import { test as base, expect, type Page } from "@playwright/test";

import { ApiBudget } from "./api-budget";
import { awaitReflexReady, markWire, watchReflexWire } from "./reflex-wire";

/**
 * Test fixtures for Datanika E2E.
 *
 * Credentials are populated by `e2e/global-setup.ts`, which shells
 * `make e2e-seed-ci` before any test runs and writes the fixture shape
 * into `process.env.DATANIKA_E2E_*`. Fixtures here just read the env.
 *
 * Payload contract (pinned in tests/test_scripts/test_e2e_seed.py and
 * documented in datanika-cloud/docs/billing_contract.md):
 *   DATANIKA_E2E_ORG_ID, DATANIKA_E2E_ORG_SLUG,
 *   DATANIKA_E2E_USER_ID, DATANIKA_E2E_USER_EMAIL, DATANIKA_E2E_USER_PASSWORD,
 *   DATANIKA_E2E_CONNECTION_ID, DATANIKA_E2E_CONNECTION_NAME,
 *   DATANIKA_E2E_CONNECTION_TYPE, DATANIKA_E2E_SEEDED_AT
 */

export type TestUser = {
  email: string;
  password: string;
  orgSlug: string;
};

export type TestConnection = {
  id: number;
  name: string;
  type: string;
};

export type Fixtures = {
  testUser: TestUser;
  testConnection: TestConnection;
  loggedInPage: Page;
};

export type WorkerFixtures = {
  /**
   * Shared request budget for the seeded API keys (core#699).
   *
   * Worker-scoped on purpose: the `/api/v1/*` rate limit is a **server-side**
   * resource shared by every spec that uses the same key, and the specs that
   * spend it (`tenant-isolation`, `tenant-jwt-boundary`, `rbac`, `sso-oidc`)
   * live in separate files. A per-file tracker would under-count exactly the
   * way the suite already did, so the tracker has to outlive the file.
   */
  apiBudget: ApiBudget;
};

function mustEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(
      `${name} not set — did global-setup.ts run? Check that make e2e-seed-ci ` +
        "succeeded against the target stack, or set DATANIKA_E2E_SKIP_SEED=1 " +
        "and export the DATANIKA_E2E_* fixture fields manually.",
    );
  }
  return value;
}

/**
 * Navigate to `path` and do not return until Reflex says the page is live — or
 * throw naming what was missing.
 *
 * Reflex drives all interactivity over the `/_event` WebSocket and only attaches
 * event handlers (e.g. a form's `on_submit`) once hydration completes. Interacting
 * before that either drops the event (Reflex's own `processEvent`: "otherwise we
 * throw the event into the void") or falls back to a native GET form submit — the
 * root-cause flake behind core#295, where the golden-path signup clicked too early
 * and the browser navigated to `/signup?email=...&password=...` instead.
 *
 * ⚠️ This function USED TO BE UNABLE TO FAIL, and that is what core#744 turned
 * out to be about. It waited for the socket to go QUIET for 600ms and then
 * returned successfully regardless of what it had observed — so a socket that
 * never opened produced a silent 12-second sleep, and a socket that opened and
 * then DIED satisfied the quiet condition faster than a healthy one. Its success
 * condition was best satisfied by the exact failure it existed to prevent, and no
 * caller could tell the difference.
 *
 * It now waits on three things it reads off the wire — a new `/_event` socket for
 * this navigation, a server frame carrying Reflex's own `is_hydrated_rx_state_`
 * marker, and that socket still being open on return — and raises a named error
 * when any of them is absent. See `fixtures/reflex-wire.ts`.
 */
export async function gotoReady(page: Page, path: string): Promise<void> {
  const wire = watchReflexWire(page);
  const mark = markWire(wire);

  await page.goto(path);
  await page.waitForLoadState("networkidle").catch(() => {});

  await awaitReflexReady(page, mark, `\`${path}\``);
}

/** Regex matching an authenticated landing route (root / dashboard / etc.). */
const APP_ROUTE = /\/(dashboard|connections|onboarding)?$/;

/**
 * Sign up a fresh user and wait until the app navigates to an authenticated
 * route. Retries if the Reflex form falls back to a native GET submit before
 * hydration (core#295) — a native GET to `/signup` does not create a user
 * server-side (signup is handled only over the WebSocket), and each attempt
 * uses a unique email, so retrying is safe. Returns the credentials used.
 */
export async function signUp(
  page: Page,
  opts: { fullName?: string; password?: string } = {},
): Promise<{ email: string; password: string }> {
  const fullName = opts.fullName ?? "QA Golden Path";
  const password = opts.password ?? "QaGoldenPath-2026";
  let lastUrl = "";
  for (let attempt = 1; attempt <= 3; attempt++) {
    const email = `qa-${Date.now()}-${attempt}@datanika.test`;
    await gotoReady(page, "/signup");
    await page.getByLabel(/full name/i).fill(fullName);
    await page.getByLabel(/email/i).fill(email);
    await page.getByLabel(/password/i).fill(password);
    await page.getByRole("button", { name: /sign up|create account/i }).click();
    try {
      await page.waitForURL(APP_ROUTE, { timeout: 10_000 });
      return { email, password };
    } catch {
      lastUrl = page.url();
    }
  }
  throw new Error(`signup did not reach the app after 3 attempts (last url: ${lastUrl})`);
}

export const test = base.extend<Fixtures, WorkerFixtures>({
  apiBudget: [
    async ({}, use) => {
      await use(new ApiBudget());
    },
    { scope: "worker" },
  ],

  testUser: async ({}, use) => {
    await use({
      email: mustEnv("DATANIKA_E2E_USER_EMAIL"),
      password: mustEnv("DATANIKA_E2E_USER_PASSWORD"),
      orgSlug: mustEnv("DATANIKA_E2E_ORG_SLUG"),
    });
  },

  testConnection: async ({}, use) => {
    await use({
      id: Number(mustEnv("DATANIKA_E2E_CONNECTION_ID")),
      name: mustEnv("DATANIKA_E2E_CONNECTION_NAME"),
      type: mustEnv("DATANIKA_E2E_CONNECTION_TYPE"),
    });
  },

  // core#927. This is the harness's least-hardened navigation path, and it was
  // the site of one of the two `template-prefill.spec.ts` flakes: on run
  // 33528992652 it failed at the `toHaveURL` below, ten seconds after the click,
  // and passed on retry -- a GATING spec retried into green.
  //
  // The other two login paths in this harness are both more forgiving, and the
  // asymmetry was accidental rather than reasoned: `signUp` makes three attempts
  // (core#295 -- a Reflex form clicked before hydration falls back to a native
  // GET, which does nothing server-side), and `loginAsViewer` in
  // viewer-role-ui.spec.ts passes an explicit 15 s. This one took Playwright's
  // 5 s `toHaveURL` default for a round trip that is bcrypt + a DB read + a JWT
  // + a websocket-driven redirect on a shared staging box.
  //
  // What is deliberately NOT done here is a retry around the assertion
  // (QA_RULES §12). The budget is raised to match the other two paths and the
  // wait is left able to fail.
  loggedInPage: async ({ page, testUser }, use) => {
    await gotoReady(page, "/login");
    await page.getByLabel(/email/i).fill(testUser.email);
    await page.getByLabel(/password/i).fill(testUser.password);
    await page.getByRole("button", { name: /log in|sign in/i }).click();
    await expect(page).toHaveURL(/\/dashboard|\/connections|\/$/, { timeout: 15_000 });
    await use(page);
  },
});

export { expect };
export { ApiBudget, ApiRateLimitExceeded } from "./api-budget";
export { ORG_A_KEY, ORG_A_READONLY_KEY, ORG_B_KEY } from "./api-budget";
