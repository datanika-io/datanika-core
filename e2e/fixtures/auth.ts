import { test as base, expect, type Page } from "@playwright/test";

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
 * Navigate to `path` and wait for Reflex to finish hydrating before returning.
 *
 * Reflex drives all interactivity over the `/_event` WebSocket and only attaches
 * event handlers (e.g. a form's `on_submit`) once the initial hydration/state-
 * sync burst completes. Interacting before that either drops the event (showing
 * "Connection Error") or falls back to a native GET form submit — the root-cause
 * flake behind core#295, where the golden-path signup clicked too early and the
 * browser navigated to `/signup?email=...&password=...` instead.
 *
 * We track WebSocket frame activity and wait for the socket to go quiet (no
 * frames for `quietMs`), which reliably marks "hydration burst done, handlers
 * attached". Falls back to a best-effort return after `maxWaitMs`.
 */
export async function gotoReady(page: Page, path: string): Promise<void> {
  let lastWsActivity = 0;
  page.on("websocket", (ws) => {
    if (!ws.url().includes("/_event")) return;
    const mark = () => {
      lastWsActivity = Date.now();
    };
    ws.on("framereceived", mark);
    ws.on("framesent", mark);
  });

  await page.goto(path);
  await page.waitForLoadState("networkidle").catch(() => {});

  const quietMs = 600;
  const deadline = Date.now() + 12_000;
  while (Date.now() < deadline) {
    if (lastWsActivity > 0 && Date.now() - lastWsActivity >= quietMs) break;
    await page.waitForTimeout(100);
  }
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

export const test = base.extend<Fixtures>({
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

  loggedInPage: async ({ page, testUser }, use) => {
    await gotoReady(page, "/login");
    await page.getByLabel(/email/i).fill(testUser.email);
    await page.getByLabel(/password/i).fill(testUser.password);
    await page.getByRole("button", { name: /log in|sign in/i }).click();
    await expect(page).toHaveURL(/\/dashboard|\/connections|\/$/);
    await use(page);
  },
});

export { expect };
