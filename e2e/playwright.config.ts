import { defineConfig, devices } from "@playwright/test";

const BASE_URL = process.env.DATANIKA_E2E_BASE_URL ?? "http://localhost:3000";

// staging-app.datanika.io is fronted by Cloudflare Access (see
// plans/infra/PLAN_INFRASTRUCTURE.md §P1). When both env vars are set,
// every browser request is authenticated via CF Access service token;
// unset (local dev against localhost:3000) they're harmlessly absent.
const extraHTTPHeaders: Record<string, string> = {};
const cfAccessClientId = process.env.DATANIKA_STAGING_CF_ACCESS_CLIENT_ID;
const cfAccessClientSecret = process.env.DATANIKA_STAGING_CF_ACCESS_CLIENT_SECRET;
if (cfAccessClientId && cfAccessClientSecret) {
  extraHTTPHeaders["CF-Access-Client-Id"] = cfAccessClientId;
  extraHTTPHeaders["CF-Access-Client-Secret"] = cfAccessClientSecret;
}

export default defineConfig({
  testDir: "./tests",
  timeout: 60_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: 1,
  globalSetup: require.resolve("./global-setup"),
  // @slow tests run only when DATANIKA_E2E_SLOW=1.
  //
  // The `e2e-staging` job sets it (core#484). Before 2026-07-22 **no job did**,
  // and this comment claimed a "PRs to `master`" workflow that did not exist —
  // so every @slow spec was dropped from every run and the job reported success
  // having executed 5 of 62 tests. Local runs default to the fast subset.
  //
  // If you are tempted to unset it to speed CI up: that is the bug, not the fix.
  // The job asserts the collected count precisely to stop it regressing.
  grepInvert: process.env.DATANIKA_E2E_SLOW === "1" ? undefined : /@slow/,
  reporter: [
    ["list"],
    ["html", { outputFolder: "playwright-report", open: "never" }],
    // JSON report for core#757's flaky-gating detector.
    //
    // ⚠️ The path is deliberately OUTSIDE `playwright-report/`, and that is not
    // tidiness. The "Assert the @slow specs were actually collected" step in
    // ci.yml runs `npx playwright test --list` three times AFTER the tests, and
    // each one regenerates the report folder from a zero-test run. The report
    // really uploaded for de00365 — the run with a flaky golden-path that
    // reached production — is:
    //
    //   {"files":[],"stats":{"total":0,"flaky":0,"ok":true}}   (duration 61ms)
    //
    // i.e. it asserts success having recorded nothing. A JSON report written
    // into that folder would be destroyed identically and the detector would
    // read an empty file, which is the same silent green one level up.
    //
    // PLAYWRIGHT_JSON_OUTPUT_NAME (Playwright's own env var) overrides this so
    // the gating and informational runs can write to separate files.
    [
      "json",
      {
        outputFile:
          process.env.PLAYWRIGHT_JSON_OUTPUT_NAME ?? "results-gating.json",
      },
    ],
  ],
  use: {
    baseURL: BASE_URL,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
    extraHTTPHeaders,
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
