import { defineConfig, devices } from "@playwright/test";

const BASE_URL = process.env.DATANIKA_E2E_BASE_URL ?? "http://localhost:3000";

export default defineConfig({
  testDir: "./tests",
  timeout: 60_000,
  expect: { timeout: 10_000 },
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: 1,
  globalSetup: require.resolve("./global-setup"),
  // @slow tests run only when explicitly included via --grep.
  // CI workflow for PRs to `master` sets DATANIKA_E2E_SLOW=1 to include them;
  // PRs to `dev` run the fast subset. See plans/qa/PLAN_QA.md §P0 #1.
  grepInvert: process.env.DATANIKA_E2E_SLOW === "1" ? undefined : /@slow/,
  reporter: [
    ["list"],
    ["html", { outputFolder: "playwright-report", open: "never" }],
  ],
  use: {
    baseURL: BASE_URL,
    trace: "retain-on-failure",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
});
