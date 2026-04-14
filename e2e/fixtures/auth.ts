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
    await page.goto("/login");
    await page.getByLabel(/email/i).fill(testUser.email);
    await page.getByLabel(/password/i).fill(testUser.password);
    await page.getByRole("button", { name: /log in|sign in/i }).click();
    await expect(page).toHaveURL(/\/dashboard|\/connections|\/$/);
    await use(page);
  },
});

export { expect };
