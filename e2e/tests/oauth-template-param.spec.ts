import { test, expect } from "../fixtures/auth";

/**
 * Regression for core PR #101: the ?template=<slug> query param must
 * survive the full OAuth redirect chain (landing → /signup → Google/
 * GitHub OAuth → callback → /pipelines/new?template=<slug>).
 *
 * If this breaks, Option C public-template landing pages drop users on
 * an empty pipeline builder after OAuth signup — the entire funnel loses
 * its conversion mechanic.
 *
 * The email-signup subtest is unblocked by core#109 (e2e-seed). The Google
 * and GitHub OAuth subtests stay skipped — staging has OAuth disabled
 * (callbacks aren't registered for staging-app.datanika.io), and the local
 * compose stack has mock providers but the call chain isn't wired here
 * yet. Re-enable both with a mock IdP once SSO scoping lands (tracked as a
 * `[QA]` issue; `plans/qa/PLAN_QA.md` was retired 2026-08-31).
 */
test.describe("?template= preservation through OAuth signup", () => {
  test("email signup preserves template param", async ({ page }) => {
    await page.goto("/signup?template=stripe-to-postgres");
    await page.getByLabel(/email/i).fill(`qa-${Date.now()}@datanika.test`);
    await page.getByLabel(/password/i).fill("QaTemplateOauth-2026");
    await page.getByRole("button", { name: /create account/i }).click();
    await expect(page).toHaveURL(/template=stripe-to-postgres/);
  });

  test.skip("Google OAuth signup preserves template param", async ({ page }) => {
    // Mock the OAuth provider: intercept the /auth/google redirect and
    // short-circuit back to /auth/google/callback?code=fake&state=<state>
    // state should encode ?template=stripe-to-postgres
    await page.goto("/signup?template=stripe-to-postgres");
    // await page.getByRole('button', { name: /google/i }).click();
    // ... assert final URL still has template param
  });

  test.skip("GitHub OAuth signup preserves template param", async ({ page }) => {
    // Same structure as Google, different provider route
  });
});
