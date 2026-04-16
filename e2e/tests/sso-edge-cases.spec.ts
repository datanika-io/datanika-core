import { test, expect } from "@playwright/test";

/**
 * SSO edge-case and misconfiguration tests.
 *
 * Reference: PLAN_QA.md Q3.
 *
 * These tests exercise error paths and boundary conditions in the SSO
 * flow. Some require the Authentik container; others test purely against
 * Datanika's SSO endpoints with invalid inputs.
 *
 * Split from sso-oidc/sso-saml to keep the happy-path specs clean.
 */

const SSO_GATE = process.env.DATANIKA_E2E_SSO_AUTHENTIK !== "1";
const ORG_SLUG = process.env.DATANIKA_E2E_ORG_SLUG ?? "e2e-fixture";

test.describe("SSO edge cases @slow", () => {
  test("login with nonexistent org slug returns 404", async ({ request }) => {
    const response = await request.get("/api/auth/sso/login/org-that-does-not-exist");
    expect(response.status()).toBe(404);
  });

  test("login with org that has no SSO config returns error", async ({ request }) => {
    // The seed org (e2e-fixture) may or may not have SSO configured.
    // If it doesn't, this should return 404 or a descriptive error, not 5xx.
    const response = await request.get(`/api/auth/sso/login/${ORG_SLUG}`);
    // Accept 302 (has config) or 404 (no config) — reject 5xx
    expect(response.status()).toBeLessThan(500);
  });

  test("callback with missing state cookie returns 400", async ({ request }) => {
    // No sso_state cookie → the callback should reject
    const response = await request.get("/api/auth/sso/callback?code=fakecode&state=fakestate");
    expect(response.status()).toBeGreaterThanOrEqual(400);
    expect(response.status()).toBeLessThan(500);
  });

  test("callback with empty code returns 400", async ({ request }) => {
    const response = await request.get("/api/auth/sso/callback?code=&state=fakestate");
    expect(response.status()).toBeGreaterThanOrEqual(400);
    expect(response.status()).toBeLessThan(500);
  });

  test("metadata for nonexistent org returns 404", async ({ request }) => {
    const response = await request.get("/api/auth/sso/metadata/org-that-does-not-exist");
    expect(response.status()).toBe(404);
  });

  test.describe("OIDC misconfiguration", () => {
    test.skip(() => SSO_GATE, "Requires Authentik container");

    test("OIDC with unreachable issuer_url fails gracefully", async ({ request }) => {
      // This would require setting up an SSO config pointing to a dead URL.
      // The test verifies that the login endpoint returns a user-facing error,
      // not a 500 with a stack trace.
      // Placeholder — requires API or direct DB access to create a misconfigured SSO entry.
      test.skip(true, "Requires SSO config API or direct DB seeding of a bad OIDC config");
    });
  });

  test.describe("Quota gating (Enterprise only)", () => {
    test("creating SSO config on Free plan is rejected by cloud hook", async ({ request }) => {
      // This tests the cloud plugin's sso_config.before_create hook.
      // Requires DATANIKA_EDITION=cloud and a Free-plan org.
      // The hook raises QuotaExceededError("SSO requires an Enterprise plan").
      test.skip(
        true,
        "Requires cloud edition + Free-plan org. Covered by cloud unit tests " +
          "(test_plan_restrictions.py:507-524). E2E coverage deferred until " +
          "SSO admin UI ships.",
      );
    });
  });
});
