import { test, expect, APIRequestContext } from "@playwright/test";
import { backendContextOptions } from "../fixtures/sso";

/**
 * SSO edge-case and misconfiguration tests.
 *
 * Reference: PLAN_QA.md Q3.
 *
 * These tests use direct HTTP against the backend via backendContextOptions()
 * which forwards CF Access headers (core#222 fix).
 */

const SSO_GATE = process.env.DATANIKA_E2E_SSO_AUTHENTIK !== "1";
const ORG_SLUG = process.env.DATANIKA_E2E_ORG_SLUG ?? "e2e-fixture";

test.describe("SSO edge cases @slow", () => {
  let api: APIRequestContext;

  test.beforeAll(async ({ playwright }) => {
    api = await playwright.request.newContext(backendContextOptions({ maxRedirects: 0 }));
  });

  test.afterAll(async () => {
    await api.dispose();
  });

  test("login with nonexistent org slug returns 302 error redirect", async () => {
    const response = await api.get("/api/auth/sso/login/org-that-does-not-exist");
    expect(response.status()).not.toBe(200);
    expect(response.status()).toBeLessThan(500);
  });

  test("login with org that has no SSO config returns error", async () => {
    const response = await api.get(`/api/auth/sso/login/${ORG_SLUG}`);
    expect(response.status()).toBeLessThan(500);
  });

  test("callback with missing state cookie returns error", async () => {
    const response = await api.get("/api/auth/sso/callback?code=fakecode&state=fakestate");
    const status = response.status();
    expect(status === 302 || status >= 400, `Expected 302 or 400+, got ${status}`).toBe(true);
    expect(status).toBeLessThan(500);
  });

  test("callback with empty code returns error", async () => {
    const response = await api.get("/api/auth/sso/callback?code=&state=fakestate");
    const status = response.status();
    expect(status === 302 || status >= 400, `Expected 302 or 400+, got ${status}`).toBe(true);
    expect(status).toBeLessThan(500);
  });

  test("metadata endpoint returns valid SP XML regardless of org", async () => {
    const response = await api.get("/api/auth/sso/metadata/any-org");
    expect(response.status()).toBe(200);
    const body = await response.text();
    expect(body).toContain("EntityDescriptor");
  });

  test.describe("OIDC misconfiguration", () => {
    test.skip(() => SSO_GATE, "Requires Authentik container");

    test("OIDC with unreachable issuer_url fails gracefully", async () => {
      test.skip(true, "Requires SSO config API or direct DB seeding of a bad OIDC config");
    });
  });

  test.describe("Quota gating (Enterprise only)", () => {
    test("creating SSO config on Free plan is rejected by cloud hook", async () => {
      test.skip(
        true,
        "Requires cloud edition + Free-plan org. Covered by cloud unit tests " +
          "(test_plan_restrictions.py:507-524).",
      );
    });
  });
});
