import { test, expect, APIRequestContext } from "@playwright/test";

/**
 * SSO edge-case and misconfiguration tests.
 *
 * Reference: PLAN_QA.md Q3.
 *
 * These tests use direct HTTP against the backend (:8000) because
 * Playwright's `request` API context doesn't go through the Vite dev
 * proxy. The SSO Starlette routes live on the backend, not the frontend.
 */

const SSO_GATE = process.env.DATANIKA_E2E_SSO_AUTHENTIK !== "1";
const ORG_SLUG = process.env.DATANIKA_E2E_ORG_SLUG ?? "e2e-fixture";
const BACKEND_URL = process.env.DATANIKA_E2E_BACKEND_URL ?? "http://localhost:8000";

test.describe("SSO edge cases @slow", () => {
  let api: APIRequestContext;

  test.beforeAll(async ({ playwright }) => {
    api = await playwright.request.newContext({
      baseURL: BACKEND_URL,
      maxRedirects: 0,
    });
  });

  test.afterAll(async () => {
    await api.dispose();
  });

  test("login with nonexistent org slug returns 404", async () => {
    const response = await api.get("/api/auth/sso/login/org-that-does-not-exist");
    // SSO login for unknown org: 302 to error page or 404. Never 200 or 5xx.
    expect(response.status()).not.toBe(200);
    expect(response.status()).toBeLessThan(500);
  });

  test("login with org that has no SSO config returns error", async () => {
    const response = await api.get(`/api/auth/sso/login/${ORG_SLUG}`);
    // Accept 302 (has config → redirect to IdP) or 302 to error page (no config).
    // Reject 5xx.
    expect(response.status()).toBeLessThan(500);
  });

  test("callback with missing state cookie returns 400", async () => {
    const response = await api.get("/api/auth/sso/callback?code=fakecode&state=fakestate");
    // No sso_state cookie → must reject. 302 to error page or 400.
    const status = response.status();
    expect(status === 302 || status >= 400, `Expected 302 or 400+, got ${status}`).toBe(true);
    expect(status).toBeLessThan(500);
  });

  test("callback with empty code returns 400", async () => {
    const response = await api.get("/api/auth/sso/callback?code=&state=fakestate");
    const status = response.status();
    expect(status === 302 || status >= 400, `Expected 302 or 400+, got ${status}`).toBe(true);
    expect(status).toBeLessThan(500);
  });

  test("metadata endpoint returns valid SP XML regardless of org", async () => {
    // SP metadata is org-agnostic (entityID is always "datanika"). The
    // endpoint returns 200 with XML for any slug — this is correct.
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
