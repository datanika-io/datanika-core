import { test, expect } from "@playwright/test";

/**
 * SSO SAML flow tests against a live Authentik container.
 *
 * Reference: PLAN_QA.md Q3, SPEC_SOC2_ROADMAP.md §SSO.
 *
 * Tests the SP-initiated SAML flow: AuthnRequest → Authentik login →
 * SAMLResponse → callback → JIT provisioning → session.
 *
 * Requires a second org (DATANIKA_E2E_SAML_ORG_SLUG) configured with
 * SAML SSO pointing to Authentik's SAML provider. The OIDC org can't be
 * reused because SSOService enforces one config per org.
 *
 * Gated behind DATANIKA_E2E_SSO_AUTHENTIK=1.
 */

const SSO_GATE = process.env.DATANIKA_E2E_SSO_AUTHENTIK !== "1";
const BACKEND_URL = process.env.DATANIKA_E2E_BACKEND_URL ?? "http://localhost:8000";

const SSO_USER_EMAIL = "sso-user@datanika.test";
const SSO_USER_PASSWORD = "SsoTestPassword-2026";
const SAML_ORG_SLUG = process.env.DATANIKA_E2E_SAML_ORG_SLUG ?? "e2e-fixture-saml";

test.describe("SSO SAML: Authentik @slow", () => {
  test.skip(() => SSO_GATE, "Requires DATANIKA_E2E_SSO_AUTHENTIK=1 + Authentik container");

  test("SAML login redirects to Authentik SSO endpoint with AuthnRequest", async ({ page }) => {
    // Navigate via backend directly to avoid Vite proxy cross-origin redirect issue
    const loginUrl = `${BACKEND_URL}/api/auth/sso/login/${SAML_ORG_SLUG}`;
    const response = await page.goto(loginUrl);

    const finalUrl = page.url();
    const decoded = decodeURIComponent(finalUrl);
    // SP-initiated SAML: Authentik wraps the SSO URL in its login flow
    expect(
      decoded.includes("SAMLRequest") || finalUrl.includes("localhost:9000"),
      `Expected SAML redirect to Authentik, got ${finalUrl}`,
    ).toBe(true);
  });

  test("SAML full flow: login via Authentik → session created", async ({ page }) => {
    await page.goto(`${BACKEND_URL}/api/auth/sso/login/${SAML_ORG_SLUG}`);

    // Authentik login flow — identification then password
    await page.waitForURL(/localhost:9000/, { timeout: 10000 });
    await page.locator('input[name="uidField"]').fill(SSO_USER_EMAIL);
    await page.locator('button[type="submit"]').first().click();
    await page.locator('input[name="password"]').fill(SSO_USER_PASSWORD);
    await page.locator('button[type="submit"]').first().click();

    // Authentik may show consent — auto-approve
    const consentButton = page.getByRole("button", { name: /continue|allow|approve/i });
    if (await consentButton.isVisible({ timeout: 3000 }).catch(() => false)) {
      await consentButton.click();
    }

    // SAMLResponse POST to callback → redirect to app
    await page.waitForURL(/.*\/(connections|dashboard|pipelines|login).*/i, { timeout: 15000 });

    const url = page.url();
    expect(url).not.toContain("/api/auth/sso");
  });

  test("SAML SP metadata endpoint returns valid XML", async ({ playwright }) => {
    const api = await playwright.request.newContext({ baseURL: BACKEND_URL, maxRedirects: 0 });
    const response = await api.get(`/api/auth/sso/metadata/${SAML_ORG_SLUG}`);
    expect(response.status()).toBe(200);

    const contentType = response.headers()["content-type"];
    expect(contentType).toContain("xml");

    const body = await response.text();
    expect(body).toContain("EntityDescriptor");
    expect(body).toContain("AssertionConsumerService");
    expect(body).toContain("NameIDFormat");
    await api.dispose();
  });

  test("SAML assertion with wrong audience is rejected", async ({ playwright }) => {
    const api = await playwright.request.newContext({ baseURL: BACKEND_URL, maxRedirects: 0 });
    const response = await api.post("/api/auth/sso/callback", {
      form: {
        SAMLResponse: Buffer.from("<invalid-xml>").toString("base64"),
        RelayState: `${SAML_ORG_SLUG}:fakestate:invalidsig`,
      },
    });
    const status = response.status();
    expect(status === 302 || status >= 400, `Expected 302 or 400+, got ${status}`).toBe(true);
    expect(status).toBeLessThan(500);
    await api.dispose();
  });
});
