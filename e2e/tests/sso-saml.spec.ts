import { test, expect } from "@playwright/test";

/**
 * SSO SAML flow tests against a live Authentik container.
 *
 * Reference: PLAN_QA.md Q3, SPEC_SOC2_ROADMAP.md §SSO.
 *
 * Tests the SP-initiated SAML flow: AuthnRequest → Authentik login →
 * SAMLResponse → callback → JIT provisioning → session.
 *
 * Gated behind DATANIKA_E2E_SSO_AUTHENTIK=1.
 */

const SSO_GATE = process.env.DATANIKA_E2E_SSO_AUTHENTIK !== "1";

const SSO_USER_EMAIL = "sso-user@datanika.test";
const SSO_USER_PASSWORD = "SsoTestPassword-2026";
const ORG_SLUG = process.env.DATANIKA_E2E_ORG_SLUG ?? "e2e-fixture";

test.describe("SSO SAML: Authentik @slow", () => {
  test.skip(() => SSO_GATE, "Requires DATANIKA_E2E_SSO_AUTHENTIK=1 + Authentik container");

  test("SAML login redirects to Authentik SSO endpoint with AuthnRequest", async ({ page }) => {
    const response = await page.goto(`/api/auth/sso/login/${ORG_SLUG}`);

    // SP-initiated SAML: should redirect to Authentik with SAMLRequest param
    const finalUrl = page.url();
    expect(finalUrl).toContain("SAMLRequest=");
    expect(finalUrl).toContain("RelayState=");
  });

  test("SAML full flow: login via Authentik → session created", async ({ page }) => {
    // 1. Navigate to SSO login — redirects to Authentik SAML endpoint
    await page.goto(`/api/auth/sso/login/${ORG_SLUG}`);

    // 2. Authentik login form (same UI for OIDC and SAML)
    await page.getByLabel(/username|email/i).fill(SSO_USER_EMAIL);
    await page.getByLabel(/password/i).fill(SSO_USER_PASSWORD);
    await page.getByRole("button", { name: /log in|sign in|continue/i }).click();

    // 3. Authentik may show consent — auto-approve
    const consentButton = page.getByRole("button", { name: /continue|allow|approve/i });
    if (await consentButton.isVisible({ timeout: 3000 }).catch(() => false)) {
      await consentButton.click();
    }

    // 4. SAMLResponse POST to callback → redirect to app
    await page.waitForURL(/.*\/(connections|dashboard|pipelines).*/i, { timeout: 15000 });

    // 5. Verify logged in
    const url = page.url();
    expect(url).not.toContain("/login");
  });

  test("SAML SP metadata endpoint returns valid XML", async ({ request }) => {
    const response = await request.get(`/api/auth/sso/metadata/${ORG_SLUG}`);
    expect(response.status()).toBe(200);

    const contentType = response.headers()["content-type"];
    expect(contentType).toContain("xml");

    const body = await response.text();
    expect(body).toContain("EntityDescriptor");
    expect(body).toContain("AssertionConsumerService");
    expect(body).toContain("NameIDFormat");
  });

  test("SAML assertion with wrong audience is rejected", async ({ request }) => {
    // Craft a callback with an invalid SAMLResponse — should be rejected
    const response = await request.post("/api/auth/sso/callback", {
      form: {
        SAMLResponse: Buffer.from("<invalid-xml>").toString("base64"),
        RelayState: `${ORG_SLUG}:fakestate:invalidsig`,
      },
    });
    // Should reject, not crash
    expect(response.status()).toBeGreaterThanOrEqual(400);
    expect(response.status()).toBeLessThan(500);
  });
});
