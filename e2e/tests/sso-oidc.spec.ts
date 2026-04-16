import { test, expect } from "@playwright/test";

/**
 * SSO OIDC flow tests against a live Authentik container.
 *
 * Reference: PLAN_QA.md Q3, SPEC_SOC2_ROADMAP.md §SSO.
 *
 * Prerequisites:
 *   docker compose -f e2e/docker-compose.test.yml up -d
 *   bash e2e/scripts/bootstrap-authentik.sh
 *   DATANIKA_E2E_SSO_AUTHENTIK=1 npx playwright test sso-oidc
 *
 * These tests exercise the real Authentik↔Datanika OIDC flow end-to-end:
 * discovery, authorization redirect, Authentik login, code exchange,
 * userinfo fetch, JIT provisioning, session creation.
 *
 * Gated behind DATANIKA_E2E_SSO_AUTHENTIK=1 since they require the
 * Authentik container running. Skipped in regular CI.
 */

const SSO_GATE = process.env.DATANIKA_E2E_SSO_AUTHENTIK !== "1";

// Fixture values from bootstrap-authentik.sh → .sso-fixture.json
const SSO_USER_EMAIL = "sso-user@datanika.test";
const SSO_USER_PASSWORD = "SsoTestPassword-2026";
const ORG_SLUG = process.env.DATANIKA_E2E_ORG_SLUG ?? "e2e-fixture";

test.describe("SSO OIDC: Authentik @slow", () => {
  test.skip(() => SSO_GATE, "Requires DATANIKA_E2E_SSO_AUTHENTIK=1 + Authentik container");

  test("OIDC login redirects to Authentik authorization endpoint", async ({ page }) => {
    const ssoLoginUrl = `/api/auth/sso/login/${ORG_SLUG}`;
    const response = await page.goto(ssoLoginUrl);

    // The SSO login endpoint should redirect (302) to Authentik's authorize URL
    const finalUrl = page.url();
    expect(finalUrl).toContain("/application/o/authorize/");
    expect(finalUrl).toContain("client_id=datanika-oidc-e2e");
    expect(finalUrl).toContain("response_type=code");
    expect(finalUrl).toContain("redirect_uri=");
  });

  test("OIDC full flow: login via Authentik → session created", async ({ page }) => {
    // 1. Navigate to SSO login — redirects to Authentik
    await page.goto(`/api/auth/sso/login/${ORG_SLUG}`);

    // 2. Authentik login form
    await page.waitForURL(/.*\/application\/o\/authorize\/.*/);
    await page.getByLabel(/username|email/i).fill(SSO_USER_EMAIL);
    await page.getByLabel(/password/i).fill(SSO_USER_PASSWORD);
    await page.getByRole("button", { name: /log in|sign in|continue/i }).click();

    // 3. Authentik may show a consent screen — auto-approve if present
    const consentButton = page.getByRole("button", { name: /continue|allow|approve/i });
    if (await consentButton.isVisible({ timeout: 3000 }).catch(() => false)) {
      await consentButton.click();
    }

    // 4. Callback redirects back to Datanika with tokens
    await page.waitForURL(/.*\/(connections|dashboard|pipelines).*/i, { timeout: 15000 });

    // 5. Verify session — user should be logged in
    const url = page.url();
    expect(url).not.toContain("/login");
    expect(url).not.toContain("/signup");
  });

  test("OIDC JIT provisioning: new SSO user gets Membership with VIEWER role", async ({
    request,
  }) => {
    // After the OIDC flow, the user should exist in the org with VIEWER role.
    // This test uses the API to verify the provisioning side-effect.
    const apiKey = process.env.DATANIKA_E2E_API_KEY_ORG_A;
    test.skip(!apiKey, "Requires API key from seed — run with extended seed");

    const membersResp = await request.get("/api/v1/members", {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    expect(membersResp.status()).toBe(200);
    const members = await membersResp.json();

    const ssoMember = members.find(
      (m: { email: string }) => m.email === SSO_USER_EMAIL,
    );
    expect(ssoMember, `SSO user ${SSO_USER_EMAIL} not provisioned in org`).toBeDefined();
    expect(ssoMember.role).toBe("viewer");
  });

  test("OIDC with invalid org slug returns error", async ({ page }) => {
    const response = await page.goto("/api/auth/sso/login/nonexistent-org-slug");
    // Should get a 404 or error page, not a crash
    expect(response?.status()).toBeGreaterThanOrEqual(400);
    expect(response?.status()).toBeLessThan(500);
  });

  test("OIDC state tampering is rejected", async ({ page, context }) => {
    // Start a legitimate OIDC flow to get a state cookie
    await page.goto(`/api/auth/sso/login/${ORG_SLUG}`);
    await page.waitForURL(/.*\/application\/o\/authorize\/.*/);

    // Tamper with the sso_state cookie
    const cookies = await context.cookies();
    const ssoCookie = cookies.find((c) => c.name === "sso_state");
    if (ssoCookie) {
      await context.addCookies([
        { ...ssoCookie, value: "tampered:state:invalidsig" },
      ]);
    }

    // Simulate callback with tampered state
    const callbackResp = await page.goto(
      "/api/auth/sso/callback?code=fake&state=tampered",
    );
    // Should reject with 400/403, never 200
    expect(callbackResp?.status()).toBeGreaterThanOrEqual(400);
  });
});
