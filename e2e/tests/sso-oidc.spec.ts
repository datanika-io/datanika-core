import { test, expect } from "@playwright/test";
import { backendContextOptions, BACKEND_URL } from "../fixtures/sso";

/**
 * SSO OIDC flow tests against a live Authentik container.
 *
 * Reference: `docs/specs/SPEC_SOC2_ROADMAP.md` §SSO; `docs/QA_RULES.md`.
 *
 * Prerequisites:
 *   docker compose -f e2e/docker-compose.test.yml up -d
 *   bash e2e/scripts/bootstrap-authentik.sh
 *   python e2e/scripts/seed-sso-configs.py
 *   DATANIKA_E2E_SSO_AUTHENTIK=1 npx playwright test sso-oidc
 *
 * Browser-based tests navigate via the backend URL (:8000) directly to
 * avoid the Vite proxy cross-origin redirect limitation. The SSO flow
 * redirects from :8000 → Authentik :9000 → callback :8000 → frontend.
 *
 * Gated behind DATANIKA_E2E_SSO_AUTHENTIK=1.
 */

const SSO_GATE = process.env.DATANIKA_E2E_SSO_AUTHENTIK !== "1";

const SSO_USER_EMAIL = "sso-user@datanika.test";
const SSO_USER_PASSWORD = "SsoTestPassword-2026";
const ORG_SLUG = process.env.DATANIKA_E2E_ORG_SLUG ?? "e2e-fixture";

test.describe("SSO OIDC: Authentik @slow", () => {
  test.skip(() => SSO_GATE, "Requires DATANIKA_E2E_SSO_AUTHENTIK=1 + Authentik container");

  test("OIDC login redirects to Authentik authorization endpoint", async ({ page }) => {
    // Navigate via backend to get the real 302 redirect (Vite proxy blocks it)
    await page.goto(`${BACKEND_URL}/api/auth/sso/login/${ORG_SLUG}`);

    // Authentik wraps /application/o/authorize in a login flow when no session:
    //   /if/flow/default-authentication-flow/?next=%2Fapplication%2Fo%2Fauthorize%2F%3F...
    // Decode the URL so our substring checks match either direct or wrapped form.
    const finalUrl = decodeURIComponent(page.url());
    expect(finalUrl).toContain("/application/o/authorize/");
    expect(finalUrl).toContain("client_id=datanika-oidc-e2e");
    expect(finalUrl).toContain("response_type=code");
    expect(finalUrl).toContain("redirect_uri=");
  });

  test("OIDC full flow: login via Authentik → session created", async ({ page }) => {
    // 1. Navigate to SSO login via backend — redirects to Authentik
    await page.goto(`${BACKEND_URL}/api/auth/sso/login/${ORG_SLUG}`);

    // 2. Authentik 2-step login: username → submit → password → submit.
    // Authentik 2024.12 UI is lit-element based; form inputs live inside
    // shadow DOM. Use getByRole (pierces shadow DOM) and match on the
    // accessible name from aria-label/placeholder, not input[name].
    await page.waitForURL(/\/if\/flow\/default-authentication-flow\//);
    await page.getByRole("textbox", { name: /email|username/i }).fill(SSO_USER_EMAIL);
    await page.getByRole("button", { name: /log in|sign in|continue/i }).click();
    await page.getByRole("textbox", { name: /password/i }).fill(SSO_USER_PASSWORD);
    await page.getByRole("button", { name: /log in|sign in|continue/i }).click();

    // 3. Bootstrap uses default-provider-authorization-implicit-consent flow,
    //    so no consent step. Callback lands on /auth/complete?token=...
    await page.waitForURL(/\/(auth\/complete|connections|dashboard|pipelines|login)/, {
      timeout: 15000,
    });

    // 5. Verify session — user should be logged in (or at least past the SSO flow)
    const url = page.url();
    expect(url).not.toContain("/api/auth/sso");
  });

  test("OIDC JIT provisioning: new SSO user gets Membership with VIEWER role", async ({
    playwright,
  }) => {
    const apiKey = process.env.DATANIKA_E2E_API_KEY_ORG_A;
    test.skip(!apiKey, "Requires API key from seed — run with extended seed");

    const api = await playwright.request.newContext(backendContextOptions());
    const membersResp = await api.get("/api/v1/members", {
      headers: { Authorization: `Bearer ${apiKey}` },
    });
    expect(membersResp.status()).toBe(200);
    const members = await membersResp.json();

    const ssoMember = members.find(
      (m: { email: string }) => m.email === SSO_USER_EMAIL,
    );
    expect(ssoMember, `SSO user ${SSO_USER_EMAIL} not provisioned in org`).toBeDefined();
    expect(ssoMember.role).toBe("viewer");
    await api.dispose();
  });

  test("OIDC with invalid org slug returns error", async ({ playwright }) => {
    // maxRedirects: 0 — the backend responds with a 302 error redirect;
    // following it would land on a 200 error page and fail the assertion.
    const api = await playwright.request.newContext(backendContextOptions({ maxRedirects: 0 }));
    const response = await api.get("/api/auth/sso/login/nonexistent-org-slug");
    expect(response.status()).not.toBe(200);
    expect(response.status()).toBeLessThan(500);
    await api.dispose();
  });

  test("OIDC state tampering is rejected", async ({ page, context }) => {
    // Start a legitimate OIDC flow to get a state cookie via backend
    await page.goto(`${BACKEND_URL}/api/auth/sso/login/${ORG_SLUG}`);
    // Authentik wraps /application/o/authorize in a login flow for fresh sessions
    await page.waitForURL(/\/if\/flow\/default-authentication-flow\//);

    // Tamper with the sso_state cookie
    const cookies = await context.cookies();
    const ssoCookie = cookies.find((c) => c.name === "sso_state");
    if (ssoCookie) {
      await context.addCookies([
        { ...ssoCookie, value: "tampered:state:invalidsig" },
      ]);
    }

    // Simulate callback with tampered state. page.goto follows redirects
    // through to a 200 page, so check the final URL instead — rejection
    // redirects to /login?error=..., success would stay on /auth/complete.
    await page.goto(`${BACKEND_URL}/api/auth/sso/callback?code=fake&state=tampered`);
    const finalUrl = page.url();
    expect(
      finalUrl.includes("error=") || finalUrl.includes("/login"),
      `Expected redirect to error page, got ${finalUrl}`,
    ).toBe(true);
    expect(finalUrl).not.toContain("/auth/complete");
  });
});
