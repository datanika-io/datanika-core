import { test, expect } from "@playwright/test";
import { backendContextOptions, BACKEND_URL } from "../fixtures/sso";

/**
 * SSO SAML flow tests against a live Authentik container.
 *
 * Reference: `docs/specs/SPEC_SOC2_ROADMAP.md` §SSO; `docs/QA_RULES.md`.
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

const SSO_USER_EMAIL = "sso-user@datanika.test";
const SSO_USER_PASSWORD = "SsoTestPassword-2026";
const SAML_ORG_SLUG = process.env.DATANIKA_E2E_SAML_ORG_SLUG ?? "e2e-fixture-saml";

/** SP entity id bootstrap-authentik.sh configures. A matching audience is accepted. */
const SP_AUDIENCE = "datanika";
/** Not the SP. Authentik will sign an assertion that carries this audience. */
const WRONG_AUDIENCE = "https://wrong-audience.example/not-this-sp";
const AUTHENTIK_URL = process.env.DATANIKA_E2E_AUTHENTIK_URL ?? "http://e2e-authentik-server:9000";
/** The token `e2e/scripts/bootstrap-authentik.sh` inserts. Not a production credential. */
const AUTHENTIK_TOKEN = "e2e-bootstrap-token";

async function authentikSamlProvider(): Promise<{ pk: number; audience: string }> {
  const response = await fetch(`${AUTHENTIK_URL}/api/v3/providers/saml/?page_size=100`, {
    headers: { Authorization: `Bearer ${AUTHENTIK_TOKEN}` },
  });
  if (!response.ok) {
    throw new Error(`Authentik provider list returned ${response.status}`);
  }
  const body = (await response.json()) as {
    results: { pk: number; name: string; audience: string }[];
  };
  const provider = body.results.find((item) => item.name === "datanika-saml-e2e");
  if (!provider) {
    throw new Error("Authentik has no datanika-saml-e2e SAML provider");
  }
  return { pk: provider.pk, audience: provider.audience };
}

async function setSamlAudience(pk: number, audience: string): Promise<void> {
  const response = await fetch(`${AUTHENTIK_URL}/api/v3/providers/saml/${pk}/`, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${AUTHENTIK_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ audience }),
  });
  if (!response.ok) {
    throw new Error(`Authentik audience patch returned ${response.status}`);
  }
}

test.describe("SSO SAML: Authentik @slow", () => {
  test.skip(() => SSO_GATE, "Requires DATANIKA_E2E_SSO_AUTHENTIK=1 + Authentik container");

  test("SAML login redirects to Authentik SSO endpoint with AuthnRequest", async ({ page }) => {
    // Navigate via backend directly to avoid Vite proxy cross-origin redirect issue
    const loginUrl = `${BACKEND_URL}/api/auth/sso/login/${SAML_ORG_SLUG}`;
    const response = await page.goto(loginUrl);

    // Decode — Authentik may wrap /application/saml/.../sso in a login flow
    // where the SAMLRequest param ends up URL-encoded inside ?next=
    const finalUrl = decodeURIComponent(page.url());
    // SP-initiated SAML: should redirect to Authentik with SAMLRequest param
    expect(
      finalUrl.includes("SAMLRequest=") || (response?.status() ?? 0) === 302,
      `Expected SAML redirect, got ${finalUrl} (status ${response?.status()})`,
    ).toBe(true);
  });

  test("SAML full flow: login via Authentik → session created", async ({ page }) => {
    await page.goto(`${BACKEND_URL}/api/auth/sso/login/${SAML_ORG_SLUG}`);

    // Fresh session: Authentik wraps SAML SSO in the login flow first.
    // 2-step: username → submit → password → submit (Authentik 2024.12).
    // Form inputs live inside lit-element shadow DOM — use getByRole which
    // pierces shadow DOM.
    await page.waitForURL(/\/if\/flow\/default-authentication-flow\//);
    await page.getByRole("textbox", { name: /email|username/i }).fill(SSO_USER_EMAIL);
    await page.getByRole("button", { name: /log in|sign in|continue/i }).click();
    await page.getByRole("textbox", { name: /password/i }).fill(SSO_USER_PASSWORD);
    await page.getByRole("button", { name: /log in|sign in|continue/i }).click();

    // Implicit consent flow: no consent screen.
    // SAMLResponse POST to callback → /auth/complete?token=... or app route
    await page.waitForURL(/\/(auth\/complete|connections|dashboard|pipelines|login)/, {
      timeout: 15000,
    });

    const url = page.url();
    expect(url).not.toContain("/api/auth/sso");
  });

  test("SAML SP metadata endpoint returns valid XML", async ({ playwright }) => {
    const api = await playwright.request.newContext(backendContextOptions());
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

  test("SAML assertion with wrong audience is rejected", async ({ page, playwright }) => {
    // Authentik signs the assertion. Editing the captured response would break
    // that signature, and a redirect from a callback that never saw sso_state
    // is the invalid-state bail, not the audience check.
    //
    // Run 42753941004: Audience was WRONG_AUDIENCE, the POST's only cookie was
    // CF_Authorization, and the callback answered 302
    // /login?auth_error=sso_invalid_state. That path returns before _saml_parse,
    // so the AuthnRequest id is still stored. Resubmit the same signed bytes
    // with the cookie the login set.
    const provider = await authentikSamlProvider();
    expect(
      provider.audience,
      "IdP audience is not the SP entity id, so this assertion would not be the wrong one",
    ).toBe(SP_AUDIENCE);

    let postedXml = "";
    let samlResponse = "";
    let callbackStatus = 0;
    let callbackLocation = "";
    let callbackBody = "";
    const posted = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error("IdP never posted a SAMLResponse to the callback")),
        20_000,
      );
      page.on("response", async (response) => {
        const request = response.request();
        if (!request.url().includes("/api/auth/sso/callback") || request.method() !== "POST") {
          return;
        }
        const params = new URLSearchParams(request.postData() ?? "");
        samlResponse = params.get("SAMLResponse") ?? "";
        postedXml = Buffer.from(samlResponse, "base64").toString("utf8");
        callbackStatus = response.status();
        callbackLocation = response.headers()["location"] ?? "";
        if (callbackStatus === 401) {
          callbackBody = Buffer.from(await response.body()).toString("utf8");
        }
        clearTimeout(timer);
        resolve();
      });
    });

    try {
      await setSamlAudience(provider.pk, WRONG_AUDIENCE);
      await page.goto(`${BACKEND_URL}/api/auth/sso/login/${SAML_ORG_SLUG}`);
      await page.waitForURL(/\/if\/flow\/default-authentication-flow\//);
      await page.getByRole("textbox", { name: /email|username/i }).fill(SSO_USER_EMAIL);
      await page.getByRole("button", { name: /log in|sign in|continue/i }).click();
      await page.getByRole("textbox", { name: /password/i }).fill(SSO_USER_PASSWORD);
      await page.getByRole("button", { name: /log in|sign in|continue/i }).click();
      await posted;
    } finally {
      await setSamlAudience(provider.pk, provider.audience);
    }

    expect(postedXml, "no signed assertion was posted, so a 401 would prove nothing").toContain(
      WRONG_AUDIENCE,
    );
    expect(postedXml).not.toContain(`>${SP_AUDIENCE}<`);
    expect(postedXml, "the posted assertion was not signed").toContain("Signature");

    let verdictStatus = callbackStatus;
    let verdictBody = callbackBody;
    if (callbackStatus !== 401) {
      expect(
        callbackLocation,
        `browser callback was ${callbackStatus} ${callbackLocation}, so the audience check did not run`,
      ).toContain("sso_invalid_state");
      const state = (await page.context().cookies()).find((cookie) => cookie.name === "sso_state");
      expect(
        state?.value,
        "no sso_state cookie: the callback bails before SAML validation and a 401 would prove nothing",
      ).toBeTruthy();
      const api = await playwright.request.newContext(
        backendContextOptions({
          maxRedirects: 0,
          extraHTTPHeaders: { cookie: `sso_state=${state?.value ?? ""}` },
        }),
      );
      const replayed = await api.post("/api/auth/sso/callback", {
        form: { SAMLResponse: samlResponse },
      });
      verdictStatus = replayed.status();
      verdictBody = await replayed.text();
      await api.dispose();
    }

    expect(verdictStatus, "the endpoint accepted a wrong-audience assertion").toBe(401);
    expect(verdictBody).toBe("Unauthorized: SAML assertion failed validation.");
  });

  test("a state cookie that does not verify never reaches SAML validation", async ({
    playwright,
  }) => {
    const api = await playwright.request.newContext(backendContextOptions({ maxRedirects: 0 }));
    const login = await api.get(`/api/auth/sso/login/${SAML_ORG_SLUG}`);
    expect(login.status()).toBe(302);
    const stored = await api.storageState();
    expect(
      stored.cookies.some((c) => c.name === "sso_state"),
      "no sso_state cookie: the callback bails before SAML validation and a 401 would prove nothing",
    ).toBe(true);
    await api.dispose();

    // 64 hex chars: hmac.compare_digest raises on a shorter signature, which is a 500.
    const broken = await playwright.request.newContext(
      backendContextOptions({
        maxRedirects: 0,
        extraHTTPHeaders: {
          cookie: `sso_state=${SAML_ORG_SLUG}:${"a".repeat(32)}:${"0".repeat(64)}`,
        },
      }),
    );
    const response = await broken.post("/api/auth/sso/callback", {
      form: {
        SAMLResponse: Buffer.from(`<Audience>${WRONG_AUDIENCE}</Audience>`).toString("base64"),
      },
    });
    expect(response.status()).toBe(302);
    expect(response.headers().location ?? "").toContain("sso_invalid_state");
    await broken.dispose();
  });
});
