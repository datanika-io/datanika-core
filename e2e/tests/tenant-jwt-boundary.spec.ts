import { test, expect, ORG_A_KEY, ORG_B_KEY } from "../fixtures/auth";

/**
 * Staging-flavored port of tests/test_security/test_tenant_jwt_boundary.py
 * (core#143, CEO scope Q1). See core#166 for scope + rationale.
 *
 * The unit-level ancestor covers the boundary contract at the route
 * handler with an in-memory sqlite DB and mocked auth dispatch. This
 * spec exercises the same 25 mutation routes against the real staging
 * stack: real Postgres, real encryption, real auth middleware, real
 * rate limit middleware. It exists to catch regressions that unit-level
 * mocks hide — e.g. a handler that accidentally commits a cross-tenant
 * write inside an error path, an ORM relationship that eager-loads a
 * victim row before the org check, a caching layer that pre-authorizes
 * by id.
 *
 * Threat model: an attacker with a valid org-A API key discovers a
 * resource id belonging to org B (leaked URL, enumeration, side channel)
 * and tries to mutate it. Expected: clean 404 (or 400/403 for routes
 * whose body parsing runs before the org check). Forbidden: 2xx
 * (cross-tenant write leaked) or 5xx (handler crashed past the org
 * check — still a bug, could mask a bypass).
 *
 * If this ever turns red after Engineering ships the seed extension,
 * do NOT "just fix the assertion." Escalate to Engineering and file a
 * CVE-grade issue.
 *
 * Seed extensions shipped in core#172 (2026-04-16). Org B + API keys are
 * always seeded (no opt-in flags needed). global-setup.ts maps all 25
 * seed fields to DATANIKA_E2E_* env vars. .skip markers removed in QU2.
 */
// @slow — 25 routes × 2 tests × real HTTP = expensive.
// @slow. Runs wherever DATANIKA_E2E_SLOW=1 is set — which is EVERY
// `e2e-staging` run on `dev` (ci.yml sets it on the job), not only
// promotion PRs.
// See core#719 for how the route table is derived.

type MutationRoute = {
  method: "PUT" | "DELETE" | "POST" | "PATCH";
  pathTemplate: string;
  resourceKey:
    | "connection"
    | "upload"
    | "pipeline"
    | "transformation"
    | "schedule"
    | "run"
    | "notification"
    | "channel";
  body: Record<string, unknown> | null;
};

// Same 25-route table as tests/test_security/test_tenant_jwt_boundary.py
// MUTATION_ROUTES. If that table grows (new endpoint, new resource),
// this one must grow in lockstep — add a parity check as a follow-up
// once both tests are green.
const MUTATION_ROUTES: MutationRoute[] = [
  // Connections
  { method: "PUT", pathTemplate: "/api/v1/connections/{id}", resourceKey: "connection", body: { name: "pwned" } },
  { method: "DELETE", pathTemplate: "/api/v1/connections/{id}", resourceKey: "connection", body: null },
  { method: "POST", pathTemplate: "/api/v1/connections/{id}/test", resourceKey: "connection", body: null },
  { method: "POST", pathTemplate: "/api/v1/connections/{id}/introspect", resourceKey: "connection", body: {} },
  { method: "POST", pathTemplate: "/api/v1/connections/{id}/columns", resourceKey: "connection", body: { table: "x" } },
  { method: "POST", pathTemplate: "/api/v1/connections/{id}/preview", resourceKey: "connection", body: { table: "x" } },
  { method: "POST", pathTemplate: "/api/v1/connections/{id}/query", resourceKey: "connection", body: { query: "SELECT 1" } },
  // Uploads
  { method: "PUT", pathTemplate: "/api/v1/uploads/{id}", resourceKey: "upload", body: { name: "pwned" } },
  { method: "DELETE", pathTemplate: "/api/v1/uploads/{id}", resourceKey: "upload", body: null },
  { method: "POST", pathTemplate: "/api/v1/uploads/{id}/run", resourceKey: "upload", body: null },
  // Pipelines
  { method: "PUT", pathTemplate: "/api/v1/pipelines/{id}", resourceKey: "pipeline", body: { name: "pwned" } },
  { method: "DELETE", pathTemplate: "/api/v1/pipelines/{id}", resourceKey: "pipeline", body: null },
  { method: "POST", pathTemplate: "/api/v1/pipelines/{id}/run", resourceKey: "pipeline", body: null },
  // Transformations
  { method: "PUT", pathTemplate: "/api/v1/transformations/{id}", resourceKey: "transformation", body: { name: "pwned" } },
  { method: "DELETE", pathTemplate: "/api/v1/transformations/{id}", resourceKey: "transformation", body: null },
  { method: "POST", pathTemplate: "/api/v1/transformations/{id}/run", resourceKey: "transformation", body: null },
  { method: "POST", pathTemplate: "/api/v1/transformations/{id}/compile", resourceKey: "transformation", body: null },
  { method: "POST", pathTemplate: "/api/v1/transformations/{id}/preview", resourceKey: "transformation", body: null },
  // Schedules
  { method: "PUT", pathTemplate: "/api/v1/schedules/{id}", resourceKey: "schedule", body: { cron_expression: "*/5 * * * *" } },
  { method: "DELETE", pathTemplate: "/api/v1/schedules/{id}", resourceKey: "schedule", body: null },
  // Runs
  { method: "POST", pathTemplate: "/api/v1/runs/{id}/cancel", resourceKey: "run", body: null },
  // In-app notifications
  { method: "PATCH", pathTemplate: "/api/v1/notifications/{id}/read", resourceKey: "notification", body: null },
  { method: "DELETE", pathTemplate: "/api/v1/notifications/{id}", resourceKey: "notification", body: null },
  // Notification channels
  { method: "PUT", pathTemplate: "/api/v1/notifications/channels/{id}", resourceKey: "channel", body: { name: "pwned" } },
  { method: "DELETE", pathTemplate: "/api/v1/notifications/channels/{id}", resourceKey: "channel", body: null },
];

function mustEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(
      `${name} not set — global-setup.ts must map it from the seed payload. ` +
        "Check e2e/global-setup.ts and datanika/scripts/e2e_seed.py.",
    );
  }
  return value;
}

function orgBResourceId(resourceKey: MutationRoute["resourceKey"]): number {
  const envName = `DATANIKA_E2E_ORG_B_${resourceKey.toUpperCase()}_ID`;
  const value = process.env[envName];
  if (value && Number(value) > 0) {
    return Number(value);
  }
  // Resources not seeded in org B (run, notification, channel) use a
  // synthetic ID. The boundary test is still valid — org A must get 404
  // for any ID it doesn't own, whether the row exists or not.
  return 999999;
}

test.describe("Tenant JWT boundary: /api/v1/* mutation surface @slow", () => {
  // core#699 — every request below is budgeted through `apiBudget`, which may
  // pause once for the server's rate-limit window to roll over (the suite needs
  // 36 requests on org A's key and the Free plan allows 30/minute). The pause is
  // bounded by one minute, so the per-test timeout has to clear it.
  test.describe.configure({ timeout: 120_000 });

  for (const route of MUTATION_ROUTES) {
    const id = `${route.method} ${route.pathTemplate}`;
    test(`${id} — org A cannot mutate org B resource`, async ({ request, apiBudget }) => {
      const apiKeyA = mustEnv("DATANIKA_E2E_API_KEY_ORG_A");
      const victimId = orgBResourceId(route.resourceKey);
      const url = route.pathTemplate.replace("{id}", String(victimId));

      // A 429 never reaches the assertions below: `apiBudget.fetch` raises it as
      // an explicit harness failure. Rate-limit rejection happens *before* the
      // handler runs, so accepting it here would let this spec report success
      // for requests that never exercised tenant isolation. See core#699.
      const response = await apiBudget.fetch(request, ORG_A_KEY, url, {
        method: route.method,
        headers: { Authorization: `Bearer ${apiKeyA}` },
        data: route.body ?? undefined,
        label: `${id} (cross-tenant probe)`,
      });
      const status = response.status();

      // Forbidden outcomes
      expect(
        [200, 201, 202, 204].includes(status),
        `${id} leaked org B resource to org A (status=${status})`,
      ).toBe(false);
      expect(status, `${id} returned ${status} on cross-tenant probe`).toBeLessThan(500);

      // Accepted outcomes: 404 preferred, 400/403 tolerated
      expect([400, 403, 404]).toContain(status);
    });
  }

  test("write-through guard — PUT does not mutate B-owned connection", async ({
    request,
    apiBudget,
  }) => {
    // Belt-and-suspenders: even if a cross-tenant PUT returned an error,
    // verify the B-owned row's content is unchanged. Catches a "error
    // response but the update still committed" bug.
    const apiKeyA = mustEnv("DATANIKA_E2E_API_KEY_ORG_A");
    const apiKeyB = mustEnv("DATANIKA_E2E_API_KEY_ORG_B");
    const victimId = orgBResourceId("connection");

    // Snapshot the victim's current state via B's own key
    const before = await apiBudget.fetch(request, ORG_B_KEY, `/api/v1/connections/${victimId}`, {
      method: "GET",
      headers: { Authorization: `Bearer ${apiKeyB}` },
      label: "write-through guard: snapshot B's connection",
    });
    expect(before.status()).toBe(200);
    const originalName = (await before.json()).name;

    // Attempt cross-tenant PUT with A's key
    const attack = await apiBudget.fetch(request, ORG_A_KEY, `/api/v1/connections/${victimId}`, {
      method: "PUT",
      headers: { Authorization: `Bearer ${apiKeyA}` },
      data: { name: "pwned-by-a" },
      label: "write-through guard: cross-tenant PUT",
    });
    expect(attack.status()).toBe(404);

    // Re-read via B's key and assert nothing changed
    const after = await apiBudget.fetch(request, ORG_B_KEY, `/api/v1/connections/${victimId}`, {
      method: "GET",
      headers: { Authorization: `Bearer ${apiKeyB}` },
      label: "write-through guard: re-read B's connection",
    });
    expect(after.status()).toBe(200);
    expect((await after.json()).name).toBe(originalName);
  });

  test("sanity: org B can still access own resource with own key", async ({
    request,
    apiBudget,
  }) => {
    // The boundary check must not accidentally wall off the rightful
    // owner. Using B's own key, the same endpoint that 404'd for A
    // must succeed.
    const apiKeyB = mustEnv("DATANIKA_E2E_API_KEY_ORG_B");
    const victimId = orgBResourceId("connection");

    const response = await apiBudget.fetch(request, ORG_B_KEY, `/api/v1/connections/${victimId}`, {
      method: "GET",
      headers: { Authorization: `Bearer ${apiKeyB}` },
      label: "sanity: org B reads its own connection",
    });
    expect(response.status()).toBe(200);
  });
});
