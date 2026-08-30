import { test, expect, ORG_A_READONLY_KEY } from "../fixtures/auth";

/**
 * API-key SCOPE enforcement.
 *
 * The `/api/v1` surface authorizes by API-key scope
 * (`@api_endpoint(required_scope="connections:write")` etc.), NOT by the
 * caller's membership role: an unscoped key has full access, and a scoped key
 * is limited to its scopes (see datanika/services/api_key_service.py +
 * api_middleware.py). Membership-role RBAC — "a VIEWER cannot destroy
 * resources" — is enforced separately in the Reflex UI state layer
 * (`require_role` in datanika/ui/state/*_state.py), and viewers cannot mint API
 * keys (key creation is admin-gated). A UI E2E test for the viewer-role case is
 * tracked as a follow-up to #297.
 *
 * This spec proves the API's scope boundary: a read-only-scoped key can read
 * but is rejected on every write endpoint. If a `*:read`-only key ever succeeds
 * on a `*:write` route, scope enforcement is broken — a privilege bug, not an
 * assertion to "just fix".
 */
// @slow — real HTTP against a seeded read-only key; gated to master promotion
// PRs via DATANIKA_E2E_SLOW=1. See plans/qa/PLAN_QA.md §P1 and #297.

function mustEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(
      `${name} not set — global-setup.ts must map it from the seed payload ` +
        "(needs E2E_SEED_INCLUDE_API_KEYS=1). See datanika/scripts/e2e_seed.py.",
    );
  }
  return value;
}

test.describe("API key scope enforcement: read-only key is denied writes @slow", () => {
  test("read-only key can GET but cannot DELETE/PUT/POST-write a connection", async ({
    request,
    apiBudget,
  }) => {
    const readOnlyKey = mustEnv("DATANIKA_E2E_API_KEY_READONLY");
    const connId = Number(mustEnv("DATANIKA_E2E_CONNECTION_ID")); // org A's own connection
    const auth = { Authorization: `Bearer ${readOnlyKey}` };

    // core#699 — budgeted like every other API call in the suite. The denied
    // writes below never reach the limiter (a missing scope 401s in
    // `authenticate_api_key`, before the rate-limit check), so this key's real
    // spend is only the successful GETs; counting all six is deliberately
    // conservative. What matters is that a 429 can never be mistaken for the
    // 401/403 this spec is asserting.
    const read = await apiBudget.fetch(request, ORG_A_READONLY_KEY, `/api/v1/connections/${connId}`, {
      method: "GET",
      headers: auth,
      label: "read-only key GETs its own connection",
    });
    expect(read.status(), "read-only key should be allowed to GET its own connection").toBe(200);

    const list = await apiBudget.fetch(request, ORG_A_READONLY_KEY, `/api/v1/connections`, {
      method: "GET",
      headers: auth,
      label: "read-only key lists connections",
    });
    expect(list.status(), "read-only key should be allowed to list connections").toBe(200);

    // Denied — every write endpoint requires `connections:write`, which the key
    // lacks. authenticate_api_key returns None on missing scope → the endpoint
    // replies 401 (403 tolerated). Never 2xx.
    const writes = [
      { method: "DELETE" as const, url: `/api/v1/connections/${connId}` },
      {
        method: "PUT" as const,
        url: `/api/v1/connections/${connId}`,
        data: { name: "scope-escalation-attempt" },
      },
      { method: "POST" as const, url: `/api/v1/connections/${connId}/test` },
    ];
    for (const w of writes) {
      const res = await apiBudget.fetch(request, ORG_A_READONLY_KEY, w.url, {
        method: w.method,
        headers: auth,
        data: w.data,
        label: `${w.method} ${w.url} with a read-only key`,
      });
      const status = res.status();
      expect(
        [200, 201, 202, 204].includes(status),
        `${w.method} ${w.url} succeeded with a read-only key (status=${status}) — scope enforcement bypassed`,
      ).toBe(false);
      expect([401, 403]).toContain(status);
    }

    // The connection must still exist — the denied DELETE must not have committed.
    const after = await apiBudget.fetch(request, ORG_A_READONLY_KEY, `/api/v1/connections/${connId}`, {
      method: "GET",
      headers: auth,
      label: "connection survives the denied writes",
    });
    expect(after.status(), "connection should survive the denied writes").toBe(200);
  });
});
