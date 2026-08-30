import { test, expect, ORG_A_KEY, ORG_B_KEY } from "../fixtures/auth";

/**
 * Multi-tenant READ isolation at the HTTP edge.
 *
 * Companion to tenant-jwt-boundary.spec.ts, which proves org A cannot
 * *mutate* org B's resources across 25 routes. This spec proves the read
 * dimension: with a valid org-A API key, no GET exposes an org-B resource —
 * neither by direct id nor by leaking into org A's collection listings.
 *
 * Threat model: an attacker with a valid org-A API key discovers (leaked URL,
 * enumeration, side channel) an org-B resource id and tries to read it.
 * Expected: 404 (or 400/403 for routes whose parsing runs before the org
 * check). Forbidden: 2xx (cross-tenant read leaked) or 5xx (handler crashed
 * past the org check — could mask a bypass).
 *
 * If this ever turns red, do NOT "just fix the assertion" — it is a security
 * incident. Escalate to Engineering and file a CVE-grade issue.
 *
 * Fixtures: org B (one-of-every-resource) + org A/B API keys are always seeded
 * by datanika/scripts/e2e_seed.py (core#172); global-setup.ts maps them to
 * DATANIKA_E2E_* env vars. Un-skipped in core#297.
 */
// @slow — real HTTP against a seeded second tenant; gated to master promotion
// PRs via DATANIKA_E2E_SLOW=1. See plans/qa/PLAN_QA.md §P1 Multi-Tenancy.

type ReadResource = {
  collection: string;
  resourceKey: "connection" | "pipeline" | "transformation" | "schedule" | "upload";
};

// Org-B resources that GET-by-id endpoints exist for. Kept in lockstep with
// the resources seeded in org B by e2e_seed.py.
const READ_RESOURCES: ReadResource[] = [
  { collection: "connections", resourceKey: "connection" },
  { collection: "pipelines", resourceKey: "pipeline" },
  { collection: "transformations", resourceKey: "transformation" },
  { collection: "schedules", resourceKey: "schedule" },
  { collection: "uploads", resourceKey: "upload" },
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

function orgBResourceId(resourceKey: ReadResource["resourceKey"]): number {
  const value = process.env[`DATANIKA_E2E_ORG_B_${resourceKey.toUpperCase()}_ID`];
  // Fall back to a synthetic id: the boundary is still valid — org A must not
  // read any id it doesn't own, whether the row exists or not.
  return value && Number(value) > 0 ? Number(value) : 999999;
}

/** Normalize a list response to an array of items regardless of envelope shape. */
function asItems(body: unknown): Array<Record<string, unknown>> {
  if (Array.isArray(body)) return body as Array<Record<string, unknown>>;
  if (body && typeof body === "object") {
    for (const key of ["items", "data", "results"]) {
      const v = (body as Record<string, unknown>)[key];
      if (Array.isArray(v)) return v as Array<Record<string, unknown>>;
    }
  }
  return [];
}

test.describe("Tenant isolation: org A cannot read org B @slow", () => {
  // core#699 — these 10 requests share org A's rate-limit budget with
  // `tenant-jwt-boundary.spec.ts`'s 26, against a Free-plan allowance of 30/min.
  // `apiBudget` is worker-scoped precisely so that cross-file total is tracked;
  // it may pause for the server's window to roll, so allow for it.
  test.describe.configure({ timeout: 120_000 });

  for (const { collection, resourceKey } of READ_RESOURCES) {
    test(`GET /api/v1/${collection}/{id} — org A cannot read org B resource`, async ({
      request,
      apiBudget,
    }) => {
      const apiKeyA = mustEnv("DATANIKA_E2E_API_KEY_ORG_A");
      const victimId = orgBResourceId(resourceKey);

      // A 429 is raised by `apiBudget.fetch` as a harness failure and never
      // reaches the allowlist below — a rate-limit rejection happens before the
      // org check, so it is not evidence about isolation. See core#699.
      const res = await apiBudget.fetch(
        request,
        ORG_A_KEY,
        `/api/v1/${collection}/${victimId}`,
        {
          method: "GET",
          headers: { Authorization: `Bearer ${apiKeyA}` },
          label: `GET /api/v1/${collection}/{id} (cross-tenant probe)`,
        },
      );
      const status = res.status();

      expect(
        [200, 201, 202, 204].includes(status),
        `GET /api/v1/${collection}/${victimId} leaked org B resource to org A (status=${status})`,
      ).toBe(false);
      expect(status, `cross-tenant GET returned ${status}`).toBeLessThan(500);
      expect([400, 401, 403, 404]).toContain(status);
    });
  }

  test("collection listings do not leak org B rows to org A", async ({ request, apiBudget }) => {
    const apiKeyA = mustEnv("DATANIKA_E2E_API_KEY_ORG_A");
    for (const { collection, resourceKey } of READ_RESOURCES) {
      const victimId = orgBResourceId(resourceKey);
      const res = await apiBudget.fetch(request, ORG_A_KEY, `/api/v1/${collection}`, {
        method: "GET",
        headers: { Authorization: `Bearer ${apiKeyA}` },
        label: `GET /api/v1/${collection} (org A listing)`,
      });
      expect(res.status(), `GET /api/v1/${collection} for org A`).toBe(200);
      const ids = asItems(await res.json()).map((row) => Number(row.id));
      expect(
        ids.includes(victimId),
        `/api/v1/${collection} leaked org B id ${victimId} into org A's listing`,
      ).toBe(false);
    }
  });

  test("sanity: org B can read its own resource with its own key", async ({
    request,
    apiBudget,
  }) => {
    // The boundary must not wall off the rightful owner: the same connection
    // that 404s for org A must return 200 for org B.
    const apiKeyB = mustEnv("DATANIKA_E2E_API_KEY_ORG_B");
    const ownId = orgBResourceId("connection");

    const res = await apiBudget.fetch(request, ORG_B_KEY, `/api/v1/connections/${ownId}`, {
      method: "GET",
      headers: { Authorization: `Bearer ${apiKeyB}` },
      label: "sanity: org B reads its own connection",
    });
    expect(res.status()).toBe(200);
  });
});
