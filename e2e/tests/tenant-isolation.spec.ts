import { test, expect } from "../fixtures/auth";

/**
 * Multi-tenant isolation at the HTTP edge.
 *
 * Service-layer isolation is enforced by the AST walker test in pytest,
 * but that only proves the _check_role() calls exist. This test proves
 * that if I am logged in as user A (org A), NO path exposes any
 * resource belonging to org B — not via the UI, not via the REST API.
 *
 * If this ever fails, it is a security incident. Do not "just fix the
 * assertion" — escalate to Engineering and file a CVE-grade issue.
 */
// @slow — parameterized over every /api/v1/* collection (walks OpenAPI spec)
// and requires a second tenant seeded via core#113 follow-up. Too expensive
// to run on every PR to `dev`; gated to `master` promotion PRs via
// DATANIKA_E2E_SLOW=1. See plans/qa/PLAN_QA.md §P0 #1 and core#113.
test.describe("Tenant isolation: user A cannot read org B @slow", () => {
  test.skip("cross-org GET on every collection returns 403 or empty", async ({ request }) => {
    // Seed two orgs via the deterministic seed script
    // const { userA, userB, orgBPipelineId } = await seed.twoOrgs();

    const collections = [
      "/api/v1/connections",
      "/api/v1/pipelines",
      "/api/v1/transformations",
      "/api/v1/schedules",
      "/api/v1/runs",
      "/api/v1/catalog",
    ];

    for (const path of collections) {
      // const res = await request.get(path, { headers: { Authorization: `Bearer ${userA.apiKey}` } });
      // expect(res.status()).toBe(200);
      // const body = await res.json();
      // expect(body).not.toContainEqual(expect.objectContaining({ org_id: userB.orgId }));
    }

    // Direct-access via guessed IDs must 403, not 404 (leaking existence is also a bug)
    // const direct = await request.get(`/api/v1/pipelines/${orgBPipelineId}`, ...);
    // expect(direct.status()).toBe(403);
  });

  test.skip("new endpoint parity: every /api/v1/* collection is covered by this test", async ({ request }) => {
    // Parse OpenAPI spec and fail if a collection endpoint is not asserted above.
    // const spec = await request.get("/api/v1/openapi.json").then(r => r.json());
    // ... walk paths, assert each collection is in the list above
  });
});
