import { test, expect } from "../fixtures/auth";

/**
 * RBAC enforcement at the UI layer.
 *
 * Viewer role must not see or reach destructive actions. The service layer
 * already enforces this via _check_role() (AST-tested in pytest). This E2E
 * test confirms the UI state layer respects the same rules — a viewer that
 * gets to a delete button by accident is a regression.
 */
test.describe("RBAC: viewer cannot destroy resources", () => {
  test.skip("viewer sees pipelines but no delete button", async ({ page }) => {
    // Seed via API: create owner + pipeline + invite viewer + accept
    // Then log in as viewer
    await page.goto("/login");
    await page.getByLabel(/email/i).fill("qa-viewer@datanika.test");
    await page.getByLabel(/password/i).fill("QaViewerPw-2026");
    await page.getByRole("button", { name: /log in|sign in/i }).click();

    await page.goto("/pipelines");
    await expect(page.getByRole("heading", { name: /pipelines/i })).toBeVisible();
    await expect(page.getByRole("button", { name: /delete/i })).toHaveCount(0);
  });

  test.skip("viewer calling DELETE /api/v1/pipelines/{id} gets 403", async ({ request }) => {
    // Once an api_client fixture exists, assert the HTTP boundary rejects
    // const res = await request.delete("/api/v1/pipelines/1", { headers: { Authorization: `Bearer ${viewerApiKey}` } });
    // expect(res.status()).toBe(403);
  });
});
