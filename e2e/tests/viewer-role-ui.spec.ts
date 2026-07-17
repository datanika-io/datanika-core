import { test, expect, gotoReady } from "../fixtures/auth";
import type { Page } from "@playwright/test";

/**
 * Membership-role RBAC at the UI layer (viewer role).
 *
 * Complements rbac.spec.ts (API-key SCOPE enforcement). Role-based access is
 * enforced in the Reflex UI state handlers (`_check_role` in
 * datanika/ui/state/*_state.py) and is only reachable through the UI, so this
 * is a browser test. A viewer gets read access but must not reach — or per
 * least privilege, see — destructive actions.
 *
 * Fixtures: the viewer user (`DATANIKA_E2E_VIEWER_USER_*`) + an org-A connection
 * are always seeded by datanika/scripts/e2e_seed.py. Implemented in core#305.
 */
// @slow — UI login + navigation; gated to master promotion PRs via
// DATANIKA_E2E_SLOW=1. See plans/qa/PLAN_QA.md §P1 and #305.

function mustEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`${name} not set — global-setup.ts maps the seeded viewer fields.`);
  }
  return value;
}

async function loginAsViewer(page: Page): Promise<void> {
  await gotoReady(page, "/login");
  await page.getByLabel(/email/i).fill(mustEnv("DATANIKA_E2E_VIEWER_USER_EMAIL"));
  await page.getByLabel(/password/i).fill(mustEnv("DATANIKA_E2E_VIEWER_USER_PASSWORD"));
  await page.getByRole("button", { name: /log in|sign in/i }).click();
  await expect(page).toHaveURL(/\/(dashboard|connections)?$/, { timeout: 15_000 });
}

test.describe("RBAC: viewer role at the UI layer @slow", () => {
  test("viewer has read access to the connections list", async ({ page }) => {
    await loginAsViewer(page);
    await gotoReady(page, "/connections");
    // Read access: the seeded org-A connection is visible to the viewer.
    await expect(page.getByText(mustEnv("DATANIKA_E2E_CONNECTION_NAME"))).toBeVisible({
      timeout: 10_000,
    });
  });

  test(
    "viewer does not see destructive/create controls on connections",
    async ({ page }) => {
      // core#313 (shipped in #318, live via promotion #322): /connections gates
      // the create form + Edit/Copy/Delete controls behind AuthState.can_edit /
      // can_delete, so a VIEWER (can_edit == can_delete == false) never renders
      // them. Enforcement still lives server-side (_check_role in
      // connection_state.py); this locks the least-privilege visibility gate.
      await loginAsViewer(page);
      await gotoReady(page, "/connections");
      await expect(
        page.getByRole("button", { name: /create connection|add connection|new connection/i }),
      ).toHaveCount(0);
      await expect(page.getByRole("button", { name: /^delete$/i })).toHaveCount(0);
      await expect(page.getByRole("button", { name: /^edit$/i })).toHaveCount(0);
    },
  );
});
