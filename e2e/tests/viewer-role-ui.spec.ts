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
// @slow — UI login + navigation.
// @slow. Runs wherever DATANIKA_E2E_SLOW=1 is set — which is EVERY
// `e2e-staging` run on `dev` (ci.yml sets it on the job), not only
// promotion PRs. See #305.

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

      // The create form is `rx.cond(AuthState.can_edit, connection_form())`, and
      // AuthState hydrates WITH the page rather than with the connection load,
      // so this absence is meaningful the moment gotoReady returns. It keeps its
      // position deliberately: the three assertions have two different readiness
      // dependencies and moving this one buys nothing.
      // absence-ok: gated on AuthState.can_edit, which hydrates with the page
      // rather than with the connections load, so there is no load to wait for.
      await expect(
        page.getByRole("button", { name: /create connection|add connection|new connection/i }),
      ).toHaveCount(0);

      // core#1008. Edit/Delete live INSIDE table rows, and gotoReady waits for
      // Reflex *hydration* -- an open socket and a mounted page -- not for
      // `load_connections` to have answered. Asserting their absence here used
      // to be satisfied on the first poll by a table that had not rendered yet,
      // so the test could not tell a correctly-gated viewer from an unloaded
      // table. Measured in e2e/scripts/probe-1008-absence-ordering.mjs: against
      // a page that renders Edit/Delete 1.2s after hydration -- i.e. a viewer
      // gate that has broken -- the old order PASSES and this order FAILS.
      //
      // So wait for the positive artifact first. The seeded org-A connection is
      // the same row test 1 asserts on, and its presence is proof the table has
      // answered; only after that does an absence mean anything.
      await expect(page.getByText(mustEnv("DATANIKA_E2E_CONNECTION_NAME"))).toBeVisible({
        timeout: 10_000,
      });
      await expect(page.getByRole("button", { name: /^delete$/i })).toHaveCount(0);
      await expect(page.getByRole("button", { name: /^edit$/i })).toHaveCount(0);
    },
  );
});
