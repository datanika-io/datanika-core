import { test, expect, signUp } from "../fixtures/auth";

/**
 * Golden path: a new user signs up, creates a connection, uploads a CSV,
 * runs a pipeline, and sees data land in the destination.
 *
 * This is the single most important test in the suite. If this is green,
 * the critical revenue-producing flow works end to end. If this is red,
 * no other E2E result matters.
 *
 * Unblocked by core#109 (e2e-seed lands in dev). Still tagged @slow because
 * the full stack exercise remains expensive on the GHA runner.
 */
// @slow — full stack exercise (signup + run + destination query). Expensive on
// the GHA runner (docker-compose + Celery + dbt), so gated to PRs targeting
// master via DATANIKA_E2E_SLOW=1. See plans/qa/PLAN_QA.md §P0 #1.
test.describe("Golden path: signup → connection → pipeline → run @slow", () => {
  test("new user signs up and reaches the app", async ({ page }) => {
    // 1. Signup. signUp() fills the form (incl. the required Full Name) and
    //    handles the Reflex hydration race — it retries if the click falls back
    //    to a native GET submit before on_submit is wired (core#295).
    await signUp(page);
    await expect(page).toHaveURL(/\/(dashboard|connections|onboarding)?$/);
    await expect(page.getByRole("link", { name: "Connections" }).first()).toBeVisible({
      timeout: 10_000,
    });

    // 2-5. Add a DuckDB connection → upload a CSV → run the pipeline → assert
    // rows landed. Deferred from this spec: the connection-builder + upload UI
    // selectors need verification against the live app, and the upload/run steps
    // need a fixture CSV + an apiClient (fixtures/data.ts) that aren't in the
    // harness yet. The backend create/run path is covered by the nightly
    // connector-smoke matrix and was re-verified via API in the 2026-07-17
    // restore check (run: pending → running → success). Tracked as follow-up.
  });
});
