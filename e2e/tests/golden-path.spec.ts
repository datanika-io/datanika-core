import { test, expect } from "../fixtures/auth";

/**
 * Golden path: a new user signs up, creates a connection, uploads a CSV,
 * runs a pipeline, and sees data land in the destination.
 *
 * This is the single most important test in the suite. If this is green,
 * the critical revenue-producing flow works end to end. If this is red,
 * no other E2E result matters.
 *
 * Blocked on:
 *   - e2e-seed script (Engineering) to reset DB to known state
 *   - DuckDB or local Postgres destination so we don't depend on a cloud warehouse
 */
// @slow — full stack exercise (signup + run + destination query). Expensive on
// the GHA runner (docker-compose + Celery + dbt), so gated to PRs targeting
// master via DATANIKA_E2E_SLOW=1. See plans/qa/PLAN_QA.md §P0 #1.
test.describe("Golden path: signup → connection → pipeline → run @slow", () => {
  test.skip("new user signs up and runs their first pipeline", async ({ page }) => {
    // 1. Signup
    await page.goto("/signup");
    await page.getByLabel(/email/i).fill(`qa-${Date.now()}@datanika.test`);
    await page.getByLabel(/password/i).fill("QaGoldenPath-2026");
    await page.getByRole("button", { name: /sign up|create account/i }).click();
    await expect(page).toHaveURL(/\/(dashboard|connections|onboarding)/);

    // 2. Add a local DuckDB destination
    await page.goto("/connections");
    await page.getByRole("button", { name: /add connection|new connection/i }).click();
    await page.getByText(/duckdb/i).click();
    await page.getByLabel(/name/i).fill("qa-duckdb");
    await page.getByRole("button", { name: /test connection/i }).click();
    await expect(page.getByText(/connection (ok|successful|valid)/i)).toBeVisible();
    await page.getByRole("button", { name: /save|create/i }).click();

    // 3. Upload a small CSV
    await page.goto("/uploads");
    await page.getByRole("button", { name: /upload|new upload/i }).click();
    // fixture CSV path TBD once e2e-seed lands
    // await page.setInputFiles('input[type="file"]', "fixtures/sample.csv");

    // 4. Trigger the pipeline
    // await page.getByRole('button', { name: /run/i }).click();

    // 5. Assert the destination now has rows
    // TODO: `apiClient` will come from fixtures/data.ts (README §Structure),
    // not yet in the diff. When it lands, it wraps `/api/v1/connections/{id}/query`
    // scoped to the fixture org via DATANIKA_E2E_* creds from global-setup.ts.
    // const rowCount = await apiClient.query("SELECT COUNT(*) FROM qa_duckdb.sample");
    // expect(rowCount).toBeGreaterThan(0);
  });
});
