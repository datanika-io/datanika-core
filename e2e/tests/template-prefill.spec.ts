import { test, expect } from "../fixtures/auth";

/**
 * Pipeline templates: the ?template= query param flow.
 *
 * Landing site → app with ?template=<slug> → template prefills the pipeline
 * builder. This is Growth's P0 public-template funnel; if the prefill breaks,
 * every Option C landing page drives users into a broken experience.
 */
test.describe("Pipeline template prefill via ?template=", () => {
  test("stripe-to-postgres template prefills builder", async ({ loggedInPage: page }) => {
    await page.goto("/pipelines/new?template=stripe-to-postgres");
    await expect(page.getByText(/stripe/i)).toBeVisible();
    await expect(page.getByText(/postgres/i)).toBeVisible();
    // Template should select source + destination connection types automatically
    await expect(page.locator('[data-test="source-type"]')).toHaveValue(/stripe/i);
    await expect(page.locator('[data-test="dest-type"]')).toHaveValue(/postgres/i);
  });

  test("template_selected Plausible event fires", async ({ page }) => {
    // Register the interception BEFORE navigation. Registering after
    // page.goto() is a race — the template_selected beacon fires from the
    // landing-page mount and would already be in flight by the time the
    // route handler attaches. Caught in Engineering review of PR #112.
    const events: string[] = [];
    await page.route("**/api/event", async (route) => {
      events.push((await route.request().postData()) ?? "");
      await route.fulfill({ status: 202 });
    });
    await page.goto("/pipelines/new?template=stripe-to-postgres");
    // Give the client one tick to flush pending beacons before asserting.
    await page.waitForLoadState("networkidle");
    expect(events.some((e) => e.includes("template_selected"))).toBeTruthy();
  });
});
