import { test, expect, gotoReady } from "../fixtures/auth";

/**
 * Pipeline templates: the ?template= query param flow.
 *
 * Landing site → /pipelines/templates → click card → /connections?template=<slug>
 * → ConnectionState.load_template_from_query prefills form_type + form fields.
 *
 * This is Growth's P0 public-template funnel; if the prefill breaks,
 * every Option C landing page drives users into a broken experience.
 *
 * ── core#927: this file was the gating tier's resident flake, 2 of 18 ─────────
 * Two flakes, two different tests, and the shared property is this file rather
 * than either test: it is the ONLY spec in the suite that uses the
 * `loggedInPage` fixture, and it was the only gating spec that then navigated
 * with a bare `page.goto` instead of `gotoReady`. Both flakes are one class --
 * a Reflex interaction performed without waiting for a positive signal that the
 * page is ready to receive it.
 *
 *   d4a49ff3 / run 33490485253  spec, line 37: `firstCard.click()` waited the
 *     whole 60 s test timeout for `a[href*="template="]`. A bare `page.goto`
 *     resolves on `load`, before Reflex has hydrated and rendered the cards, so
 *     nothing here waited for the content the click needs.
 *   11ab292c / run 33528992652  fixture, auth.ts: the login `toHaveURL` timed
 *     out ten seconds after the click. See the note on `loggedInPage`.
 *
 * The discriminator against "staging was down" is in the logs of both runs: the
 * OTHER test in this file passed, against the same deploy, minutes apart. An
 * unhealthy box fails both.
 */
test.describe("Pipeline template prefill via ?template=", () => {
  test("stripe-to-postgres template prefills connection form", async ({ loggedInPage: page }) => {
    // gotoReady, not page.goto: `load_template_from_query` is an on_load handler
    // and runs after hydration, so the button below does not exist yet when a
    // bare goto resolves. `toBeVisible` auto-retries and usually rides it out,
    // which is why this presented as a flake rather than as a failure.
    await gotoReady(page, "/connections?template=stripe-to-postgres");

    // The connection type picker is a custom searchable_select (popover trigger button),
    // not a native <select>. When form_type is prefilled, the button shows the value.
    await expect(page.getByRole("button", { name: /stripe/i })).toBeVisible();
  });

  // 🚨 RENAMED, and the old name is the point (core#927).
  //
  // This was `template_selected Plausible event fires on templates page`. It
  // captured `/api/event` posts, checked whether any contained
  // `template_selected`, and on finding none wrote a line to the console. There
  // was **no assertion about the event anywhere in the body** -- the only things
  // that could fail were the navigation and the click. So a GATING test named
  // for a Plausible event could not fail if that event never fired, which is in
  // fact what happens today (landing#212: Plausible CE recorded 0 custom events
  // over 30 days).
  //
  // The name now says what the body asserts. The event capture stays, because
  // the diagnostic is worth having and costs nothing, but it is no longer what
  // the test claims to be about. Tightening it to a real assertion is
  // landing#212's job, not this file's.
  test("a template card navigates to /connections with the template in the query", async ({
    loggedInPage: page,
  }) => {
    // Diagnostic only. template_selected fires on /pipelines/templates when a
    // card is clicked, NOT on the /connections target page; it is injected by
    // the cloud plugin via get_page_scripts("pipeline_templates") and is empty
    // on open-source builds.
    const events: string[] = [];
    await page.route("**/api/event", async (route) => {
      events.push((await route.request().postData()) ?? "");
      await route.fulfill({ status: 202 });
    });

    // gotoReady, not page.goto: the cards are Reflex-rendered, so a bare goto
    // resolves before they exist. On run 33490485253 that left `click()` waiting
    // out the entire 60 s test timeout on a locator that never appeared.
    await gotoReady(page, "/pipelines/templates");

    // Wait for the card as a positive artifact before acting on it, so a
    // failure here says "the templates page rendered no cards" rather than
    // "a click timed out" -- two different processes, two different fixes.
    const firstCard = page.locator('a[href*="template="]').first();
    await expect(firstCard).toBeVisible({ timeout: 15_000 });
    await firstCard.click();

    await page.waitForURL(/\/connections\?template=/);

    if (!events.some((e) => e.includes("template_selected"))) {
      console.log("template_selected event not captured (diagnostic; see landing#212)");
    }
  });
});
