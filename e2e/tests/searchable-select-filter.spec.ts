import { expect, gotoReady, test } from "../fixtures/auth";

/**
 * The Search box in every searchable_select does nothing (core#1613).
 *
 * `rx.script` runs at page load. The Radix popover mounts its input and list
 * on open, so both getElementById calls return null, the guard returns, and
 * no input listener is attached. The guard stays; the bind has to move to
 * when the content exists.
 *
 * Tier: INFORMATIONAL. A new spec graduates into the gate on three
 * consecutive dev greens. It does not enter the gate in this PR.
 *
 * The visible count before typing is the control. A locator that counts
 * display:none nodes would stay at 36 after a working filter, and a count
 * of 0 would pass a filter assertion against a list the test never saw.
 */
test.describe("searchable select filters once the popover is open @informational", () => {
  test.setTimeout(120_000);

  test("typing filters the list, clearing restores it, and a second select on the page filters too", async ({
    loggedInPage: page,
  }) => {
    await gotoReady(page, "/connections");

    const typeButton = page.getByRole("button", { name: /Connection type/ });
    const visibleItems = () => page.locator('[id$="_list"] > *').filter({ visible: true });

    await typeButton.click();
    await expect(visibleItems()).toHaveCount(36);
    await page.getByLabel("Search...").fill("parquet");
    await expect(visibleItems()).toHaveCount(1);
    await expect(page.getByText("parquet", { exact: true })).toBeVisible();

    await page.getByLabel("Search...").fill("");
    await expect(visibleItems()).toHaveCount(36);

    await page.keyboard.press("Escape");
    await expect(visibleItems()).toHaveCount(0);
    await typeButton.click();
    await expect(visibleItems()).toHaveCount(36);
    await page.getByLabel("Search...").fill("parquet");
    await expect(visibleItems()).toHaveCount(1);

    await page.keyboard.press("Escape");
    // Sidebar language switcher: the other searchable_select on this page.
    // Nine locales. `de` matches one of them.
    await page.getByRole("button", { name: /^en$/ }).click();
    await expect(visibleItems()).toHaveCount(9);
    await page.getByLabel("Language...").fill("de");
    await expect(visibleItems()).toHaveCount(1);
    await expect(page.getByText("de", { exact: true })).toBeVisible();
  });
});
