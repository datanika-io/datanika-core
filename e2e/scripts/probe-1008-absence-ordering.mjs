/**
 * core#1008 -- proof that `toHaveCount(0)` before the table load cannot fail.
 *
 * The defect is in the ORDER of the assertions, not in the app, so the page here
 * only has to reproduce the timing: hydration completes immediately, the row
 * (and, in the broken arm, the Edit/Delete controls) render later, exactly as
 * `load_connections` renders after Reflex hydration.
 *
 * 2x2, because either arm alone proves nothing:
 *   - SHIPPED order on a BROKEN gate  -> passes  (the false green being reported)
 *   - FIXED   order on a BROKEN gate  -> fails   (the fix discriminates)
 *   - SHIPPED order on a GOOD   gate  -> passes  (both arms agree when correct)
 *   - FIXED   order on a GOOD   gate  -> passes  (false-positive control:
 *                                                 the fix does not just fail more)
 */
import { chromium } from "playwright";
import { expect } from "@playwright/test";

const CONNECTION_NAME = "e2e-seeded-connection";

/** @param {boolean} gateBroken  render Edit/Delete for the viewer (a real defect) */
const page_html = (gateBroken) => `
<!doctype html><meta charset="utf-8"><title>connections</title>
<body>
  <h1>Connections</h1>
  <div id="table">loading...</div>
  <script>
    // The connections table answers ~1.2s after hydration, as on staging.
    setTimeout(() => {
      const host = document.getElementById("table");
      host.textContent = "";
      const table = document.createElement("table");
      const row = table.insertRow();
      row.insertCell().textContent = ${JSON.stringify(CONNECTION_NAME)};
      const cell = row.insertCell();
      if (${gateBroken}) {
        for (const label of ["Edit", "Delete"]) {
          const b = document.createElement("button");
          b.textContent = label;
          cell.appendChild(b);
        }
      }
      host.appendChild(table);
    }, 1200);
  </script>
</body>`;

async function shippedOrder(page) {
  // e2e/tests/viewer-role-ui.spec.ts as it stands on origin/dev.
  await expect(
    page.getByRole("button", { name: /create connection|add connection|new connection/i }),
  ).toHaveCount(0);
  await expect(page.getByRole("button", { name: /^delete$/i })).toHaveCount(0);
  await expect(page.getByRole("button", { name: /^edit$/i })).toHaveCount(0);
}

async function fixedOrder(page) {
  // The create form gates on AuthState.can_edit, which hydrates WITH the page,
  // so this assertion keeps its position -- a different readiness dependency.
  await expect(
    page.getByRole("button", { name: /create connection|add connection|new connection/i }),
  ).toHaveCount(0);
  // Edit/Delete live inside table rows. Wait for a positive artifact that the
  // table has answered before an absence means anything.
  await expect(page.getByText(CONNECTION_NAME)).toBeVisible({ timeout: 10_000 });
  await expect(page.getByRole("button", { name: /^delete$/i })).toHaveCount(0);
  await expect(page.getByRole("button", { name: /^edit$/i })).toHaveCount(0);
}

const browser = await chromium.launch();
const rows = [];
for (const [gateLabel, gateBroken] of [
  ["BROKEN gate (viewer sees Edit/Delete)", true],
  ["GOOD gate   (viewer sees neither)", false],
]) {
  for (const [orderLabel, run] of [
    ["shipped", shippedOrder],
    ["fixed  ", fixedOrder],
  ]) {
    const page = await browser.newPage();
    await page.setContent(page_html(gateBroken));
    let verdict = "PASS";
    let detail = "";
    try {
      await run(page);
    } catch (e) {
      verdict = "FAIL";
      detail = String(e).split("\n")[0].slice(0, 90);
    }
    rows.push({ gateLabel, orderLabel, verdict, detail });
    await page.close();
  }
}
await browser.close();

console.log("");
console.log("  gate                                     order     verdict");
console.log("  " + "-".repeat(78));
for (const r of rows) {
  console.log(`  ${r.gateLabel.padEnd(40)} ${r.orderLabel}   ${r.verdict}  ${r.detail}`);
}

const want = [
  ["BROKEN gate (viewer sees Edit/Delete)", "shipped", "PASS"],
  ["BROKEN gate (viewer sees Edit/Delete)", "fixed  ", "FAIL"],
  ["GOOD gate   (viewer sees neither)", "shipped", "PASS"],
  ["GOOD gate   (viewer sees neither)", "fixed  ", "PASS"],
];
const ok = want.every(([g, o, v]) =>
  rows.some((r) => r.gateLabel === g && r.orderLabel === o && r.verdict === v),
);
console.log("");
console.log(
  ok
    ? "  ARMED: the shipped order is green against a broken gate; the fixed order\n" +
        "         catches it and stays green when the gate is correct."
    : "  NOT ARMED -- the 2x2 did not come out as declared.",
);
process.exit(ok ? 0 : 1);
