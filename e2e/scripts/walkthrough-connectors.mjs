// QA walkthrough driver for landing#144 follow-up.
//
// Signs up a fresh user on staging, navigates to the New Connection form for
// each connector in CONNECTOR_TYPES in turn, screenshots each form, dumps
// every visible form field label + input type, and writes the findings to
// e2e/.walkthrough-out/findings.json. Not a test — deliberately outside the
// tests/ directory so Playwright test runners don't pick it up.
//
// Usage:
//   DATANIKA_E2E_BASE_URL=https://staging-app.datanika.io/ \
//     node scripts/walkthrough-connectors.mjs
//
// Override the connector list:
//   DATANIKA_E2E_CONNECTORS=duckdb,postgres node scripts/walkthrough-connectors.mjs
//
// Owner: QA. Referenced from the landing#144 comment thread.

import { chromium } from "@playwright/test";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT_DIR = join(__dirname, "..", ".walkthrough-out");
mkdirSync(OUT_DIR, { recursive: true });

const BASE_URL = process.env.DATANIKA_E2E_BASE_URL ?? "https://staging-app.datanika.io/";
// Unique per run — both email AND full_name must be unique on every run,
// because auth_state.signup() uses _slugify(full_name) to build the org
// slug, which has a UNIQUE constraint. Reusing the same full_name from a
// previous run collides and the signup handler's broad except swallows
// the IntegrityError into a generic "Signup failed. Please try again."
// toast. Filed as a side finding in the walkthrough comment.
const STAMP = Date.now();
const EMAIL = `qa-walkthrough-${STAMP}@datanika.test`;
const PASSWORD = "QaWalkthrough-2026!";
const FULL_NAME = `QA Walkthrough ${STAMP}`;

const CONNECTOR_TYPES = (
  process.env.DATANIKA_E2E_CONNECTORS ?? "duckdb,sqlite,csv"
)
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const findings = {
  base_url: BASE_URL,
  run_started_at: new Date().toISOString(),
  connectors_requested: CONNECTOR_TYPES,
  auth: { email: EMAIL, path_taken: null, status: null, url_after: null, error: null },
};
for (const t of CONNECTOR_TYPES) {
  findings[t] = { status: null, fields: [], screenshot: null, notes: [], error: null };
}

/** Dump every labelled form input on the current page. */
async function dumpFormFields(page) {
  return page.evaluate(() => {
    const out = [];
    const inputs = document.querySelectorAll("input, select, textarea");
    for (const el of inputs) {
      let label = "";
      if (el.id) {
        const l = document.querySelector(`label[for="${el.id}"]`);
        if (l) label = l.innerText.trim();
      }
      if (!label && el.getAttribute("aria-label")) {
        label = el.getAttribute("aria-label").trim();
      }
      if (!label && el.placeholder) label = `(placeholder) ${el.placeholder}`;
      const type = el.tagName.toLowerCase() + (el.type ? `[${el.type}]` : "");
      const visible = !!(el.offsetWidth || el.offsetHeight || el.getClientRects().length);
      if (visible) out.push({ label: label || "(unlabelled)", type, name: el.name || null });
    }
    const visibleText = document.body.innerText.slice(0, 2000);
    return { inputs: out, visible_text_head: visibleText };
  });
}

async function waitForReflexHydration(page, { timeout = 20000 } = {}) {
  // Reflex apps ship a client that opens a WebSocket to /_event; until the
  // WS is up and the initial state frame arrives, the DOM shows a spinner.
  // `networkidle` never fires on a page with an open WS, so wait for the
  // spinner to disappear + at least one input/button to be in the DOM.
  const deadline = Date.now() + timeout;
  while (Date.now() < deadline) {
    const ready = await page.evaluate(() => {
      const inputs = document.querySelectorAll("input, button");
      return inputs.length > 0;
    });
    if (ready) return;
    await page.waitForTimeout(500);
  }
  throw new Error(`Reflex did not hydrate within ${timeout}ms`);
}

async function tryLogin(page) {
  await page.goto(new URL("/login", BASE_URL).href, { waitUntil: "domcontentloaded" });
  await waitForReflexHydration(page);
  await page.screenshot({ path: join(OUT_DIR, "login-00-loaded.png"), fullPage: true });
  const inputs = await page.locator("input:visible").all();
  if (inputs.length < 2) {
    throw new Error(`login: expected >=2 inputs, got ${inputs.length}`);
  }
  // Login form order: Email, Password
  await inputs[0].fill(EMAIL);
  await inputs[1].fill(PASSWORD);
  const submit = page
    .getByRole("button", { name: /log in|sign in|continue/i })
    .first();
  await submit.click({ timeout: 10000 });
  try {
    await page.waitForURL((url) => !/\/login/.test(url.toString()), { timeout: 15000 });
  } catch {
    /* may stay on /login if creds wrong */
  }
  await page.waitForTimeout(2000);
  await page.screenshot({ path: join(OUT_DIR, "login-01-after.png"), fullPage: true });
  return !/\/login/.test(page.url());
}

async function trySignUp(page) {
  await page.goto(new URL("/signup", BASE_URL).href, { waitUntil: "domcontentloaded" });
  await waitForReflexHydration(page);
  await page.screenshot({ path: join(OUT_DIR, "signup-00-loaded.png"), fullPage: true });
  const inputs = await page.locator("input:visible").all();
  if (inputs.length < 3) {
    throw new Error(`signup: expected >=3 inputs, got ${inputs.length}`);
  }
  // Signup form order: Full Name, Email, Password (verified via signup-00-loaded.png)
  await inputs[0].fill(FULL_NAME);
  await inputs[1].fill(EMAIL);
  await inputs[2].fill(PASSWORD);
  await page.screenshot({ path: join(OUT_DIR, "signup-01-filled.png"), fullPage: true });
  const submit = page
    .getByRole("button", { name: /sign up|create account|get started|continue|register/i })
    .first();
  await submit.click({ timeout: 10000 });
  try {
    await page.waitForURL((url) => !/\/signup/.test(url.toString()), { timeout: 15000 });
  } catch {
    /* may stay on /signup if validation/error */
  }
  await page.waitForTimeout(2000);
  await page.screenshot({ path: join(OUT_DIR, "signup-02-after-submit.png"), fullPage: true });
  return !/\/signup/.test(page.url());
}

async function authenticate(page) {
  // Fresh signup on every run. full_name + email are timestamped so
  // _slugify(full_name) always produces a new unique org slug.
  try {
    const signedUp = await trySignUp(page);
    if (signedUp) {
      findings.auth.path_taken = "signup";
      findings.auth.status = "ok";
      findings.auth.url_after = page.url();
      return;
    }
  } catch (e) {
    findings.auth.error = `signup threw: ${e}`;
  }
  findings.auth.path_taken = "signup";
  findings.auth.status = "failed";
  findings.auth.url_after = page.url();
}

async function walkConnectionForm(page, connectorType, out) {
  // Observed staging UI (2026-04-14): the /connections page has an inline
  // "New Connection" form already rendered. Type is chosen via a <select>
  // dropdown; fields below the dropdown change to match the selected type.
  // There is no modal, no card picker, no "click Add then pick a card" flow.
  // This is itself a major finding — the landing#139 guides assume a
  // picker-first flow.
  await page.goto(new URL("/connections", BASE_URL).href, { waitUntil: "domcontentloaded" });
  await waitForReflexHydration(page, { timeout: 20000 });
  await page.waitForTimeout(2000);
  await page.screenshot({
    path: join(OUT_DIR, `${connectorType}-00-connections-index.png`),
    fullPage: true,
  });

  // Dump every element in the New Connection card that looks like a
  // type picker so we can figure out what Reflex is rendering.
  const probe = await page.evaluate(() => {
    // Heuristic: find the "New Connection" heading and collect every
    // interactive descendant in the same card.
    const heading = Array.from(document.querySelectorAll("*")).find(
      (el) => el.textContent?.trim() === "New Connection",
    );
    if (!heading) return { error: "no New Connection heading" };
    // Walk up to a reasonable container
    let card = heading;
    for (let i = 0; i < 6 && card.parentElement; i++) card = card.parentElement;
    const picks = [];
    for (const el of card.querySelectorAll("button, select, [role='combobox'], [role='button'], input")) {
      const text = (el.innerText ?? el.value ?? "").slice(0, 40);
      picks.push({
        tag: el.tagName.toLowerCase(),
        role: el.getAttribute("role") ?? "",
        text: text.replace(/\s+/g, " ").trim(),
        name: el.getAttribute("name") ?? "",
        data_test: el.getAttribute("data-test") ?? "",
      });
    }
    return { picks };
  });
  out.notes.push(`probe=${JSON.stringify(probe)}`);

  // The type dropdown is a native <button> whose inner text is the name
  // of the currently-selected connector. On a fresh load it says "postgres";
  // after a prior iteration picks sqlite it says "sqlite", etc. Match any
  // connector-looking name.
  const KNOWN_TYPES_RX = /^(postgres|mysql|sqlite|duckdb|mongodb|mssql|csv|json|parquet|bigquery|snowflake|redshift|clickhouse|s3|stripe|salesforce|hubspot|shopify|google_analytics|kafka|databricks|rest_api|xlsx|tsv)$/i;
  let clicked = false;
  const strategies = [
    () => page.getByRole("button", { name: KNOWN_TYPES_RX }).first(),
    () => page.locator("button").filter({ hasText: KNOWN_TYPES_RX }).first(),
    () => page.getByRole("combobox").first(),
    () => page.locator("select").nth(1), // sidebar lang picker is nth(0)
  ];
  for (const [i, make] of strategies.entries()) {
    try {
      const loc = make();
      await loc.click({ timeout: 3000 });
      out.notes.push(`strategy_${i}_clicked`);
      clicked = true;
      break;
    } catch (e) {
      out.notes.push(`strategy_${i}_failed=${String(e).slice(0, 80)}`);
    }
  }
  if (!clicked) {
    await page.screenshot({ path: join(OUT_DIR, `${connectorType}-no-combo.png`), fullPage: true });
    throw new Error("No type dropdown strategy succeeded; see probe notes");
  }
  await page.waitForTimeout(800);
  // After opening, click the option. Reflex options may be <div role="option">
  // or plain <li> / <div>.
  const optStrategies = [
    () => page.getByRole("option", { name: new RegExp(`^${connectorType}$`, "i") }).first(),
    () => page.locator(`[role='option']`).filter({ hasText: new RegExp(`^${connectorType}$`, "i") }).first(),
    () => page.locator("li, div").filter({ hasText: new RegExp(`^${connectorType}$`, "i") }).first(),
    () => page.getByText(new RegExp(`^${connectorType}$`, "i")).first(),
  ];
  let optClicked = false;
  for (const [i, make] of optStrategies.entries()) {
    try {
      await make().click({ timeout: 3000 });
      out.notes.push(`opt_strategy_${i}_clicked`);
      optClicked = true;
      break;
    } catch (e) {
      out.notes.push(`opt_strategy_${i}_failed=${String(e).slice(0, 80)}`);
    }
  }
  if (!optClicked) {
    await page.screenshot({ path: join(OUT_DIR, `${connectorType}-no-option.png`), fullPage: true });
    throw new Error("No option strategy succeeded");
  }
  await page.waitForTimeout(1500);
  const shot = join(OUT_DIR, `${connectorType}-02-form.png`);
  await page.screenshot({ path: shot, fullPage: true });
  out.screenshot = shot;

  const dump = await dumpFormFields(page);
  out.fields = dump.inputs;
  out.visible_text_head = dump.visible_text_head;
  out.status = "ok";
}

const browser = await chromium.launch();
const context = await browser.newContext({ ignoreHTTPSErrors: true });
const page = await context.newPage();

try {
  await authenticate(page);
} catch (e) {
  findings.auth.status = "failed";
  findings.auth.error = String(e);
  await page.screenshot({ path: join(OUT_DIR, "auth-failed.png"), fullPage: true });
}

if (findings.auth.status === "ok") {
  for (const t of CONNECTOR_TYPES) {
    // Reload fresh each iteration so the dropdown is back at "postgres"
    // (known starting state). Avoids having to track the currently-selected
    // connector label across runs.
    try {
      await walkConnectionForm(page, t, findings[t]);
    } catch (e) {
      findings[t].status = "failed";
      findings[t].error = String(e);
      await page.screenshot({
        path: join(OUT_DIR, `${t}-error.png`),
        fullPage: true,
      });
    }
  }
}

await browser.close();

writeFileSync(
  join(OUT_DIR, "findings.json"),
  JSON.stringify(findings, null, 2),
);
console.log(JSON.stringify(findings, null, 2));
