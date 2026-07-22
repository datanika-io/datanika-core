import { expect, type Page } from "@playwright/test";
import { gotoReady } from "./auth";

/**
 * Builders for the golden path: connections, uploads, runs.
 *
 * These drive the **real UI**, not the API. That distinction is the whole point
 * of core#482: the backend create/run path already had API-level coverage (the
 * nightly connector-smoke matrix, the 2026-07-17 restore check) and both P0s of
 * 2026-07-22 still reached production through it —
 *
 *   - #452 the connections upload handler was annotated bare `list` instead of
 *     `list[rx.UploadFile]`, so the drop zone returned HTTP 500. Reflex locates
 *     the upload param by scanning for a generic alias; no API test touches that.
 *   - #456 `run.*_completed` was emitted with kwargs the registered notification
 *     handlers could not accept, so `emit()` raised *after* the run finished and
 *     the surrounding `except` overwrote it with `fail_run(...)`. The run really
 *     did complete — only the recorded status was wrong.
 *
 * So: click what a user clicks, and assert the status a user sees.
 */

/** A connection type as the picker lists it (see connections.py PICKER_TYPES). */
export type ConnectionKind = "duckdb" | "csv";

/**
 * `ConnectionState.set_form_name` runs `re.sub(r"[^a-zA-Z0-9 ]", "", value)` on
 * every keystroke, so a name with a dash or underscore is silently NOT the name
 * you typed. Mirror that here rather than asserting on the typed string — a test
 * that types `qa-golden-1` and looks for `qa-golden-1` fails for a reason that
 * has nothing to do with the behaviour under test.
 */
export function sanitizeName(value: string): string {
  return value.replace(/[^a-zA-Z0-9 ]/g, "");
}

/**
 * Pick a value from a `searchable_select` (components/searchable_select.py).
 *
 * It is a Radix popover, not a `<select>`: the trigger is a button whose
 * accessible name is the placeholder until something is chosen, and the options
 * are `rx.box`es inside the popover content. We scope option lookup to the
 * popover — the same text ("duckdb") also appears in the connections table, so
 * an unscoped `getByText` matches the row behind the overlay and clicks nothing.
 */
export async function selectSearchable(
  page: Page,
  placeholder: string,
  optionText: string,
): Promise<void> {
  await page.getByRole("button", { name: placeholder, exact: true }).click();

  // Radix portals popover content into a positioned wrapper at the body root.
  const popover = page.locator("[data-radix-popper-content-wrapper]").last();
  await expect(popover).toBeVisible();

  // The filter input is pure frontend JS (rx.script), so typing narrows the
  // list without a round trip. Filtering first keeps the click unambiguous
  // when one option's text is a prefix of another's.
  await popover.getByPlaceholder("Search...").fill(optionText);

  await popover.getByText(optionText, { exact: true }).click();
  await expect(popover).toBeHidden();
}

/**
 * Create a DuckDB destination connection and return the name as saved.
 *
 * `dbPath` must be reachable from BOTH the web container and the Celery worker:
 * the load runs in the worker, so a web-only path fails at run time rather than
 * at save time — the slower, more confusing failure (#471, same reasoning as the
 * `uploaded_files` volume).
 */
export async function createDuckDbConnection(
  page: Page,
  name: string,
  dbPath: string,
): Promise<string> {
  const saved = sanitizeName(name);
  await gotoReady(page, "/connections");

  await page.getByPlaceholder("Connection name").fill(name);
  await selectSearchable(page, "Connection type", "duckdb");
  await page.getByPlaceholder("/data/warehouse.duckdb").fill(dbPath);
  await page.getByRole("button", { name: "Create Connection" }).click();

  await expect(
    page.getByRole("cell", { name: saved, exact: true }),
    `DuckDB connection "${saved}" did not appear in the connections table`,
  ).toBeVisible({ timeout: 15_000 });
  return saved;
}

/**
 * Create a CSV source connection by uploading a file through the drop zone.
 *
 * **This is #452's exact path.** `rx.upload(id="file_upload", no_drag=True)`
 * renders a react-dropzone with a hidden `<input type="file">`; setting files on
 * it fires the same `on_drop` → `handle_file_upload` the button does. The bug
 * returned HTTP 500 on that handler, so asserting the "File uploaded" confirmation
 * is the assertion that matters — not merely that the connection saved.
 */
export async function createCsvConnection(
  page: Page,
  name: string,
  csvPath: string,
): Promise<string> {
  const saved = sanitizeName(name);
  await gotoReady(page, "/connections");

  await page.getByPlaceholder("Connection name").fill(name);
  await selectSearchable(page, "Connection type", "csv");

  await page.locator('input[type="file"]').setInputFiles(csvPath);

  // connections.file_uploaded — proves the upload handler returned 2xx. A bare
  // "connection saved" assertion passes even when the drop zone 500s, because
  // the file path field is optional.
  await expect(
    page.getByText("File uploaded"),
    "CSV upload did not confirm — the connections upload handler may be failing (see #452)",
  ).toBeVisible({ timeout: 20_000 });

  await page.getByRole("button", { name: "Create Connection" }).click();
  await expect(
    page.getByRole("cell", { name: saved, exact: true }),
    `CSV connection "${saved}" did not appear in the connections table`,
  ).toBeVisible({ timeout: 15_000 });
  return saved;
}

/**
 * Create an upload (extract+load) wiring a source connection to a destination.
 *
 * Note there is no "pipeline builder" and no "Configure pipeline" button — the
 * extract-load object is an Upload at `/uploads`. The connector guides describe
 * a UI that does not exist here (landing#272); the UI is the source of truth.
 */
export async function createUpload(
  page: Page,
  name: string,
  sourceOption: string,
  destOption: string,
): Promise<string> {
  const saved = sanitizeName(name);
  await gotoReady(page, "/uploads");

  await page.getByPlaceholder("Upload name").fill(name);
  await selectSearchable(page, "Source connection", sourceOption);
  await selectSearchable(page, "Destination connection", destOption);
  await page.getByRole("button", { name: "Create Upload" }).click();

  await expect(
    page.getByRole("cell", { name: saved, exact: true }),
    `Upload "${saved}" did not appear in the uploads table`,
  ).toBeVisible({ timeout: 15_000 });
  return saved;
}

/**
 * The connection picker on /uploads labels options `"{id} — {name} ({type})"`
 * (upload_state.py). Callers know the name and type but not the id, so match on
 * the stable tail and read the full label back off the DOM.
 */
export async function uploadOptionFor(
  page: Page,
  placeholder: string,
  connectionName: string,
  kind: ConnectionKind,
): Promise<string> {
  await page.getByRole("button", { name: placeholder, exact: true }).click();
  const popover = page.locator("[data-radix-popper-content-wrapper]").last();
  await expect(popover).toBeVisible();

  const suffix = `${connectionName} (${kind})`;
  const option = popover.getByText(new RegExp(`\\d+ — ${suffix}$`));
  await expect(
    option,
    `no ${placeholder} option ending "${suffix}" — is the connection the right type?`,
  ).toBeVisible({ timeout: 10_000 });

  const label = (await option.textContent())?.trim() ?? "";
  await page.keyboard.press("Escape");
  await expect(popover).toBeHidden();
  return label;
}

export type RunOutcome = { status: string; rows: number };

/**
 * Trigger an upload run and wait for it to reach a terminal state.
 *
 * Returns whatever terminal state it reached — deliberately does NOT assert
 * success. The caller asserts, so a failure reads `expected "success", got
 * "failed"` rather than a timeout, and so #465 (nothing announces
 * `status="failed"`) cannot be mistaken for a hung run.
 */
export async function runUploadAndAwait(
  page: Page,
  uploadName: string,
  timeoutMs = 180_000,
): Promise<RunOutcome> {
  await gotoReady(page, "/uploads");
  const row = page.getByRole("row").filter({ hasText: uploadName });
  await expect(row, `upload "${uploadName}" is not listed`).toBeVisible();
  await row.getByRole("button", { name: "Run", exact: true }).click();

  const deadline = Date.now() + timeoutMs;
  let last: RunOutcome = { status: "(no run row appeared)", rows: 0 };

  while (Date.now() < deadline) {
    await gotoReady(page, "/runs");
    const runRow = page.getByRole("row").filter({ hasText: uploadName }).first();

    if (await runRow.isVisible().catch(() => false)) {
      const cells = runRow.getByRole("cell");
      // runs.py column order: ID | Target | Status | Started | Finished | Rows | Error | Logs
      const status = ((await cells.nth(2).textContent()) ?? "").trim();
      const rows = Number(((await cells.nth(5).textContent()) ?? "0").trim() || 0);
      last = { status, rows };
      if (status === "success" || status === "failed") return last;
    }
    await page.waitForTimeout(3_000);
  }
  throw new Error(
    `run for "${uploadName}" did not reach a terminal state within ${timeoutMs}ms ` +
      `(last seen: status="${last.status}", rows=${last.rows}). ` +
      "Is the Celery worker running?",
  );
}
