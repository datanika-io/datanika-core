import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { test, expect, signUp } from "../fixtures/auth";
import {
  createCsvConnection,
  createDuckDbConnection,
  createUpload,
  runUploadAndAwait,
  uploadOptionFor,
} from "../fixtures/data";

/**
 * Golden path: a new user signs up, creates a connection, uploads a CSV,
 * runs a pipeline, and sees data land in the destination.
 *
 * This is the single most important test in the suite. If this is green,
 * the critical revenue-producing flow works end to end. If this is red,
 * no other E2E result matters.
 *
 * ── Why steps 2–5 are code now (core#482) ────────────────────────────────────
 * They used to be a comment. The deferral said the backend create/run path was
 * "covered by the nightly connector-smoke matrix and re-verified via API in the
 * 2026-07-17 restore check" — both API-level. On 2026-07-22 two P0s reached
 * production straight through the gap between that coverage and a browser:
 *
 *   #452  the connections upload handler returned HTTP 500 (Reflex could not
 *         find the upload param behind a bare `list` annotation)
 *   #456  every successful run was recorded FAILED — `emit()` raised on the
 *         completion hooks and the surrounding `except` called `fail_run(...)`
 *
 * Both were found by a person clicking through prod, while `e2e-staging`
 * reported success on every push. Hence the shape of this spec: drive the UI a
 * user drives, and assert the terminal run status a user sees.
 *
 * #456 is why `expect(status).toBe("success")` is the assertion and "a run row
 * appeared" is not: the run *did* complete — only its recorded status was
 * wrong. A test that waited for a run to exist would have passed throughout.
 *
 * ── Cost ─────────────────────────────────────────────────────────────────────
 * Tagged @slow: signup + two connections + an upload + a real Celery run.
 * @slow specs are excluded unless DATANIKA_E2E_SLOW=1 — which for a long time
 * no CI job set, so nothing here ran at all (core#484). If you are reading this
 * because the job is slow, the fix is a smaller fixture — not dropping the tag,
 * and not un-setting the flag.
 */

const CSV_ROWS = 3;
const CSV_CONTENT = ["id,name,amount", "1,alpha,100", "2,beta,200", "3,gamma,300", ""].join("\n");

/**
 * A path both the web container and the Celery worker can reach.
 * `/app/uploaded_files` is a named volume mounted rw by app, app_b and celery
 * (docker-compose.yml, #471); anything web-only fails at run time rather than
 * at save time — the slower, more confusing failure.
 */
const SHARED_DIR = process.env.DATANIKA_E2E_SHARED_DIR ?? "/app/uploaded_files";

test.describe("Golden path: signup → connection → pipeline → run @slow", () => {
  // Signup, two connections, an upload and a Celery round trip. The default
  // 60s budget covers none of that; runUploadAndAwait alone allows 180s.
  test.setTimeout(360_000);

  test("new user signs up, wires CSV → DuckDB, runs it, and sees rows land", async ({ page }) => {
    const stamp = `${Date.now()}`;
    // set_form_name strips everything outside [a-zA-Z0-9 ], so keep names in
    // that alphabet — otherwise the saved name is not the one typed.
    const destName = `qa golden dest ${stamp}`;
    const srcName = `qa golden src ${stamp}`;
    const uploadName = `qa golden upload ${stamp}`;

    // 1. Signup. signUp() fills the form (incl. the required Full Name) and
    //    handles the Reflex hydration race — it retries if the click falls back
    //    to a native GET submit before on_submit is wired (core#295).
    await signUp(page);
    await expect(page).toHaveURL(/\/(dashboard|connections|onboarding)?$/);
    await expect(page.getByRole("link", { name: "Connections" }).first()).toBeVisible({
      timeout: 10_000,
    });

    // 2. Destination: a DuckDB file on the shared volume.
    const savedDest = await createDuckDbConnection(
      page,
      destName,
      `${SHARED_DIR}/qa_golden_${stamp}.duckdb`,
    );

    // 3. Source: a CSV uploaded through the drop zone — #452's path.
    const csvPath = join(mkdtempSync(join(tmpdir(), "qa-golden-")), "orders.csv");
    writeFileSync(csvPath, CSV_CONTENT, "utf8");
    const savedSrc = await createCsvConnection(page, srcName, csvPath);

    // 4. Wire them together as an Upload (there is no "pipeline builder" UI).
    const sourceOption = await uploadOptionFor(page, "Source connection", savedSrc, "csv");
    const destOption = await uploadOptionFor(page, "Destination connection", savedDest, "duckdb");
    const savedUpload = await createUpload(page, uploadName, sourceOption, destOption);

    // 5. Run it, and assert the status a user would see.
    const outcome = await runUploadAndAwait(page, savedUpload);

    expect(
      outcome.status,
      "the run did not end in `success` — a completed-but-FAILED run is #456's " +
        "signature: check whether emit() raised on the completion hooks after " +
        "the run had already finished",
    ).toBe("success");

    expect(
      outcome.rows,
      `expected the ${CSV_ROWS} CSV rows to land in DuckDB, got ${outcome.rows}. ` +
        "A `success` run that moved zero rows is a load that silently did nothing.",
    ).toBeGreaterThanOrEqual(CSV_ROWS);
  });
});
