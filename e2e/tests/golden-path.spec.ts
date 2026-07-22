import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { test, expect, signUp } from "../fixtures/auth";
import {
  createCsvConnection,
  createDuckDbConnection,
  createUpload,
  runUploadAndAwait,
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
 *
 * ── Tier: @informational (core#521, supersedes the #520 quarantine) ──────────
 * This spec runs on every push to `dev` and uploads artifacts, but its result
 * does NOT hold the promotion. It was authored 2026-07-22 and has not yet
 * passed; a test with no track record was never protecting production, and
 * letting it gate meant a spec written that morning held back a P0 the same
 * afternoon (#520).
 *
 * The line, and it is load-bearing: a spec that USED TO PASS must stay gating —
 * demoting one hides a regression. This tier is for specs that have not yet
 * earned in, never a bolt-hole for ones that broke.
 *
 * **Graduation: 3 consecutive greens on `dev` → remove `@informational`.** The
 * run log prints `INFORMATIONAL_RESULT=` each time, and a green emits a CI
 * warning, so the evidence is mechanical rather than remembered. Procedure and
 * owner: `plans/qa/PLAN_QA.md` §E2E tiers.
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

// Artifacts unconditionally while this spec is @informational (core#521).
//
// The global config is `retain-on-failure`, which would in fact cover a genuine
// failure — this is belt and braces, deliberately. An informational spec exists
// to produce evidence, and its whole value is the trace of the round it is
// currently losing; every fix so far (#508, #515, #529) came out of one. Losing
// that to a config nuance would leave a spec that runs and teaches nothing.
//
// (It previously read "while quarantined (core#520)" and justified itself by
// `test.fail()` classifying failures as `expected`. That marker is gone — the
// tier replaced it — so the reasoning is restated rather than left stale.)
//
// Must be file top-level, NOT inside the describe: `use({ trace })` forces a new
// worker, and Playwright refuses it in a describe group — which fails collection
// of the WHOLE suite, not just this spec. Caught by running `--list`; it would
// have taken all 62 tests down.
test.use({ trace: "on", screenshot: "on", video: "retain-on-failure" });

test.describe("Golden path: signup → connection → pipeline → run @slow @informational", () => {
  // Signup, two connections, an upload and a Celery round trip. The default
  // 60s covers none of that. Deliberately NOT larger: every interaction is
  // individually bounded in fixtures/data.ts, so this cap should never be what
  // fails — if it is, something is hanging that no per-action timeout covers.
  test.setTimeout(300_000);

  test("new user signs up, wires CSV → DuckDB, runs it, and sees rows land", async ({ page }) => {

    const stamp = `${Date.now()}`;
    // set_form_name strips everything outside [a-zA-Z0-9 ], so keep names in
    // that alphabet — otherwise the saved name is not the one typed.
    const destName = `qa golden dest ${stamp}`;
    const srcName = `qa golden src ${stamp}`;
    const uploadName = `qa golden upload ${stamp}`;

    // Each phase is a named step so a failure reports WHICH phase broke. The
    // first CI run of this spec died as an unqualified 6-minute timeout and the
    // report could not say where — a result that costs a full run to learn
    // nothing (core#501).
    await test.step("1. sign up and reach the app", async () => {
      // signUp() fills the form (incl. the required Full Name) and handles the
      // Reflex hydration race — it retries if the click falls back to a native
      // GET submit before on_submit is wired (core#295).
      await signUp(page);
      await expect(page).toHaveURL(/\/(dashboard|connections|onboarding)?$/);
      await expect(page.getByRole("link", { name: "Connections" }).first()).toBeVisible({
        timeout: 10_000,
      });
    });

    const savedDest = await test.step("2. create the DuckDB destination", async () =>
      createDuckDbConnection(page, destName, `${SHARED_DIR}/qa_golden_${stamp}.duckdb`));

    const savedSrc = await test.step("3. create the CSV source via the drop zone", async () => {
      const csvPath = join(mkdtempSync(join(tmpdir(), "qa-golden-")), "orders.csv");
      writeFileSync(csvPath, CSV_CONTENT, "utf8");
      return createCsvConnection(page, srcName, csvPath);
    });

    const savedUpload = await test.step("4. wire them together as an Upload", async () =>
      createUpload(
        page,
        uploadName,
        { name: savedSrc, kind: "csv" },
        { name: savedDest, kind: "duckdb" },
      ));

    const outcome = await test.step("5. run it and read the terminal status", async () =>
      runUploadAndAwait(page, savedUpload));

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
