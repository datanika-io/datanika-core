import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { expect, signUp, test } from "../fixtures/auth";
import { gotoReady } from "../fixtures/auth";
import {
  createCsvConnection,
  createDuckDbConnection,
  createTransformation,
  createUpload,
  runUploadAndAwait,
} from "../fixtures/data";
import { clickAndAwaitReflexEvent } from "../fixtures/reflex-wire";

/**
 * A saved /dag edge from an upload to a transformation, then the upload run.
 *
 * Tier: INFORMATIONAL. The marker is on this describe, the same place as the
 * transformation half of golden-path.spec.ts. `--grep-invert "@informational"`
 * leaves it out of the promotion gate. `@slow` because it signs up and waits
 * on the upload's Celery run.
 *
 * The runs table has no cause column. The target carries `caused_by_run_id`
 * as `data-caused-by`. The assertion reads that attribute and compares it to
 * the upload run's id. A transformation row with an empty attribute is a run
 * that exists without that upload.
 *
 * The poll refreshes /runs. The check-timeframe schedule stays in the unit
 * tests. This signup creates no schedule, so the starter's active-schedule
 * skip does not apply.
 *
 * Labels come from en.json. The option values (`upload`, `transformation`,
 * `minutes`) are the values dag.py puts on the controls.
 */

const en = JSON.parse(
  readFileSync(join(__dirname, "../../datanika/i18n/en.json"), "utf8"),
) as Record<string, string>;

function label(key: string): string {
  const value = en[key];
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`en.json is missing ${key}`);
  }
  return value;
}

const ADD_DEPENDENCY = label("dag.add_dependency");
const UPSTREAM_TYPE = label("dag.upstream_type");
const DOWNSTREAM_TYPE = label("dag.downstream_type");
const UPSTREAM_NAME = label("dag.upstream_name");
const DOWNSTREAM_NAME = label("dag.downstream_name");
const CHECK_TIMEFRAME = label("dag.check_timeframe");
const ADD = label("common.add");

const CSV_ROWS = 3;
const CSV_CONTENT = ["id,name,amount", "1,alpha,100", "2,beta,200", "3,gamma,300", ""].join("\n");
const SHARED_DIR = process.env.DATANIKA_E2E_SHARED_DIR ?? "/app/uploaded_files";
const TIMEFRAME_MINUTES = "30";

test.use({ trace: "on", screenshot: "on", video: "retain-on-failure" });

function datasetNameForUpload(uploadName: string): string {
  return uploadName.trim().replace(/\s+/g, "_").toLowerCase();
}

test.describe(
  "Saved dependency: a successful upload starts its downstream transformation @slow @informational",
  () => {
    test.setTimeout(480_000);

    test("the upload run is the cause of the transformation run", async ({ page }) => {
      const stamp = `${Date.now()}`;
      const destName = `qa dep dest ${stamp}`;
      const srcName = `qa dep src ${stamp}`;
      const uploadName = `qa dep upload ${stamp}`;
      const modelName = `qa_dep_${stamp}`;

      await test.step("1. sign up and reach the app", async () => {
        await signUp(page);
        await expect(page).toHaveURL(/\/(dashboard|connections|onboarding)?$/);
        await expect(page.getByRole("link", { name: "Connections" }).first()).toBeVisible({
          timeout: 10_000,
        });
      });

      const savedDest = await test.step("2. create the DuckDB destination", async () =>
        createDuckDbConnection(page, destName, `${SHARED_DIR}/qa_dep_${stamp}.duckdb`));

      const savedSrc = await test.step("3. create the CSV source via the drop zone", async () => {
        const csvPath = join(mkdtempSync(join(tmpdir(), "qa-dep-")), "orders.csv");
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

      const dataset = datasetNameForUpload(savedUpload);
      const sql = [
        "select id, name, amount * 10 as amount_x10",
        `from {{ source('${dataset}', 'orders') }}`,
      ].join("\n");

      await test.step("5. save a transformation over that upload", async () =>
        createTransformation(page, modelName, { name: savedDest, kind: "duckdb" }, sql));

      await test.step("6. save the dependency with a check timeframe", async () => {
        await gotoReady(page, "/dag");
        await expect(page.getByRole("heading", { name: ADD_DEPENDENCY })).toBeVisible();

        await page.getByRole("combobox", { name: UPSTREAM_TYPE }).selectOption("upload");
        await page.getByRole("combobox", { name: DOWNSTREAM_TYPE }).selectOption("transformation");

        await page.getByLabel(UPSTREAM_NAME).click();
        await clickAndAwaitReflexEvent(
          page,
          page.locator("#dag-upstream-popover-box").getByText(savedUpload, { exact: true }),
          {
            handlerMatch: "select_upstream_suggestion",
            what: `the upstream suggestion "${savedUpload}"`,
          },
        );
        await expect(page.getByLabel(UPSTREAM_NAME)).toHaveValue(savedUpload);

        await page.getByLabel(DOWNSTREAM_NAME).click();
        await clickAndAwaitReflexEvent(
          page,
          page.locator("#dag-downstream-popover-box").getByText(modelName, { exact: true }),
          {
            handlerMatch: "select_downstream_suggestion",
            what: `the downstream suggestion "${modelName}"`,
          },
        );
        await expect(page.getByLabel(DOWNSTREAM_NAME)).toHaveValue(modelName);

        await expect(page.getByText(CHECK_TIMEFRAME, { exact: true })).toBeVisible();
        await page.getByRole("spinbutton").fill(TIMEFRAME_MINUTES);
        await page.getByRole("combobox", { name: CHECK_TIMEFRAME }).selectOption("minutes");

        await clickAndAwaitReflexEvent(page, page.getByRole("button", { name: ADD, exact: true }), {
          handlerMatch: "add_dependency",
          what: `the ${ADD} button on /dag`,
        });

        const edge = page
          .getByRole("row")
          .filter({ hasText: `upload: ${savedUpload}` })
          .filter({ hasText: `transformation: ${modelName}` });
        await expect(edge).toBeVisible();
        await expect(
          edge.getByRole("cell", { name: `${TIMEFRAME_MINUTES} minutes`, exact: true }),
        ).toBeVisible();
      });

      const outcome = await test.step("7. run the upload and read the terminal status", async () =>
        runUploadAndAwait(page, savedUpload));

      expect(outcome.status, "the upload run did not succeed").toBe("success");
      expect(outcome.rows, "the upload reported fewer rows than the CSV contains").toBeGreaterThanOrEqual(
        CSV_ROWS,
      );

      const uploadRunId = await test.step("8. read the upload run id", async () => {
        const uploadRow = page.getByRole("row").filter({ hasText: `upload: ${savedUpload}` });
        await expect(uploadRow).toBeVisible();
        const id = ((await uploadRow.getByRole("cell").first().textContent()) ?? "").trim();
        expect(id, "the upload row's first cell is its run id").toMatch(/^\d+$/);
        return id;
      });

      await test.step("9. the transformation run's cause is that upload", async () => {
        const deadline = Date.now() + 60_000;
        let sawRow = false;
        let cause = "";
        while (Date.now() < deadline) {
          const xformRow = page
            .getByRole("row")
            .filter({ hasText: `transformation: ${modelName}` });
          if (await xformRow.isVisible().catch(() => false)) {
            sawRow = true;
            cause =
              (await xformRow.locator("[data-caused-by]").getAttribute("data-caused-by")) ?? "";
            if (cause === uploadRunId) return;
            if (cause !== "") {
              throw new Error(
                `transformation run cause is "${cause}" and the upload run is "${uploadRunId}"`,
              );
            }
          }
          await page.waitForTimeout(2_000);
          await gotoReady(page, "/runs");
        }
        throw new Error(
          sawRow
            ? `the transformation run for "${modelName}" is on /runs and data-caused-by is empty. The upload run is ${uploadRunId}.`
            : `upload run ${uploadRunId} reached success and no transformation run for "${modelName}" appeared within 60s. The starter writes that row in the same commit as the upload success.`,
        );
      });
    });
  },
);
