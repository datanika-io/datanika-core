/**
 * Accessibility sweep — core#720.
 *
 * ── Why this exists ──────────────────────────────────────────────────────────
 * Three E2E specs failed deterministically for months with
 * `locator.fill: Test timeout 60000ms exceeded`. The cause was not a flake and not a
 * selector style: `page.getByLabel(/email/i)` matches `<label>`, `aria-label` or
 * `aria-labelledby`, and the Reflex auth forms rendered `rx.text("Email")` as a sibling
 * `<p>` before an unlabelled `rx.input`. **The inputs had no accessible name at all** —
 * a screen reader announced nothing.
 *
 * That was found because a *test harness* tripped over it, not because anything checked.
 * Found by accident, by a tool looking for something else, is the definition of unmeasured.
 *
 * ── Surfaces: the nine core#720 names ────────────────────────────────────────
 * * **public app** — login, signup (the forms this issue was filed about);
 * * **behind the login** — the connections list, the connection form with a connector's
 *   fields rendered, the upload wizard, settings;
 * * **landing** — home, pricing, docs index, scanned at the landing site rather than at this
 *   suite's baseURL, which is the app.
 *
 * ── Tier: INFORMATIONAL ──────────────────────────────────────────────────────
 * New spec, so it enters the informational tier and graduates on three consecutive
 * greens on `dev` (`docs/QA_RULES.md` §10) — read from the printed
 * `INFORMATIONAL_RESULT=` line and never from the step's tick, which is masked by
 * `continue-on-error`. Every test here inherits the tier from the outer describe's title.
 *
 * 🚨 That is not paperwork here. A brand-new a11y sweep on an app that has never had one
 * surfaces a long tail, and a legitimately-red gating test makes "loosen the assertion"
 * the cheapest route to a merge. core#720 says this in as many words.
 *
 * ── Severity policy ──────────────────────────────────────────────────────────
 * **Fail on Critical and Serious. Report Moderate and Minor.** Deliberately not
 * stricter on day one: a gate that must be loosened on the day it lands teaches
 * everyone that gates get loosened.
 */

import AxeBuilder from "@axe-core/playwright";
import type { Page } from "@playwright/test";

import { expect, gotoReady, test } from "../fixtures/auth";

/** Surfaces reachable without a session. The auth forms are the ones core#720 is about. */
const PUBLIC_SURFACES = [
  { name: "login", path: "/login" },
  { name: "signup", path: "/signup" },
] as const;

/**
 * Surfaces behind the login.
 *
 * 🔑 **The connection form is visited twice, and the second visit is the one that matters.**
 * With no connector chosen the form renders a name input and a type picker and nothing else —
 * the ~80 connector fields core#720's static ratchet counts behind `secure_input.py` exist only
 * once a type is selected. `?template=postgres-to-bigquery` preselects PostgreSQL through the
 * same `on_load` handler `template-prefill.spec.ts` drives, so the fields render without driving
 * a searchable select.
 *
 * `mustRead` names an input axe must have **evaluated** on that page. Without it, "0 violations"
 * is also what a form that never rendered its fields reports — the failure mode this whole issue
 * is an instance of.
 */
const AUTHENTICATED_SURFACES = [
  { name: "connections list", path: "/connections", mustRead: "#cfg-name" },
  {
    name: "connection form (PostgreSQL fields)",
    path: "/connections?template=postgres-to-bigquery",
    mustRead: "#cfg-host",
  },
  { name: "upload wizard", path: "/uploads", mustRead: null },
  { name: "settings", path: "/settings", mustRead: null },
] as const;

/** Landing pages live on another deployment. Overridable for a local landing build. */
const LANDING_BASE = process.env.DATANIKA_E2E_LANDING_URL ?? "https://datanika.io";
const LANDING_SURFACES = [
  { name: "landing home", path: "/" },
  { name: "landing pricing", path: "/pricing" },
  { name: "landing docs index", path: "/docs" },
] as const;

const BLOCKING_IMPACTS = new Set(["critical", "serious"]);

type NodeResult = { target: unknown[] };

type Violation = {
  id: string;
  impact?: string | null;
  help: string;
  nodes: NodeResult[];
};

/**
 * 🚨 `best-practice` is in this list because WITHOUT it this sweep could not have caught the
 * defect it was built for.
 *
 * core#720 exists because the auth inputs had **no accessible name at all**. Read out of the
 * installed axe-core, the `label` rule's `any:` is:
 *
 *     [implicit-label, explicit-label, aria-label, aria-labelledby,
 *      non-empty-title, non-empty-placeholder, presentational-role]
 *
 * **`non-empty-placeholder` satisfies it**, and `login.py`'s input has a placeholder — so the
 * pre-fix inputs would have passed a wcag-only scan. The rule that catches placeholder-only
 * labelling is `label-title-only`, whose tags are `["cat.forms", "best-practice"]`, and the
 * original tag set excluded it.
 *
 * Measured: this adds 30 best-practice rules. That is a lot of new *reporting* on an app never
 * swept before, and it is wanted — the severity policy still fails only on critical/serious, so
 * the tail becomes visible without becoming a gate.
 */
const TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "best-practice"];

/** The rule that covers this issue's own defect class. Asserted to have RUN, not merely listed. */
const FORM_LABEL_RULE = "label-title-only";

type AxeResult = {
  violations: Violation[];
  passes: { id: string; nodes: NodeResult[] }[];
  incomplete: { id: string; nodes: NodeResult[] }[];
  inapplicable: { id: string }[];
};

async function scanFull(page: Page): Promise<AxeResult> {
  const result = await new AxeBuilder({ page }).withTags(TAGS).analyze();
  return result as unknown as AxeResult;
}

async function scan(page: Page): Promise<Violation[]> {
  return (await scanFull(page)).violations;
}

/** Every rule axe actually evaluated — a rule that ran lands in exactly one of these four. */
function rulesThatRan(r: AxeResult): Set<string> {
  return new Set(
    [...r.violations, ...r.passes, ...r.incomplete, ...r.inapplicable].map((x) => x.id),
  );
}

/** The rules axe evaluated ON one element — the proof that a scan reached it. */
function rulesEvaluatedOn(r: AxeResult, selector: string): string[] {
  return [...r.violations, ...r.passes, ...r.incomplete]
    .filter((rule) => rule.nodes.some((node) => node.target.some((t) => String(t) === selector)))
    .map((rule) => rule.id)
    .sort();
}

/**
 * The per-page report core#720 AC2 asks for.
 *
 * 🔑 It prints the count **including zero**, on every page, every run — and the number of rules
 * evaluated beside it. A reporter that only speaks when it finds something cannot be
 * distinguished from one that never ran, which is the failure mode this whole issue is an
 * instance of.
 */
function report(name: string, result: AxeResult): void {
  const violations = result.violations;
  const by = (want: boolean) =>
    violations.filter((v) => BLOCKING_IMPACTS.has(v.impact ?? "") === want);
  const blocking = by(true);
  const advisory = by(false);
  console.log(
    `[a11y] ${name}: ${violations.length} violation(s) — ` +
      `${blocking.length} critical/serious, ${advisory.length} moderate/minor ` +
      `(rules evaluated: ${rulesThatRan(result).size})`,
  );
  for (const v of violations) {
    // The first few targets make a violation routable to whoever owns the markup — core#720
    // asks for per-surface work grouped by owner, not one bulk item.
    const sample = v.nodes
      .slice(0, 3)
      .map((node) => node.target.map(String).join(" "))
      .join(" | ");
    console.log(
      `[a11y]   ${v.impact ?? "unknown"}  ${v.id}  (${v.nodes.length} node(s))  ${v.help}` +
        (sample ? `  e.g. ${sample}` : ""),
    );
  }
}

function expectNoBlocking(name: string, result: AxeResult): void {
  const blocking = result.violations.filter((v) => BLOCKING_IMPACTS.has(v.impact ?? ""));
  expect(
    blocking,
    `${name}: ${blocking.length} critical/serious violation(s):\n` +
      blocking.map((v) => `  ${v.impact} ${v.id}: ${v.help}`).join("\n"),
  ).toHaveLength(0);
}

test.describe("Accessibility sweep @informational", () => {
  test.setTimeout(120_000);

  for (const surface of PUBLIC_SURFACES) {
    test(`${surface.name} has no critical or serious a11y violations`, async ({ page }) => {
      await page.goto(surface.path);
      // The form must actually be there before the scan means anything. A scan of a page
      // that never rendered its inputs reports zero violations and looks like a pass.
      await expect(page.locator("input").first()).toBeVisible({ timeout: 30_000 });

      const result = await scanFull(page);
      report(surface.name, result);
      expectNoBlocking(surface.name, result);
    });
  }

  for (const surface of AUTHENTICATED_SURFACES) {
    test(`${surface.name} has no critical or serious a11y violations`, async ({
      loggedInPage: page,
    }) => {
      await gotoReady(page, surface.path);

      // An expired session or a role that cannot edit renders a DIFFERENT page — /login, or
      // this page without its form. Scanning that and reporting it under this surface's name
      // is a clean result about the wrong page, so both are asserted before the scan.
      const pathname = surface.path.split("?")[0];
      await expect(page, `${surface.name}: the session did not stay on ${pathname}`).toHaveURL(
        new RegExp(`${pathname}(\\?|$)`),
        { timeout: 15_000 },
      );
      const anchor = page.locator(surface.mustRead ?? "input").first();
      await expect(
        anchor,
        `${surface.name}: ${surface.mustRead ?? "no input"} rendered — the scan would read an empty page`,
      ).toBeVisible({ timeout: 30_000 });

      const result = await scanFull(page);
      report(surface.name, result);

      if (surface.mustRead) {
        const evaluated = rulesEvaluatedOn(result, surface.mustRead);
        console.log(`[a11y]   rules evaluated on ${surface.mustRead}: ${evaluated.join(", ") || "NONE"}`);
        expect(
          evaluated,
          `${surface.name}: axe did not evaluate \`${FORM_LABEL_RULE}\` on ${surface.mustRead}. ` +
            "The element is visible, so either axe is not reaching it or the rule no longer " +
            `applies to it. Rules it did evaluate there: ${evaluated.join(", ") || "none"}.`,
        ).toContain(FORM_LABEL_RULE);
      }

      expectNoBlocking(surface.name, result);
    });
  }

  test.describe("landing pages", () => {
    // Another deployment, reached directly. No session — and no CF Access headers either: those
    // authorise the staging app, and nothing else should be handed them.
    test.use({ extraHTTPHeaders: {} });

    for (const surface of LANDING_SURFACES) {
      test(`${surface.name} has no critical or serious a11y violations`, async ({ page }) => {
        const url = new URL(surface.path, LANDING_BASE).toString();
        const response = await page.goto(url);
        expect(response?.status(), `${url} did not answer 200`).toBe(200);
        // A heading of EITHER level proves the content rendered. Not `h1` alone: the first run
        // of this sweep found /pricing serving no <h1> at all, and a readiness check that
        // presumes one turns an accessibility FINDING into a harness failure that never scans.
        // axe's own `page-has-heading-one` is what reports a missing <h1>.
        await expect(page.locator("h1, h2").first(), `${url} rendered no heading`).toBeVisible({
          timeout: 30_000,
        });

        const result = await scanFull(page);
        report(surface.name, result);
        expectNoBlocking(surface.name, result);
      });
    }
  });

  /**
   * The rule that covers this issue's own defect class must actually RUN.
   *
   * 🔑 This exists because the first version of this sweep silently did not run it: a tag set of
   * wcag-only excludes `label-title-only`, which is `best-practice`. The sweep would have passed
   * the exact inputs core#720 was filed about. Asserting the rule is *evaluated* — not that it
   * passes — is what stops a future tag edit reopening that gap without anyone noticing.
   */
  test(`the ${FORM_LABEL_RULE} rule is actually evaluated`, async ({ page }) => {
    await page.goto("/login");
    await expect(page.locator("input").first()).toBeVisible({ timeout: 30_000 });

    const ran = rulesThatRan(await scanFull(page));
    expect(
      ran.has(FORM_LABEL_RULE),
      `axe did not evaluate \`${FORM_LABEL_RULE}\` on /login. It is tagged best-practice, so a ` +
        `wcag-only tag set switches it off — and with it the only rule that catches an input ` +
        `labelled solely by its placeholder, which is precisely core#720's defect. ` +
        `Rules that ran: ${[...ran].sort().slice(0, 12).join(", ")}…`,
    ).toBe(true);

    // Control: the scan is genuinely evaluating a broad rule set, not one rule by accident.
    expect(ran.size, `only ${ran.size} rule(s) evaluated — the tag set is not being applied`)
      .toBeGreaterThan(20);
  });

  /**
   * core#720 AC1 — the forced red, and the only thing that makes the greens above mean
   * anything: *"strip the `html_for` off one input and watch the scan name that input.
   * If it stays green, the scan is not reaching the rendered form."*
   *
   * ⚠️ The attribute is removed from the **rendered DOM** rather than from the source.
   * That is deliberate and it is the stronger arming: it needs no rebuild, and it proves
   * the scan reads *this page as served* and reacts to a change in it. A source edit
   * would prove the same thing one build removed.
   */
  test("ARMING: removing a label's for= makes the scan name that input", async ({ page }) => {
    await page.goto("/login");
    await expect(page.locator("input").first()).toBeVisible({ timeout: 30_000 });

    const before = await scan(page);
    const beforeLabelIssues = before.filter((v) => v.id === "label").length;

    const stripped = await page.evaluate(() => {
      const label = document.querySelector("label[for]");
      if (!label) return null;
      const id = label.getAttribute("for");
      label.removeAttribute("for");
      // 🚨 EVERY route to an accessible name, not just the label. The first version of this
      // arming removed `for`, `aria-label` and `aria-labelledby` and reported
      // `before=0 after=0` — because axe's `label` rule also accepts a non-empty
      // `placeholder` or `title`, and `login.py`'s input has a placeholder. Removing one
      // route while another survives tests nothing.
      const input = id ? document.getElementById(id) : null;
      for (const attr of ["aria-label", "aria-labelledby", "placeholder", "title"]) {
        input?.removeAttribute(attr);
      }
      return id;
    });

    expect(
      stripped,
      "no <label for=...> on /login to strip — either the fix for core#720's original " +
        "defect has regressed, or this page no longer uses html_for and this arming " +
        "must be re-pointed rather than deleted",
    ).not.toBeNull();

    const after = await scan(page);
    const afterLabelIssues = after.filter((v) => v.id === "label").length;

    console.log(
      `[a11y] ARMING: label violations before=${beforeLabelIssues} after=${afterLabelIssues} ` +
        `(stripped for="${stripped}")`,
    );

    // The discriminating assertion: the scan must NOTICE. Comparing before-and-after rather
    // than asserting `after > 0` means a page that was already failing cannot satisfy it.
    //
    // ⚠️ The message names BOTH causes deliberately. Its first version said only "the scan is
    // not reaching the rendered form", and when this test did fire that was FALSE — the scan
    // had found colour-contrast and image-alt violations on the same page. A message that
    // names one cause sends the reader past the other.
    expect(
      afterLabelIssues,
      "stripping the label's for= (and aria-label/aria-labelledby/placeholder/title) produced " +
        "no new `label` violation. Either the scan is not reaching the rendered form, OR the " +
        "input still has an accessible name by a route this arming does not remove. Check the " +
        "second first: the other violations reported above are evidence the scan DID reach it.",
    ).toBeGreaterThan(beforeLabelIssues);
  });
});
