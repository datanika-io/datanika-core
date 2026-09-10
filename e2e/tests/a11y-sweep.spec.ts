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
 * ── Tier: INFORMATIONAL ──────────────────────────────────────────────────────
 * New spec, so it enters the informational tier and graduates on three consecutive
 * greens on `dev` (`docs/QA_RULES.md` §10) — read from the printed
 * `INFORMATIONAL_RESULT=` line and never from the step's tick, which is masked by
 * `continue-on-error`.
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
import { expect, test } from "@playwright/test";

/** Surfaces reachable without a session. The auth forms are the ones core#720 is about. */
const PUBLIC_SURFACES = [
  { name: "login", path: "/login" },
  { name: "signup", path: "/signup" },
] as const;

const BLOCKING_IMPACTS = new Set(["critical", "serious"]);

type Violation = {
  id: string;
  impact?: string | null;
  help: string;
  nodes: { target: unknown[] }[];
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
  passes: { id: string }[];
  incomplete: { id: string }[];
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

/**
 * The per-page report core#720 AC2 asks for.
 *
 * 🔑 It prints the count **including zero**, on every page, every run. A reporter that
 * only speaks when it finds something cannot be distinguished from one that never ran —
 * which is the failure mode this whole issue is an instance of.
 */
function report(name: string, violations: Violation[]): void {
  const by = (want: boolean) =>
    violations.filter((v) => BLOCKING_IMPACTS.has(v.impact ?? "") === want);
  const blocking = by(true);
  const advisory = by(false);
  console.log(
    `[a11y] ${name}: ${violations.length} violation(s) — ` +
      `${blocking.length} critical/serious, ${advisory.length} moderate/minor`,
  );
  for (const v of violations) {
    console.log(`[a11y]   ${v.impact ?? "unknown"}  ${v.id}  (${v.nodes.length} node(s))  ${v.help}`);
  }
}

test.describe("Accessibility sweep @informational", () => {
  test.setTimeout(120_000);

  for (const surface of PUBLIC_SURFACES) {
    test(`${surface.name} has no critical or serious a11y violations`, async ({ page }) => {
      await page.goto(surface.path);
      // The form must actually be there before the scan means anything. A scan of a page
      // that never rendered its inputs reports zero violations and looks like a pass.
      await expect(page.locator("input").first()).toBeVisible({ timeout: 30_000 });

      const violations = await scan(page);
      report(surface.name, violations);

      const blocking = violations.filter((v) => BLOCKING_IMPACTS.has(v.impact ?? ""));
      expect(
        blocking,
        `${surface.name}: ${blocking.length} critical/serious violation(s):\n` +
          blocking.map((v) => `  ${v.impact} ${v.id}: ${v.help}`).join("\n"),
      ).toHaveLength(0);
    });
  }

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
