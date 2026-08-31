"""Detect flaky GATING specs in a Playwright JSON report — core#757.

## Why this exists

`e2e/playwright.config.ts` sets ``retries: process.env.CI ? 1 : 0``. A gating spec
that fails and then passes on retry is reported by Playwright as ``flaky`` and
Playwright exits **0**. The step succeeds, the job succeeds, and every alert in
``ci.yml`` is guarded by ``if: failure()`` — so an intermittent product bug
produces a green run, no Telegram alert and no filed issue.

That is how [core#744] reached production: `de00365` was `1 flaky / 16 skipped /
45 passed`, concluded ``success``, and was promoted. `docs/QA_RULES.md` §12
already forbids retrying an assertion; the pipeline did not implement it.

## The three states, and why "clean" is not the default

An empty report must NOT read as clean. This is not hypothetical — the
`playwright-report` artifact really uploaded for `de00365` is::

    {"files":[],"stats":{"total":0,"expected":0,"unexpected":0,"flaky":0,
     "skipped":0,"ok":true}}

`total: 0`, `flaky: 0`, `ok: true`, for a run that genuinely had one flaky
gating spec — because the three ``npx playwright test --list`` invocations in the
"Assert the @slow specs were actually collected" step run *after* the tests and
regenerate the report folder from a zero-test run. A detector that reported
"0 flaky, all good" on that input would reproduce the very defect it exists to
catch, one level up.

So:

===============  ==========================================================
``flaky``        at least one gating spec was flaky — alert and file
``clean``        the report has tests and none of them were flaky
``no-evidence``  the report has no tests at all, or could not be read
===============  ==========================================================

Only ``clean`` is a green. ``no-evidence`` is neither — exactly like a skip.

## Scope

Only GATING specs count. A spec is informational when ``@informational`` appears
in its own title or in any enclosing ``describe`` title (the tier policy puts the
marker in the describe title). Informational specs do not hold the gate, so a
flaky one is not news.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

INFORMATIONAL_MARKER = "@informational"


def iter_specs(node: dict, ancestry: tuple[str, ...] = ()) -> list[tuple[str, dict]]:
    """Walk the suite tree, yielding (full_title, spec) for every spec.

    Playwright nests ``suites`` arbitrarily deep (file suite -> describe ->
    nested describe). The tier marker lives in a describe title, so the full
    ancestry is what has to be tested, not the spec's own title.
    """
    found: list[tuple[str, dict]] = []
    title = node.get("title", "")
    here = ancestry + ((title,) if title else ())

    for spec in node.get("specs", []) or []:
        full = " > ".join([*here, spec.get("title", "")])
        found.append((full, spec))

    for child in node.get("suites", []) or []:
        found.extend(iter_specs(child, here))

    return found


def spec_is_flaky(spec: dict) -> bool:
    """True when any test in the spec ended with Playwright's ``flaky`` status.

    ``flaky`` means: failed at least once, then passed on retry. That is the
    status that exits 0 and produces a green run.
    """
    return any(test.get("status") == "flaky" for test in spec.get("tests", []) or [])


def analyse(report: dict) -> dict:
    """Classify a Playwright JSON report into one of the three states."""
    specs: list[tuple[str, dict]] = []
    for suite in report.get("suites", []) or []:
        specs.extend(iter_specs(suite))

    gating = [(t, s) for t, s in specs if INFORMATIONAL_MARKER not in t]
    informational = [(t, s) for t, s in specs if INFORMATIONAL_MARKER in t]

    flaky_gating = sorted(t for t, s in gating if spec_is_flaky(s))
    flaky_informational = sorted(t for t, s in informational if spec_is_flaky(s))

    if not specs:
        status = "no-evidence"
    elif flaky_gating:
        status = "flaky"
    else:
        status = "clean"

    return {
        "status": status,
        "total_specs": len(specs),
        "gating_specs": len(gating),
        "informational_specs": len(informational),
        "flaky_gating": flaky_gating,
        "flaky_informational": flaky_informational,
    }


def load(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("report", type=Path, help="Playwright JSON report from the GATING run")
    ap.add_argument(
        "--github-output",
        action="store_true",
        help="append status/count/specs to $GITHUB_OUTPUT",
    )
    args = ap.parse_args(argv)

    report = load(args.report)
    if report is None:
        result = {
            "status": "no-evidence",
            "total_specs": 0,
            "gating_specs": 0,
            "informational_specs": 0,
            "flaky_gating": [],
            "flaky_informational": [],
        }
        print(f"::warning::Could not read a Playwright JSON report at {args.report}.")
    else:
        result = analyse(report)

    print(f"GATING_FLAKY_STATUS={result['status']}")
    print(f"GATING_FLAKY_COUNT={len(result['flaky_gating'])}")
    print(
        f"specs: {result['total_specs']} total "
        f"({result['gating_specs']} gating, {result['informational_specs']} informational)"
    )
    for title in result["flaky_gating"]:
        print(f"  FLAKY GATING: {title}")
    for title in result["flaky_informational"]:
        print(f"  flaky informational (not gating, no alert): {title}")

    if result["status"] == "flaky":
        joined = "; ".join(result["flaky_gating"])
        print(
            f"::error::{len(result['flaky_gating'])} GATING spec(s) were flaky — "
            f"a red that retried into a green. QA_RULES.md §12: a retry around an "
            f"assertion hides a product bug. Specs: {joined}"
        )
    elif result["status"] == "no-evidence":
        print(
            "::warning::The gating report contains no tests. This is NOT a clean run — "
            "it is an absent one, and it counts toward nothing."
        )

    if args.github_output and (gh_out := os.environ.get("GITHUB_OUTPUT")):
        with open(gh_out, "a", encoding="utf-8") as fh:
            fh.write(f"status={result['status']}\n")
            fh.write(f"count={len(result['flaky_gating'])}\n")
            fh.write(f"specs={'; '.join(result['flaky_gating'])}\n")

    # Always exit 0: this is a detector, not the gate. core#757 deliberately
    # leaves the gate's pass/fail unchanged until core#753 stops staging
    # injecting infrastructure noise — hardening the gate first would punish
    # honest specs, which is the failure the tier policy exists to prevent.
    return 0


if __name__ == "__main__":
    sys.exit(main())
