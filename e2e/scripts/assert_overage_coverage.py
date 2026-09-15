#!/usr/bin/env python3
"""The overage soak must report how many specs it actually EXECUTED (core#1301).

`overage-e2e-nightly.yml` is, by its own header, the last gate before the overage-charge
cutover. Its only verdict is the exit code of `npx playwright test`, and **Playwright exits
0 when every collected test skips**. Measured, with the GATE env removed and everything else
mirroring the job:

    Running 3 tests using 1 worker
      -  1 ... cycle: seed -> T-23h notify -> T+0 charge -> retry no-op
      -  2 ... Paddle 4xx response marks Charge failed with reason
      -  3 ... no charge when usage under included
      3 skipped
    playwright exit=0

Exit 0 means the step succeeds, the job is `success`, and both `if: failure()` steps never
fire. A night that tested nothing is byte-identical to a night that passed. The workflow
header already says so in prose -- *"A 'green' run where all 3 tests SKIPPED is NOT a passing
soak night"* -- and nothing enforced it. That prose is the whole reason this file exists: a
rule with no mechanism holds only while somebody happens to check.

The trip-wire is realistic rather than theoretical: the two `PADDLE_SANDBOX_*` secrets live
on the `production` **environment**, not at repo level, so renaming or rotating either one
turns the soak into a silent nightly no-op that reports green.

**A skip is in neither tier and counts toward nothing.** The list reporter -- which is what
the run log shows -- prints a skipped test's title and *drops its reason*, so for 7.5 weeks
every run has read `1 skipped` with no way to tell an accounted skip from an unaccounted one.
The JSON reporter does carry it, as
`tests[].annotations[] = [{"type": "skip", "description": ..., "location": {...}}]`, so this
reads the reason out of the run's **own artifact** rather than restating it here. That matters:
a description restated in a guard is a second copy that drifts from the source.

Two different questions, and both are asked:
  * Playwright's recorded reason answers *why did it skip* -- and it is always current,
    because the runner wrote it.
  * `ACCOUNTED_SKIPS` answers *who makes it stop skipping, and when* -- which no annotation
    can know, and which is the half that rots. The entry below replaced a reason string
    citing **core#361, an issue closed 2026-07-20 that was never about this**.

WARNING -- the floor is CHECKED IN, not derived from the spec file. A floor computed from the
tests present falls as they are deleted and agrees with the attack (core#1130).
`tests/test_deploy/test_overage_coverage_guard.py` asserts the constant still matches the real
spec, so lowering it is a deliberate act visible at PR time, in the same diff as the exemption
that made it necessary.

NOTE: `walk()` is deliberately duplicated from `assert_sso_coverage.py` rather than shared.
A private helper with a cross-module consumer has broken another department's gate twice
(core#1205, core#1273); ten lines of duplication is the cheaper side of that trade.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

#: The soak runs exactly one spec file. Grading anything else would mean grading a report
#: from a different run -- which is the failure this guard exists to make impossible.
OVERAGE_SPEC_FILE = "overage-charge-cycle.spec.ts"

#: How many specs must actually EXECUTE. Checked in on purpose -- see the module note.
#: 3 tests exist; one is accounted for below, so 2 must run.
OVERAGE_EXECUTED_FLOOR = 2

#: Skips that are ACCOUNTED FOR, with the exit condition. A skip is either executed or
#: explicitly out of the set with a reason recorded -- never an unexplained absence inside
#: the count (core#1130 AC3).
#:
#: WARNING: each entry is a gap that is TRACKED, not a gap that is fine. Adding one lowers
#: real coverage of the charge path, so OVERAGE_EXECUTED_FLOOR must be lowered in the same
#: commit and both land in one diff.
ACCOUNTED_SKIPS: dict[str, str] = {
    "Paddle 4xx response marks Charge failed with reason": (
        "the PRE-ACCEPTANCE Paddle 4xx path needs a sandbox product/subscription that forces "
        "a 4xx on POST /subscriptions/{id}/charge; no such harness exists. Not an uncovered "
        "branch: cloud's tests/test_billing_tasks.py::TestChargeCycleOverages4xxTerminal "
        "drives it and asserts status=FAILED plus last_error, and retry exhaustion is covered "
        "twice. What is missing is an end-to-end run against the real Paddle sandbox. "
        "Exit condition: core#1302 -- open, and deliberately not closed by the PR that added "
        "this guard."
    ),
}

_SKIPPED = "skipped"


def walk(node: dict, inherited_file: str | None = None) -> list[tuple[str, str, dict]]:
    """`(spec_file_basename, full_title, spec)` for every spec, file inherited from the suite."""
    here = node.get("file") or inherited_file
    found: list[tuple[str, str, dict]] = []
    for spec in node.get("specs") or []:
        path = spec.get("file") or here or "<unknown>"
        found.append((path.replace("\\", "/").rsplit("/", 1)[-1], spec.get("title", ""), spec))
    for child in node.get("suites") or []:
        found.extend(walk(child, here))
    return found


def _recorded_skip_reason(spec: dict) -> tuple[str | None, str | None]:
    """Playwright's own `skip` annotation for this spec: (description, 'file:line')."""
    for test in spec.get("tests") or []:
        for ann in test.get("annotations") or []:
            if ann.get("type") == "skip":
                loc = ann.get("location") or {}
                where = None
                if loc.get("file"):
                    fname = str(loc["file"]).replace("\\", "/").rsplit("/", 1)[-1]
                    where = f"{fname}:{loc.get('line', '?')}"
                return ann.get("description"), where
    return None, None


def classify(report: dict) -> dict:
    """Executed / skipped / collected for the overage spec, with each skip's recorded reason."""
    specs: list[tuple[str, str, dict]] = []
    for suite in report.get("suites") or []:
        specs.extend(walk(suite))

    rows = []
    for file_name, title, spec in specs:
        if file_name != OVERAGE_SPEC_FILE:
            continue
        statuses = [t.get("status") for t in spec.get("tests") or []]
        reason, where = _recorded_skip_reason(spec)
        rows.append(
            {
                "title": title,
                "executed": bool(statuses) and any(s != _SKIPPED for s in statuses),
                "reason": reason,
                "where": where,
            }
        )

    return {
        "rows": rows,
        "collected": len(rows),
        "executed": sum(1 for r in rows if r["executed"]),
        "skipped": [r for r in rows if not r["executed"]],
    }


def render(summary: dict) -> list[str]:
    """Print the split unconditionally -- `2 passed` reads as full coverage until you look."""
    lines = [
        f"Overage soak coverage: {summary['executed']} executed / "
        f"{len(summary['skipped'])} skipped / {summary['collected']} collected   "
        f"(floor {OVERAGE_EXECUTED_FLOOR})",
    ]
    for row in summary["skipped"]:
        lines.append(f"  SKIPPED    : {row['title']}")
        recorded = row["reason"] or "(Playwright recorded no reason)"
        where = f"   [{row['where']}]" if row["where"] else ""
        lines.append(f"    reported : {recorded}{where}")
        accounted = ACCOUNTED_SKIPS.get(row["title"])
        lines.append(f"    tracking : {'accounted: ' + accounted if accounted else 'UNACCOUNTED'}")
    return lines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Assert the overage soak actually executed.")
    ap.add_argument("report", type=Path)
    args = ap.parse_args(argv)

    if not args.report.exists():
        print(f"Overage soak coverage: {args.report} does not exist -- the run produced NO report.")
        print("  That is not 'every spec passed'; it is no reading at all. Refusing.")
        return 2
    try:
        report = json.loads(args.report.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Overage soak coverage: could not read {args.report}: {exc}")
        print("  No reading. Refusing rather than reporting a pass.")
        return 2

    summary = classify(report)

    if summary["collected"] == 0:
        print(f"Overage soak coverage: no specs from {OVERAGE_SPEC_FILE} in {args.report}.")
        print("  The soak runs exactly that file, so an empty reading means the run did not")
        print("  happen, or this is another run's report. Either way it is not a pass.")
        return 2

    for line in render(summary):
        print(line)

    # 🔑 ORDERING IS DELIBERATE AND IS ASSERTED by the test suite.
    #
    # "nothing executed" is graded BEFORE "a skip is unaccounted", even though a wholly
    # skipped run trivially satisfies both. The guard's own first run is what taught this:
    # against the real all-skipped report it refused with *"UNACCOUNTED: cycle: seed -> ..."*,
    # which is true and sends the reader to hunt a badly-written skip -- when the actual cause
    # is the describe-level GATE tripping because a PADDLE_SANDBOX_* secret went missing.
    #
    # Same shape as core#1130's classifier ordering: report the most informative true thing,
    # not merely the first true thing. An aggregate that attributes to nothing must not
    # masquerade as a finding about one member.
    if summary["executed"] == 0:
        print()
        print("REFUSED: the soak executed NOTHING — every collected spec skipped (core#1301).")
        print(
            "  This is the vacuous green the header of overage-e2e-nightly.yml warns about:\n"
            "  Playwright exits 0 when every test skips, so without this check the night\n"
            "  reads `success` having tested nothing.\n"
            "  The usual cause is the describe-level GATE — DATANIKA_E2E_OVERAGE_CHARGE unset,\n"
            "  or a PADDLE_SANDBOX_* secret renamed or rotated on the `production` environment.\n"
            "  The 'reported' lines above are Playwright saying which."
        )
        return 2

    unaccounted = [r for r in summary["skipped"] if r["title"] not in ACCOUNTED_SKIPS]
    if unaccounted:
        print()
        print("REFUSED: a soak spec skipped with no recorded exit condition (core#1130 AC3):")
        for row in unaccounted:
            print(f"  - {row['title']}")
        print(
            "  Playwright's reason says WHY it skipped; it cannot say who makes it stop.\n"
            "  Either make it execute, or add it to ACCOUNTED_SKIPS with a LIVE issue AND\n"
            "  lower OVERAGE_EXECUTED_FLOOR in the same commit, so the coverage given up is\n"
            "  visible in the same diff as the exemption that gave it up."
        )
        return 2

    if summary["executed"] < OVERAGE_EXECUTED_FLOOR:
        print()
        print(
            f"REFUSED: {summary['executed']} specs executed, floor is "
            f"{OVERAGE_EXECUTED_FLOOR} (core#1301)."
        )
        print(
            "  Playwright exits 0 when every collected test skips, so the job's own outcome\n"
            "  cannot tell this from a full green. The usual cause is the GATE tripping --\n"
            "  DATANIKA_E2E_OVERAGE_CHARGE or a PADDLE_SANDBOX_* secret missing -- and the\n"
            "  'reported' line above is Playwright telling you which."
        )
        return 2

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
