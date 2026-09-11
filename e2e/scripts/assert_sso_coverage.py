#!/usr/bin/env python3
"""The SSO tier must report how many IdP specs it actually EXECUTED (core#1130 AC1-AC4).

`e2e-sso`'s classifier reads `steps.sso_specs.outcome`, which is a **process exit code**, and
Playwright exits 0 when every collected test skips. So a run in which no IdP spec ran is
indistinguishable from a run in which all of them passed — and #1099 exists precisely because
this job is one fixture line from going green for the first time.

🚨 **Two routes are open and one is not hypothetical.**

* **Route A, live now.** `sso-oidc.spec.ts:74` skips on `!process.env.DATANIKA_E2E_API_KEY_ORG_A`.
  That is not the `process.env.X !== "v"` shape `test_e2e_sso_tier_is_measured.py` scans for, and
  the job supplies no such variable, so **the tier has always reported on 8 of its 9 IdP specs**
  and the absent one asserts an *authorization* property. `12 passed` has been read as this
  tier's coverage at least twice, including by its author.
* **Route B.** Deleting or renaming `sso-oidc.spec.ts` leaves `npx playwright test sso-`
  collecting the ungated edge-case specs, which pass. `0 failed` -> exit 0 -> `clean`.

⚠️ **The floor is CHECKED IN, not derived from the spec files**, and that is the whole point of
Route B: a floor computed from the files present would fall as the files are deleted, and the
guard would agree with the attack. `test_sso_coverage_floor.py` asserts the constant still
matches reality, so changing it is a deliberate act at PR time.

⚠️ **A skip is not an execution.** Playwright reports a skipped test with `status: "skipped"`,
and it contributes to `N passed` in neither direction — but the human summary line is what gets
read, so this prints the split explicitly (AC4).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

#: The IdP specs. `sso-edge-cases.spec.ts` is deliberately NOT here: it exercises error paths
#: that need no IdP, runs unconditionally, and is exactly what a deleted IdP file would hide
#: behind.
IDP_SPEC_FILES = ("sso-oidc.spec.ts", "sso-saml.spec.ts")

#: How many IdP specs must actually EXECUTE. Checked in on purpose — see the module note.
#: 9 IdP specs exist (5 OIDC + 4 SAML); one is accounted for below, so 8 must run.
SSO_IDP_EXECUTED_FLOOR = 8

#: Skips that are ACCOUNTED FOR, with the reason. AC3: a skip is either executed or explicitly
#: out of the set with the reason recorded — never an unexplained absence inside the count.
#:
#: ⚠️ Each entry is a gap that is tracked, not a gap that is fine. Adding one lowers the real
#: coverage of an Enterprise-only auth path, so the floor above must be lowered in the same
#: commit and both are visible in the same diff.
ACCOUNTED_SKIPS: dict[str, str] = {
    "OIDC JIT provisioning: new SSO user gets Membership with VIEWER role": (
        "needs DATANIKA_E2E_API_KEY_ORG_A to read /api/v1/members, and this job runs with "
        "DATANIKA_E2E_SKIP_SEED=1 against a pre-seeded staging fixture, so nothing supplies "
        "one. Runnable the day staging's seed emits org-A keys (E2E_SEED_INCLUDE_API_KEYS=1) "
        "and the job passes the key through. core#1130 AC3."
    ),
}

_SKIPPED = "skipped"


def walk(node: dict, inherited_file: str | None = None) -> list[tuple[str, str, dict]]:
    """`(spec_file, full_title, spec)` for every spec, file inherited from the enclosing suite."""
    here = node.get("file") or inherited_file
    found: list[tuple[str, str, dict]] = []
    for spec in node.get("specs") or []:
        path = spec.get("file") or here or "<unknown>"
        found.append((path.replace("\\", "/").rsplit("/", 1)[-1], spec.get("title", ""), spec))
    for child in node.get("suites") or []:
        found.extend(walk(child, here))
    return found


def classify(report: dict) -> dict:
    """Executed / skipped / total, overall and for the IdP set."""
    specs: list[tuple[str, str, dict]] = []
    for suite in report.get("suites") or []:
        specs.extend(walk(suite))

    rows = []
    for file_name, title, spec in specs:
        statuses = [t.get("status") for t in spec.get("tests") or []]
        executed = any(s != _SKIPPED for s in statuses)
        rows.append(
            {
                "file": file_name,
                "title": title,
                "executed": executed,
                "idp": file_name in IDP_SPEC_FILES,
            }
        )

    idp = [r for r in rows if r["idp"]]
    return {
        "rows": rows,
        "total": len(rows),
        "executed": sum(1 for r in rows if r["executed"]),
        "idp_total": len(idp),
        "idp_executed": sum(1 for r in idp if r["executed"]),
        "idp_skipped": [r["title"] for r in idp if not r["executed"]],
    }


def render(summary: dict) -> list[str]:
    """AC4: print the split, because `12 passed` reads as full coverage until you look."""
    lines = [
        f"SSO coverage: {summary['executed']} executed / "
        f"{summary['total'] - summary['executed']} skipped / {summary['total']} collected",
        f"  IdP specs  : {summary['idp_executed']} executed / "
        f"{summary['idp_total'] - summary['idp_executed']} skipped / {summary['idp_total']} "
        f"collected   (floor {SSO_IDP_EXECUTED_FLOOR})",
    ]
    for title in summary["idp_skipped"]:
        why = ACCOUNTED_SKIPS.get(title)
        lines.append(f"  SKIPPED    : {title}")
        lines.append(f"               {'accounted: ' + why if why else 'UNACCOUNTED'}")
    return lines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("report", type=Path)
    args = ap.parse_args(argv)

    if not args.report.exists():
        print(f"SSO coverage: {args.report} does not exist — the run produced NO report.")
        print("  That is not 'every spec passed'; it is no reading at all. Refusing.")
        return 2
    try:
        report = json.loads(args.report.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"SSO coverage: could not read {args.report}: {exc}")
        print("  No reading. Refusing rather than reporting a pass.")
        return 2

    summary = classify(report)
    for line in render(summary):
        print(line)

    unaccounted = [t for t in summary["idp_skipped"] if t not in ACCOUNTED_SKIPS]
    if unaccounted:
        print()
        print("REFUSED: an IdP spec skipped with no recorded reason (core#1130 AC3):")
        for title in unaccounted:
            print(f"  - {title}")
        print(
            "  A silent skip inside the count is how this tier reported 8 of 9 for months.\n"
            "  Either make it execute, or add it to ACCOUNTED_SKIPS with the reason AND lower\n"
            "  SSO_IDP_EXECUTED_FLOOR in the same commit, so the cost is visible in one diff."
        )
        return 2

    if summary["idp_executed"] < SSO_IDP_EXECUTED_FLOOR:
        print()
        print(
            f"REFUSED: {summary['idp_executed']} IdP specs executed, floor is "
            f"{SSO_IDP_EXECUTED_FLOOR} (core#1130 AC1/AC2)."
        )
        print(
            "  Playwright exits 0 when every collected test skips, so the job's own outcome\n"
            "  cannot tell this from a full green. Deleting an IdP spec file, or a gate the\n"
            "  PR-time scanner cannot see, both land here."
        )
        return 2

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
