"""Emit ONE graduation verdict PER SPEC, not one for the whole tier (core#1221).

``staging.yml`` emitted a single ``INFORMATIONAL_RESULT=`` for the whole informational
run, and ``docs/QA_RULES.md`` §10 graduates **per spec**. Two consequences, both already
realised on the real history:

* a new spec entering the tier zeroes every incumbent's progress — ``reflex-wire.spec.ts``
  went from **seven** consecutive greens to nothing, having done nothing;
* a persistently-red new spec makes graduation impossible for **every** spec while it is
  red — and §10 *requires* new specs to enter the tier, so the churn that resets the
  counter is by design.

🔑 **A counter that resets whenever the thing it counts is added to is not a counter.**

This reads the Playwright JSON report the run already writes and prints one line per spec
file::

    INFORMATIONAL_SPEC_RESULT=reflex-wire.spec.ts:success
    INFORMATIONAL_SPEC_RESULT=a11y-sweep.spec.ts:failure

⚠️ **The tier-wide line stays.** Every log written before this script exists carries only
that line, and ``tests/test_deploy/test_image_probe.py`` asserts exactly one step owns it.
Removing it would rewrite history rather than extend it.

⚠️ **Attribution is by spec FILE**, because that is the unit §10 graduates. A file's verdict
is ``failure`` if *any* of its tests failed: graduating half a file is not a thing the policy
can express.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

MARKER = "INFORMATIONAL_SPEC_RESULT"

#: A test status that means "this spec did not pass". `flaky` counts as a failure **for
#: graduation**: it failed at least once and then passed, and the property a graduating spec
#: has to demonstrate is stability. `detect_flaky_gating.py` exists precisely because a flaky
#: run exits 0.
NOT_PASSING = {"failed", "timedOut", "interrupted", "unexpected", "flaky"}

#: Statuses that mean the spec did not run at all. A skip is in neither tier and counts toward
#: nothing (`QA_RULES` §11), so a file whose every test skipped produces NO line — silence,
#: not a green.
SKIPPED = {"skipped"}


def _walk(node: dict, inherited_file: str | None = None) -> list[tuple[str, dict]]:
    """``(spec_file, spec)`` for every spec under ``node``.

    The file is read from the spec when Playwright puts it there and inherited from the
    enclosing file-suite otherwise. Both forms appear in reports this repo has captured, and
    guessing one would silently attribute a whole run to ``None``.
    """
    here = node.get("file") or inherited_file
    found: list[tuple[str, dict]] = []
    for spec in node.get("specs") or []:
        found.append((spec.get("file") or here or "<unknown>", spec))
    for child in node.get("suites") or []:
        found.extend(_walk(child, here))
    return found


def spec_results(report: dict) -> dict[str, str]:
    """``spec file -> "success" | "failure"``, over files that actually ran."""
    per_file: dict[str, str] = {}
    for suite in report.get("suites") or []:
        for path, spec in _walk(suite):
            statuses = [t.get("status") for t in spec.get("tests") or []]
            ran = [s for s in statuses if s not in SKIPPED]
            if not ran:
                continue
            verdict = "failure" if any(s in NOT_PASSING for s in ran) else "success"
            # A file is green only if every spec in it is green; one failure decides.
            if per_file.get(path) != "failure":
                per_file[path] = verdict
    return per_file


def basename(path: str) -> str:
    """`tests/reflex-wire.spec.ts` -> `reflex-wire.spec.ts`.

    The streak reader is asked about a spec by the name a human uses. Normalising here rather
    than at the reader keeps one definition of what a spec is called.
    """
    return path.replace("\\", "/").rsplit("/", 1)[-1]


def render(results: dict[str, str]) -> list[str]:
    lines = [f"{MARKER}={basename(p)}:{v}" for p, v in sorted(results.items())]
    if not lines:
        # Distinct from "everything failed" and from "the tier is empty" — this is the report
        # carrying no spec at all, which is what a crashed or misdirected run produces.
        lines.append(f"{MARKER}=<none>:no_evidence")
    return lines


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("report", type=Path)
    args = ap.parse_args(argv)

    if not args.report.exists():
        print(f"{MARKER}=<none>:no_evidence")
        print(f"::warning::{args.report} does not exist — no per-spec graduation evidence.")
        return 0
    try:
        report = json.loads(args.report.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"{MARKER}=<none>:no_evidence")
        print(f"::warning::could not read {args.report}: {exc}")
        return 0

    results = spec_results(report)
    for line in render(results):
        print(line)
    print(
        f"::notice::per-spec graduation evidence for {len(results)} spec file(s). "
        "Each spec's streak is its own; a new spec entering the tier no longer resets it."
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
