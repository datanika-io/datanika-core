"""Describe an overage E2E workflow failure from its two step outcomes."""

from __future__ import annotations

import sys


def describe_outcome(soak: str, coverage: str) -> str:
    """Classify the failure without calling a skipped test step a test failure."""
    if soak in ("", "skipped"):
        return (
            "the soak step never ran (an earlier setup step failed or the job was cancelled); "
            "no conclusion about staging behavior is available"
        )
    if soak == "success" and coverage != "success":
        return (
            "the specs passed but the coverage assertion refused; inspect its output to see "
            "whether the run executed anything"
        )
    if soak != "success" and coverage == "success":
        return "the specs themselves failed"
    if soak == "failure" and coverage == "failure":
        return "both the specs and the coverage assertion are non-green"
    return f"the workflow outcomes need review (soak={soak!r}, coverage={coverage!r})"


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: classify_overage_failure.py SOAK_OUTCOME COVERAGE_OUTCOME")
    print(describe_outcome(sys.argv[1], sys.argv[2]))
