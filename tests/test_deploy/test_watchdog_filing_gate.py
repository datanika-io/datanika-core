"""The watchdog must file an issue even when it broke BEFORE it could look.

core#1243. `.github/workflows/scheduled-workflow-watchdog.yml` gates its filing
step on the check step's outcome. The original condition was::

    if: steps.check.outcome == 'failure'

which is true only when the check step *ran* and returned non-zero. When a step
BEFORE it fails -- checkout, setup-python, ``pip install pyyaml`` -- the check
step never runs, ``steps.check.outcome`` is the empty string, the filing step is
SKIPPED, and the job ends red having filed nothing.

That matters here more than it would anywhere else, because this workflow's
*correct detection* path also ends red (it files an issue and then ``exit 1``).
So "I broke before I could look" and "I found a dead cron" produced the same
colour AND the same absence of a new issue. That is core#691 one level up,
inside the very thing built to catch core#691's class.

These tests evaluate the real expression from the workflow against the outcome
values GitHub actually supplies, rather than grepping for a substring -- and each
one is paired with the OLD expression as a negative control, so a test that could
not fail is visible as such.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

WORKFLOW = (
    Path(__file__).resolve().parents[2]
    / ".github"
    / "workflows"
    / "scheduled-workflow-watchdog.yml"
)

OLD_CONDITION = "steps.check.outcome == 'failure'"


def _filing_step() -> dict:
    doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = doc["jobs"]["watchdog"]["steps"]
    matches = [s for s in steps if "File an issue" in str(s.get("name", ""))]
    assert len(matches) == 1, f"expected exactly one filing step, found {len(matches)}"
    return matches[0]


def _evaluate(expr: str, *, outcome: str, cancelled: bool) -> bool:
    """Evaluate the small subset of GitHub expression syntax used by this gate.

    Deliberately narrow: it understands `!cancelled()`, `&&`, and equality /
    inequality against `steps.check.outcome`. Anything else raises, so a future
    rewrite into a form this cannot read fails loudly instead of silently
    returning a default -- which is the failure mode the whole issue is about.
    """
    body = expr.strip()
    if body.startswith("${{") and body.endswith("}}"):
        body = body[3:-2].strip()

    result = True
    for term in [t.strip() for t in body.split("&&")]:
        if term == "!cancelled()":
            result = result and not cancelled
        elif term == "always()":
            result = result and True
        elif m := re.fullmatch(r"steps\.check\.outcome\s*(==|!=)\s*'([a-z]*)'", term):
            op, want = m.group(1), m.group(2)
            result = result and ((outcome == want) if op == "==" else (outcome != want))
        else:
            raise AssertionError(f"gate uses syntax this test cannot evaluate: {term!r}")
    return result


@pytest.mark.parametrize(
    ("outcome", "should_file", "why"),
    [
        ("success", False, "nothing wrong: the check ran and found no problem"),
        ("failure", True, "the check ran and found a problem (or crashed)"),
        ("", True, "THE BUG: the check NEVER RAN, so nothing was verified"),
        ("skipped", True, "the check was skipped, so nothing was verified"),
    ],
)
def test_gate_fires_whenever_the_check_did_not_succeed(outcome, should_file, why):
    expr = str(_filing_step()["if"])
    assert _evaluate(expr, outcome=outcome, cancelled=False) is should_file, why


def test_gate_does_not_fire_on_a_cancelled_run():
    """A cancelled job is neither green nor red and carries no steps (core#975)."""
    expr = str(_filing_step()["if"])
    assert _evaluate(expr, outcome="", cancelled=True) is False


def test_negative_control_the_old_condition_would_fail_these_tests():
    """Anti-vacuity: prove these assertions can distinguish fixed from broken.

    If this ever passes for the empty outcome, the tests above have stopped
    discriminating and would keep passing against the original defect.
    """
    assert _evaluate(OLD_CONDITION, outcome="failure", cancelled=False) is True
    assert _evaluate(OLD_CONDITION, outcome="", cancelled=False) is False, (
        "the old gate is supposed to MISS the never-ran case -- that is the bug"
    )
    assert _evaluate(OLD_CONDITION, outcome="", cancelled=True) is False


def test_the_gate_is_not_still_the_old_condition():
    expr = str(_filing_step()["if"]).strip()
    normalised = expr[3:-2].strip() if expr.startswith("${{") else expr
    assert normalised != OLD_CONDITION, "the core#1243 fix has been reverted"


def test_the_check_step_still_continues_on_error():
    """The gate only works if the check step is allowed to fail without ending the job."""
    doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = doc["jobs"]["watchdog"]["steps"]
    check = [s for s in steps if s.get("id") == "check"]
    assert len(check) == 1, "the filing gate reads steps.check -- that id must exist"
    assert check[0].get("continue-on-error") is True
