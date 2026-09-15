"""The watchdog's filing step: WHEN it runs (core#1243) and what its exit status MEANS (core#1272).

WHEN IT RUNS -- core#1243
-------------------------
`.github/workflows/scheduled-workflow-watchdog.yml` gates its filing step on the check
step's outcome. The original condition was::

    if: steps.check.outcome == 'failure'

which is true only when the check step *ran* and returned non-zero. When a step BEFORE it
fails -- checkout, setup-python, ``pip install pyyaml`` -- the check step never runs,
``steps.check.outcome`` is the empty string, the filing step is SKIPPED, and the job ends
red having filed nothing. Because the correct-detection path ALSO ended red at the time,
"I broke before I could look" and "I found a dead cron" produced the same colour AND the
same absence of a new issue: core#691 one level up.

Those tests evaluate the real expression from the workflow against the outcome values
GitHub actually supplies, each paired with the OLD expression as a negative control.

WHAT ITS EXIT STATUS MEANS -- core#1272
--------------------------------------
``check_scheduled_workflows.py`` grades every scheduled workflow's recent RUN conclusions,
this workflow's included. The report branches ended in ``exit 1``, so every correct report
was a red run, two reports were a "FIRED AND FAILED" streak, and filing that was red again:
from 2026-09-11 the watchdog flagged itself every day with nothing else wrong.

A filed report now ends GREEN; the run is red only when nothing was verified or the report
could not be filed. The tests below RUN the real step script under bash, with ``gh`` stubbed
as a shell function, and assert each branch's exit status -- and run the pre-core#1272 script
as a negative control, so a revert cannot pass them.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
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
_BROKEN_TITLE = "Scheduled workflow watchdog is failing to run"
_FINDING_TITLE = "Scheduled workflow stopped firing"


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


# ---------------------------------------------------------------------------------------------
# core#1272 -- what the step's exit status MEANS, established by running it.
# ---------------------------------------------------------------------------------------------

# `gh` as a shell FUNCTION, which bash resolves before PATH on every platform: no stub
# directory, no PATH translation between Windows and Git Bash, and no way to reach the real
# CLI. An unexpected subcommand fails loudly rather than returning a default.
_GH_STUB = r"""
gh() {
  printf '%s\n' "$*" >> gh-calls.log
  case "$1 $2" in
    "issue list")
      case "$*" in
        *"failing to run"*) printf '%s' "${STUB_BROKEN_ISSUE:-}" ;;
        *) printf '%s' "${STUB_EXISTING_ISSUE:-}" ;;
      esac ;;
    "issue comment"|"issue create")
      if [ "${STUB_GH_WRITE_FAILS:-0}" = 1 ]; then echo "stub gh: HTTP 502" >&2; return 1; fi
      echo "https://github.com/o/r/issues/1" ;;
    *) echo "stub gh: unexpected call: $*" >&2; return 2 ;;
  esac
}
"""


def _rendered_script(outcome: str) -> str:
    """The filing step's REAL ``run:`` script, rendered the way the runner renders it.

    Exactly two substitutions: the one ``${{ }}`` expression it uses, which GitHub renders
    before bash sees the text, and ``/tmp/`` -> ``./`` so each case writes inside its own
    temporary directory. If the script ever gains another expression this refuses, rather
    than running a script bash would reject for a reason unrelated to the test.
    """
    script = str(_filing_step()["run"])
    expressions = set(re.findall(r"\$\{\{.*?\}\}", script))
    assert expressions <= {"${{ steps.check.outcome }}"}, (
        f"the filing script uses an expression this harness does not render: {expressions}"
    )
    return script.replace("${{ steps.check.outcome }}", outcome).replace("/tmp/", "./")


@pytest.fixture
def run_filing(tmp_path: Path):
    bash = shutil.which("bash")
    if not bash or "system32" in bash.lower():
        # A System32 bash is WSL, not Git Bash (see test_promotion_gate.py); it would fail
        # for reasons unrelated to the step. CI runs Linux, where this never skips.
        pytest.skip("needs a real bash: Git Bash on Windows, any bash on Linux")
    cases = {"n": 0}

    def run(
        *,
        problems: bool,
        existing: str = "",
        write_fails: bool = False,
        outcome: str = "failure",
        script: str | None = None,
    ) -> tuple[subprocess.CompletedProcess[str], list[str]]:
        cases["n"] += 1
        work = tmp_path / f"case{cases['n']}"
        work.mkdir()
        if problems:
            (work / "problems.md").write_text("- a finding\n", encoding="utf-8", newline="\n")
        body = _rendered_script(outcome) if script is None else script
        # newline="\n" is load-bearing on Windows: a CRLF script makes bash read `exit 0\r`.
        (work / "filing.sh").write_text(_GH_STUB + body, encoding="utf-8", newline="\n")
        env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
        env.update(
            GITHUB_SERVER_URL="https://github.com",
            GITHUB_REPOSITORY="o/r",
            GITHUB_RUN_ID="1",
            STUB_EXISTING_ISSUE=existing,
            STUB_GH_WRITE_FAILS="1" if write_fails else "0",
        )
        # `-e`: the runner's default shell for a `run:` with no `shell:` is `bash -e {0}`.
        result = subprocess.run(
            [bash, "-e", "filing.sh"],
            cwd=work,
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        log = work / "gh-calls.log"
        calls = log.read_text(encoding="utf-8").splitlines() if log.exists() else []
        return result, calls

    return run


def _creates(calls: list[str]) -> list[str]:
    return [c for c in calls if c.startswith("issue create")]


class TestTheFilingStepIsRedOnlyWhenItBroke:
    def test_a_new_finding_is_filed_and_the_run_stays_green(self, run_filing):
        result, calls = run_filing(problems=True)
        assert result.returncode == 0, result.stdout + result.stderr
        assert any(_FINDING_TITLE in c for c in _creates(calls)), calls

    def test_a_repeat_finding_is_commented_and_the_run_stays_green(self, run_filing):
        result, calls = run_filing(problems=True, existing="1272")
        assert result.returncode == 0, result.stdout + result.stderr
        assert any(c.startswith("issue comment 1272") for c in calls), calls
        assert not _creates(calls), "a repeat finding must not open a second issue"

    @pytest.mark.parametrize("outcome", ["failure", ""])
    def test_nothing_verified_is_filed_under_its_own_title_and_goes_red(self, run_filing, outcome):
        result, calls = run_filing(problems=False, outcome=outcome)
        assert result.returncode == 1, result.stdout + result.stderr
        assert any(_BROKEN_TITLE in c for c in _creates(calls)), calls
        assert not any(_FINDING_TITLE in c for c in _creates(calls)), (
            "a run that verified nothing must never file a finding"
        )

    def test_a_report_that_could_not_be_filed_goes_red(self, run_filing):
        result, _ = run_filing(problems=True, write_fails=True)
        assert result.returncode != 0, "a finding that reached nobody must not read as green"

    def test_negative_control_the_pre_1272_script_is_red_on_a_filed_report(self, run_filing):
        """If this stops failing on the old script, the green assertions above prove nothing."""
        fixed = _rendered_script("failure")
        old = fixed.replace("exit 0", "exit 1")
        assert fixed.count("exit 0") >= 2 and old != fixed, "the mutation changed nothing"
        result, calls = run_filing(problems=True, script=old)
        assert result.returncode == 1, result.stdout + result.stderr
        assert _creates(calls), (
            "the old script still FILED -- it went red while succeeding, which is the defect"
        )
