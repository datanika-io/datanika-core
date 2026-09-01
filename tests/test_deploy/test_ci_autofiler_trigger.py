"""An auto-filer must key on the step that ran the tests, not on the job (core#873).

What went wrong
---------------
`e2e-staging`'s filer and its Telegram alert fired on job-level ``failure()``. On
2026-09-01 a GitHub artifact-service timeout on ``Upload Playwright report`` made the
job red for a run in which **every spec passed** — so the filer commented "Another
e2e-staging failure" onto #858 and paged the founder, asserting a gating E2E failure
that had not happened. The body it wrote even said *"Only the GATING tier can open
this"*, which was by then false: that sentence is the one a reader uses to decide
whether to believe the report.

The run mattered. It was the post-promotion verification for a 15-commit production
release, and both `CLAUDE.md` and `RUNBOOK_DEV_TO_MASTER.md` say a promotion CD run
carries no E2E verdict and that the reading lands on the resync push — i.e. on exactly
this run. Its colour said the promotion's E2E failed.

This is the sibling of core#818. That one fixed *where* a filer writes (dedup channels);
this one fixes *when* it writes at all. A red that does not mean what it says teaches
people to stop reading reds.

The two invariants, and why the second one exists
-------------------------------------------------
1. **A claiming step must be step-keyed whenever a step that can fail runs after the
   work.** If a job has an ``always()``/``failure()`` upload or cleanup between the
   tests and the alert, then job-red is strictly weaker than tests-red, and
   ``if: failure()`` claims more than it knows. Where nothing can fail after the work,
   the two are the same event and ``failure()`` is sound.

   ⚠️ **`smoke-staging` used to be that case and no longer is (2026-09-01, core#876).**
   It gained a SHA-attribution assertion *before* the probe, so a step that can fail now
   runs on a path where the probe never ran at all — job-red became strictly weaker than
   probe-red in the other direction. Its alert is step-keyed now. The lesson generalises:
   this exemption is a property of a job's current step list, not a property of the job,
   and it expires silently the moment anyone inserts a step. Which is why invariant 1 is
   derived here rather than written down as a list of blessed job names.

2. **A claiming step's condition must contain a status function.** This is the trap in
   core#873's own proposed fix, which read::

       if: steps.gating.outcome == 'failure'

   GitHub implicitly wraps an ``if:`` containing none of
   ``success()``/``always()``/``cancelled()``/``failure()`` in ``success() && ...``.
   When the gating step fails the job is already failing, ``success()`` is false, and
   the step is **skipped** — so that spelling never fires again on any real failure.
   It converts a false alarm into total silence, which is worse and much harder to
   notice. Every claiming condition must lead with ``always() &&``.

   Note the shipped fix is correct under *either* reading of the implicit-wrapping
   rule: ``always() && X`` is what you want whether or not ``success()`` would have
   been inserted.

Not checked here, on purpose
----------------------------
Whether the job goes red at all. It should — losing a test report is worth knowing.
core#873 is about what we *claim* the red means, not about suppressing it.

What this CANNOT catch — say it out loud rather than let a green imply it
-------------------------------------------------------------------------
A condition that is step-keyed but keyed on the **wrong** step. `e2e-sso`'s
``Telegram alert on new failure`` was exactly that: it read
``steps.file_issue.outputs.dedup``, so gating the *filer* alone would not have fixed
it — on an artifact-upload red the filer is skipped, ``dedup`` is unset,
``'' != 'commented'`` is true, ``failure()`` is true, and it pages anyway. It looks
step-keyed to any parser. That one was found by reading, and fixed by adding the
verdict conjunct; nothing mechanical here would have named it.

Run against the pre-fix `ci.yml` from `origin/dev`, this auditor reports the three
steps it *can* see (both `e2e-staging` claimers and `e2e-sso`'s filer) and zero
against the fixed file — which is the evidence that it discriminates, rather than
being a checker that has never rejected anything.

Derived, not restated
---------------------
Claiming steps are found by what they do (``gh issue create``/``gh issue comment``, or
a POST to the Telegram API), and the hazard is found by scanning each job's own step
list. Nothing below names a job or a step, so a new alert in a new workflow is covered
without editing this file.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"

_STATUS_FN = re.compile(r"\b(always|success|failure|cancelled)\s*\(\s*\)")
_JOB_FAILURE = re.compile(r"\bfailure\s*\(\s*\)")
_STEP_REF = re.compile(r"\bsteps\.([A-Za-z0-9_-]+)\.")
# A hazard is a step that can redden the job **while the work went green** — that is
# the only way a claiming step ends up asserting a failure that did not happen.
#
# `always()` is exactly that shape, and it is the one that produced core#873: both
# artifact uploads in `e2e-staging` are `always()`, so either can fail on a run whose
# specs all passed.
#
# ⚠️ An `if: failure()` upload is deliberately NOT a hazard. It runs only when the job
# is already red, so it cannot turn a green verdict into a red claim — the filer
# downstream of it was going to fire anyway, and truthfully.
# `overage-e2e-nightly.yml` is that shape today and is correct because of it. If that
# upload is ever widened to `always()` (which is better practice — see the note on
# `Upload raw test-results`, core#501), it acquires this defect, and this test is what
# will say so at PR time.
_RUNS_AFTER_TROUBLE = re.compile(r"\balways\s*\(\s*\)")


def _claims_a_failure(run: str) -> bool:
    """Does this step assert to a human that something failed?"""
    return "gh issue create" in run or "gh issue comment" in run or "api.telegram.org" in run


def _audit(sources: dict[str, str]) -> dict[str, list[str]]:
    """Return the two problem classes. Empty lists mean clean."""
    job_keyed: list[str] = []
    fails_closed: list[str] = []

    for name, text in sources.items():
        doc = yaml.safe_load(text) or {}
        for job_id, job in (doc.get("jobs") or {}).items():
            steps = job.get("steps") or []
            # Steps whose failure cannot redden the job. A condition referring to one
            # of these is NOT implicitly success()-gated into silence, because the job
            # is still succeeding when it is evaluated. `image-cve`'s canary and
            # `scheduled-workflow-watchdog`'s check are both this shape, deliberately.
            swallowed = {
                s.get("id")
                for s in steps
                if s.get("continue-on-error") in (True, "true") and s.get("id")
            }
            # Does any step that survives trouble sit before this claiming step?
            hazard_before = False
            for i, step in enumerate(steps):
                run = step.get("run")
                cond = str(step.get("if") or "")
                label = f"{name}::{job_id}::{step.get('name') or f'step[{i}]'}"
                claiming = isinstance(run, str) and _claims_a_failure(run)

                if claiming:
                    refs = set(_STEP_REF.findall(cond))
                    if not refs:
                        # Job-keyed. Only a problem if something between the work and
                        # here could have reddened the job on its own.
                        if _JOB_FAILURE.search(cond) and hazard_before:
                            job_keyed.append(
                                f"{label} triggers on job-level failure() while an "
                                f"always() step runs before it — that step can redden "
                                f"the job on a run whose work went green, so this "
                                f"cannot tell a failing test from a failing upload"
                            )
                    elif not _STATUS_FN.search(cond) and not (refs & swallowed):
                        fails_closed.append(
                            f"{label} keys on a step outcome with no status function: "
                            f"if: {cond!r} — implicitly success()-gated, so it will "
                            f"never fire on a real failure"
                        )
                elif _RUNS_AFTER_TROUBLE.search(cond):
                    # An upload/teardown that outlives a failure. Anything claiming a
                    # test failure after this point is downstream of a hazard.
                    hazard_before = True

    return {"job_keyed": job_keyed, "fails_closed": fails_closed}


def _real_sources() -> dict[str, str]:
    sources = {p.name: p.read_text(encoding="utf-8") for p in sorted(WORKFLOWS.glob("*.yml"))}
    assert sources, f"no workflows found under {WORKFLOWS}"
    return sources


@pytest.fixture(scope="module")
def report() -> dict[str, list[str]]:
    return _audit(_real_sources())


def test_the_auditor_actually_finds_the_claiming_steps() -> None:
    """Guard the guard. A parser that finds nothing reports everything as clean —
    this project's signature defect wearing a test's clothes."""
    found = 0
    for text in _real_sources().values():
        doc = yaml.safe_load(text) or {}
        for job in (doc.get("jobs") or {}).values():
            for step in job.get("steps") or []:
                run = step.get("run")
                if isinstance(run, str) and _claims_a_failure(run):
                    found += 1
    assert found >= 6, f"expected at least 6 issue-filing / paging steps, parsed {found}"


def test_no_claiming_step_is_keyed_on_the_job_when_something_can_fail_after_the_work(
    report: dict[str, list[str]],
) -> None:
    """core#873: the artifact upload reddened the job and the filer claimed a test failure."""
    assert report["job_keyed"] == [], (
        "auto-filer / alert cannot distinguish a failing test from a failing "
        "upload or teardown:\n  " + "\n  ".join(report["job_keyed"])
    )


def test_no_claiming_step_fails_closed(report: dict[str, list[str]]) -> None:
    """The trap in core#873's own suggested fix — silence instead of a false alarm."""
    assert report["fails_closed"] == [], (
        "condition is implicitly wrapped in success() and so can never fire:\n  "
        + "\n  ".join(report["fails_closed"])
    )


# ── negative controls ────────────────────────────────────────────────────────────────
# The real shapes, not invented ones. If any of these behaves like the shipped file,
# the tests above cannot fail and prove nothing.

_PRE_FIX = """
jobs:
  e2e-staging:
    steps:
      - name: Run gating E2E specs against staging
        run: npx playwright test --grep-invert "@informational"
      - name: Upload Playwright report
        if: always()
        uses: actions/upload-artifact@v4
      - name: File issue on failure
        if: failure()
        run: |
          gh issue create --label e2e-staging-failure --title "boom"
"""

# core#873's own proposed spelling. It dedups, it is step-keyed, it reads correctly —
# and it never fires, because the implicit success() is false the moment gating fails.
_NAIVE_FIX = """
jobs:
  e2e-staging:
    steps:
      - name: Run gating E2E specs against staging
        id: gating
        run: npx playwright test --grep-invert "@informational"
      - name: Upload Playwright report
        if: always()
        uses: actions/upload-artifact@v4
      - name: File issue on failure
        if: steps.gating.outcome == 'failure'
        run: |
          gh issue create --label e2e-staging-failure --title "boom"
"""

# `smoke-staging`: job-keyed, and legitimately so. Nothing runs after the probe, so
# job-red and probe-red are the same event. This must NOT be reported.
_LEGITIMATE_JOB_KEYED = """
jobs:
  smoke-staging:
    steps:
      - name: Run smoke probes
        id: smoke
        run: pytest scripts/smoke/
      - name: Telegram alert on failure
        if: failure()
        run: |
          curl -X POST "https://api.telegram.org/bot$T/sendMessage"
"""

_SHIPPED_FIX = """
jobs:
  e2e-staging:
    steps:
      - name: Run gating E2E specs against staging
        id: gating
        run: npx playwright test --grep-invert "@informational"
      - name: Upload Playwright report
        if: always()
        uses: actions/upload-artifact@v4
      - name: Classify what this job's result means
        id: verdict
        if: always()
        run: echo "state=gating_failed" >> "$GITHUB_OUTPUT"
      - name: File issue on failure
        if: always() && steps.verdict.outputs.state == 'gating_failed'
        run: |
          gh issue create --label e2e-staging-failure --title "boom"
"""


def test_auditor_rejects_the_pre_fix_shape() -> None:
    report = _audit({"pre-fix.yml": _PRE_FIX})
    assert len(report["job_keyed"]) == 1, report
    assert "File issue on failure" in report["job_keyed"][0]
    assert report["fails_closed"] == []


def test_auditor_rejects_the_naive_fix_that_would_never_fire() -> None:
    report = _audit({"naive.yml": _NAIVE_FIX})
    assert report["job_keyed"] == [], (
        "the naive fix IS step-keyed — that is what makes it plausible"
    )
    assert len(report["fails_closed"]) == 1, report
    assert "never fire" in report["fails_closed"][0]


def test_auditor_accepts_job_keying_when_nothing_can_fail_after_the_work() -> None:
    """The carve-out has to hold, or the rule becomes a blanket ban and gets relaxed."""
    assert _audit({"smoke.yml": _LEGITIMATE_JOB_KEYED}) == {"job_keyed": [], "fails_closed": []}


def test_auditor_accepts_the_shipped_fix() -> None:
    assert _audit({"fixed.yml": _SHIPPED_FIX}) == {"job_keyed": [], "fails_closed": []}


# ---------------------------------------------------------------------------
# core#880: the defect that stops the workflow from running AT ALL
# ---------------------------------------------------------------------------
# The fix above introduced `IS_CANCELLED: ${{ cancelled() }}` into a step
# `env:`. GitHub allows the status-check functions -- always/success/failure/
# cancelled -- ONLY inside an `if:`. Anywhere else the whole FILE fails
# template validation, and the failure mode is the nastiest one this repo
# keeps rediscovering: the run is created with ZERO jobs and conclusion
# `failure`, so `commits/<sha>/check-runs` returns an EMPTY LIST rather than a
# red one. The PR then shows NO checks -- byte-identical to "CI has not started
# yet" -- and sits BLOCKED indefinitely while reading as pending.
#
# Measured on run 33451148336: created_at == updated_at == run_started_at,
# latest_check_runs_count 0, and `gh run view` saying only "This run likely
# failed because of a workflow file issue."
#
# A plain grep cannot decide this, because `if: ${{ always() && ... }}` is the
# legal WRAPPED form and looks identical to the illegal one. The key the string
# hangs off is what discriminates, so this parses the YAML.

_INTERPOLATION = re.compile(r"\$\{\{(.*?)\}\}", re.S)


def _status_fn_outside_if(doc: object, path: tuple[str, ...] = ()) -> list[str]:
    """Every `${{ ... }}` interpolation calling a status fn under a non-`if` key."""
    found: list[str] = []
    if isinstance(doc, dict):
        for key, value in doc.items():
            found += _status_fn_outside_if(value, (*path, str(key)))
    elif isinstance(doc, list):
        for index, value in enumerate(doc):
            found += _status_fn_outside_if(value, (*path, str(index)))
    elif isinstance(doc, str):
        if path and path[-1] == "if":
            return found  # legal, wrapped or bare
        for expr in _INTERPOLATION.findall(doc):
            if _STATUS_FN.search(expr):
                found.append(f"{'.'.join(path)}: ${{{{{expr.strip()}}}}}")
    return found


def _validity_audit(sources: dict[str, str]) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for name, text in sources.items():
        hits = _status_fn_outside_if(yaml.safe_load(text))
        if hits:
            out[name] = hits
    return out


_STATUS_FN_IN_ENV = """
jobs:
  e2e:
    runs-on: ubuntu-latest
    steps:
      - name: Run specs
        id: specs
        run: pytest
      - name: Classify
        id: verdict
        if: always()
        env:
          OUTCOME: ${{ steps.specs.outcome }}
          IS_CANCELLED: ${{ cancelled() }}
        run: echo "$IS_CANCELLED"
"""

_STATUS_FN_ONLY_IN_IF = """
jobs:
  e2e:
    runs-on: ubuntu-latest
    steps:
      - name: Run specs
        id: specs
        run: pytest
      - name: Classify
        id: verdict
        if: ${{ always() }}
        env:
          OUTCOME: ${{ steps.specs.outcome }}
          JOB_STATUS: ${{ job.status }}
        run: echo "$OUTCOME"
      - name: Alert
        if: always() && steps.verdict.outputs.state == 'failed'
        run: curl -fsS https://example.invalid
"""


def test_the_validity_auditor_can_fail() -> None:
    """Negative control. A checker that cannot go red is not evidence of anything --
    and this one has to catch the exact line that broke run 33451148336."""
    hits = _validity_audit({"ci.yml": _STATUS_FN_IN_ENV})
    assert list(hits) == ["ci.yml"], hits
    assert any("cancelled()" in h for h in hits["ci.yml"]), hits


def test_the_validity_auditor_allows_status_fns_inside_if() -> None:
    """Both spellings of a legal `if:` -- wrapped `${{ always() }}` and bare
    `always() && ...` -- must stay clean, or the rule is a blanket ban on the
    one construct the #873 fix depends on and somebody will delete it."""
    assert _validity_audit({"ci.yml": _STATUS_FN_ONLY_IN_IF}) == {}


def test_no_workflow_uses_a_status_function_outside_an_if() -> None:
    """The shipped workflows. A hit here means the file will not START on GitHub:
    zero jobs, zero check-runs, and a PR that looks like CI is still pending."""
    assert _validity_audit(_real_sources()) == {}
