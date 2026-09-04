"""`e2e-staging` must not report `gating_failed` when zero specs ran (core#1029).

When Playwright's `globalSetup` dies, the `Run gating E2E specs against staging` step exits
non-zero **having collected nothing**. The classifier keys on that step's outcome alone, so it
emits `gating_failed` and the job says, in an `::error::`:

    GATING E2E specs FAILED. This red is a real test failure and holds the promotion.

Every word of which is false. No spec ran; there is no reading in either direction. The
promotion pre-flight then goes looking for a failing test that does not exist, and — worse —
a later green reads as "the flake cleared" rather than "the harness finally started".

**The signal already exists and is already computed.** `scripts/detect_flaky_gating.py`
distinguishes exactly this case and has since core#757:

    ``no-evidence``  the report has no tests at all, or could not be read
    Only ``clean`` is a green. ``no-evidence`` is neither -- exactly like a skip.

It runs as `id: flaky_gate` under `if: always()` and writes `status=` to `$GITHUB_OUTPUT`, so
`steps.flaky_gate.outputs.status` is available to the classifier. It simply was not consulted.

**Why this file executes the classifier instead of grepping `ci.yml`.** The property is
behavioural — *which state comes out for a given combination of inputs* — and the branch
ORDER is half of it. A text assertion that the classifier "mentions no-evidence" is satisfied
by a comment mentioning it (`docs/QA_RULES.md` §26: a guard reading a workflow's raw step text
can be satisfied by the step's own comment), and would also pass if the new branch were placed
*after* `gating_failed`, where it can never be reached. So the real `run:` block is extracted
and run under `bash` against a matrix of inputs.

`test_a_wrong_build_with_no_report_is_still_wrong_build` is the control that matters most: an
attribution refusal ALSO leaves no report, and misclassifying it as `no_verdict` would page
"fix the harness" for a harness that is fine. core#876 put `wrong_build` ahead of `no_verdict`
deliberately, and the fix for #1029 must not disturb that ordering.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CI = REPO_ROOT / ".github" / "workflows" / "ci.yml"

JOB = "e2e-staging"
STEP_NAME = "Classify what this job's result means"


def _bash() -> str:
    exe = shutil.which("bash")
    if exe is None:  # pragma: no cover - a box with no bash cannot run CI either
        pytest.fail("bash not found; this suite must not silently stop testing the classifier")
    return exe


@pytest.fixture(scope="module")
def classifier_script() -> str:
    """The real `run:` body of the e2e-staging verdict step, straight out of `ci.yml`."""
    doc = yaml.safe_load(CI.read_text(encoding="utf-8"))
    job = (doc.get("jobs") or {}).get(JOB)
    assert job is not None, f"{JOB} job vanished from ci.yml -- this guard is now testing nothing"
    for step in job.get("steps") or []:
        if step.get("name") == STEP_NAME:
            script = step.get("run")
            assert script, f"{JOB} / {STEP_NAME} has no `run:` block"
            return script
    pytest.fail(
        f"no step named {STEP_NAME!r} in the {JOB} job. If it was renamed, rename it here too "
        "rather than deleting this guard -- a guard that cannot find its target passes silently."
    )


def classify(
    script: str,
    tmp_path: Path,
    *,
    job_status: str = "success",
    gating: str = "success",
    attribution: str = "success",
    flaky_status: str = "clean",
) -> str:
    """Run the classifier and return the `state=` it wrote to $GITHUB_OUTPUT.

    Env is inherited, not replaced: on Windows a bare env strips PATH and SystemRoot and the
    subprocess dies for a reason that has nothing to do with the assertion.
    """
    out_file = tmp_path / "gh_output"
    out_file.write_text("", encoding="utf-8")
    proc = subprocess.run(
        [_bash(), "-c", script],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env={
            **__import__("os").environ,
            "GITHUB_OUTPUT": out_file.as_posix(),
            "JOB_STATUS": job_status,
            "GATING_OUTCOME": gating,
            "ATTRIB_OUTCOME": attribution,
            "FLAKY_STATUS": flaky_status,
        },
    )
    written = out_file.read_text(encoding="utf-8")
    m = re.search(r"^state=(\S+)$", written, re.M)
    assert m, (
        f"the classifier wrote no state= line.\n"
        f"--- $GITHUB_OUTPUT ---\n{written}\n--- stdout ---\n{proc.stdout}\n"
        f"--- stderr ---\n{proc.stderr}"
    )
    return m.group(1)


# ---------------------------------------------------------------------------
# The criterion (core#1029). Red before the fix.
# ---------------------------------------------------------------------------


def test_a_failed_gating_step_that_collected_nothing_is_no_verdict(classifier_script, tmp_path):
    """globalSetup died: the step is red, the report has no tests. That is not a spec failure.

    This is the exact shape of the run on the issue: `Run gating E2E specs` failed, and the
    detector -- which runs on `always()` and therefore did run -- reported `no-evidence`.
    """
    state = classify(
        classifier_script,
        tmp_path,
        job_status="failure",
        gating="failure",
        flaky_status="no-evidence",
    )
    assert state == "no_verdict", (
        f"classified {state!r}. A gating step that failed having collected ZERO specs is an "
        "absent reading, not a failing test -- and `gating_failed` tells the promotion "
        "pre-flight to go find a red spec that does not exist (core#1029)."
    )


def test_an_empty_report_is_not_clean_even_when_the_step_passed(classifier_script, tmp_path):
    """The de00365 shape: `{\"files\":[],\"stats\":{\"total\":0,\"ok\":true}}`.

    The detector's own docstring is explicit that an empty report must not read as clean.
    A passing step over zero specs is the single most dangerous state in this pipeline,
    because it is the one that looks most like success.
    """
    state = classify(
        classifier_script,
        tmp_path,
        job_status="success",
        gating="success",
        flaky_status="no-evidence",
    )
    assert state == "no_verdict", (
        f"classified {state!r}. Zero specs collected is never a green: 'clean' here would "
        "promote a commit that no E2E spec ever ran against."
    )


# ---------------------------------------------------------------------------
# Controls. Green BEFORE and AFTER the fix -- they are what stop the cheap wrong
# fix (classifying everything `no_verdict`, or reordering the branches).
# ---------------------------------------------------------------------------


def test_a_real_spec_failure_is_still_gating_failed(classifier_script, tmp_path):
    """The control that matters: specs ran, one went red. Must still hold the promotion."""
    state = classify(
        classifier_script, tmp_path, job_status="failure", gating="failure", flaky_status="clean"
    )
    assert state == "gating_failed", (
        f"classified {state!r}. A genuine red spec must still be reported as one -- a fix that "
        "turns every failure into `no_verdict` would silence the gate entirely."
    )


def test_a_wrong_build_with_no_report_is_still_wrong_build(classifier_script, tmp_path):
    """core#876's ordering. An attribution refusal skips the gating step, so it ALSO leaves
    no report -- but the right response is 're-deploy this commit', not 'fix the harness'."""
    state = classify(
        classifier_script,
        tmp_path,
        job_status="failure",
        gating="skipped",
        attribution="failure",
        flaky_status="no-evidence",
    )
    assert state == "wrong_build", (
        f"classified {state!r}. The no-evidence branch must sit AFTER the attribution branch, "
        "or every wrong-build run starts paging as a broken harness (core#876)."
    )


def test_a_cancelled_run_is_still_cancelled(classifier_script, tmp_path):
    """A cancelled run also has no report, and is neither green nor red."""
    state = classify(
        classifier_script,
        tmp_path,
        job_status="cancelled",
        gating="cancelled",
        flaky_status="no-evidence",
    )
    assert state == "cancelled", f"classified {state!r}; a cancelled run must stay `cancelled`."


def test_a_clean_run_is_still_clean(classifier_script, tmp_path):
    """The acceptance control. Without this, 'never say clean' passes everything above."""
    state = classify(
        classifier_script, tmp_path, job_status="success", gating="success", flaky_status="clean"
    )
    assert state == "clean", f"classified {state!r}; a normal green run must still be `clean`."


def test_a_flaky_but_passing_run_is_still_clean(classifier_script, tmp_path):
    """The detector deliberately does not change the gate's verdict (core#757) -- it alerts
    and files separately. Consulting its status must not accidentally start gating on it."""
    state = classify(
        classifier_script, tmp_path, job_status="success", gating="success", flaky_status="flaky"
    )
    assert state == "clean", (
        f"classified {state!r}. core#757 leaves the gate's pass/fail unchanged until core#753; "
        "reading `status` for the no-evidence case must not turn `flaky` into a gate."
    )


def test_a_green_specs_step_in_a_red_job_is_still_infra_only(classifier_script, tmp_path):
    """core#873: the job can be red for artifact upload. That is not an E2E failure."""
    state = classify(
        classifier_script, tmp_path, job_status="failure", gating="success", flaky_status="clean"
    )
    assert state == "infra_only", f"classified {state!r}; expected `infra_only` (core#873)."
