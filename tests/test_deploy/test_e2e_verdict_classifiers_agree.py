"""Both E2E classifiers must call an absent reading `no_verdict` — not a spec failure.

`e2e-staging` and `e2e-sso` each carry a *"Classify what this job's result means"* step, and
the two are the same design: lead with `cancelled`, then `wrong_build` (core#876), then the
real spec verdict, with an `else` catching everything that produced no reading at all.

`tests/test_deploy/test_e2e_staging_verdict_classifier.py` pins that behaviour for
**`e2e-staging` only** — it does not mention `e2e-sso` anywhere. This file covers the other
job and the invariants the two share.

**Why that gap is worth closing, when `e2e-sso` is already correct.** It is correct *by
accident of its shape*, and the accident is one refactor deep:

* In `e2e-staging`, the harness runs inside the Playwright call — `globalSetup` dies, the
  gating step exits non-zero **having collected nothing**, and `GATING_OUTCOME` is `failure`.
  That is core#1029: the classifier read `failure` and announced a spec failure that never
  happened. Fixing it needed a second signal (`FLAKY_STATUS=no-evidence`).
* In `e2e-sso`, the harness is a **separate step** — Authentik bootstrap, the tunnel, the
  compose stack. When it dies, `Run SSO specs` is `skipped`, `SPECS_OUTCOME` is `skipped`, and
  the `else` branch correctly yields `no_verdict`. No second signal is needed *for that route*.

🚨 **CORRECTED 2026-09-05 (core#1099). The sentence above used to end "No second signal is
needed", full stop, and that overstated what this file covers.** It reasons about the harness
*dying*, which produces `skipped`. It does not reason about the specs *running and all
skipping*, which produces `success` — and `e2e-sso`'s nine Authentik specs sit behind
`process.env.DATANIKA_E2E_SSO_AUTHENTIK !== "1"`, so one env var makes every one of them skip
while Playwright exits 0. `SPECS_OUTCOME=success` then classifies **`clean`**, and the
`no_verdict` alert — which exists precisely so an absent verdict is not misread as a fix —
cannot fire, because it keys on the specs step *not* being `success`.

So `e2e-staging` is covered against its analogue of that route by two things this job has
neither of: `Assert the @slow specs were actually collected`, and the `FLAKY_STATUS` tally.
The asymmetry this file pins is real; it was simply not the only one. The gate half is now
guarded by `test_e2e_sso_tier_is_measured.py`; the residual — a `success` from a run that
executed zero IdP specs by any other route — needs the step to read its own tally and is
`ci.yml`'s owner's call, tracked on core#1099.

**The lesson worth more than the correction: a guard that states why something is safe is
making a claim, and that claim needs the same scrutiny as the code.** This one enumerated the
routes it had thought of and read as though it had enumerated all of them.

So `e2e-sso` avoids #1029 because of where its harness lives, not because anyone decided it
should. Move that bootstrap inside the specs call — or add a `|| true` that turns a skip into
a red — and it acquires #1029 exactly, with no guard to notice. core#951 is the precedent that
this is not hypothetical: the SSO tier has already been wedged by a harness-side defect (PII
tables missing from the E2E seed teardown) while the job's colour was the only thing anyone
read.

The failure mode being prevented is a **unification pass**: the two classifiers look alike
enough that "factor these into one" is an obvious tidy-up, and the obvious way to do it is to
key both on their spec step's outcome. That regresses the job that was right.

**These tests execute the real `run:` block**, for the reason the sibling file gives: the
property is *which state comes out for a given combination of inputs*, and branch ORDER is
half of it. A text assertion is satisfied by a comment mentioning the right word
(`docs/QA_RULES.md` §26), and would also pass with a branch placed where it can never be
reached.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CI = REPO_ROOT / ".github" / "workflows" / "ci.yml"
STEP_NAME = "Classify what this job's result means"


@dataclass(frozen=True)
class ClassifierSpec:
    """How one job's classifier is driven, and what it calls a real spec failure."""

    job: str
    #: env var carrying the outcome of the step that runs the specs
    specs_var: str
    #: the state this job emits when the specs genuinely ran and genuinely failed
    failed_state: str
    #: every env var the `run:` block reads, with a benign default
    defaults: dict[str, str]


STAGING = ClassifierSpec(
    job="e2e-staging",
    specs_var="GATING_OUTCOME",
    failed_state="gating_failed",
    defaults={
        "JOB_STATUS": "success",
        "GATING_OUTCOME": "success",
        "ATTRIB_OUTCOME": "success",
        "FLAKY_STATUS": "clean",
    },
)

SSO = ClassifierSpec(
    job="e2e-sso",
    specs_var="SPECS_OUTCOME",
    failed_state="specs_failed",
    defaults={
        "JOB_STATUS": "success",
        "SPECS_OUTCOME": "success",
        "ATTRIB_OUTCOME": "success",
        "ATTRIB_POST_OUTCOME": "success",
    },
)

BOTH = [STAGING, SSO]
IDS = [s.job for s in BOTH]


def _bash() -> str:
    exe = shutil.which("bash")
    if exe is None:  # pragma: no cover - a box with no bash cannot run CI either
        pytest.fail("bash not found; this suite must not silently stop testing the classifiers")
    return exe


def _classifier_script(job: str) -> str:
    """The real `run:` body of one job's verdict step, straight out of `ci.yml`."""
    doc = yaml.safe_load(CI.read_text(encoding="utf-8"))
    jobs = doc.get("jobs") or {}
    assert job in jobs, (
        f"the {job!r} job vanished from ci.yml -- this guard is now testing nothing. "
        "If it was renamed, rename it here rather than deleting the guard."
    )
    for step in jobs[job].get("steps") or []:
        if step.get("name") == STEP_NAME:
            script = step.get("run")
            assert script, f"{job} / {STEP_NAME} has no `run:` block"
            return script
    pytest.fail(
        f"no step named {STEP_NAME!r} in the {job!r} job. A guard that cannot find its "
        "target passes silently, so this is a failure rather than a skip."
    )


def classify(spec: ClassifierSpec, tmp_path: Path, **overrides: str) -> str:
    """Run one job's classifier and return the `state=` it wrote to $GITHUB_OUTPUT."""
    unknown = set(overrides) - set(spec.defaults)
    assert not unknown, (
        f"{sorted(unknown)} is not read by the {spec.job} classifier. Setting an env var the "
        "script never reads is a test that silently stops discriminating."
    )
    env = {**os.environ, **spec.defaults, **overrides}
    out_file = tmp_path / f"gh_output_{spec.job}"
    out_file.write_text("", encoding="utf-8")
    env["GITHUB_OUTPUT"] = out_file.as_posix()

    proc = subprocess.run(
        [_bash(), "-c", _classifier_script(spec.job)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
    )
    written = out_file.read_text(encoding="utf-8")
    m = re.search(r"^state=(\S+)$", written, re.M)
    assert m, (
        f"the {spec.job} classifier wrote no state= line.\n"
        f"--- $GITHUB_OUTPUT ---\n{written}\n--- stdout ---\n{proc.stdout}\n"
        f"--- stderr ---\n{proc.stderr}"
    )
    return m.group(1)


# ---------------------------------------------------------------------------
# The shared invariant, across both jobs.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("spec", BOTH, ids=IDS)
def test_a_spec_step_that_never_ran_is_no_verdict(spec: ClassifierSpec, tmp_path: Path) -> None:
    """The harness died before any spec started, so the step is `skipped`.

    This is the state core#951 produced on the SSO tier (a wedged E2E seed teardown) and the
    state core#1029 produced on staging by a different route. There is no reading in either
    direction: not a pass, not a regression.
    """
    state = classify(spec, tmp_path, **{spec.specs_var: "skipped", "JOB_STATUS": "failure"})
    assert state == "no_verdict", (
        f"{spec.job}: a job whose spec step never ran classified as {state!r}. An absent "
        f"reading must be `no_verdict` -- classifying it {spec.failed_state!r} announces a "
        "test failure that did not happen and sends the promotion pre-flight looking for it "
        "(core#1029), while classifying it `clean` promotes a commit no spec ever ran "
        "against (core#951)."
    )


@pytest.mark.parametrize("spec", BOTH, ids=IDS)
def test_a_cancelled_job_is_neither_green_nor_red(spec: ClassifierSpec, tmp_path: Path) -> None:
    """A cancelled verdict is its own state and must not fall through to a spec failure."""
    state = classify(spec, tmp_path, JOB_STATUS="cancelled")
    assert state == "cancelled", (
        f"{spec.job}: a cancelled job classified as {state!r}. Nothing reads more like clean "
        "than an absence, and a cancelled run is neither green nor red."
    )


@pytest.mark.parametrize("spec", BOTH, ids=IDS)
def test_wrong_build_outranks_the_spec_verdict(spec: ClassifierSpec, tmp_path: Path) -> None:
    """core#876: a run that cannot honestly report on this commit says so first.

    Tested with the spec step ALSO failing, because that is the ordering that matters — if
    `wrong_build` were placed after the failure branch it would be unreachable in exactly the
    case it exists for, and a purely textual guard would still have passed.
    """
    state = classify(
        spec,
        tmp_path,
        ATTRIB_OUTCOME="failure",
        **{spec.specs_var: "failure", "JOB_STATUS": "failure"},
    )
    assert state == "wrong_build", (
        f"{spec.job}: an attribution refusal classified as {state!r}. The specs were never "
        "started against this commit, so a spec verdict here describes somebody else's build "
        "(core#876)."
    )


@pytest.mark.parametrize("spec", BOTH, ids=IDS)
def test_a_genuine_spec_failure_is_still_reported(spec: ClassifierSpec, tmp_path: Path) -> None:
    """The control. Narrowing a classifier until it never reports a real failure is worse
    than the bug it was narrowed to fix, and it is silent."""
    state = classify(spec, tmp_path, **{spec.specs_var: "failure", "JOB_STATUS": "failure"})
    assert state == spec.failed_state, (
        f"{spec.job}: a real spec failure classified as {state!r} rather than "
        f"{spec.failed_state!r}. This guard must not be satisfiable by a classifier that has "
        "stopped reporting failures at all."
    )


@pytest.mark.parametrize("spec", BOTH, ids=IDS)
def test_a_clean_run_is_still_clean(spec: ClassifierSpec, tmp_path: Path) -> None:
    """The other control: the happy path must survive every guard above."""
    state = classify(spec, tmp_path)
    assert state == "clean", f"{spec.job}: a fully green run classified as {state!r}."


# ---------------------------------------------------------------------------
# The asymmetry, pinned so a "unify these" pass has to confront it.
# ---------------------------------------------------------------------------


def test_staging_can_tell_a_red_harness_from_a_red_suite(tmp_path: Path) -> None:
    """The staging half of the asymmetry, asserted BEHAVIOURALLY (core#1029).

    staging's harness dies INSIDE the specs call, so its step outcome is `failure` and a
    second signal — the report tally — is required to tell "the specs failed" from "no spec
    ran". sso's harness is a separate step, so a skip already carries that information and no
    tally is needed.

    ⚠️ **This assertion was originally written as `"FLAKY_STATUS" in <script>` and that was
    wrong.** Deleting the `no-evidence` branch from the real `ci.yml` left the guard GREEN,
    because the variable is still interpolated into the `no_verdict` error message a few lines
    below — so the check was satisfied by a *mention* of the fix rather than by the fix. Same
    family as `docs/QA_RULES.md` §26. It was caught by mutating the shipped workflow; the
    guard's own suite passed throughout. Assert what comes out, never what is written down.
    """
    state = classify(
        STAGING,
        tmp_path,
        GATING_OUTCOME="failure",
        FLAKY_STATUS="no-evidence",
        JOB_STATUS="failure",
    )
    assert state == "no_verdict", (
        f"e2e-staging classified a red gating step that collected ZERO specs as {state!r}. "
        "With no report tally consulted, a harness that died inside globalSetup is "
        "indistinguishable from a real suite failure, and the job announces a test failure "
        "that never happened -- core#1029."
    )

    clean_run = classify(
        STAGING, tmp_path, GATING_OUTCOME="failure", FLAKY_STATUS="clean", JOB_STATUS="failure"
    )
    assert clean_run == STAGING.failed_state, (
        "the control: with a real report present, a failing gating step must still be "
        f"{STAGING.failed_state!r} and not {clean_run!r}. A tally check narrowed until every "
        "red becomes `no_verdict` silently stops reporting failures."
    )
