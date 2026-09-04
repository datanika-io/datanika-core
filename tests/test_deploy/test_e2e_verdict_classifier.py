"""The staging/SSO verdict classifiers, executed rather than read (core#1029).

`e2e-staging` used to resolve a FAILED gating step to `gating_failed`
unconditionally. A `globalSetup` crash exits that step exactly as a red spec does
-- in seconds, with ZERO specs run -- so the run told the reader

    "So it means the gating specs themselves went red — not that some other step
     failed."

which is false for that run and aims a responder at twelve specs none of which
executed. The two states need opposite responses: a red spec is a QA/Engineering
question about a spec; a dead `globalSetup` is a seed/schema question (the
core#951 teardown-FK shape) that no amount of looking at specs will find.

⚠️ **The obvious framing -- "the fix already exists one job over" -- is true of
the structure and wrong about the mechanism.** Both classifiers are already
identical in shape and both already have a `no_verdict` branch; copying
`e2e-sso`'s verbatim changes nothing. `e2e-sso` is correct because its harness
failure happens in a SEPARATE step, so `Run SSO specs` is `skipped`. Here the
harness dies inside the playwright invocation, so the step ran and failed. The
missing input is the TALLY, which `detect_flaky_gating.py` had already computed
and printed two steps earlier and which nothing read.

These tests EXECUTE the script out of `ci.yml` under bash, so they are bound to
the shipped text rather than to a restatement of it. A restatement is the thing
that goes stale silently.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CI = _REPO_ROOT / ".github" / "workflows" / "ci.yml"

_BASH = shutil.which("bash")
pytestmark = pytest.mark.skipif(_BASH is None, reason="bash is required to execute the classifier")


def _verdict_script(job: str) -> str:
    """The `run:` body of the named job's `verdict` step, verbatim from ci.yml."""
    workflow = yaml.safe_load(_CI.read_text(encoding="utf-8"))
    steps = workflow["jobs"][job]["steps"]
    matches = [s for s in steps if s.get("id") == "verdict"]
    assert len(matches) == 1, f"expected exactly one verdict step in {job}, found {len(matches)}"
    return matches[0]["run"]


def _run(job: str, tmp_path: Path, **env: str) -> str:
    """Execute the classifier and return its stdout.

    `GITHUB_OUTPUT` is pointed at a scratch file so the `>> "$GITHUB_OUTPUT"`
    write does not fail the script -- that redirect is not what is under test.
    """
    script = tmp_path / "verdict.sh"
    # write_bytes, not write_text: on Windows the latter emits CRLF and bash then
    # reads `failure\r`, so every comparison is false for a correct value.
    script.write_bytes(_verdict_script(job).encode("utf-8"))
    out_file = tmp_path / "gh_output"
    out_file.write_text("", encoding="utf-8")
    proc = subprocess.run(
        [_BASH, str(script)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env={"GITHUB_OUTPUT": str(out_file), "PATH": "/usr/bin:/bin", **env},
    )
    return proc.stdout + proc.stderr


class TestEveryJobHasAVerdictStep:
    """If the step is renamed or dropped, every test below silently tests nothing."""

    @pytest.mark.parametrize("job", ["e2e-staging", "e2e-sso"])
    def test_the_step_exists_and_is_extractable(self, job):
        assert "STATE=" in _verdict_script(job)


class TestStagingClassifier:
    JOB = "e2e-staging"

    def test_a_failed_step_with_no_tally_is_no_verdict(self, tmp_path):
        """core#1029's run 33667999719: globalSetup died, zero specs ran."""
        out = _run(
            self.JOB,
            tmp_path,
            GATING_OUTCOME="failure",
            ATTRIB_OUTCOME="success",
            JOB_STATUS="failure",
            FLAKY_STATUS="no-evidence",
        )
        assert "verdict: no_verdict" in out
        assert "verdict: gating_failed" not in out

    def test_a_failed_step_with_a_real_tally_is_still_gating_failed(self, tmp_path):
        """The control. Without this the change could be 'always no_verdict',
        which would hide every real E2E regression -- a far worse trade."""
        out = _run(
            self.JOB,
            tmp_path,
            GATING_OUTCOME="failure",
            ATTRIB_OUTCOME="success",
            JOB_STATUS="failure",
            FLAKY_STATUS="clean",
        )
        assert "verdict: gating_failed" in out
        assert "verdict: no_verdict" not in out

    def test_a_green_run_is_unaffected(self, tmp_path):
        out = _run(
            self.JOB,
            tmp_path,
            GATING_OUTCOME="success",
            ATTRIB_OUTCOME="success",
            JOB_STATUS="success",
            FLAKY_STATUS="clean",
        )
        assert "verdict: clean" in out

    def test_a_wrong_build_still_outranks_the_tally(self, tmp_path):
        """core#876's ordering must survive: an attribution refusal skips the
        gating step, so its tally is also absent -- and `wrong_build` (re-deploy)
        is a different instruction from `no_verdict` (fix the harness)."""
        out = _run(
            self.JOB,
            tmp_path,
            GATING_OUTCOME="skipped",
            ATTRIB_OUTCOME="failure",
            JOB_STATUS="failure",
            FLAKY_STATUS="no-evidence",
        )
        assert "verdict: wrong_build" in out

    def test_a_cancelled_run_still_outranks_everything(self, tmp_path):
        out = _run(
            self.JOB,
            tmp_path,
            GATING_OUTCOME="cancelled",
            ATTRIB_OUTCOME="success",
            JOB_STATUS="cancelled",
            FLAKY_STATUS="no-evidence",
        )
        assert "verdict: cancelled" in out

    def test_the_no_verdict_message_names_the_tally(self, tmp_path):
        """The message is the artifact a responder acts on. Naming the tally is
        what stops the next person re-reading twelve specs that never ran."""
        out = _run(
            self.JOB,
            tmp_path,
            GATING_OUTCOME="failure",
            ATTRIB_OUTCOME="success",
            JOB_STATUS="failure",
            FLAKY_STATUS="no-evidence",
        )
        assert "tally=no-evidence" in out
        assert "globalSetup" in out


class TestSsoClassifierIsUnchanged:
    """core#854 established that e2e-sso classifies correctly already -- its
    harness failure lands in a separate step, so the specs step is `skipped`.
    Pinned so a future 'unify the classifiers' pass cannot quietly regress it."""

    JOB = "e2e-sso"

    def test_skipped_specs_are_no_verdict(self, tmp_path):
        out = _run(
            self.JOB,
            tmp_path,
            SPECS_OUTCOME="skipped",
            ATTRIB_OUTCOME="success",
            ATTRIB_POST_OUTCOME="success",
            JOB_STATUS="failure",
        )
        assert "verdict: no_verdict" in out

    def test_failed_specs_are_specs_failed(self, tmp_path):
        out = _run(
            self.JOB,
            tmp_path,
            SPECS_OUTCOME="failure",
            ATTRIB_OUTCOME="success",
            ATTRIB_POST_OUTCOME="success",
            JOB_STATUS="failure",
        )
        assert "verdict: specs_failed" in out
