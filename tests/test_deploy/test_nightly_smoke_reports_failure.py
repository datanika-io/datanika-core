"""core#827 — the nightly connector smoke must be able to report a failure.

## The defect this guards

`.github/workflows/nightly-connector-smoke.yml` ran

    pytest tests/test_connector_smoke/ -v --tb=short -rs | tee /tmp/smoke.log

A pipeline's exit status is its **last** command's — `tee`, which always succeeds —
and GitHub's default shell for `run:` is ``bash -e {0}``, **without** ``-o pipefail``
(that is only added when a step sets ``shell: bash``). So pytest's exit code was
discarded, and the only thing left that could fail the step was the ``skipped`` grep.

Measured before the fix: the job concluded ``success`` on **eight consecutive
nights** while pytest reported ``12 failed, 9 passed``, and ``Telegram alert on
failure`` was ``skipped`` every night because ``if: failure()`` never became true.

The two halves that composed into the hole were each correct alone: the conftest was
deliberately changed to turn missing credentials into *failures* rather than *skips*
(so that "any skip at all" became a usable alarm), which moved every real failure
into precisely the blind spot the pipe had created.

## Scope — deliberately narrow, and that is the point

This does **not** assert "no workflow may pipe". Two pipes in this repo are correct
and a blanket rule would break both:

* ``deploy-pointer.yml``'s ``prune-docker-cache.sh | tee`` is *documented* as
  non-fatal on a prune failure — the fatal condition is asserted inside the script;
* ``ci.yml``'s ``npx playwright test --list | tail -1`` substitutions rely on the
  default no-pipefail shell. A comment in that file records that they would exit 1
  under ``bash -e -o pipefail`` and take the step down.

A sweep that "fixed" all pipes would therefore have broken CI. The rule worth
enforcing is about *this* step: the command whose verdict the job exists to report
must not have its exit code laundered.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest
import yaml


def _run_sed(expr: str, line: str) -> str:
    """Run a real `sed -E` with `expr`, via `-f` rather than as an argument.

    ⚠️ The expression MUST NOT be passed as an argv element. It contains `"`
    characters, and Windows has no argv array — Python renders the list into a
    single command line and CreateProcess re-splits it, mangling the quotes. The
    symptom is `sed` exiting 1 for every input, which reads like the expression
    being wrong rather than like the harness being broken. `-f scriptfile` removes
    the shell/quoting layer entirely and behaves identically on both platforms.
    """
    sed = shutil.which("sed")
    if sed is None:  # pragma: no cover - CI always has it
        pytest.skip("sed not available")
    fd, path = tempfile.mkstemp(suffix=".sed")
    try:
        with os.fdopen(fd, "w", newline="\n") as fh:
            fh.write(expr + "\n")
        proc = subprocess.run([sed, "-E", "-f", path], input=line, capture_output=True, text=True)
        assert proc.returncode == 0, f"sed failed: rc={proc.returncode} err={proc.stderr!r}"
        return proc.stdout.strip()
    finally:
        os.unlink(path)


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "nightly-connector-smoke.yml"


@pytest.fixture(scope="module")
def workflow_text() -> str:
    assert WORKFLOW.is_file(), f"missing workflow: {WORKFLOW}"
    text = WORKFLOW.read_text(encoding="utf-8")
    # Arming check: if the file were empty or unreadable every assertion below
    # would pass vacuously.
    assert "Run connector smoke tests" in text, "workflow does not contain the smoke step"
    return text


def _smoke_step(text: str) -> str:
    """The `run:` block of the `Run connector smoke tests` step."""
    start = text.index("- name: Run connector smoke tests")
    nxt = text.index("- name: Telegram alert on failure", start)
    block = text[start:nxt]
    assert "pytest tests/test_connector_smoke/" in block, "smoke step no longer runs pytest"
    return block


class TestPytestExitCodeSurvives:
    def test_the_smoke_step_does_not_launder_pytest_exit_code(self, workflow_text: str) -> None:
        block = _smoke_step(workflow_text)
        if "| tee" not in block and "|tee" not in block:
            return  # no pipe at all — nothing to launder
        handled = (
            "set -o pipefail" in block
            or "set -eo pipefail" in block
            or "set -euo pipefail" in block
            or "PIPESTATUS" in block
            or re.search(r"^\s*shell:\s*bash", block, re.MULTILINE) is not None
        )
        assert handled, (
            "The smoke step pipes pytest into tee without pipefail, shell: bash, or "
            "PIPESTATUS. A pipeline's exit status is tee's, and GitHub's default "
            "run: shell is `bash -e {0}` with no -o pipefail — so pytest's failure "
            "is discarded and the job reports success. This is core#827, which hid "
            "12 failing connector probes for eight consecutive nights."
        )

    def test_a_nonzero_pytest_exit_actually_fails_the_step(self, workflow_text: str) -> None:
        """Capturing the status is not enough — something must act on it."""
        block = _smoke_step(workflow_text)
        assert re.search(r'exit\s+"?\$(\{)?rc', block) or "PIPESTATUS" in block, (
            "The step captures pytest's exit code but never exits non-zero on it. "
            "Recording a failure without acting on it is the same green."
        )

    def test_the_skip_guard_is_kept(self, workflow_text: str) -> None:
        """The skip guard is still the only thing that catches a dropped env gate."""
        block = _smoke_step(workflow_text)
        assert "skipped" in block and "::error::" in block, (
            "The skip guard was removed. It catches the one hole pipefail cannot: if "
            "DATANIKA_CONNECTOR_SMOKE were dropped, every probe would skip at "
            "collection time and pytest would exit 0."
        )


class TestNoShellSideCredentialParsing:
    """core#827 + core#983 — was 'the quote-stripping sed must handle digits'.

    core#827's defect was real: the sed used ``[A-Z_]+``, so a name like
    ``GA4_PROPERTY_ID`` fell through unchanged and reached ``$GITHUB_ENV`` with its
    quote characters attached — core#270's bug, recreated for every digit-bearing
    name, and invisible from both ends because the file has the value and the
    secret is non-empty.

    🚨 **core#983 deleted the sed, along with the whole shell-side parsing layer.**
    Credentials are now individual secrets and variables, which GitHub injects into
    the process environment verbatim — there is no dotenv text to quote, unquote or
    misparse. The class therefore flips from *"the parser must handle digits"* to
    *"there must be no parser"*, which is the stronger statement and, unlike the
    old one, is non-vacuous today.

    ``test_the_sed_in_the_workflow_actually_strips_a_digit_bearing_name`` was
    removed rather than relaxed: it extracted the live expression and executed it,
    so with no expression to extract it could only be made to pass by asserting
    nothing. ``_run_sed`` and the control below are kept — the control is
    self-contained, it is the record that the defect was real, and it is what keeps
    the harness itself honest if a parser ever comes back.
    """

    def test_the_workflow_does_no_shell_side_credential_parsing(self, workflow_text: str) -> None:
        """The core#983 invariant, stated positively so it can actually fail.

        A bundle decoded in a ``run:`` block is a value the runner has never been
        given, so it is unmasked by construction and every downstream step prints
        it (core#943, 53 public logs). Registering each credential removes the
        possibility rather than patching it.

        🚨 **Asserted over the PARSED steps, never over the file text.** The first
        version of this test searched ``workflow_text`` for ``base64 -d`` and
        ``QA_CONNECTOR_CREDENTIALS`` and was failed by the comment block in the
        workflow *explaining that the bundle had been removed*. An absence
        assertion over a document is satisfied — and defeated — by the document
        describing the absence. It is the same defect as asserting on the prose
        rather than on the executable line, arriving from the other direction, and
        it cost three iterations in one session before it was named.
        """
        offenders = self._offenders(yaml.safe_load(workflow_text))
        assert not offenders, (
            f"the nightly parses credentials in shell again: {offenders}. Values decoded "
            "at runtime are strings the runner has never seen, so GitHub cannot mask them "
            "and every later step's `##[group]Run` header prints them — on a PUBLIC repo. "
            "Register each credential as its own secret or variable instead (core#983); "
            "if a bundle is genuinely unavoidable, it needs the ::add-mask:: loop back, "
            "and tests/test_deploy/test_workflow_secret_masking.py is what enforces that."
        )

    def test_the_no_parsing_check_can_actually_fail(self, workflow_text: str) -> None:
        """Negative control — the assertion above is over an empty list today.

        Without this, a parse that silently yielded no steps would read exactly
        like a workflow that does no shell-side parsing. Re-injects the pre-fix
        decode into a copy of the real document and confirms it is caught.

        ⚠️ Asserted as a **delta** against the same document's own baseline, not as
        an absolute count of 2. An absolute count is correct only while the
        workflow is clean — so the moment somebody actually reintroduces a bundle,
        the control that exists to detect that would itself fail, with a message
        about its own arithmetic rather than about the leak. A control must not
        misreport in precisely the state it was written for.
        """
        baseline = self._offenders(yaml.safe_load(workflow_text))

        data = yaml.safe_load(workflow_text)
        steps = data["jobs"]["smoke"]["steps"]
        assert len(steps) >= 5, f"only {len(steps)} steps parsed out of the smoke job"
        steps.append(
            {
                "name": "Materialize connector credentials",
                "env": {"QA_CONNECTOR_CREDENTIALS": "${{ secrets.QA_CONNECTOR_CREDENTIALS }}"},
                "run": 'printf %s "$QA_CONNECTOR_CREDENTIALS" | base64 -d > /tmp/c.env\n',
            }
        )
        mutated = self._offenders(data)
        assert len(mutated) - len(baseline) == 2, (
            "Re-injecting the pre-fix materializer was not detected on both the run "
            f"block and the env binding, so the check above cannot fire. baseline="
            f"{baseline} mutated={mutated}"
        )

    @staticmethod
    def _offenders(data: dict) -> list[str]:
        """Shell-side credential parsing, per step. Shared by the check and its control."""
        out: list[str] = []
        for job_id, job in (data.get("jobs") or {}).items():
            for i, step in enumerate(job.get("steps") or []):
                if not isinstance(step, dict):
                    continue
                where = f"{job_id}/{step.get('name') or i}"
                run = str(step.get("run") or "")
                for marker in ("base64 -d", "base64 --decode"):
                    if marker in run:
                        out.append(f"{where}: run contains {marker!r}")
                env = step.get("env") or {}
                if isinstance(env, dict):
                    for key, value in env.items():
                        if "CREDENTIALS" in str(key).upper() and "secrets." in str(value):
                            out.append(f"{where}: env binds a credential bundle {key!r}")
        return out

    def test_no_sed_uses_a_digitless_character_class(self, workflow_text: str) -> None:
        """Forward guard. Passes vacuously today — deliberately, and it is cheap.

        There is no quote-stripping sed to check, and that is the point of the test
        above. This one costs nothing and is the thing that fires if a parser
        returns carrying core#827's exact defect.
        """
        offenders = re.findall(r"sed -E 's/\^\(\[A-Z_\]\+\)=", workflow_text)
        assert not offenders, (
            f"{len(offenders)} quote-stripping sed(s) use [A-Z_]+, which does not match "
            "a variable name containing a digit (GA4_PROPERTY_ID, S3_BUCKET). Those "
            "lines fall through unchanged and reach $GITHUB_ENV with their quotes "
            "attached — core#270's bug, re-created for every digit-bearing name."
        )

    def test_the_control_shows_the_old_expression_failing(self) -> None:
        """Negative control: the pre-fix expression must FAIL this same check.

        Without this, the test above would pass against an expression that happens
        to work for an unrelated reason, and we would never know it could fail.
        """
        old = r's/^([A-Z_]+)="(.*)"$/\1=\2/'
        got = _run_sed(old, 'GA4_PROPERTY_ID="value1"\n')
        assert got == 'GA4_PROPERTY_ID="value1"', (
            "The pre-fix expression stripped the quotes, so this whole test class is "
            "guarding a defect that does not exist as described. Re-derive before "
            "trusting the fix."
        )
