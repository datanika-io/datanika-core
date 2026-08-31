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


class TestCredentialNamesWithDigits:
    """core#827 — the quote-stripping sed dropped every name containing a digit."""

    def test_no_sed_uses_a_digitless_character_class(self, workflow_text: str) -> None:
        offenders = re.findall(r"sed -E 's/\^\(\[A-Z_\]\+\)=", workflow_text)
        assert not offenders, (
            f"{len(offenders)} quote-stripping sed(s) use [A-Z_]+, which does not match "
            "a variable name containing a digit (GA4_PROPERTY_ID, S3_BUCKET). Those "
            "lines fall through unchanged and reach $GITHUB_ENV with their quotes "
            "attached — core#270's bug, re-created for every digit-bearing name."
        )

    def test_the_sed_in_the_workflow_actually_strips_a_digit_bearing_name(
        self, workflow_text: str
    ) -> None:
        """Run the real expression, extracted from the workflow, against real input.

        Asserting on the regex *text* would pass for any expression that merely
        contains `A-Z0-9`. This executes it.
        """
        exprs = [e for e in re.findall(r"sed -E '([^']+)'", workflow_text) if e.startswith("s/^(")]
        assert exprs, "no quote-stripping sed found in the workflow — has it been renamed?"

        for expr in exprs:
            for name in ("GA4_PROPERTY_ID", "S3_BUCKET", "HUBSPOT_ACCESS_TOKEN"):
                got = _run_sed(expr, f'{name}="value1"\n')
                assert got == f"{name}=value1", (
                    f"sed -E '{expr}' failed to strip quotes from {name}: got {got!r}. "
                    "A digit in the variable name is enough to defeat a [A-Z_]+ class."
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
