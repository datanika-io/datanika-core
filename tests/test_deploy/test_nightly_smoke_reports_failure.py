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
    """The `run:` block of the `Run connector smoke tests` step, RAW.

    Includes comments. Only use where prose is legitimately part of the claim;
    for anything asserting what the step *does*, use `_smoke_code`.
    """
    start = text.index("- name: Run connector smoke tests")
    nxt = text.index("- name: Telegram alert on failure", start)
    block = text[start:nxt]
    assert "pytest tests/test_connector_smoke/" in block, "smoke step no longer runs pytest"
    return block


def _strip_comments(block: str) -> str:
    """Drop whole-line `#` comments. See core#1055.

    🚨 **Every assertion about what this step DOES must go through here.** All
    three exit-code guards below used to assert containment against the raw
    block, and this step's prose names `PIPESTATUS`, `pipefail`, `skipped` and
    `::error::`. Measured on `origin/dev`:

        PIPESTATUS in whole block : True
        PIPESTATUS in code only   : False    <- comment-only, always has been

    So `or "PIPESTATUS" in block` was permanently true, and deleting
    `set -o pipefail` — which *is* core#827 — left the guard GREEN. Same for
    the `exit "$rc"` branch and the skip guard. Three guards, none able to
    fail, protecting a defect that already cost eight consecutive nights of
    `success` over `12 failed, 9 passed`.

    Whole-line only: a trailing `#` inside a shell command can be a literal
    (`grep '#'`), and stripping those would corrupt the command being asserted
    on. `TestTheCommentStripper` pins both directions.
    """
    return "\n".join(line for line in block.splitlines() if not line.lstrip().startswith("#"))


def _smoke_code(text: str) -> str:
    """The smoke step with its commentary removed — what it actually runs."""
    return _strip_comments(_smoke_step(text))


# --- The predicates, as functions, so their ability to fail is testable -----
#
# core#1055: an assertion buried inside a test can only be exercised by
# mutating the real workflow from an external harness. Pulled out here, each
# one is fed a deliberately-broken block by `TestTheGuardsCanFail` below, in
# the same run, so the guard's discrimination is itself a CI-gated property
# rather than something a past session claims to have measured once.


def _pipes_without_pipefail(code: str) -> bool:
    """True when the step pipes but nothing preserves the pipeline's status."""
    if "| tee" not in code and "|tee" not in code:
        return False  # no pipe at all — nothing to launder
    handled = (
        "set -o pipefail" in code
        or "set -eo pipefail" in code
        or "set -euo pipefail" in code
        or "PIPESTATUS" in code
        or re.search(r"^\s*shell:\s*bash", code, re.MULTILINE) is not None
    )
    return not handled


def _branch_then_action(code: str, condition: str, action: str, window: int = 8) -> bool:
    """True when a line matching `condition` is followed by one matching `action`.

    🚨 **A token check is not a branch check, and that gap is independent of
    the comment one (core#1055).** Both predicates below used to be
    "does this string appear anywhere in the step". Under that rule, replacing
    a guard's condition with `false` — leaving its body, its `::error::` text
    and its `exit` intact but unreachable — kept them GREEN. Measured, after
    comment-stripping was already in place.

    An unreachable `exit "$rc"` is not an exit, and `n_skipped=$(field skipped)`
    is not a skip guard. So the shape asserted is *a condition, and an action
    close enough below it to be inside the same block*.

    `window` is generous because these blocks carry a long `::error::` message
    between the `if` and the `exit`.
    """
    lines = code.splitlines()
    for i, line in enumerate(lines):
        if re.search(condition, line) and any(
            re.search(action, nxt) for nxt in lines[i + 1 : i + 1 + window]
        ):
            return True
    return False


def _acts_on_the_exit_code(code: str) -> bool:
    """True when a non-zero pytest status actually exits the step non-zero.

    Requires the *branch* on `$rc` and an `exit` inside it — not merely the
    presence of the word somewhere in the step.
    """
    if "PIPESTATUS" in code:
        return True
    return _branch_then_action(
        code,
        condition=r'if\s+\[\s*"?\$\{?rc\}?"?\s+-ne\s+0',
        action=r'exit\s+"?\$\{?rc',
    )


def _has_skip_guard(code: str) -> bool:
    """True when the step still fails on a skipped probe.

    Requires a branch testing the reporter's own skip count and a non-zero exit
    inside it. `n_skipped=$(field skipped)` merely *reads* the count; a guard
    is a branch that acts on it.
    """
    return _branch_then_action(
        code,
        condition=r'if\s+.*n_skipped.*!=\s*"0"',
        action=r"exit\s+1",
    )


class TestPytestExitCodeSurvives:
    def test_the_smoke_step_does_not_launder_pytest_exit_code(self, workflow_text: str) -> None:
        assert not _pipes_without_pipefail(_smoke_code(workflow_text)), (
            "The smoke step pipes pytest into tee without pipefail, shell: bash, or "
            "PIPESTATUS. A pipeline's exit status is tee's, and GitHub's default "
            "run: shell is `bash -e {0}` with no -o pipefail — so pytest's failure "
            "is discarded and the job reports success. This is core#827, which hid "
            "12 failing connector probes for eight consecutive nights."
        )

    def test_a_nonzero_pytest_exit_actually_fails_the_step(self, workflow_text: str) -> None:
        """Capturing the status is not enough — something must act on it."""
        assert _acts_on_the_exit_code(_smoke_code(workflow_text)), (
            "The step captures pytest's exit code but never exits non-zero on it. "
            "Recording a failure without acting on it is the same green."
        )

    def test_the_skip_guard_is_kept(self, workflow_text: str) -> None:
        """The skip guard is still the only thing that catches a dropped env gate."""
        assert _has_skip_guard(_smoke_code(workflow_text)), (
            "The skip guard was removed. It catches the one hole pipefail cannot: if "
            "DATANIKA_CONNECTOR_SMOKE were dropped, every probe would skip at "
            "collection time and pytest would exit 0."
        )


class TestTheCommentStripper:
    """core#1055 — the stripper is now load-bearing, so pin both directions.

    A stripper that removed too much would silently delete the commands being
    asserted on, and every guard above would fail open in the other direction:
    `_pipes_without_pipefail` would see no `| tee` and return False.
    """

    def test_a_comment_line_is_removed(self) -> None:
        assert _strip_comments("  # set -o pipefail\n  echo hi") == "  echo hi"

    def test_a_real_command_survives(self) -> None:
        assert "set -o pipefail" in _strip_comments("  set -o pipefail\n  # noise")

    def test_a_hash_inside_a_command_is_not_treated_as_a_comment(self) -> None:
        line = "  grep -c '#' /tmp/x"
        assert _strip_comments(line) == line

    def test_the_real_step_still_contains_its_pipeline_after_stripping(
        self, workflow_text: str
    ) -> None:
        """Anti-vacuity for every guard above: if stripping ate the command,
        `_pipes_without_pipefail` returns False for the wrong reason."""
        code = _smoke_code(workflow_text)
        assert "pytest tests/test_connector_smoke/" in code
        assert "| tee" in code, (
            "the pipeline vanished from the stripped code — the guards above are "
            "now passing because there is nothing left to check"
        )


class TestTheGuardsCanFail:
    """core#1055 — the arming, in-suite, on synthetic blocks.

    All three guards above used to be structurally unable to fail: their
    assertions were satisfied by the step's own comments. Measured on the real
    workflow — deleting `set -o pipefail`, neutering the `exit "$rc"` branch,
    and neutering the skip guard each left the corresponding test **PASSED**.

    A guard proved discriminating once by an external harness is a claim about
    a past session. These run every time CI does.
    """

    _PIPED = "          pytest tests/test_connector_smoke/ -v | tee /tmp/smoke.log || rc=$?\n"

    def test_pipefail_predicate_catches_the_core_827_shape(self) -> None:
        assert _pipes_without_pipefail(self._PIPED), (
            "the exact core#827 defect — a bare pipe into tee — is not detected"
        )

    def test_pipefail_predicate_accepts_the_fixed_shape(self) -> None:
        assert not _pipes_without_pipefail("          set -o pipefail\n" + self._PIPED)

    def test_pipefail_predicate_is_not_fooled_by_a_comment(self) -> None:
        """The defect itself: prose naming PIPESTATUS must not satisfy it."""
        commented = "          # ${PIPESTATUS[0]} rather than plain -e\n" + self._PIPED
        assert _pipes_without_pipefail(_strip_comments(commented)), (
            "a comment mentioning PIPESTATUS still satisfies the pipefail check — "
            "this is core#1055 unfixed"
        )

    def test_exit_code_predicate_catches_a_captured_but_unused_status(self) -> None:
        assert not _acts_on_the_exit_code(self._PIPED)

    _RC_GUARD = (
        '          if [ "$rc" -ne 0 ]; then\n'
        '            echo "::error::Connector smoke probes FAILED"\n'
        '            exit "$rc"\n'
        "          fi\n"
    )

    def test_exit_code_predicate_accepts_a_real_exit(self) -> None:
        assert _acts_on_the_exit_code(self._PIPED + self._RC_GUARD)

    def test_exit_code_predicate_rejects_an_unreachable_exit(self) -> None:
        """The second half of core#1055, independent of comments.

        Disabling the branch while leaving its body intact is what a careless
        edit looks like, and a token check cannot see it: `exit "$rc"` is still
        right there in the file, and it can never run.
        """
        disabled = self._PIPED + self._RC_GUARD.replace('if [ "$rc" -ne 0 ]', "if false")
        assert not _acts_on_the_exit_code(disabled), (
            'an unreachable `exit "$rc"` still satisfies the exit-code check'
        )

    _SKIP_GUARD = (
        '          if [ "${n_skipped:-1}" != "0" ]; then\n'
        '            echo "::error::Smoke probes were SKIPPED"\n'
        "            exit 1\n"
        "          fi\n"
    )

    def test_skip_guard_predicate_accepts_the_real_guard(self) -> None:
        assert _has_skip_guard(self._SKIP_GUARD)

    def test_skip_guard_predicate_catches_removal(self) -> None:
        assert not _has_skip_guard(self._PIPED)

    def test_skip_guard_predicate_rejects_a_mere_mention_of_the_count(self) -> None:
        """`n_skipped=$(field skipped)` reads the count; it does not guard on it.

        Under the old token check this was indistinguishable from the guard,
        because the step also carries five other `::error::` lines.
        """
        reads_but_does_not_guard = (
            "          n_skipped=$(field skipped)\n"
            '          echo "::error::something else entirely"\n'
            "          exit 1\n"
        )
        assert not _has_skip_guard(reads_but_does_not_guard)

    def test_skip_guard_predicate_rejects_a_disabled_guard(self) -> None:
        disabled = self._SKIP_GUARD.replace('if [ "${n_skipped:-1}" != "0" ]', "if false")
        assert not _has_skip_guard(disabled)


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
