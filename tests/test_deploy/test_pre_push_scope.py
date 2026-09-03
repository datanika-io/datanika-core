"""The pre-push hook's test scope, and the lock stamp that must not outrun it (#964).

The founder trimmed the hook from ``pytest tests/`` to ``pytest tests/test_deploy/``
(1105.95s -> 79.89s per push, measured 2026-09-03 on ``8caac53``). Three things about that
change can rot silently, and each of them fails in the reassuring direction:

1. Someone "restores" the full suite by hardcoding a path again, and the env overrides stop
   working -- with no symptom except that pushes get slow again.
2. The lock stamp gets re-gated on ``RAN_TESTS`` instead of ``RAN_FULL``. Then a narrowed run
   marks the venv as proven against ``uv.lock`` and **suppresses the stale-venv NOTE** -- the
   one pointer at the gutted-venv symptom that cost QA, and then Growth, four reinstalls.
   That is a green that proves nothing, which is this project's signature defect.
3. A scope assignment gets written as ``[ cond ] && VAR=x`` in a position where ``set -e``
   makes it fatal, aborting the hook with every check below it silently skipped.

On (3), the exact rule -- measured on this machine's bash, because the plausible version of
it is wrong and I shipped the wrong version first:

    set -e; [ 1 = 2 ] && V=1; echo hi      -> survives (rc 0)
    set -e; while read ...; do [ x ] && V=1; done; more   -> survives (rc 0)
    set -e; f(){ [ 1 = 2 ] && V=1; }; f    -> ABORTS (rc 1)
    set -e; if true; then [ 1 = 2 ] && V=1; fi   (nothing after fi)  -> ABORTS (rc 1)

So the construct is fatal **only when its non-zero status becomes the exit status of an
enclosing scope** -- the last command of the script, of a function body, or of a branch with
nothing after it. It is harmless when any command follows at the same or an outer level.

That is why ``pre-push`` line 22 (``[ "$lsha" = "$HEAD_SHA" ] && PUSHING_HEAD=1``, inside a
``while read`` loop with the whole hook after it) is **correct and must not be "fixed"** -- a
first draft of this file asserted a blanket ban and went red against it. A blanket textual ban
was therefore dropped in favour of the behavioural test below, which executes the real block
under ``set -e`` rather than pattern-matching for a shape whose danger is positional.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[2] / "scripts" / "hooks" / "pre-push"


@pytest.fixture(scope="module")
def hook_text() -> str:
    # Anti-vacuity: if the hook is moved or renamed, every assertion below would pass
    # trivially against an empty string. Fail loudly instead.
    assert HOOK.is_file(), f"pre-push hook missing at {HOOK} — this guard tests nothing"
    text = HOOK.read_text(encoding="utf-8")
    assert len(text) > 2000, "hook is implausibly short; did something truncate it?"
    return text


class TestScopeIsConfigurable:
    def test_pytest_is_invoked_through_the_scope_variable(self, hook_text: str) -> None:
        """Not a hardcoded path — that is what makes the override work at all."""
        assert '-m pytest "$PYTEST_SCOPE"' in hook_text, (
            "the hook no longer invokes pytest through $PYTEST_SCOPE; a hardcoded path "
            "silently disables DATANIKA_PREPUSH_FULL and DATANIKA_PREPUSH_SCOPE"
        )
        assert "-m pytest tests/ " not in hook_text, "hardcoded full-suite path is back"

    def test_default_scope_is_test_deploy(self, hook_text: str) -> None:
        assert 'PYTEST_SCOPE="${DATANIKA_PREPUSH_SCOPE:-tests/test_deploy}"' in hook_text


class TestScopeSelectionActuallyBehaves:
    """Execute the selection logic. A text match cannot tell working from broken."""

    @staticmethod
    def _block(hook_text: str) -> str:
        m = re.search(
            r'^(PYTEST_SCOPE="\$\{DATANIKA_PREPUSH_SCOPE.*?^RAN_FULL=0)',
            hook_text,
            re.S | re.M,
        )
        assert m, "could not locate the scope-selection block"
        return m.group(1)

    def _run(self, block: str, env_line: str) -> subprocess.CompletedProcess:
        bash = shutil.which("bash")
        if not bash:
            pytest.skip("bash unavailable")
        script = f'set -e\n{env_line}\n{block}\necho "SCOPE=$PYTEST_SCOPE"\n'
        return subprocess.run(
            [bash, "-c", script], capture_output=True, text=True, encoding="utf-8"
        )

    def test_default(self, hook_text: str) -> None:
        r = self._run(self._block(hook_text), "")
        assert r.returncode == 0, f"scope block aborted under set -e: {r.stderr}"
        assert "SCOPE=tests/test_deploy" in r.stdout

    def test_full_override(self, hook_text: str) -> None:
        r = self._run(self._block(hook_text), "export DATANIKA_PREPUSH_FULL=1")
        assert r.returncode == 0, f"scope block aborted under set -e: {r.stderr}"
        assert "SCOPE=tests" in r.stdout and "SCOPE=tests/test_deploy" not in r.stdout

    def test_explicit_scope_override(self, hook_text: str) -> None:
        r = self._run(self._block(hook_text), "export DATANIKA_PREPUSH_SCOPE=tests/test_hooks")
        assert r.returncode == 0
        assert "SCOPE=tests/test_hooks" in r.stdout

    def test_block_survives_set_e_when_condition_is_false(self, hook_text: str) -> None:
        """The whole point: the default path takes the FALSE branch of the FULL check.

        Written as `[ cond ] && VAR=x`, that returns 1 and `set -e` kills the hook before
        ruff, before pytest, before the helm gate — and the push still succeeds.
        """
        r = self._run(self._block(hook_text), "export DATANIKA_PREPUSH_FULL=0")
        assert r.returncode == 0, (
            "scope block exits non-zero when DATANIKA_PREPUSH_FULL is unset/0. Under "
            f"set -e that aborts the whole hook. stderr={r.stderr!r}"
        )


class TestLockStampDoesNotOutrunItsEvidence:
    def test_stamp_is_gated_on_the_full_suite(self, hook_text: str) -> None:
        assert '[ "$RAN_FULL" = "1" ] && [ -f uv.lock ]' in hook_text, (
            "the uv.lock stamp is no longer gated on RAN_FULL. Gated on RAN_TESTS it would "
            "be refreshed by a tests/test_deploy run, which imports none of the dependency "
            "graph — marking an unproven venv as proven and suppressing the stale-venv NOTE"
        )

    def test_ran_full_is_only_set_by_the_full_scope(self, hook_text: str) -> None:
        assert 'if [ "$PYTEST_SCOPE" = "tests" ]; then' in hook_text
        assert "RAN_FULL=1" in hook_text
        assert hook_text.count("RAN_FULL=1") == 1, "RAN_FULL set in more than one place"


class TestTheSixChecksCiCannotDoAreStillPresent:
    """CI can run the suite. It cannot do any of these.

    Each needle is the **executable line**, never the prose that explains it. The first
    version of this class asserted the bare substring ``UV_NO_SYNC=1``, which also occurs in
    the eight-line comment above the invocation -- so deleting the flag from the actual
    pytest call left this class green. Caught by mutation, not by review.
    """

    @pytest.mark.parametrize(
        "needle, what",
        [
            ("skipping rebase and tests", "refspec guard (#556)"),
            ("git rebase origin/dev --quiet", "auto-rebase onto dev"),
            ("pre-push WARNING: branch/commit mismatch", "Closes/branch consistency"),
            ("-m ruff check datanika tests", "ruff check"),
            ("-m ruff format --check datanika tests", "ruff format --check"),
            ("pre-push NOTE: uv.lock has changed", "stale-venv NOTE (#557)"),
            ('UV_NO_SYNC=1 "$PY" -m pytest', "Windows venv-gutting guard"),
        ],
    )
    def test_check_survives_the_trim(self, hook_text: str, needle: str, what: str) -> None:
        assert needle in hook_text, f"the trim removed {what}, which CI cannot replace"
