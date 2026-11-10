"""A push that prints nothing from the pre-push hook is not a success (core#1293).

One push exited 0 with no hook output. The cause was not reproduced, and this
file does not guess at it. What is reproducible is the reading: exit 0 plus no
hook line looks clean. ``scripts/hooks/pre-push`` prints
``pre-push: ran id=<id>`` before any other work, and
``scripts/hooks/assert-pre-push-ran.sh`` fails unless that exact line is
present for the id the caller exported.

The dry-run rebase is not exercised here and is not changed.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
HOOK = REPO_ROOT / "scripts" / "hooks" / "pre-push"
CHECKER = REPO_ROOT / "scripts" / "hooks" / "assert-pre-push-ran.sh"

bash = shutil.which("bash")
pytestmark = pytest.mark.skipif(bash is None, reason="needs bash to execute the hook")


def _clean_env() -> dict:
    """Strip ``GIT_*`` so a push inside this file cannot retarget the real repo.

    This file runs from inside ``pre-push``, which exports ``GIT_DIR``. The same
    scrub is why ``test_pre_push_noop_push.py`` exists in this shape.
    """
    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


def git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=check,
        env=_clean_env(),
    )


def _check(output: str, run_id: str) -> subprocess.CompletedProcess:
    env = _clean_env()
    env["DATANIKA_PREPUSH_RUN_ID"] = run_id
    return subprocess.run(
        [bash, str(CHECKER)],
        input=output,
        capture_output=True,
        text=True,
        env=env,
    )


@pytest.fixture
def up_to_date(tmp_path: Path) -> Path:
    """A clone whose ``master`` is already on the remote, hook armed.

    The second push updates nothing. git still runs the hook, with zero stdin
    lines and both arguments set, which is the path that exits 0.
    """
    remote = tmp_path / "remote.git"
    git(tmp_path, "init", "-q", "--bare", "-b", "master", str(remote))

    work = tmp_path / "work"
    work.mkdir()
    git(work, "init", "-q", "-b", "master")
    git(work, "config", "user.email", "t@example.com")
    git(work, "config", "user.name", "t")
    git(work, "remote", "add", "origin", str(remote))
    (work / "a.txt").write_text("one\n")
    git(work, "add", "a.txt")
    git(work, "commit", "-qm", "first")
    # The setup push must not run the hook under test.
    git(work, "push", "-q", "--no-verify", "origin", "master")

    hooks = work / ".githooks"
    hooks.mkdir()
    shutil.copy(HOOK, hooks / "pre-push")
    (hooks / "pre-push").chmod(0o755)
    git(work, "config", "core.hooksPath", ".githooks")
    return work


def _push(work: Path, run_id: str) -> subprocess.CompletedProcess:
    env = _clean_env()
    env["DATANIKA_PREPUSH_RUN_ID"] = run_id
    return subprocess.run(
        ["git", "push", "origin", "master"],
        cwd=work,
        capture_output=True,
        text=True,
        env=env,
    )


def test_a_push_that_runs_the_hook_prints_the_caller_run_id(up_to_date: Path) -> None:
    """The hook's own noop path must carry the caller's id, or the checker is testing a comment."""
    run_id = "core1293-this-push"
    before = git(up_to_date, "rev-parse", "HEAD").stdout.strip()

    pushed = _push(up_to_date, run_id)
    combined = pushed.stdout + pushed.stderr

    assert pushed.returncode == 0, combined
    assert "pre-push: nothing to push" in combined, (
        "the push exited 0 without the hook's noop line, so this did not run the hook "
        f"under test.\n{combined}"
    )
    assert git(up_to_date, "rev-parse", "HEAD").stdout.strip() == before, (
        "printing the run id moved the branch"
    )

    verdict = _check(combined, run_id)
    assert verdict.returncode == 0, (
        "the hook ran and the checker still rejected the push.\n"
        f"push={combined!r}\nchecker={verdict.stderr!r}"
    )


def test_a_successful_push_with_no_hook_output_is_rejected(up_to_date: Path) -> None:
    """git exits 0 and the output has no hook line. That used to read as success."""
    hook = up_to_date / ".githooks" / "pre-push"
    hook.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    hook.chmod(0o755)
    run_id = "core1293-silent"

    pushed = _push(up_to_date, run_id)
    combined = pushed.stdout + pushed.stderr

    assert pushed.returncode == 0, combined
    assert f"pre-push: ran id={run_id}" not in combined

    verdict = _check(combined, run_id)
    assert verdict.returncode != 0, (
        "a push that printed no hook line was accepted. Silence still looks successful."
    )


def test_a_different_run_id_is_not_this_push() -> None:
    """A leftover line from an earlier run must not satisfy this one."""
    stale = "pre-push: ran id=core1293-earlier\n"
    verdict = _check(stale, "core1293-this-push")
    assert verdict.returncode != 0


def test_mentioning_the_marker_is_not_the_marker() -> None:
    """A sentence that contains the line is the denial trap (WORKFLOW_RULES §4)."""
    denial = "this output is not a line that reads pre-push: ran id=core1293-this-push\n"
    verdict = _check(denial, "core1293-this-push")
    assert verdict.returncode != 0


def test_an_empty_run_id_measures_nothing() -> None:
    verdict = _check("pre-push: ran id=\n", "")
    assert verdict.returncode == 2
