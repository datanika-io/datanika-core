"""A push with nothing to update must not rewrite the checked-out branch (core#1355).

git runs `pre-push` even when the push will update no ref, and writes **zero lines** to the
hook's stdin. `scripts/hooks/pre-push` skips its auto-rebase only when it saw at least one line
and none of them was HEAD, so zero lines falls through to the rebase at `:45-58` and rewrites
whatever branch happens to be checked out -- a branch the push does not touch.

Observed after a promotion: `dev` had already been fast-forwarded server-side, the scripted
resync `git push origin origin/master:refs/heads/dev` had nothing to do, and it rebased PR
#1344's feature branch and printed `error: failed to push some refs`.

🔑 **These tests drive a real `git push` against a real (local, bare) remote.** The fix rests on
what git actually does with an up-to-date push, so a harness that fed the hook a hand-made stdin
would be testing my description of git rather than git. The one case that *is* driven by hand is
the hand-invocation control, because that is precisely the case git never produces.

⚠️ The tmp repos have no `.venv`, so a push that reaches the venv check exits 1 with
`no .venv found`. That is not an accident of the harness -- it is the discriminator. A no-op push
must exit **0 before** that point; a real push must reach it.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
HOOK = REPO_ROOT / "scripts" / "hooks" / "pre-push"

bash = shutil.which("bash")
pytestmark = pytest.mark.skipif(bash is None, reason="needs bash to execute the hook")


def _clean_env() -> dict:
    """An environment with every ``GIT_*`` variable stripped.

    git exports ``GIT_DIR`` to its hooks, and this file runs *inside* the hook it tests. Without
    the scrub every git call below would retarget the real repository no matter what ``cwd=``
    says -- the measured 19-of-20 failure recorded in ``test_pre_push_gating.py``.
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


def sha(repo: Path, rev: str) -> str:
    return git(repo, "rev-parse", rev).stdout.strip()


@pytest.fixture
def world(tmp_path: Path) -> tuple[Path, Path]:
    """A bare remote with `master` and `dev` at the same commit, and a clone whose checked-out
    feature branch is BEHIND that commit.

    This is the exact shape the resync runs in: `dev` already equals `master`, so
    `git push origin origin/master:refs/heads/dev` has nothing to update.
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
    first = sha(work, "HEAD")

    (work / "b.txt").write_text("two\n")
    git(work, "add", "b.txt")
    git(work, "commit", "-qm", "second")

    git(work, "push", "-q", "origin", "master")
    git(work, "push", "-q", "origin", "master:refs/heads/dev")
    git(work, "fetch", "-q", "origin")

    # The worktree sits on a feature branch that is behind origin/dev, which is what makes the
    # buggy rebase observable at all.
    git(work, "checkout", "-q", "-b", "feature", first)

    hooks = work / ".githooks"
    hooks.mkdir()
    shutil.copy(HOOK, hooks / "pre-push")
    (hooks / "pre-push").chmod(0o755)
    git(work, "config", "core.hooksPath", ".githooks")
    return work, remote


def test_control_the_fixture_really_is_behind_dev(world) -> None:
    """Anti-vacuity, and it runs first.

    If `feature` were already an ancestor of `origin/dev`, the hook would skip the rebase for an
    entirely different reason and every assertion below would pass while testing nothing.
    """
    work, _ = world
    r = git(work, "merge-base", "--is-ancestor", "origin/dev", "HEAD", check=False)
    assert r.returncode != 0, "feature is NOT behind origin/dev — this fixture arms nothing"


def test_a_noop_refspec_push_does_not_rewrite_the_checked_out_branch(world) -> None:
    """core#1355 itself. `dev` already equals `master`, so this push updates nothing."""
    work, _ = world
    before = sha(work, "feature")

    r = git(work, "push", "origin", "origin/master:refs/heads/dev", check=False)

    after = sha(work, "feature")
    assert after == before, (
        "a push that updates nothing rebased the checked-out branch: "
        f"{before[:8]} -> {after[:8]}.\nstdout={r.stdout!r}\nstderr={r.stderr!r}"
    )
    assert r.returncode == 0, (
        f"an up-to-date push was refused by the hook.\nstdout={r.stdout!r}\nstderr={r.stderr!r}"
    )


def test_a_noop_push_of_the_current_branch_also_leaves_it_alone(world) -> None:
    """The second shape the issue derives from source but never exercised: a redundant second
    `git push` of a branch that is already on the remote and has since fallen behind `dev`."""
    work, _ = world
    # Setup must NOT run the hook. A hooked first push would rebase `feature` onto origin/dev --
    # the very bug under test -- leaving the branch up to date, so the redundant push below would
    # no longer exercise the behind-dev path and this test would pass for the wrong reason.
    git(work, "push", "-q", "--no-verify", "origin", "feature")
    before = sha(work, "feature")

    r = git(work, "push", "origin", "feature", check=False)

    after = sha(work, "feature")
    assert after == before, (
        f"a redundant push rebased the branch: {before[:8]} -> {after[:8]}\nstderr={r.stderr!r}"
    )
    assert r.returncode == 0, f"a redundant push was refused.\nstderr={r.stderr!r}"


def test_control_a_push_that_really_updates_a_ref_still_runs_the_checks(world) -> None:
    """🔑 The control that stops the fix from becoming 'skip everything'.

    A push carrying real work must still reach the venv check and be refused there. Without
    this, exiting 0 unconditionally would satisfy every test above.
    """
    work, _ = world
    git(work, "checkout", "-q", "master")
    (work / "c.txt").write_text("three\n")
    git(work, "add", "c.txt")
    git(work, "commit", "-qm", "third")

    r = git(work, "push", "origin", "master", check=False)

    assert r.returncode != 0, "a real push sailed past the hook's checks"
    assert "no .venv found" in (r.stdout + r.stderr), (
        "a real push was refused, but not by the check we expected — the hook may be exiting "
        f"early for the wrong reason.\nstdout={r.stdout!r}\nstderr={r.stderr!r}"
    )


def test_control_a_refspec_push_that_does_update_still_skips_via_the_556_guard(world) -> None:
    """The existing #556 path must keep working: one stdin line, naming a ref that is not HEAD."""
    work, remote = world
    git(work, "push", "-q", "--no-verify", "origin", "+master~1:refs/heads/dev")
    git(work, "fetch", "-q", "origin")
    before = sha(work, "feature")

    r = git(work, "push", "origin", "origin/master:refs/heads/dev", check=False)

    assert r.returncode == 0, f"the refspec guard refused a real resync.\nstderr={r.stderr!r}"
    assert sha(work, "feature") == before
    assert "refspec push" in (r.stdout + r.stderr), (
        "a resync that DID update a ref should have taken the #556 refspec path, which names "
        f"itself in the output.\nstdout={r.stdout!r}\nstderr={r.stderr!r}"
    )


def test_control_a_hand_invocation_with_no_arguments_still_fails_closed(world) -> None:
    """The fix must discriminate on git's ARGUMENTS, not merely on empty stdin.

    `scripts/hooks/pre-push:36-37` deliberately treats zero stdin lines as a normal push so a
    hand invocation is checked rather than silently skipped. git always passes the remote name
    and URL; a human running the hook directly passes neither. That is the discriminator, and
    this control is what stops the fix from turning the hand path into a no-op too.
    """
    work, _ = world
    r = subprocess.run(
        [bash, str(work / ".githooks" / "pre-push")],
        cwd=work,
        capture_output=True,
        text=True,
        input="",
        env=_clean_env(),
    )
    assert r.returncode != 0, (
        "a hand invocation with no arguments exited 0 — the fix keyed on empty stdin alone and "
        f"disabled the fail-closed path.\nstdout={r.stdout!r}\nstderr={r.stderr!r}"
    )
