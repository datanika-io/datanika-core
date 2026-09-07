"""core#1178 — the reconciler must actually be invoked by the deploy, in the right place.

core#747's lesson, one level up: `deploy/server/` shipped the correct content to a
path nothing read, and every signal stayed green. A reconciler the workflow never
calls fails the same way — it looks like a fix while orphans keep accumulating.

⚠️ These assertions parse the workflow's STEPS. An earlier version of this file
compared `body.index(...)` positions instead, and passed for the wrong reason: the
first textual occurrence of `reconcile-deployed-tree.sh` is line 112, inside a
comment, while `deploy-bluegreen.sh` first appears at line 98 in another comment.
Moving the real step after the build would have left that version green. A guard
that measures a proxy for the thing is not a guard on the thing.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
WF = REPO / ".github" / "workflows" / "deploy-pointer.yml"
SUT = REPO / "scripts" / "reconcile-deployed-tree.sh"


def _steps() -> list[dict]:
    doc = yaml.safe_load(WF.read_text(encoding="utf-8"))
    return doc["jobs"]["deploy"]["steps"]


def _executable_run(step: dict) -> str:
    """A step's `run:` body with shell comments stripped.

    Load-bearing. The `Build source tarball` step carries a comment naming
    reconcile-deployed-tree.sh, so a naive substring match resolves "which step
    runs the reconciler" to the tarball step and every ordering assertion below
    silently measures the wrong step. Caught by this file's own extraction test.
    """
    lines = str(step.get("run", "")).splitlines()
    return "\n".join(ln for ln in lines if not ln.strip().startswith("#"))


def _index_of_step_running(needle: str) -> int:
    """Index of the first step whose `run:` body actually executes `needle`."""
    for i, s in enumerate(_steps()):
        if needle in _executable_run(s):
            return i
    raise AssertionError(f"no deploy step runs {needle!r}")


def test_reconciler_exists() -> None:
    assert SUT.is_file(), "reconcile-deployed-tree.sh is missing"


def test_workflow_builds_a_manifest() -> None:
    _index_of_step_running("tar tzf /tmp/src.tgz")


def test_workflow_actually_invokes_the_reconciler() -> None:
    """Not 'mentions it somewhere' — runs it. A comment is not an invocation."""
    _index_of_step_running("reconcile-deployed-tree.sh")


@pytest.mark.parametrize(
    "later,label",
    [("docker compose build", "the image build"), ("deploy-bluegreen.sh", "the blue/green swap")],
)
def test_reconcile_runs_before(later: str, label: str) -> None:
    """A bad reconcile must abort while production still serves the old container."""
    i_rec = _index_of_step_running("reconcile-deployed-tree.sh")
    i_later = _index_of_step_running(later)
    assert i_rec < i_later, (
        f"the reconciler runs at step {i_rec}, after {label} at step {i_later}; "
        f"a bad reconcile would land mid-deploy instead of aborting harmlessly"
    )


def test_reconcile_runs_after_the_extraction() -> None:
    """It diffs against the tree the tarball just produced."""
    i_x = _index_of_step_running("tar xzf -")
    i_rec = _index_of_step_running("reconcile-deployed-tree.sh")
    assert i_x < i_rec, "the reconciler runs before the transfer; it would diff a stale tree"


def test_manifest_transfer_is_verified_not_assumed() -> None:
    """ssh exiting 0 says the session closed, not that the right bytes landed."""
    i = _index_of_step_running("manifest transfer truncated")
    assert i >= 0


def test_reconciler_keys_on_the_manifest_not_on_git() -> None:
    """The measured reason this design exists: a git-keyed rule deletes .secrets/."""
    body = SUT.read_text(encoding="utf-8")
    assert "$STATE" in body and "comm -23" in body, (
        "the reconciler no longer diffs manifests; if it now consults git it will "
        "delete .env.docker and .secrets/ on its first run"
    )
