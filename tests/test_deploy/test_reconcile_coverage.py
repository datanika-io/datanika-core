"""core#1178 — the reconciler must actually be invoked by the deploy.

core#747's lesson, one level up: `deploy/server/` shipped the correct content to
a path nothing read, and every signal stayed green. A reconciler the workflow
never calls fails in exactly the same shape — it looks like a fix, and the
orphans keep accumulating.
"""

from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
WF = REPO / ".github" / "workflows" / "deploy-pointer.yml"
SUT = REPO / "scripts" / "reconcile-deployed-tree.sh"


def test_reconciler_exists() -> None:
    assert SUT.is_file(), "reconcile-deployed-tree.sh is missing"


def test_workflow_builds_a_manifest() -> None:
    assert "tar tzf /tmp/src.tgz" in WF.read_text(encoding="utf-8"), (
        "the deploy no longer emits a manifest; the reconciler has nothing to diff"
    )


def test_workflow_invokes_the_reconciler() -> None:
    assert "reconcile-deployed-tree.sh" in WF.read_text(encoding="utf-8"), (
        "the reconciler is never invoked by the deploy — orphans keep accumulating "
        "while the script sits in the repo looking like a fix"
    )


def test_reconcile_runs_before_the_build() -> None:
    """A bad reconcile must abort while production still serves the old container."""
    body = WF.read_text(encoding="utf-8")
    i_rec = body.index("reconcile-deployed-tree.sh")
    i_build = body.index("docker compose build")
    assert i_rec < i_build, (
        "the reconciler runs after the build; a bad reconcile would land mid-deploy"
    )


def test_manifest_transfer_is_verified_not_assumed() -> None:
    """ssh exiting 0 says the session closed, not that the right bytes landed."""
    body = WF.read_text(encoding="utf-8")
    assert "manifest transfer truncated" in body, (
        "the manifest transfer is unverified; a truncated manifest is the one input "
        "that could make the reconciler delete the application"
    )


def test_reconciler_keys_on_the_manifest_not_on_git() -> None:
    """The measured reason this design exists — see core#1178."""
    body = SUT.read_text(encoding="utf-8")
    assert "$STATE" in body and "comm -23" in body, (
        "the reconciler no longer diffs manifests; if it now consults git it will "
        "delete .env.docker and .secrets/ on its first run"
    )
