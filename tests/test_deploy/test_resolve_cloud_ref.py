"""`resolve-cloud-ref.sh` must refuse a merge-queue branch, not hand it to checkout (core#923).

This is the defect that forced core's merge queue to be rolled back an hour after it was
enabled. `ci.yml` paired the cloud checkout with
``${{ github.base_ref || github.ref_name }}``; `github.base_ref` is populated only for
`pull_request`, so on a `merge_group` event the fallback resolved to
`gh-readonly-queue/dev/pr-N-<sha>` — a branch that exists only in `datanika-core`. The
cloud checkout 404'd, `image-probe` (a **required** check) failed in ~46 seconds, and every
entry was ejected `reason: failed_checks`.

Two readings were available and both were wrong: `image-cve` (red by design, and a red
*non-required* check does not block queue entry — measured) and the token (its availability
step passed).

**Why these tests execute the script rather than reading `ci.yml`.** core#923 warns that the
field name `github.event.merge_group.base_ref` cannot be verified right now — core has no
merge queue enabled, which is the very thing being fixed — and that a wrong field is empty,
falls through to `ref_name`, and reproduces the identical failure. A text assertion that
`ci.yml` mentions `merge_group` would go green on exactly that. So the property under test is
behavioural: **whatever the field turns out to be, a queue branch or an empty resolution must
end the job with a named error rather than reaching `actions/checkout`.**

`test_the_wrong_field_name_is_refused_loudly` is the one that matters. It simulates the
precise thing that cannot be checked in advance.
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / ".github" / "scripts" / "resolve-cloud-ref.sh"
CI = REPO_ROOT / ".github" / "workflows" / "ci.yml"

QUEUE_BRANCH = "gh-readonly-queue/dev/pr-922-c7da4ce9f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6"


def _bash() -> str:
    exe = shutil.which("bash")
    if exe is None:  # pragma: no cover - a box with no bash cannot run CI either
        pytest.fail("bash not found; this suite must not silently stop testing the resolver")
    return exe


def run(tmp_path: Path, **env: str) -> tuple[int, str, str]:
    """Execute the real script with a real $GITHUB_OUTPUT and return (rc, stdout, output)."""
    out_file = tmp_path / "gh_output"
    out_file.write_bytes(b"")
    proc = subprocess.run(
        [_bash(), SCRIPT.as_posix()],
        capture_output=True,
        text=True,
        encoding="utf-8",
        # Inherit os.environ: replacing it strips PATH/SystemRoot on Windows and the
        # failure surfaces somewhere unrecognisable.
        env={
            **os.environ,
            "MERGE_GROUP_BASE": "",
            "PR_BASE": "",
            "REF_NAME": "",
            "GITHUB_OUTPUT": out_file.as_posix(),
            **env,
        },
    )
    return proc.returncode, (proc.stdout or "") + (proc.stderr or ""), out_file.read_text("utf-8")


def resolved(output: str) -> str | None:
    for line in output.splitlines():
        if line.startswith("ref="):
            return line[len("ref=") :]
    return None


def test_the_script_exists_and_is_wired_into_both_cloud_checkouts():
    # Anti-vacuity for everything below: a behavioural suite over a script that no
    # workflow calls proves nothing about CI.
    assert SCRIPT.is_file()
    ci = CI.read_text("utf-8")
    # ⚠️ Count the INVOCATION, not the filename. A bare `count("resolve-cloud-ref.sh")`
    # reads 3, because the comment explaining the fix names the script too — a guard that
    # reds on its own documentation teaches people to delete the documentation. Third
    # instance of this shape in one session; the rule is to assert the construct, never
    # the word.
    assert ci.count("run: bash .github/scripts/resolve-cloud-ref.sh") == 2, (
        "both image-probe and image-cve must resolve the cloud ref through the script"
    )
    assert ci.count("ref: ${{ steps.cloudref.outputs.ref }}") == 2
    # The old expression must be gone from the cloud checkouts. Scoped to the `ref:` key
    # for the same reason: the comment above the step quotes it deliberately.
    assert "ref: ${{ github.base_ref || github.ref_name }}" not in ci


def test_pull_request_resolves_to_the_target_branch(tmp_path):
    rc, log, out = run(tmp_path, PR_BASE="dev", REF_NAME="922/merge")
    assert rc == 0, log
    assert resolved(out) == "dev"


def test_push_resolves_to_the_branch_being_pushed(tmp_path):
    rc, log, out = run(tmp_path, REF_NAME="master")
    assert rc == 0, log
    assert resolved(out) == "master"


def test_merge_group_resolves_to_the_queue_base(tmp_path):
    rc, log, out = run(tmp_path, MERGE_GROUP_BASE="dev", REF_NAME=QUEUE_BRANCH)
    assert rc == 0, log
    assert resolved(out) == "dev"


def test_a_full_ref_is_normalised(tmp_path):
    # It is not established whether the merge-group payload carries `dev` or
    # `refs/heads/dev`, and that uncertainty is the reason this script exists. Both
    # must work, so neither shape can turn into an incident.
    rc, log, out = run(tmp_path, MERGE_GROUP_BASE="refs/heads/dev", REF_NAME=QUEUE_BRANCH)
    assert rc == 0, log
    assert resolved(out) == "dev"


def test_the_wrong_field_name_is_refused_loudly(tmp_path):
    """The scenario core#923 says cannot be verified in advance.

    If `github.event.merge_group.base_ref` is not the right field, the expression yields
    empty and resolution falls through to `github.ref_name` — the queue branch. Before this
    script that produced a 404 inside `actions/checkout`, an ejected queue entry, and a
    symptom indistinguishable from a flaky required check.
    """
    rc, log, out = run(tmp_path, MERGE_GROUP_BASE="", REF_NAME=QUEUE_BRANCH)
    assert rc == 1
    assert "::error::" in log
    assert "gh-readonly-queue" in log
    assert "core#923" in log
    # The diagnosis must name the layer, because the two wrong hypotheses cost real time.
    assert "NOT a token problem" in log
    # And it must not have produced a ref for checkout to use.
    assert resolved(out) is None


def test_everything_empty_is_refused(tmp_path):
    rc, log, out = run(tmp_path)
    assert rc == 1
    assert "::error::" in log
    assert resolved(out) is None


def test_the_three_inputs_are_printed_for_the_first_real_merge_group_run(tmp_path):
    # The verification core#923 asks for, obtained the only way available: by running.
    # The first genuine merge-group run records the payload's actual value in its log.
    rc, log, _ = run(tmp_path, MERGE_GROUP_BASE="dev", PR_BASE="", REF_NAME=QUEUE_BRANCH)
    assert rc == 0
    assert "merge_group.base_ref = [dev]" in log
    assert "github.base_ref      = []" in log
    assert f"github.ref_name      = [{QUEUE_BRANCH}]" in log


def test_the_guard_can_fail(tmp_path):
    """Negative control on the REAL script: neuter the refusal and confirm the harness sees it.

    A synthetic fixture agrees with the check including where the check is wrong, so the
    mutation is applied to the actual file's bytes.
    """
    original = SCRIPT.read_bytes()
    assert b"gh-readonly-queue/*)" in original, "anchor missing — the mutation would be inert"
    broken = tmp_path / "broken.sh"
    broken.write_bytes(original.replace(b"gh-readonly-queue/*)", b"never-matches-this/*)"))

    proc = subprocess.run(
        [_bash(), broken.as_posix()],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env={
            **os.environ,
            "MERGE_GROUP_BASE": "",
            "PR_BASE": "",
            "REF_NAME": QUEUE_BRANCH,
            "GITHUB_OUTPUT": (tmp_path / "out").as_posix(),
        },
    )
    # Without the refusal the queue branch sails straight through — which is the bug.
    assert proc.returncode == 0
    assert QUEUE_BRANCH in (tmp_path / "out").read_text("utf-8")
