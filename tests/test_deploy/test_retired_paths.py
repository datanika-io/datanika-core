"""core#1178 — the bootstrap graveyard may only name genuinely retired files.

`scripts/retired-paths.txt` is the one mechanism in the deploy that deletes a
file from production by name rather than by manifest diff. Its danger is
obvious: a live path added to it is deleted from the box on the next deploy,
with every check green. These tests make that impossible.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
LIST = REPO / "scripts" / "retired-paths.txt"


def _entries() -> list[str]:
    return [
        ln.strip()
        for ln in LIST.read_text(encoding="utf-8").splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]


def test_list_is_not_empty_so_these_tests_can_fail() -> None:
    # Guards the guard: an empty list would make every test below vacuously true.
    assert _entries(), "retired-paths.txt has no entries; the tests below prove nothing"


@pytest.mark.parametrize("rel", _entries())
def test_retired_path_is_absent_from_the_current_tree(rel: str) -> None:
    assert not (REPO / rel).exists(), (
        f"{rel} is listed as retired but EXISTS in the tree. The deploy would "
        f"delete a live file from production."
    )


def _is_shallow() -> bool:
    return (
        subprocess.run(
            ["git", "rev-parse", "--is-shallow-repository"],
            cwd=REPO,
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        == "true"
    )


@pytest.mark.parametrize("rel", _entries())
def test_retired_path_is_not_currently_tracked(rel: str) -> None:
    """The dangerous case, and it needs no history — so it runs in CI too.

    A path that is still tracked is a live file, and listing it would delete it
    from production on the next deploy. `git ls-tree` works on a depth-1 clone,
    unlike `git log`, which is why this assertion carries the weight in CI.
    """
    out = subprocess.run(
        ["git", "ls-tree", "--name-only", "HEAD", rel],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    assert not out, (
        f"{rel} is listed as retired but is STILL TRACKED at HEAD. The deploy "
        f"would delete a live file from production."
    )


@pytest.mark.parametrize("rel", _entries())
def test_retired_path_is_recorded_in_git_history_as_deleted(rel: str) -> None:
    """Stronger, but needs real history.

    ⚠️ CI checks out with actions/checkout@v6 and no `fetch-depth`, i.e. depth 1,
    so `git log --diff-filter=D` returns nothing there for every path and this
    would fail for all of them regardless of correctness — measured on run
    34123922773, five failures, all of them false. It is skipped on a shallow
    clone rather than deleted, because on a full clone it catches a typo'd path
    that never existed.

    The skip is not a gap: a never-existed path is a no-op on the box (removing
    an absent file does nothing), while the two assertions that prevent deleting
    something real — not-tracked above, and no-box-owned-path below — run
    everywhere.
    """
    if _is_shallow():
        pytest.skip("shallow clone: git log has no history to search (see docstring)")
    out = subprocess.run(
        ["git", "log", "--diff-filter=D", "--format=%h", "-1", "--", rel],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=False,
    ).stdout.strip()
    assert out, (
        f"{rel} is listed as retired but git history records no deletion of it. "
        f"Only a path this repo actually removed may be deleted from the box."
    )


def test_no_box_owned_path_can_be_listed() -> None:
    """The five box-owned paths measured on prod must never appear here."""
    forbidden = (".env", ".secrets/", "reflex.db", "llms.txt", "backups/", "dbt_projects/")
    for rel in _entries():
        for f in forbidden:
            assert f not in rel, f"{rel} names box-owned state ({f}); never delete it"
