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


@pytest.mark.parametrize("rel", _entries())
def test_retired_path_is_recorded_in_git_history_as_deleted(rel: str) -> None:
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
