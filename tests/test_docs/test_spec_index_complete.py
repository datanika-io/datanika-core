"""Every spec in ``docs/specs/`` appears in ``docs/specs/README.md``.

**Why this exists.** The index has gone stale twice. On 2026-09-09 a Product pass
found *two* specs present on disk and absent from the index, alongside three
status cells describing a state the spec no longer had. Nothing caught either,
because nothing was looking: a positive control run on 2026-09-12 found 40 tests
referencing ``docs/`` and **zero** referencing ``docs/specs/README.md``.

An unindexed spec is not a cosmetic gap. The index is how an agent starting a
session discovers that a contract for the thing they are about to build already
exists — and the failure mode is re-deriving a decision that was already made,
differently.

⚠️ **This asserts PRESENCE, not absence.** ``WORKFLOW_RULES`` §4's standing trap:
a guard written as "the artifact must not contain X" is satisfied by deleting the
artifact. "Every spec file is named in the index" cannot be satisfied by emptying
either side, because the anti-vacuity check below fails first.
"""

from __future__ import annotations

from pathlib import Path

import pytest

SPECS_DIR = Path(__file__).resolve().parents[2] / "docs" / "specs"
INDEX = SPECS_DIR / "README.md"


def _spec_files() -> list[Path]:
    return sorted(SPECS_DIR.glob("SPEC_*.md"))


def test_the_corpus_and_the_index_both_exist() -> None:
    """Anti-vacuity. A guard whose subject has vanished passes for the wrong reason.

    Both halves are checked: an empty ``docs/specs/`` would make the main test
    vacuously true, and a missing README would make it fail for a reason that
    looks like the defect but is not.
    """
    assert INDEX.is_file(), f"the spec index is missing at {INDEX}"
    specs = _spec_files()
    assert len(specs) >= 20, (
        f"only {len(specs)} SPEC_*.md found in {SPECS_DIR}. The corpus was 31+ when this "
        "guard was written; if specs genuinely moved out, update the floor deliberately "
        "rather than letting the guard quietly stop checking anything."
    )


def test_every_spec_is_named_in_the_index() -> None:
    text = INDEX.read_text(encoding="utf-8")
    missing = [s.name for s in _spec_files() if s.name not in text]
    assert not missing, (
        "these specs exist on disk and are absent from docs/specs/README.md: "
        + ", ".join(missing)
        + ". An agent looking for an existing contract will not find it, and will write "
        "a second one. Add a row naming the file, what it governs, and its status."
    )


def test_the_index_names_no_spec_that_does_not_exist() -> None:
    """The other direction: a row pointing at a deleted file.

    Scoped to in-repo links only. The index deliberately also names specs that
    live in ``datanika-cloud`` by full URL, and those are not files here.
    """
    text = INDEX.read_text(encoding="utf-8")
    on_disk = {s.name for s in _spec_files()}

    phantom = []
    for line in text.splitlines():
        if not line.startswith("| [`SPEC_"):
            continue
        # the in-repo row form is: | [`NAME.md`](NAME.md) | ... |
        start = line.find("](")
        if start == -1:
            continue
        target = line[start + 2 : line.find(")", start)]
        if "/" in target or target.startswith("http"):
            continue  # a cross-repo or nested link, not a file in this directory
        if target not in on_disk:
            phantom.append(target)

    assert not phantom, (
        "docs/specs/README.md links to files that are not in docs/specs/: "
        + ", ".join(phantom)
        + ". A row for a deleted spec is worse than no row: it reads as a contract "
        "that exists."
    )


@pytest.mark.parametrize("name", [s.name for s in _spec_files()])
def test_each_row_carries_a_status(name: str) -> None:
    """A row with no status cell is how three cells went stale unnoticed.

    The index's value is telling a reader whether a contract is *live*; a name
    alone does not do that.
    """
    text = INDEX.read_text(encoding="utf-8")
    row = next((ln for ln in text.splitlines() if ln.startswith(f"| [`{name}`]")), None)
    if row is None:
        pytest.skip(f"{name} is not indexed; test_every_spec_is_named_in_the_index owns that")
    cells = [c.strip() for c in row.strip().strip("|").split("|")]
    assert len(cells) >= 3 and cells[-1], (
        f"the index row for {name} has no status cell: {row!r}. "
        "Every row ends with what state the contract is in."
    )
