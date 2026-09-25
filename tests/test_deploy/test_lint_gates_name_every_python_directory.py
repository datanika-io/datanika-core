"""A lint gate must name every directory that holds first-party Python (core#1558).

🔑 **This is the INVERSE of `test_lint_gates_cover_what_they_name.py`, and the pair is the point.**

```
that file  ── named  ⟹ non-empty ──  catches a DECORATIVE gate (a path ruff opens nothing under)
this file  ── holds Python ⟹ named ──  catches a BLIND SPOT  (a directory no gate looks at)
```

core#1237 closed the instance for `scripts/` by asserting the literal string `"scripts" in paths`.
That assertion is still there and still right, and it **cannot fail for a directory nobody thought
of** — which is how `.github/scripts` came to hold **1,962 lines across five programs**, two of
which decide *whether a promotion may proceed* (`cloud_pairing_gate.py`) and *what it closes*
(`promotion_refs.py`), with **no style or safety signal at all**. Pointed at that directory by hand,
ruff reported four findings; the findings were never the point, the absence of a reader was.

**The general shape, which is why this is a derived guard and not a sixth hardcoded path:** a gate
defined by an enumerated path list grows a blind spot every time a new directory appears, and
nothing goes red to say so. `scripts/` and `.github/scripts/` are one character apart and were one
`git mv` from swapping fates.

── Why `git ls-files` here, when the sibling file deliberately asks ruff ─────────────────────────

They ask different questions and each needs its own predicate.

* The sibling asks *"will the linter open a file under this path?"* — so it must consult **ruff**,
  which honours `exclude`, `extend-exclude` and `.gitignore`. A glob would answer a different
  question and miss a decorative gate.
* This file asks *"what Python does this repository ship?"* — so it must **not** consult ruff. A
  directory ruff is configured to skip is precisely the blind spot being hunted here; asking the
  linter whether it looks at the files it does not look at is a gate asserting itself
  (`QA_RULES` §30).

`git ls-files` is the honest source for that: it tracks what ships, and it cannot vary with local
build output, a stale `.venv`, or another session's untracked scratch.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from tests.test_deploy.test_lint_gates_cover_what_they_name import (
    _ci_lint_paths,
    _hook_lint_paths,
)

REPO_ROOT = Path(__file__).resolve().parents[2]

#: Tracked Python that neither gate is expected to name, and **why**.
#:
#: An entry here is a decision, not a suppression: it says *"this Python is deliberately outside
#: core's lint contract"* and names the contract it is inside instead. Adding one is cheap and
#: visible; that is the trade this guard exists to force. A new directory of Python fails this file
#: until somebody chooses between the two.
EXEMPT: dict[str, str] = {
    "datanika-mcp/": (
        "A separately distributed sub-package with its OWN [tool.ruff] in "
        "datanika-mcp/pyproject.toml, selecting E,F,I,N,UP,B,SIM — deliberately NOT core's `S`. "
        "Linting it under core's config would apply rules its own packaging does not select, and "
        "would make core's gate the authority on a tree that ships on its own release cadence "
        "(`mcp-v*`). ⚠️ It is currently linted by NO workflow either; that is a separate gap and a "
        "separate argument, not something to fix by quietly widening this list."
    ),
    ".scratch/": (
        "Per-session scratch (WORKFLOW_RULES §13), explicitly non-durable and swept without "
        "warning. These four files are tracked by accident rather than by design, so holding them "
        "to a lint contract would be asserting something about a directory whose whole rule is "
        "that nothing in it lasts. Not QA's to delete — another department's tree."
    ),
}


def _tracked_python() -> list[str]:
    """Every tracked `*.py` path, repo-relative with forward slashes."""
    out = subprocess.run(
        ["git", "ls-files", "*.py"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return sorted(line.strip() for line in out.stdout.splitlines() if line.strip())


def _covered_by(path: str, named: set[str]) -> bool:
    """Would a gate naming `named` reach `path`?

    A named token is either a directory prefix or an exact file. Prefix matching is anchored with
    a trailing slash on purpose: without it `scripts` would swallow `scripts_old/x.py`, and a
    coverage predicate that over-reports is how this class of blind spot survives a guard.
    """
    return any(path == token or path.startswith(token.rstrip("/") + "/") for token in named)


def _named_by_every_invocation(invocations: list[tuple[set[str], set[str]]]) -> set[str]:
    """Paths named by EVERY ruff invocation in a gate — the intersection, not the union.

    🚨 The union would let `ruff check` name a directory while `ruff format --check` does not, and
    report that as covered. Half a gate is the failure mode this whole file is about.
    """
    path_sets = [paths for paths, _flags in invocations if paths]
    if not path_sets:
        return set()
    return set.intersection(*path_sets)


# ── controls first: an instrument that cannot see is not evidence of a clean tree ────────────


def test_control_the_enumeration_returns_a_real_population() -> None:
    """🔑 Run first. `git ls-files` returning nothing would make every assertion below vacuous,
    and it fails that way silently — an empty list, exit 0, no error.

    This is the failure the coordinator's rule 26 is about: *an instrument that cannot see part of
    its population reports that part as clean.* Here it would report **all** of it as clean.
    """
    tracked = _tracked_python()
    assert len(tracked) > 500, (
        f"git ls-files '*.py' returned {len(tracked)} paths. This repository has well over 500 "
        "tracked Python files, so a small number means the enumeration is broken — not that the "
        "tree shrank. Every assertion in this file is vacuous when this is empty."
    )
    assert "rxconfig.py" in tracked, "a known root-level module is missing from the enumeration"
    assert any(p.startswith("datanika/services/") for p in tracked), (
        "no service module in the enumeration; git ls-files is not reading this repository"
    )


def test_control_the_coverage_predicate_can_answer_no() -> None:
    """The sharpest control is a subject that cannot exist (`QA_RULES` §31).

    A predicate that answers "covered" for everything passes the assertion below no matter what
    the gates name. Ask it about a path that is not there, and about one that merely shares a
    prefix.
    """
    named = {"datanika", "tests", "scripts"}
    assert _covered_by("datanika/config.py", named) is True
    assert _covered_by("no_such_dir_qa1558/x.py", named) is False, (
        "the coverage predicate called a fabricated path covered — it cannot distinguish a gated "
        "directory from an invented one, so a green from it means nothing"
    )
    assert _covered_by("scripts_old/x.py", named) is False, (
        "`scripts` matched `scripts_old/` — an unanchored prefix over-reports coverage, which is "
        "exactly how a blind spot survives a guard that looks like it checks for one"
    )
    assert _covered_by("scripts", {"scripts"}) is True, (
        "a gate may name an exact file (rxconfig.py); exact matches must still count"
    )


def test_control_both_gates_name_a_real_population() -> None:
    """If the imported parser returns nothing, every path is 'uncovered' and this file fails for
    the wrong reason — loudly, but pointing at the tree instead of at itself."""
    for label, invocations in (("ci.yml", _ci_lint_paths()), ("pre-push", _hook_lint_paths())):
        named = _named_by_every_invocation(invocations)
        assert "datanika" in named, (
            f"{label}: the parsed path intersection is {sorted(named)}, which does not contain "
            "`datanika`. The parser imported from test_lint_gates_cover_what_they_name is not "
            "reading this gate — fix that before reading anything else in this file."
        )


# ── the assertion ────────────────────────────────────────────────────────────────────────────


def test_every_tracked_python_file_is_named_by_both_gates_or_exempt() -> None:
    """core#1558. The completeness half of the lint contract.

    **Both gates, not either.** CI alone leaves a direct push to `dev` unlinted — that branch has
    no pull-request requirement — and the hook alone is bypassed by `--admin` and by anything not
    pushed from this machine. The sibling file makes the same argument for the same reason.
    """
    ci = _named_by_every_invocation(_ci_lint_paths())
    hook = _named_by_every_invocation(_hook_lint_paths())

    orphans = [
        path
        for path in _tracked_python()
        if not (_covered_by(path, ci) and _covered_by(path, hook))
        and not any(path.startswith(prefix) for prefix in EXEMPT)
    ]

    assert not orphans, (
        f"{len(orphans)} tracked Python file(s) are read by no lint gate and are in no documented "
        f"exemption: {orphans[:12]}{' …' if len(orphans) > 12 else ''}.\n"
        "Either add the directory to BOTH `ruff check` and `ruff format --check` — in "
        ".github/workflows/ci.yml and scripts/hooks/pre-push, which must change together — or add "
        "it to EXEMPT above with the contract it is inside instead. Silence is the one option this "
        "guard removes."
    )


def test_no_exemption_outlives_the_thing_it_exempts() -> None:
    """An exemption that matches nothing is a decision about a tree that no longer exists.

    Same reasoning as `QA_RULES` §5 on `KNOWN_VIOLATIONS`: an entry that can outlive its subject
    turns a reasoned list into accumulated sediment, and the next reader inherits it as fact. If
    `.scratch/` is ever cleaned out of this repository, this goes red and the two lines come out —
    which is the correct amount of work.
    """
    tracked = _tracked_python()
    dead = [prefix for prefix in EXEMPT if not any(p.startswith(prefix) for p in tracked)]
    assert not dead, (
        f"EXEMPT names {dead}, and no tracked Python file is under it any more. Delete the entry: "
        "an exemption nobody can hit reads as a live decision and is not one."
    )
