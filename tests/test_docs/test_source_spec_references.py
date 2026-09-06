"""Every spec path cited in source must resolve — the plan-file migration left five that do not.

**Why this is a guard and not a one-off fix (core#1083).** The issue that prompted it proposed
``git grep "plans/engineering/SPEC_"`` as the check that closes the class, and reported the count as
1. Scoping the grep to *one department's* directory is what made it 1: the same defect was
sitting in ``plans/infra/`` twice, ``plans/product/`` once, and ``plans/engineering/`` once, plus
a migration — **five dead paths, in four directories**. A search for the wording you happen to
remember cannot measure a class; only a predicate over the whole corpus can.

**The assertion is POSITIVE, deliberately.** It says *"this path resolves"*, never *"this path does
not contain the string ``plans/``"*. A ban on wording is satisfied by any prose that mentions the
wording in order to deny it (WORKFLOW_RULES §4), and it would also have to be rewritten every time
somebody invents a new wrong prefix. Resolution is the property we actually care about: a
self-hoster who clones this repo can follow a path that resolves, and cannot follow one that
does not.

**Scope, and why it stops where it does:**

* Only references carrying a directory component are checked. A bare ``SPEC_FOO.md`` makes no claim
  about *where* the file is, so there is nothing to falsify. This matters — three bare names in the
  tree (``SPEC_VOLUME_METERING.md``, ``SPEC_GB_THROUGHPUT_METRICS.md``, ``SPEC_PRICING_V2.md``) are
  correct citations of documents that live in **other** repos, and a guard demanding
  ``docs/specs/<name>`` would red-light the correct artifact. That is the failure direction
  WORKFLOW_RULES warns is worse than the bug being fixed.
* A path under a recognised sibling repo (``datanika-cloud/``, ``datanika-landing/``) is a
  deliberate cross-repo citation, accepted without resolving because it cannot resolve from here.
  ``plans/`` is **not** in that list and must never be added: ``datanika-plans`` is a private repo,
  so a ``plans/`` path in public AGPL source points a reader at something they can never open.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE = REPO_ROOT / "datanika"

#: A spec citation that carries at least one directory component.
_PATH_REF = re.compile(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*/SPEC_[A-Z0-9_]+\.md")

#: Repos that live beside this one in the monorepo. A citation into one of these is
#: correct and unresolvable from here. ``plans/`` is excluded on purpose — see the
#: module docstring.
SIBLING_REPOS = ("datanika-cloud/", "datanika-landing/")


def unresolved_spec_refs(root: Path, package: Path) -> list[tuple[str, str]]:
    """Return ``(source file, cited path)`` for each path-bearing citation resolving nowhere.

    Kept module-level so the guard's own discrimination can be armed in-suite against a
    synthetic tree, rather than proved once by a harness in a session nobody can re-run.
    """
    bad: list[tuple[str, str]] = []
    for py in sorted(package.rglob("*.py")):
        if "__pycache__" in py.parts:
            continue
        # Bytes + explicit decode: the locale codec on the dev machine is cp1251 and
        # will silently mangle every non-ASCII character in these files.
        text = py.read_bytes().decode("utf-8", errors="replace")
        for ref in _PATH_REF.findall(text):
            if ref.startswith(SIBLING_REPOS):
                continue
            # `datanika/docs/...` names the location *in this repo* (CLAUDE.md), so it
            # resolves from the repo root with the package prefix stripped.
            candidates = [root / ref]
            if ref.startswith("datanika/"):
                candidates.append(root / ref[len("datanika/") :])
            if not any(c.is_file() for c in candidates):
                bad.append((str(py.relative_to(root)).replace("\\", "/"), ref))
    return bad


def _count_path_refs(package: Path) -> int:
    total = 0
    for py in package.rglob("*.py"):
        if "__pycache__" in py.parts:
            continue
        total += len(_PATH_REF.findall(py.read_bytes().decode("utf-8", errors="replace")))
    return total


def test_the_scan_actually_finds_spec_references() -> None:
    """Anti-vacuity. Without this, a regex that matches nothing reports a perfectly clean tree.

    Every green in the test below would be free, and the more broken the pattern the healthier
    the repo would look.
    """
    found = _count_path_refs(PACKAGE)
    assert found >= 8, (
        f"only {found} path-bearing spec references found under datanika/ — the pattern has "
        "probably stopped matching. A zero here is indistinguishable from a clean tree."
    )


def test_every_cited_spec_path_resolves() -> None:
    unresolved = unresolved_spec_refs(REPO_ROOT, PACKAGE)
    assert not unresolved, "spec paths cited in source that resolve nowhere:\n" + "\n".join(
        f"  {src}  ->  {ref}" for src, ref in unresolved
    )


def test_the_guard_discriminates(tmp_path: Path) -> None:
    """Arm it in both directions, against a real tree, every time CI runs.

    A guard that has only ever been seen green has not been shown able to go red — and the
    version of this that only checked the *bad* case would still pass after being narrowed
    until it matched nothing.
    """
    root = tmp_path
    pkg = root / "datanika"
    (pkg / "services").mkdir(parents=True)
    (root / "docs" / "specs").mkdir(parents=True)
    (root / "docs" / "specs" / "SPEC_REAL.md").write_bytes(b"# real\n")

    # 1. A path that resolves is accepted.
    (pkg / "services" / "good.py").write_bytes(b'"""See docs/specs/SPEC_REAL.md."""\n')
    # 2. A path under a sibling repo is accepted without resolving.
    (pkg / "services" / "sibling.py").write_bytes(
        b'"""See datanika-cloud/docs/specs/SPEC_ELSEWHERE.md."""\n'
    )
    # 3. A bare filename makes no path claim and is not checked.
    (pkg / "services" / "bare.py").write_bytes(b'"""See SPEC_NOWHERE.md."""\n')
    assert unresolved_spec_refs(root, pkg) == [], "the guard rejected a correct citation"

    # 4. A retired plans/ path is caught — the shape that motivated this file.
    (pkg / "services" / "stale.py").write_bytes(b'"""See plans/engineering/SPEC_REAL.md."""\n')
    # 5. So is any other path that simply does not exist.
    (pkg / "services" / "typo.py").write_bytes(b'"""See docs/specs/SPEC_TYPO.md."""\n')

    caught = {ref for _, ref in unresolved_spec_refs(root, pkg)}
    assert caught == {"plans/engineering/SPEC_REAL.md", "docs/specs/SPEC_TYPO.md"}, (
        f"guard did not catch exactly the two broken citations; got {sorted(caught)}"
    )
