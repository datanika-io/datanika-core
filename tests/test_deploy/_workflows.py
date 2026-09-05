"""Where does a CI job actually live? Ask; do not hardcode the file (core#975).

Seven guards in this directory opened `.github/workflows/ci.yml` by path and looked a
staging job up inside it. When core#975 moved `deploy-staging`, `smoke-staging` and
`e2e-staging` into `staging.yml` — so that one concurrency group member covers all three —
**15 tests failed and 8 errored**, none of them because the property under test had
changed.

That is worth a module rather than seven repointed constants, because the failure mode is
the interesting one: a guard that hardcodes *where* to look answers *"is the job in this
file?"* while claiming to answer *"does our CI do X?"*. Repointing each constant at
`staging.yml` fixes today and re-arms the same trap for the next move — the very shape
core#975 exists to stop (a check that cannot survive an ordinary refactor of the thing it
checks).

Two helpers, and the difference between them is deliberate:

* `job(job_id)` — for a guard about **one job**. It raises when the job is defined nowhere,
  and raises when it is defined in more than one workflow, so a copy-paste duplication
  cannot silently satisfy a guard from the wrong file.
* `all_jobs()` / `ci_workflows()` — for a guard about a **property of our CI as a whole**,
  such as "exactly one step emits this marker". Those were the ones most badly served by a
  single path: scoped to `ci.yml`, `test_it_no_longer_emits_the_informational_marker` read
  `0 steps` and failed, when the correct answer was still exactly one.

⚠️ `workflow_call`-only files are included on purpose. `staging.yml` has no `on: push`, and
excluding it is precisely how the jobs would become invisible again.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = ROOT / ".github" / "workflows"

# Files that are not part of the CI surface these guards reason about. Deliberately a
# short, named list rather than a pattern: an over-broad exclusion here reproduces the bug
# this module exists to fix, one directory up.
_NOT_CI: set[str] = set()


@lru_cache(maxsize=1)
def ci_workflows() -> dict[str, tuple[str, dict]]:
    """`{filename: (raw text, parsed doc)}` for every workflow that defines jobs."""
    yaml = pytest.importorskip("yaml")
    found: dict[str, tuple[str, dict]] = {}
    for path in sorted(WORKFLOW_DIR.glob("*.yml")) + sorted(WORKFLOW_DIR.glob("*.yaml")):
        if path.name in _NOT_CI:
            continue
        text = path.read_text(encoding="utf-8")
        doc = yaml.safe_load(text)
        if isinstance(doc, dict) and isinstance(doc.get("jobs"), dict):
            found[path.name] = (text, doc)
    assert found, f"no workflows with jobs found under {WORKFLOW_DIR} — this scan is vacuous"
    return found


def all_jobs() -> list[tuple[str, str, dict]]:
    """`(workflow filename, job id, job dict)` across every CI workflow."""
    return [
        (name, job_id, job)
        for name, (_text, doc) in ci_workflows().items()
        for job_id, job in doc["jobs"].items()
    ]


def job(job_id: str) -> tuple[str, str, dict]:
    """`(workflow filename, raw text of that file, the job dict)` for one job id.

    Raises rather than returning `None`: a guard that silently gets nothing back is the
    thing this module was written after.
    """
    hits = [
        (name, text, doc["jobs"][job_id])
        for name, (text, doc) in ci_workflows().items()
        if job_id in doc["jobs"]
    ]
    assert hits, (
        f"no workflow under {WORKFLOW_DIR.relative_to(ROOT).as_posix()} defines a job "
        f"called {job_id!r}. Either it was renamed — in which case the guard that asked "
        f"for it is now testing nothing and must be updated deliberately — or it moved to "
        f"a file this scan skips. Known workflows: {sorted(ci_workflows())}"
    )
    assert len(hits) == 1, (
        f"{job_id!r} is defined in {len(hits)} workflows: {[h[0] for h in hits]}. A guard "
        f"asking about 'the' job cannot say which one it measured, and a duplicate would "
        f"let it pass from the wrong file."
    )
    return hits[0]


def job_text(job_id: str) -> str:
    """The RAW yaml text of one job block, ending at the next job or the file's end.

    For guards that must assert on the literal source — step ordering, a `run:` body's
    exact wording — rather than on the parsed document.

    Guards used to slice this by hand as `text[text.index("\\n  e2e-staging:") :]` up to
    `text.index("\\n  e2e-sso:")`, naming the *next* job as the terminator. That is two
    hardcoded facts, and core#975 broke both at once: `e2e-sso` stayed in `ci.yml` while
    `e2e-staging` moved, so the slice raised `ValueError` — the lucky direction. Had the
    order been the other way round the slice would have silently run to the end of the
    file and the guard would have asserted against three jobs' text.
    """
    _name, text, _block = job(job_id)
    start = text.index(f"\n  {job_id}:")
    rest = text[start + 1 :]
    ends = [
        rest.index(f"\n  {other}:")
        for _wf, other, _b in all_jobs()
        if other != job_id and f"\n  {other}:" in rest
    ]
    return rest[: min(ends)] if ends else rest


def job_steps(job_id: str) -> list[dict]:
    """The steps of one job, or its `uses:` target's — never a silent empty list."""
    _name, _text, block = job(job_id)
    steps = block.get("steps")
    assert isinstance(steps, list) and steps, (
        f"job {job_id!r} has no steps. If it became a `uses:` caller, the guard needs to "
        f"follow it to {block.get('uses')!r} rather than reading an empty list as 'the "
        f"step is gone'."
    )
    return steps
