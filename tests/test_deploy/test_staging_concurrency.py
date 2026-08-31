"""Nothing may deploy staging while something is still reading it (core#753, core#765).

The rule this enforces
----------------------
`WORKFLOW_RULES.md` §2: *anything shared, slow and stateful downstream of a merge needs a
concurrency group at the layer that owns the mutation — and for mutations, queue.*

`ci.yml` applied that to exactly one job. `deploy-staging` held `staging-deploy` and
released it when the **deploy** finished, not when **verification against that deploy**
finished. So the next push's deploy was free to recreate containers underneath a suite
mid-flight. Measured on 2026-08-31: **38 false-red gating E2E results**, and one auto-filed
issue against an innocent commit whose entire diff was a shell script.

Three consecutive runs, from the jobs API, are the whole argument:

    3da26f4  e2e 01:38:49-01:42:53   overlapped by e173cf9's deploy   ->  7 failed
    e173cf9  e2e 01:42:43-01:54:02   overlapped by 4ad944d's deploy   -> 31 failed
    4ad944d  e2e 01:47:08-01:49:26   overlapped by nothing            -> 46 passed

Every failing assertion in those runs returned 502 (x9) or 503 (x5) and none returned
anything else — containers being recreated mid-test, not a product fault. And because the
tenant-isolation and RBAC specs run last, a false red here wore the most alarming possible
headline on the night a real cross-tenant S1 was being promoted. A gate that reds on
innocent commits is how a real red gets waved through as "that known staging thing".

Why the invariants are shaped like this
---------------------------------------
1. **Every job that touches the staging stack declares a group.** Derived: `deploy-staging`
   plus everything that `needs` it. A job with no group is the core#753 defect.
2. **Every such group queues.** `cancel-in-progress: true` is right for *verdicts you can
   recompute* and wrong for mutations: cancelling a deploy mid-flight is core#572, and
   cancelling `e2e-sso` between its `up -d` and its `if: always()` teardown strands four
   Authentik containers on the production box.
3. **A job that only READS the staging stack shares the mutation's group.** A job that
   brings up a compose project *of its own* is excused — it owns a different mutation and
   needs its own lock, which is core#765 and is why `e2e-sso` is not in `staging-deploy`.
   That distinction is derived from what each job's steps actually do, not from a list of
   job names, so a new verifier is bound by rule 3 the moment it is added.

What this does NOT claim
------------------------
GitHub holds one pending job per group, and a newly queued job cancels the previously
pending one — so a burst can still cancel a run's pending verification. That is a
**cancelled** verdict, which is neither green nor red and reads as absent. It is a far
better failure than a false red, and the newest push always wins the queue, which is the
right polarity for a promotion pre-flight that needs `dev`'s exact HEAD. The property
actually guaranteed here is narrower and is the one that was breached: **no deploy runs
while a verifier holds the lock.**
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CI = ROOT / ".github" / "workflows" / "ci.yml"

MUTATION_JOB = "deploy-staging"
STAGING_PROJECT = "datanika-staging"

# `docker compose -p e2e -f docker-compose.test.yml up -d` — a project of the job's own.
_OWN_PROJECT = re.compile(r"docker\s+compose\s+-p\s+([A-Za-z0-9_.-]+)")


def _jobs(text: str) -> dict[str, dict]:
    return (yaml.safe_load(text) or {}).get("jobs") or {}


def _needs(job: dict) -> set[str]:
    raw = job.get("needs") or []
    return {raw} if isinstance(raw, str) else set(raw)


def _step_text(job: dict) -> str:
    return "\n".join(
        s.get("run", "") for s in (job.get("steps") or []) if isinstance(s.get("run"), str)
    )


def _brings_up_its_own_project(job: dict) -> bool:
    """Does this job `up` a compose project other than the staging one?"""
    text = _step_text(job)
    return any(
        project != STAGING_PROJECT
        for match in _OWN_PROJECT.finditer(text)
        for project in [match.group(1)]
        if "up" in text[match.end() : match.end() + 200]
    )


def _group(job: dict) -> str | None:
    conc = job.get("concurrency")
    if isinstance(conc, str):
        return conc
    if isinstance(conc, dict):
        return conc.get("group")
    return None


def _queues(job: dict) -> bool:
    conc = job.get("concurrency")
    return isinstance(conc, dict) and conc.get("cancel-in-progress") is False


def _staging_jobs(jobs: dict[str, dict]) -> dict[str, dict]:
    """The mutation and everything downstream of it."""
    return {
        name: job
        for name, job in jobs.items()
        if name == MUTATION_JOB or MUTATION_JOB in _needs(job)
    }


def _audit(text: str) -> dict[str, list[str]]:
    jobs = _jobs(text)
    staging = _staging_jobs(jobs)
    ungrouped = sorted(n for n, j in staging.items() if not _group(j))
    cancels = sorted(n for n, j in staging.items() if _group(j) and not _queues(j))
    unlocked_readers = sorted(
        n
        for n, j in staging.items()
        if n != MUTATION_JOB
        and _group(j)
        and not _brings_up_its_own_project(j)
        and _group(j) != _group(jobs[MUTATION_JOB])
    )
    return {"ungrouped": ungrouped, "cancels": cancels, "unlocked_readers": unlocked_readers}


@pytest.fixture(scope="module")
def ci_text() -> str:
    return CI.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def report(ci_text: str) -> dict[str, list[str]]:
    return _audit(ci_text)


def test_the_auditor_actually_found_the_staging_jobs(ci_text: str) -> None:
    """A selector that matches nothing reports a clean bill of health."""
    staging = _staging_jobs(_jobs(ci_text))
    assert MUTATION_JOB in staging, sorted(staging)
    assert len(staging) >= 4, (
        f"expected the deploy plus at least 3 dependents, got {sorted(staging)}"
    )
    assert any(_brings_up_its_own_project(j) for j in staging.values()), (
        "the own-project detector matched nothing; e2e-sso brings up `docker compose -p e2e` "
        "and rule 3 is vacuous without it"
    )


def test_every_staging_job_declares_a_concurrency_group(report) -> None:
    assert report["ungrouped"] == [], (
        "these jobs touch the staging stack with no concurrency group at all — the "
        "core#753 defect:\n  " + "\n  ".join(report["ungrouped"])
    )


def test_no_staging_job_cancels_in_progress(report) -> None:
    assert report["cancels"] == [], (
        "`cancel-in-progress` must be explicitly false on every staging job. Cancelling a "
        "deploy mid-flight is core#572; cancelling e2e-sso between its `up -d` and its "
        "`if: always()` teardown strands Authentik containers on the production box:\n  "
        + "\n  ".join(report["cancels"])
    )


def test_readers_hold_the_same_lock_as_the_deploy_they_verify(report) -> None:
    assert report["unlocked_readers"] == [], (
        "these jobs only READ the staging stack but do not hold the deploy's lock, so a "
        "deploy can recreate containers underneath them mid-test (core#753):\n  "
        + "\n  ".join(report["unlocked_readers"])
    )


# ── negative controls ────────────────────────────────────────────────────────────────

_PRE_FIX = """
jobs:
  deploy-staging:
    concurrency:
      group: staging-deploy
      cancel-in-progress: false
    steps:
      - run: docker compose -p datanika-staging up -d postgres redis app
  smoke-staging:
    needs: [deploy-staging]
    steps: [{run: 'curl https://staging-app.datanika.io/healthz'}]
  e2e-staging:
    needs: [deploy-staging]
    steps: [{run: 'npx playwright test'}]
  e2e-sso:
    needs: [deploy-staging]
    steps: [{run: 'docker compose -p e2e -f docker-compose.test.yml up -d'}]
"""

_CANCELS_IN_PROGRESS = """
jobs:
  deploy-staging:
    concurrency: {group: staging-deploy, cancel-in-progress: false}
    steps: [{run: 'docker compose -p datanika-staging up -d app'}]
  e2e-staging:
    needs: [deploy-staging]
    concurrency: {group: staging-deploy, cancel-in-progress: true}
    steps: [{run: 'npx playwright test'}]
"""

_READER_IN_ITS_OWN_GROUP = """
jobs:
  deploy-staging:
    concurrency: {group: staging-deploy, cancel-in-progress: false}
    steps: [{run: 'docker compose -p datanika-staging up -d app'}]
  e2e-staging:
    needs: [deploy-staging]
    concurrency: {group: staging-e2e, cancel-in-progress: false}
    steps: [{run: 'npx playwright test'}]
"""


def test_auditor_rejects_the_pre_fix_shape() -> None:
    report = _audit(_PRE_FIX)
    assert report["ungrouped"] == ["e2e-sso", "e2e-staging", "smoke-staging"], report


def test_auditor_rejects_a_verifier_that_cancels_in_progress() -> None:
    assert _audit(_CANCELS_IN_PROGRESS)["cancels"] == ["e2e-staging"]


def test_auditor_rejects_a_pure_reader_holding_its_own_lock() -> None:
    """The whole point of core#753: its own group does not stop a deploy underneath it."""
    assert _audit(_READER_IN_ITS_OWN_GROUP)["unlocked_readers"] == ["e2e-staging"]


def test_auditor_permits_a_job_that_owns_a_different_mutation() -> None:
    """core#765: e2e-sso brings up its own compose project, so its own group is correct."""
    text = _READER_IN_ITS_OWN_GROUP.replace(
        "npx playwright test", "docker compose -p e2e -f docker-compose.test.yml up -d"
    )
    assert _audit(text)["unlocked_readers"] == []
