"""Nothing may deploy staging while something is still reading it — and no verdict may be
displaced while its subject is still `dev`'s head (core#753, core#765, core#975).

The rule this enforces
----------------------
`WORKFLOW_RULES.md` §2: *anything shared, slow and stateful downstream of a merge needs a
concurrency group at the layer that owns the mutation — and for mutations, queue.*

**Two defects, in sequence, and the second was created by the fix for the first.**

core#753 — `ci.yml` applied that rule to exactly one job. `deploy-staging` held
`staging-deploy` and released it when the **deploy** finished, not when **verification
against that deploy** finished. So the next push's deploy was free to recreate containers
underneath a suite mid-flight. Measured on 2026-08-31: **38 false-red gating E2E results**,
and one auto-filed issue against an innocent commit whose entire diff was a shell script.

    3da26f4  e2e 01:38:49-01:42:53   overlapped by e173cf9's deploy   ->  7 failed
    e173cf9  e2e 01:42:43-01:54:02   overlapped by 4ad944d's deploy   -> 31 failed
    4ad944d  e2e 01:47:08-01:49:26   overlapped by nothing            -> 46 passed

Every failing assertion in those runs returned 502 (x9) or 503 (x5) and none returned
anything else — containers being recreated mid-test, not a product fault.

core#975 — the fix put the *same* group on all three jobs, and
**`cancel-in-progress: false` does not mean "queue everything"**: GitHub keeps one RUNNING
plus one PENDING per group, and a newer waiter **cancels the pending one**. Three members
per run, six when two `dev` pushes overlap. Four measured instances, each leaving a commit
that was `dev`'s head with **no honest staging reading and nothing red anywhere**:

    84965838   smoke cancelled at dispatch (0 s)   e2e refused (wrong_build)
    24a0a142   smoke ok                            e2e cancelled (0 s)
    95dca003   smoke cancelled after 92 s          e2e ran
    c09b843e   smoke cancelled after 104 s         e2e ok

🔑 A cancelled job is **neither green nor red AND has zero steps in the API**, so core#873's
*"drop to step level"* — the fallback every other silent-failure class in this repo has —
does not exist here. `conclusion == "cancelled"` is the only tell, and duration does not
discriminate (0 s, 92 s and 104 s all observed).

The shape that fixes it
-----------------------
**One group member per run.** The three jobs live in `staging.yml` and are reached through a
single caller job in `ci.yml` that holds `staging-deploy` for the whole life of the called
workflow. There is nothing left for a newer run to displace; it queues instead.

Measured before the shape was chosen (run 33957161411, probe branch deleted): a caller job's
group holds across the whole called workflow, including callee jobs that only start after an
earlier one finishes — a shared-group caller sat *pending* while its twin progressed through
jobs two and three, while a distinct-group control overlapped freely.

Why the invariants are shaped like this
---------------------------------------
1. **The mutation and every job downstream of it live in ONE called workflow.** A staging
   job left behind in `ci.yml` becomes a second group member and re-opens core#975.
2. **Exactly one caller holds `staging-deploy`, and it queues.** `cancel-in-progress: true`
   is right for *verdicts you can recompute* and wrong here twice over: cancelling a deploy
   mid-flight is core#572, and a staging verdict **cannot** be recomputed once the box has
   been redeployed to a newer commit, which makes it mutation-shaped rather than
   verdict-shaped.
3. 🚨 **No job inside the called workflow declares a concurrency group of its own.** Asking
   for `staging-deploy` there is not a queue but a **deadlock** — the job would wait on a
   group its own caller is holding. Any other group is core#753 again.
4. **A job that owns a *different* mutation keeps its own lock.** `e2e-sso` brings up a
   compose project of its own (core#765), so it is correctly outside `staging-deploy`.
   Derived from what each job's steps do, not from a list of names.

What this does NOT claim
------------------------
Nothing here says a staging job can never be cancelled. An intermediate commit superseded
before it is ever `dev`'s head does not need a verdict, and cancelling that is correct. The
property is *"every commit that becomes the head acquires a complete, attributed verdict, or
something goes red"* — and after core#975 the only way to lose one is for the run itself to
be cancelled, not for a sibling job to displace it.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = ROOT / ".github" / "workflows"

MUTATION_JOB = "deploy-staging"
GROUP = "staging-deploy"
STAGING_PROJECT = "datanika-staging"

# `docker compose -p e2e -f docker-compose.test.yml up -d` — a project of the job's own.
_OWN_PROJECT = re.compile(r"docker\s+compose\s+-p\s+([A-Za-z0-9_.-]+)")


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


def _group_of(job: dict) -> str | None:
    conc = job.get("concurrency")
    if isinstance(conc, str):
        return conc
    if isinstance(conc, dict):
        return conc.get("group")
    return None


def _queues(job: dict) -> bool:
    conc = job.get("concurrency")
    return isinstance(conc, dict) and conc.get("cancel-in-progress") is False


def audit(docs: dict[str, dict]) -> dict[str, list[str]]:
    """`{workflow filename: parsed doc}` -> named findings, each an empty list when clean.

    A module-level function on purpose: it is armed in-suite against synthetic broken
    shapes below, so the guard's own discrimination is re-proved on every CI run rather
    than in whichever session last ran an external harness.
    """
    jobs = {
        (wf, name): job for wf, doc in docs.items() for name, job in (doc.get("jobs") or {}).items()
    }

    home = [wf for (wf, name) in jobs if name == MUTATION_JOB]
    findings: dict[str, list[str]] = {
        "mutation_missing": [] if home else [MUTATION_JOB],
        "split_across_workflows": [],
        "callee_declares_a_group": [],
        "callers": [],
        "caller_cancels": [],
        "unlocked_readers": [],
    }
    if not home:
        return findings
    staging_wf = home[0]

    # 1. the mutation and everything downstream of it are in one workflow
    findings["split_across_workflows"] = sorted(
        f"{wf}:{name}"
        for (wf, name), job in jobs.items()
        if MUTATION_JOB in _needs(job) and wf != staging_wf
    )

    # 3. nothing inside that workflow may hold a group of its own
    findings["callee_declares_a_group"] = sorted(
        f"{name} ({_group_of(job)})"
        for (wf, name), job in jobs.items()
        if wf == staging_wf and _group_of(job)
    )

    # 2. exactly one caller of that workflow, holding GROUP, queueing
    callers = {
        name: job
        for (wf, name), job in jobs.items()
        if wf != staging_wf and str(job.get("uses") or "").endswith(f"/{staging_wf}")
    }
    findings["callers"] = sorted(n for n, j in callers.items() if _group_of(j) == GROUP)
    findings["caller_cancels"] = sorted(
        n for n, j in callers.items() if _group_of(j) == GROUP and not _queues(j)
    )

    # 4. a reader elsewhere that neither owns its own mutation nor shares the lock
    findings["unlocked_readers"] = sorted(
        f"{wf}:{name}"
        for (wf, name), job in jobs.items()
        if wf != staging_wf
        and name not in callers
        and "staging-app.datanika.io" in _step_text(job)
        and not _brings_up_its_own_project(job)
        and _group_of(job) != GROUP
    )
    return findings


@pytest.fixture(scope="module")
def real_docs() -> dict[str, dict]:
    found = {}
    for path in sorted(WORKFLOW_DIR.glob("*.yml")):
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        if isinstance(doc, dict) and isinstance(doc.get("jobs"), dict):
            found[path.name] = doc
    assert found, f"no workflows parsed under {WORKFLOW_DIR}"
    return found


@pytest.fixture(scope="module")
def report(real_docs) -> dict[str, list[str]]:
    return audit(real_docs)


def test_the_auditor_actually_found_the_staging_jobs(real_docs) -> None:
    """A selector that matches nothing reports a clean bill of health."""
    names = {n for doc in real_docs.values() for n in doc["jobs"]}
    assert MUTATION_JOB in names, sorted(names)
    downstream = {
        n for doc in real_docs.values() for n, j in doc["jobs"].items() if MUTATION_JOB in _needs(j)
    }
    assert len(downstream) >= 2, f"expected at least 2 verifiers downstream, got {downstream}"
    assert any(
        _brings_up_its_own_project(j) for doc in real_docs.values() for j in doc["jobs"].values()
    ), (
        "the own-project detector matched nothing; e2e-sso brings up `docker compose -p e2e` "
        "and invariant 4 is vacuous without it"
    )


def test_the_mutation_and_its_verifiers_live_in_one_called_workflow(report) -> None:
    assert report["split_across_workflows"] == [], (
        "these jobs depend on the staging deploy but sit in a DIFFERENT workflow, so each "
        "is a separate member of the `staging-deploy` group — which is core#975: GitHub "
        "keeps one running plus one pending per group and a newer waiter cancels the "
        "pending one, silently leaving a head commit with no verdict:\n  "
        + "\n  ".join(report["split_across_workflows"])
    )


def test_no_staging_job_declares_a_group_of_its_own(report) -> None:
    assert report["callee_declares_a_group"] == [], (
        "a job inside the called staging workflow declares its own concurrency group. If "
        "it names `staging-deploy` that is a DEADLOCK, not a queue — the job waits on a "
        "group its own caller is holding. Any other group is core#753 again:\n  "
        + "\n  ".join(report["callee_declares_a_group"])
    )


def test_exactly_one_caller_holds_the_lock(report) -> None:
    assert len(report["callers"]) == 1, (
        f"expected exactly one caller job holding `{GROUP}`, found {report['callers']}. "
        "Zero means nothing serialises staging at all (core#753); more than one restores "
        "the multi-member group core#975 removed."
    )


def test_the_caller_queues_rather_than_cancelling(report) -> None:
    assert report["caller_cancels"] == [], (
        "`cancel-in-progress` must be explicitly false on the staging caller. Cancelling a "
        "deploy mid-flight is core#572; and a staging verdict cannot be recomputed once the "
        "box has been redeployed to a newer commit, so it is mutation-shaped rather than "
        "verdict-shaped:\n  " + "\n  ".join(report["caller_cancels"])
    )


def test_readers_elsewhere_hold_the_lock_or_own_a_different_mutation(report) -> None:
    assert report["unlocked_readers"] == [], (
        "these jobs read the staging stack from outside the locked workflow and neither "
        "hold its lock nor bring up a compose project of their own, so a deploy can "
        "recreate containers underneath them mid-test (core#753):\n  "
        + "\n  ".join(report["unlocked_readers"])
    )


# ── negative controls, armed in-suite ────────────────────────────────────────────────
#
# Every shape below is one this repository has actually shipped or would plausibly ship,
# rather than a fixture written from the same mental model as the check. The first is the
# literal pre-core#975 `ci.yml`.


def _y(text: str) -> dict:
    return yaml.safe_load(text)


_THREE_SIBLINGS_ONE_GROUP = {
    "ci.yml": _y("""
jobs:
  deploy-staging:
    concurrency: {group: staging-deploy, cancel-in-progress: false}
    steps: [{run: 'docker compose -p datanika-staging up -d app'}]
  smoke-staging:
    needs: [deploy-staging]
    concurrency: {group: staging-deploy, cancel-in-progress: false}
    steps: [{run: 'curl https://staging-app.datanika.io/healthz'}]
  e2e-staging:
    needs: [deploy-staging]
    concurrency: {group: staging-deploy, cancel-in-progress: false}
    steps: [{run: 'npx playwright test'}]
""")
}

_CALLEE_KEEPS_THE_GROUP = {
    "ci.yml": _y("""
jobs:
  staging:
    concurrency: {group: staging-deploy, cancel-in-progress: false}
    uses: ./.github/workflows/staging.yml
"""),
    "staging.yml": _y("""
jobs:
  deploy-staging:
    concurrency: {group: staging-deploy, cancel-in-progress: false}
    steps: [{run: 'docker compose -p datanika-staging up -d app'}]
  e2e-staging:
    needs: [deploy-staging]
    steps: [{run: 'npx playwright test'}]
"""),
}

_CALLER_CANCELS = {
    "ci.yml": _y("""
jobs:
  staging:
    concurrency: {group: staging-deploy, cancel-in-progress: true}
    uses: ./.github/workflows/staging.yml
"""),
    "staging.yml": _y("""
jobs:
  deploy-staging:
    steps: [{run: 'docker compose -p datanika-staging up -d app'}]
"""),
}

_A_VERIFIER_LEFT_BEHIND = {
    "ci.yml": _y("""
jobs:
  staging:
    concurrency: {group: staging-deploy, cancel-in-progress: false}
    uses: ./.github/workflows/staging.yml
  smoke-staging:
    needs: [deploy-staging]
    steps: [{run: 'curl https://staging-app.datanika.io/healthz'}]
"""),
    "staging.yml": _y("""
jobs:
  deploy-staging:
    steps: [{run: 'docker compose -p datanika-staging up -d app'}]
"""),
}

_CLEAN = {
    "ci.yml": _y("""
jobs:
  staging:
    concurrency: {group: staging-deploy, cancel-in-progress: false}
    uses: ./.github/workflows/staging.yml
  e2e-sso:
    concurrency: {group: e2e-sso, cancel-in-progress: false}
    steps: [{run: 'docker compose -p e2e -f docker-compose.test.yml up -d && curl https://staging-app.datanika.io/'}]
"""),
    "staging.yml": _y("""
jobs:
  deploy-staging:
    steps: [{run: 'docker compose -p datanika-staging up -d app'}]
  smoke-staging:
    needs: [deploy-staging]
    steps: [{run: 'curl https://staging-app.datanika.io/healthz'}]
  e2e-staging:
    needs: [deploy-staging]
    steps: [{run: 'npx playwright test'}]
"""),
}


def test_the_auditor_passes_the_shape_we_actually_shipped() -> None:
    """The positive control. Without it every red below could be a broken auditor."""
    assert audit(_CLEAN) == {
        "mutation_missing": [],
        "split_across_workflows": [],
        "callee_declares_a_group": [],
        "callers": ["staging"],
        "caller_cancels": [],
        "unlocked_readers": [],
    }


def test_auditor_rejects_three_siblings_sharing_one_group() -> None:
    """core#975's own shape: valid YAML, correct-looking, and it eats verdicts."""
    found = audit(_THREE_SIBLINGS_ONE_GROUP)
    assert found["split_across_workflows"] == [], found
    assert found["callee_declares_a_group"] == [
        "deploy-staging (staging-deploy)",
        "e2e-staging (staging-deploy)",
        "smoke-staging (staging-deploy)",
    ], found
    assert found["callers"] == [], "there is no caller in the pre-975 shape"


def test_auditor_rejects_a_callee_that_kept_its_own_group() -> None:
    """The deadlock. It is the single most likely mistake when adding a job here."""
    assert audit(_CALLEE_KEEPS_THE_GROUP)["callee_declares_a_group"] == [
        "deploy-staging (staging-deploy)"
    ]


def test_auditor_rejects_a_caller_that_cancels_in_progress() -> None:
    assert audit(_CALLER_CANCELS)["caller_cancels"] == ["staging"]


def test_auditor_rejects_a_verifier_left_behind_in_the_caller() -> None:
    """A second group member is exactly what core#975 removed — one is enough to restore it."""
    assert audit(_A_VERIFIER_LEFT_BEHIND)["split_across_workflows"] == ["ci.yml:smoke-staging"]


def test_auditor_permits_a_job_that_owns_a_different_mutation() -> None:
    """core#765: e2e-sso brings up its own compose project, so its own group is correct."""
    assert audit(_CLEAN)["unlocked_readers"] == []


def test_auditor_flags_a_reader_outside_the_lock_that_owns_no_mutation() -> None:
    """Same fixture with the own-project `up` removed — the control for the control."""
    broken = {
        "ci.yml": _y("""
jobs:
  staging:
    concurrency: {group: staging-deploy, cancel-in-progress: false}
    uses: ./.github/workflows/staging.yml
  drive-by:
    steps: [{run: 'curl https://staging-app.datanika.io/'}]
"""),
        "staging.yml": _CLEAN["staging.yml"],
    }
    assert audit(broken)["unlocked_readers"] == ["ci.yml:drive-by"]
