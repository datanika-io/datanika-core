"""A staging verdict must belong to the commit it is filed against (core#876).

The defect
----------
`staging-deploy`'s concurrency group (core#753) serialises *access* to staging. It does not
pin *identity*. Two runs can be perfectly serialised and still test the wrong artifacts::

    1da0c21  deploy-staging  22:08:50 -> 22:12:44
    87da585  deploy-staging  22:12:46 -> 22:16:48   correctly queued behind
    1da0c21  e2e-staging     22:16:50 -> 22:24:05   <- exercised 87da585's build

Nothing was concurrent. The lock did exactly what it promises, and the verdict was still
filed against the wrong commit. Three instances in two days, and in every one the
attribution came out **crossed** rather than merely shifted: the commit whose build was
actually under test had its own E2E *cancelled*, so it got no reading at all.

The inverse is the dangerous direction. A commit that breaks a gating spec can be overtaken
before its own E2E runs and be verified **green** against somebody else's code. That green
is indistinguishable from an honest one — which is how the #873 analysis ended in a
correct reading of a red followed by comfort drawn from a green belonging to another
commit.

The fix, and why it is shaped like this
---------------------------------------
`deploy-staging` stamps `github.sha` on the box once the stack is up and healthy; every job
that reads the stack asserts that stamp against its own `github.sha` before measuring
anything.

1. **The deploy writes the stamp LAST.** The stamp claims "staging is serving this commit",
   which is only true after the health wait.
2. **Every reader asserts, and asserts FIRST.** Before playwright, before pytest, before
   bringing Authentik up on the production box. A refusal then costs no setup time and
   releases `staging-deploy` sooner, so the run that *should* be verifying staging gets
   there faster.
3. 🚨 **It fails, it never skips, and it is never `continue-on-error`.** A skipped verifier
   makes a run green having verified nothing, and a skip and a pass are the same colour.
4. 🚨 **The refusal must not wear an E2E failure's headline.** An attribution refusal skips
   the gating step, and the pre-existing classifier would read `skipped` as `no_verdict` —
   paging "the harness did not run" about a harness that is fine. Hence the separate
   `wrong_build` state, which pages nothing and files nothing: staging is healthy, this run
   simply cannot honestly grade it. The response is to re-deploy the commit.
5. ⚠️ **Never re-deploy from inside a verifier.** That puts a mutation inside the window the
   verifiers hold, which is core#753 reintroduced. The assertion is cheaper and it is also
   the thing that tells you the problem exists.

What this does NOT claim
------------------------
It does not stop the overtaking. Two pushes landing close together will still overtake each
other; what changes is that the second run now *refuses* instead of reporting a verdict it
did not earn. Nor does it address the **cancelled** half — GitHub keeps one pending job per
group, so a burst still cancels a run's pending verification and that commit ends with no
reading. That is a promotion-time question, answered by
`scripts/verify-e2e-attribution.py` and the pre-flight in `RUNBOOK_DEV_TO_MASTER.md`.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CI = ROOT / ".github" / "workflows" / "ci.yml"

MUTATION_JOB = "deploy-staging"
WRITER = "staging-deploy-stamp.sh"
ASSERTER = "assert-staging-sha.sh"

#: Steps that either measure staging or spend real money/time before measuring it. The
#: assertion has to come before every one of them.
_COSTLY = re.compile(r"playwright\s+test|(?:^|\s)pytest\s|docker\s+compose\s+-p\s")


def _jobs(text: str) -> dict[str, dict]:
    """Jobs across a YAML *stream* — one document, or several joined by `---`.

    Multi-document since core#975: the staging jobs left `ci.yml`, so the real tree is now
    more than one file and a single-document parse would silently see fewer jobs. The
    synthetic controls below are single documents and are unaffected.
    """
    merged: dict[str, dict] = {}
    for doc in yaml.safe_load_all(text):
        if isinstance(doc, dict):
            merged.update(doc.get("jobs") or {})
    return merged


def _needs(job: dict) -> set[str]:
    raw = job.get("needs") or []
    return {raw} if isinstance(raw, str) else set(raw)


def _steps(job: dict) -> list[dict]:
    return job.get("steps") or []


def _run(step: dict) -> str:
    return step["run"] if isinstance(step.get("run"), str) else ""


def _index_of(job: dict, needle: str) -> int | None:
    for i, step in enumerate(_steps(job)):
        if needle in _run(step):
            return i
    return None


def _first_costly(job: dict) -> int | None:
    for i, step in enumerate(_steps(job)):
        if _COSTLY.search(_run(step)):
            return i
    return None


#: What a "reader" is. Derived from what a job DOES, not from whom it `needs` (core#975).
#: The old predicate was `MUTATION_JOB in _needs(job)`, and the day `deploy-staging` moved
#: into `staging.yml` two things broke at once: `e2e-sso` legitimately switched to
#: `needs: [staging]` and stopped being recognised, and jobs in the other file were never
#: scanned at all. A predicate keyed on a job NAME is a predicate about the workflow's
#: shape; this one is about the staging box.
_TOUCHES_STAGING = re.compile(r"staging-app\.datanika\.io|datanika-staging")


def _readers(jobs: dict[str, dict]) -> dict[str, dict]:
    """Downstream of the mutation, OR touching the staging box. The union, deliberately.

    Either half alone under-selects. `needs` alone missed `e2e-sso` the moment core#975
    moved the mutation into a called workflow and `e2e-sso` switched to `needs: [staging]`;
    the step-text half alone would miss a reader that drives staging through a script whose
    hostname is in an env var. A reader that satisfies neither is not something this file
    can reason about anyway.
    """
    return {
        n: j
        for n, j in jobs.items()
        if n != MUTATION_JOB
        and _steps(j)
        and (
            MUTATION_JOB in _needs(j)
            or _TOUCHES_STAGING.search("\n".join(_run(s) for s in _steps(j)))
        )
    }


def audit(ci_text: str) -> dict[str, list[str]]:
    """Every way a run can end up reporting on a build it did not deploy."""
    jobs = _jobs(ci_text)
    deploy = jobs.get(MUTATION_JOB) or {}

    no_stamp: list[str] = []
    if _index_of(deploy, f"{WRITER}") is None or " write " not in "\n".join(
        _run(s) for s in _steps(deploy)
    ):
        no_stamp.append(MUTATION_JOB)
    elif _index_of(deploy, WRITER) != len(_steps(deploy)) - 1:
        # A stamp written before the stack is healthy claims something not yet true.
        no_stamp.append(f"{MUTATION_JOB} (stamp is not the last step)")

    unasserted: list[str] = []
    asserts_too_late: list[str] = []
    unarmed: list[str] = []
    swallowed: list[str] = []
    unclassified: list[str] = []

    for name, job in _readers(jobs).items():
        idx = _index_of(job, ASSERTER)
        if idx is None:
            unasserted.append(name)
            continue

        costly = _first_costly(job)
        if costly is not None and costly < idx:
            asserts_too_late.append(name)

        for step in _steps(job):
            if ASSERTER not in _run(step):
                continue
            if (step.get("env") or {}).get("EXPECTED_SHA") != "${{ github.sha }}":
                unarmed.append(name)
            if step.get("continue-on-error"):
                swallowed.append(name)

        # A job that classifies its own result must be able to say "wrong build", or the
        # refusal is filed under whatever the classifier's fallback happens to be.
        verdict = next((s for s in _steps(job) if s.get("id") == "verdict"), None)
        if verdict is not None and (
            "ATTRIB" not in str(verdict.get("env") or {}) or "wrong_build" not in _run(verdict)
        ):
            unclassified.append(name)

    return {
        "no_stamp": sorted(no_stamp),
        "unasserted": sorted(unasserted),
        "asserts_too_late": sorted(asserts_too_late),
        "unarmed": sorted(set(unarmed)),
        "swallowed": sorted(set(swallowed)),
        "unclassified": sorted(unclassified),
    }


# ── the selectors must not be vacuous ───────────────────────────────────────────────────


@pytest.fixture(scope="module")
def ci_text() -> str:
    """Every workflow that defines jobs, concatenated for the text-level assertions.

    ⚠️ Not `ci.yml` alone (core#975). `deploy-staging`, `smoke-staging` and `e2e-staging`
    now live in `staging.yml`; a guard reading one file by path would have found no
    readers at all and — with `_readers` returning `{}` — reported every invariant clean.
    """
    return "\n---\n".join(
        p.read_text(encoding="utf-8")
        for p in sorted((ROOT / ".github" / "workflows").glob("*.yml"))
    )


@pytest.fixture(scope="module")
def report(ci_text: str) -> dict[str, list[str]]:
    return audit(ci_text)


def test_the_scripts_the_workflow_calls_actually_exist(ci_text: str) -> None:
    """A guard over a workflow that shells out to a missing file audits nothing."""
    for script in (WRITER, ASSERTER, "test-staging-sha-stamp.sh"):
        assert (ROOT / "scripts" / script).is_file(), script


def test_the_auditor_found_the_readers(ci_text: str) -> None:
    readers = _readers(_jobs(ci_text))
    assert {"smoke-staging", "e2e-staging", "e2e-sso"} <= set(readers), sorted(readers)
    assert all(_first_costly(j) is not None for j in readers.values()), (
        "the costly-step detector matched nothing in some reader; the ordering invariant "
        "would then be vacuous for it"
    )


# ── the invariants ──────────────────────────────────────────────────────────────────────


def test_the_deploy_stamps_the_commit_it_deployed(report) -> None:
    assert report["no_stamp"] == [], (
        "the staging deploy must record which commit it left running, as its LAST step "
        "(core#876):\n  " + "\n  ".join(report["no_stamp"])
    )


def test_every_reader_asserts_the_stamp(report) -> None:
    assert report["unasserted"] == [], (
        "these jobs read the staging stack without checking WHICH BUILD it is running, so "
        "their verdict can belong to another commit (core#876):\n  "
        + "\n  ".join(report["unasserted"])
    )


def test_the_assertion_runs_before_anything_is_measured(report) -> None:
    assert report["asserts_too_late"] == [], (
        "these jobs measure staging before checking it is the right build. The verdict "
        "would already exist by the time the assertion refused it:\n  "
        + "\n  ".join(report["asserts_too_late"])
    )


def test_the_assertion_is_armed_with_this_runs_sha(report) -> None:
    assert report["unarmed"] == [], (
        "EXPECTED_SHA must be `${{ github.sha }}` verbatim. Anything that can evaluate "
        "empty compares nothing with nothing and passes — the vacuous-green shape this "
        "issue is about:\n  " + "\n  ".join(report["unarmed"])
    )


def test_the_assertion_cannot_be_swallowed(report) -> None:
    assert report["swallowed"] == [], (
        "`continue-on-error` on the attribution assertion turns a refusal into a pass, "
        "which is the defect wearing the fix's clothes:\n  " + "\n  ".join(report["swallowed"])
    )


def test_a_refusal_is_classified_as_itself(report) -> None:
    assert report["unclassified"] == [], (
        "these jobs classify their own result but cannot say `wrong_build`, so an "
        "attribution refusal is reported as a broken harness or a failed spec — a false "
        "headline on a true problem (core#873's defect class):\n  "
        + "\n  ".join(report["unclassified"])
    )


def test_no_workflow_env_value_relies_on_tilde_expansion() -> None:
    """`SSH_KEY_PATH: ~/.ssh/id_rsa` in an `env:` is a path that does not exist.

    GitHub performs no shell expansion on `env:` values, so the tilde arrives literally and
    `ssh -i` fails on it. Caught while wiring core#876 — the first draft of all four
    assertion steps had exactly this, and it would have made every one of them fail closed:
    a verifier that always refuses is a verifier nobody keeps.

    Cheap, general, and it applies to every workflow rather than only the one that had it.
    """
    offenders: list[str] = []
    for path in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        workflow = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job_name, job in (workflow.get("jobs") or {}).items():
            scopes = [("job", job.get("env"))] + [
                (str(s.get("name") or s.get("uses") or "?"), s.get("env")) for s in _steps(job)
            ]
            for where, env in scopes:
                for key, value in (env or {}).items():
                    if str(value).startswith("~"):
                        offenders.append(f"{path.name}:{job_name}:{where}:{key}={value}")
    assert offenders == [], (
        "these env values start with `~`, which GitHub does not expand:\n  "
        + "\n  ".join(offenders)
    )


def test_a_refusal_pages_nobody_and_files_nothing(ci_text: str) -> None:
    """`wrong_build` is a CI-hygiene event, not a production alarm.

    Staging is healthy; this run simply cannot honestly grade it. Paging on it would put a
    "staging is broken" alert in front of the founder several times a day for a scheduling
    artifact — and the tracker would collect an issue per overtaken commit.
    """
    jobs = _jobs(ci_text)
    for name, job in _readers(jobs).items():
        for step in _steps(job):
            cond = str(step.get("if", ""))
            if "wrong_build" not in cond:
                continue
            body = _run(step) + str(step.get("uses", ""))
            assert "telegram" not in body.lower() and "gh issue" not in body, (
                f"{name}: step {step.get('name')!r} pages or files on `wrong_build`"
            )


# ── negative controls ───────────────────────────────────────────────────────────────────
#
# Each is a shape that was on `dev` or that a plausible edit would produce.

_PRE_FIX = """
jobs:
  deploy-staging:
    steps:
      - run: docker compose -p datanika-staging up -d app
      - run: 'for i in $(seq 1 40); do :; done'
  smoke-staging:
    needs: [deploy-staging]
    steps:
      - run: pytest scripts/smoke/ -v
  e2e-staging:
    needs: [deploy-staging]
    steps:
      - run: npx playwright test --grep-invert "@informational"
"""

_ASSERTS_AFTER_MEASURING = """
jobs:
  deploy-staging:
    steps:
      - run: docker compose -p datanika-staging up -d app
      - run: ssh host "bash -s -- write ${{ github.sha }} 1 1" < scripts/staging-deploy-stamp.sh
  e2e-staging:
    needs: [deploy-staging]
    steps:
      - run: npx playwright test
      - env: {EXPECTED_SHA: '${{ github.sha }}'}
        run: bash scripts/assert-staging-sha.sh
"""

_SWALLOWED = """
jobs:
  deploy-staging:
    steps:
      - run: ssh host "bash -s -- write ${{ github.sha }} 1 1" < scripts/staging-deploy-stamp.sh
  e2e-staging:
    needs: [deploy-staging]
    steps:
      - continue-on-error: true
        env: {EXPECTED_SHA: '${{ github.sha }}'}
        run: bash scripts/assert-staging-sha.sh
      - run: npx playwright test
"""

_UNARMED = _SWALLOWED.replace("continue-on-error: true\n        ", "").replace(
    "EXPECTED_SHA: '${{ github.sha }}'", "EXPECTED_SHA: '${{ env.SOME_SHA }}'"
)

_STAMP_BEFORE_HEALTHY = """
jobs:
  deploy-staging:
    steps:
      - run: ssh host "bash -s -- write ${{ github.sha }} 1 1" < scripts/staging-deploy-stamp.sh
      - run: 'wait for staging health'
"""


def test_auditor_rejects_the_pre_fix_shape() -> None:
    report = audit(_PRE_FIX)
    assert report["no_stamp"] == ["deploy-staging"], report
    assert report["unasserted"] == ["e2e-staging", "smoke-staging"], report


def test_auditor_rejects_an_assertion_made_after_the_verdict_exists() -> None:
    report = audit(_ASSERTS_AFTER_MEASURING)
    assert report["asserts_too_late"] == ["e2e-staging"], report
    assert report["unasserted"] == [], report


def test_auditor_rejects_a_swallowed_assertion() -> None:
    assert audit(_SWALLOWED)["swallowed"] == ["e2e-staging"]


def test_auditor_rejects_an_expected_sha_that_can_evaluate_empty() -> None:
    assert audit(_UNARMED)["unarmed"] == ["e2e-staging"]


def test_auditor_rejects_a_stamp_written_before_the_stack_is_healthy() -> None:
    """The stamp asserts staging *is serving* the commit, not that a deploy started."""
    assert audit(_STAMP_BEFORE_HEALTHY)["no_stamp"] == [
        "deploy-staging (stamp is not the last step)"
    ]
