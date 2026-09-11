"""Something must run cloud's tests when CORE changes (core#1290).

The gap this closes
-------------------
`datanika-cloud` imports `datanika`, so its correctness depends on core's API. But
cloud's `ci.yml` triggers on pushes and PRs to **the cloud repo**, and core's `ci.yml`
did not run cloud's tests at all. A core-side change could therefore break cloud with
**no CI running anywhere**, and the core PR that broke it stayed green on every required
check -- because every required check was core's.

It is not hypothetical. [core#1269]: `ApiKeyService.create_api_key` gained a required
keyword-only `actor_user_id` with the service-layer role intersection ([core#681]), and
cloud's `seed_overage_tenant` was not updated. Cloud's suite went from green to
**12 failed** and stayed there for two days with nothing looking, because cloud had not
been pushed in that window. Downstream, the overage soak's first step 500'd, so **Gate 4
for V2 P5 produced no reading at all** while reading as an ordinary test failure.

The fix that does NOT work is "remember to check the other repo". That is what already
failed, against two careful changes on the same day (this one, and QA's
`parse_verdict_line` return type breaking Infra's promotion gate). Neither author had a
signal.

What these assertions are shaped by
-----------------------------------
Derived from the workflow rather than restated, because a restated list drifts -- the same
reasoning as `test_deployment_manifest_parity.py`.

A job that cannot fail is worse than no job, and this one has three ways to become that,
so each is asserted:

1. **Vacuity.** A wrong path makes pytest collect nothing. `pytest` exits 5 there, which
   is non-zero today -- but one `|| true`, one `continue-on-error`, or a future plugin
   change turns "nothing ran" into green. So the job must assert a **floor on the
   collected count**, and this test checks that the floor exists.
2. **Pairing against the wrong cloud branch.** A hardcoded `ref:` would pair a `dev` PR
   against cloud `master`. [core#923] is the precedent: `github.base_ref` is **empty** on
   a `merge_group` event, and the fallback silently resolved to a ref that does not exist
   in cloud. The resolver script already solves this and must be reused, not re-invented.
3. **Fork PRs.** They get no `CLOUD_REPO_TOKEN`. The job must degrade to a warning rather
   than a red that every fork contributor sees and nobody can fix.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
CI = ROOT / ".github" / "workflows" / "ci.yml"


@pytest.fixture(scope="module")
def workflow() -> dict:
    assert CI.is_file(), f"{CI} missing -- this guard would pass vacuously"
    data = yaml.safe_load(CI.read_text(encoding="utf-8"))
    assert data.get("jobs"), "ci.yml parsed with no jobs; the guard below would be vacuous"
    return data


@pytest.fixture(scope="module")
def raw() -> str:
    return CI.read_text(encoding="utf-8")


def _steps(job: dict) -> list[dict]:
    return [s for s in (job.get("steps") or []) if isinstance(s, dict)]


def _run_text(job: dict) -> str:
    return "\n".join(str(s.get("run", "")) for s in _steps(job))


def _cloud_suite_jobs(workflow: dict) -> dict[str, dict]:
    """Jobs that actually run cloud's test suite -- found by what they DO.

    Deliberately not `jobs["cloud-suite"]`: a rename would make a name lookup fail loudly,
    but a job that stops running pytest while keeping its name would not.
    """
    found = {}
    for name, job in (workflow.get("jobs") or {}).items():
        if not isinstance(job, dict):
            continue
        run = _run_text(job)
        if re.search(r"pytest[^\n]*\bcloud/tests\b", run):
            found[name] = job
    return found


def test_some_job_runs_the_cloud_suite(workflow: dict) -> None:
    """The whole point. Without this, a core change breaks cloud and nothing runs."""
    jobs = _cloud_suite_jobs(workflow)
    assert jobs, (
        "no job in ci.yml runs cloud's test suite. core#1290: cloud imports core, cloud's "
        "own CI only triggers on cloud pushes, so a core-side signature change breaks cloud "
        "with no CI running anywhere -- which is exactly how core#1269 sat red for two days "
        "and left Gate 4 for V2 P5 with no reading."
    )


def test_the_cloud_suite_job_asserts_it_collected_something(workflow: dict) -> None:
    """Anti-vacuity, and the reason is in this module's docstring.

    A path typo collects nothing. Today that exits non-zero, so it fails -- but the job
    must not RELY on that, because the failure mode is one `|| true` away and the symptom
    is a green required check that ran no tests.
    """
    jobs = _cloud_suite_jobs(workflow)
    assert jobs, "no cloud-suite job; test_some_job_runs_the_cloud_suite covers that"
    for name, job in jobs.items():
        run = _run_text(job)
        counts = re.search(r"--collect-only|collected", run) is not None
        compares = re.search(r"-lt |-le |MIN_|FLOOR", run) is not None
        assert counts and compares, (
            f"job {name!r} runs cloud's tests but never checks that it collected any. "
            "A wrong path, or a future `|| true`, turns 'nothing ran' into a green "
            "required check. Count the collected tests and fail below a floor."
        )


def test_the_cloud_suite_job_pairs_against_the_resolved_ref(workflow: dict) -> None:
    """core#923's lesson: `github.base_ref` is EMPTY on a merge_group event.

    A hardcoded `ref:` pairs a `dev` PR against cloud `master`; the naive fallback resolves
    to `gh-readonly-queue/...`, which does not exist in cloud. The resolver script exists
    precisely for this and must be reused rather than re-derived.
    """
    jobs = _cloud_suite_jobs(workflow)
    assert jobs, "no cloud-suite job to check"
    for name, job in jobs.items():
        checkouts = [
            s
            for s in _steps(job)
            if str((s.get("with") or {}).get("repository", "")).endswith("datanika-cloud")
        ]
        assert checkouts, f"job {name!r} runs cloud's tests without checking cloud out"
        for step in checkouts:
            ref = str((step.get("with") or {}).get("ref", ""))
            assert "cloudref" in ref, (
                f"job {name!r} checks cloud out at ref {ref!r} rather than the resolved "
                "`steps.cloudref.outputs.ref`. On a merge_group event `github.base_ref` is "
                "empty (core#923), so anything else pairs against the wrong branch -- or a "
                "branch that does not exist in cloud."
            )


def test_the_cloud_suite_job_degrades_on_a_fork_pr(workflow: dict) -> None:
    """A fork PR has no `CLOUD_REPO_TOKEN`. That must warn, not go red forever.

    Same structure `image-probe` and `core-only-image` already use: every step that needs
    the token is conditioned on the availability check.
    """
    jobs = _cloud_suite_jobs(workflow)
    assert jobs, "no cloud-suite job to check"
    for name, job in jobs.items():
        steps = _steps(job)
        token_steps = [s for s in steps if s.get("id") == "token"]
        assert token_steps, (
            f"job {name!r} has no `id: token` availability check, so a fork PR gets a "
            "confusing 404 from the cloud checkout instead of an honest warning."
        )
        idx = steps.index(token_steps[0])
        unguarded = [
            s.get("name") or s.get("uses") or "<step>"
            for s in steps[idx + 1 :]
            if "steps.token.outputs.available" not in str(s.get("if", ""))
        ]
        assert not unguarded, (
            f"job {name!r} has steps after the token check that do not test it: "
            f"{unguarded}. On a fork PR they would run without a cloud tree."
        )


def test_the_resolver_script_this_depends_on_exists(raw: str) -> None:
    """Anti-vacuity for the ref assertion: if the script were gone, `cloudref` would
    produce nothing and the ref would silently resolve to empty."""
    assert "resolve-cloud-ref.sh" in raw, "ci.yml no longer references the ref resolver"
    script = ROOT / ".github" / "scripts" / "resolve-cloud-ref.sh"
    assert script.is_file(), (
        f"{script} is referenced by ci.yml and missing; the cloud checkout would resolve "
        "to an empty ref, which checks out the default branch rather than failing"
    )
