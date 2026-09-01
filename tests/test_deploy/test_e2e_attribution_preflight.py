"""The promotion pre-flight must refuse a green that belongs to another commit (core#876).

Every fixture below is **real measured data** from `runs/<id>/jobs`, not invented shapes —
the three instances found on `dev` on 2026-08-31/09-01, plus the two heads that were
promoted and were genuinely clean. That matters more than usual here: the promoter's manual
version of this check has already been run by hand and reached the right answer, so the
question a synthetic fixture cannot answer is whether the *script* reaches the same one on
the same bytes.

The subtle case is `test_the_promoted_head_was_clean_and_that_was_luck`. `2900fce5`'s
verdict is honest, and the only reason is that it was the last push of the night. A
promoter's defence should not depend on nobody else pushing — which is exactly why this
runs in the pre-flight rather than living in a runbook paragraph.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.verify_e2e_attribution import Job, classify  # noqa: E402


def job(sha: str, name: str, started: str, completed: str, conclusion: str | None) -> Job:
    return Job(
        run_id=abs(hash((sha, name))) % 10**9,
        head_sha=sha,
        name=name,
        started_at=started,
        completed_at=completed,
        conclusion=conclusion,
    )


MUT = "deploy-staging"
A = "1da0c21b6d3f4a5e8c7b9a0d2e4f6a8b0c1d2e3f"  # overtaken; its e2e graded B's build
B = "87da585f0e1d2c3b4a5968778695a4b3c2d1e0f9"  # overtook A; its own e2e was cancelled

#: core#876's original report, re-derived from the jobs API.
ORIGINAL_INCIDENT = [
    job(A, "deploy-staging", "2026-08-31T22:08:50Z", "2026-08-31T22:12:44Z", "success"),
    job(B, "deploy-staging", "2026-08-31T22:12:46Z", "2026-08-31T22:16:48Z", "success"),
    job(A, "e2e-staging", "2026-08-31T22:16:50Z", "2026-08-31T22:24:05Z", "failure"),
    job(B, "e2e-staging", "2026-08-31T22:19:21Z", "2026-08-31T22:19:21Z", "cancelled"),
]

C = "5887ba99aa11bb22cc33dd44ee55ff6600112233"
D = "36f57991ffeeddccbbaa99887766554433221100"

#: The second pair, found inside the batch that was promoted as PR #881.
PROMOTED_BATCH = [
    job(C, "deploy-staging", "2026-08-31T22:33:46Z", "2026-08-31T22:45:52Z", "success"),
    job(C, "smoke-staging", "2026-08-31T22:45:53Z", "2026-08-31T22:45:53Z", "cancelled"),
    job(D, "deploy-staging", "2026-08-31T22:45:56Z", "2026-08-31T22:49:34Z", "success"),
    job(D, "e2e-staging", "2026-08-31T22:49:35Z", "2026-08-31T22:49:35Z", "cancelled"),
    job(C, "e2e-staging", "2026-08-31T22:49:36Z", "2026-08-31T22:53:56Z", "success"),
    job(D, "smoke-staging", "2026-08-31T22:53:59Z", "2026-08-31T22:54:37Z", "success"),
]

E = "2900fce5aabbccddeeff00112233445566778899"

#: The head actually promoted. Three seconds between its deploy and its E2E, and nothing
#: followed it.
CLEAN_HEAD = [
    job(E, "deploy-staging", "2026-08-31T23:23:04Z", "2026-08-31T23:27:25Z", "success"),
    job(E, "smoke-staging", "2026-08-31T23:27:28Z", "2026-08-31T23:27:57Z", "success"),
    job(E, "e2e-staging", "2026-08-31T23:27:59Z", "2026-08-31T23:32:06Z", "success"),
    job(E, "e2e-sso", "2026-08-31T23:28:02Z", "2026-08-31T23:41:00Z", "success"),
]


def verdicts(jobs: list[Job], sha: str) -> dict[str, str]:
    return {name: f["verdict"] for name, f in classify(jobs, sha)["jobs"].items()}


# ── the measured incidents ──────────────────────────────────────────────────────────────


def test_the_original_incident_is_refused() -> None:
    """A's e2e ran two seconds after B's deploy. Its result describes B."""
    result = classify(ORIGINAL_INCIDENT, A)
    assert result["trustworthy"] is False
    assert verdicts(ORIGINAL_INCIDENT, A)["e2e-staging"] == "misattributed"
    assert "87da585" in result["jobs"]["e2e-staging"]["detail"]


def test_the_attribution_is_crossed_not_merely_shifted() -> None:
    """B's build was graded under A's name, and B itself ended with no reading at all.

    Both halves have to be visible. A check that only protects the reader of a green
    leaves the commit that was actually under test with nothing, silently.
    """
    assert verdicts(ORIGINAL_INCIDENT, B)["e2e-staging"] == "no_reading"
    assert classify(ORIGINAL_INCIDENT, B)["trustworthy"] is False


def test_the_second_pair_inside_the_promoted_batch_is_refused() -> None:
    v = verdicts(PROMOTED_BATCH, C)
    assert v["e2e-staging"] == "misattributed", v
    assert v["smoke-staging"] == "no_reading", v


def test_a_job_that_happens_to_be_correct_is_still_reported_correctly() -> None:
    """D's `smoke-staging` ran after its own deploy and nothing followed — genuinely fine.

    An auditor that just says "this batch is dirty" would flag it. Per-job verdicts are the
    point: the promoter needs to know which readings survive.
    """
    assert verdicts(PROMOTED_BATCH, D)["smoke-staging"] == "attributed"
    # ...while D's own e2e was cancelled, so D still has no E2E reading.
    assert verdicts(PROMOTED_BATCH, D)["e2e-staging"] == "no_reading"
    assert classify(PROMOTED_BATCH, D)["trustworthy"] is False


def test_the_promoted_head_was_clean_and_that_was_luck() -> None:
    assert classify(CLEAN_HEAD, E)["trustworthy"] is True
    assert set(verdicts(CLEAN_HEAD, E).values()) == {"attributed"}


def test_the_same_head_stops_being_clean_the_moment_someone_else_pushes() -> None:
    """One extra deploy inside the window flips the verdict — nothing else changes.

    This is the whole argument for making the check structural: `2900fce5` was clean
    because it was the last push of the night, and that is not a property we control.
    """
    later = "aaaa1111bbbb2222cccc3333dddd4444eeee5555"
    intruder = job(
        later, "deploy-staging", "2026-08-31T23:27:26Z", "2026-08-31T23:27:58Z", "success"
    )
    assert classify([*CLEAN_HEAD, intruder], E)["trustworthy"] is False


# ── the shapes that must not read as clean ──────────────────────────────────────────────


def test_a_commit_with_no_staging_jobs_is_not_a_pass() -> None:
    assert set(verdicts(CLEAN_HEAD, "deadbeef" * 5).values()) == {"absent"}
    assert classify(CLEAN_HEAD, "deadbeef" * 5)["trustworthy"] is False


def test_a_verifier_with_no_successful_deploy_of_its_own_is_not_a_pass() -> None:
    jobs = [
        job(A, "deploy-staging", "2026-08-31T22:08:50Z", "2026-08-31T22:12:44Z", "failure"),
        job(A, "e2e-staging", "2026-08-31T22:16:50Z", "2026-08-31T22:24:05Z", "success"),
    ]
    assert verdicts(jobs, A)["e2e-staging"] == "no_deploy"


@pytest.mark.parametrize("conclusion", [None, "", "cancelled", "skipped"])
def test_every_non_reading_conclusion_is_refused(conclusion: str | None) -> None:
    """`cancelled` is the one that matters: it reads as absent, and absent skims past."""
    jobs = [
        job(A, "deploy-staging", "2026-08-31T22:08:50Z", "2026-08-31T22:12:44Z", "success"),
        job(A, "e2e-staging", "2026-08-31T22:16:50Z", "2026-08-31T22:24:05Z", conclusion),
    ]
    assert verdicts(jobs, A)["e2e-staging"] == "no_reading"


def test_an_overtaking_deploy_that_failed_does_not_count_as_overtaking() -> None:
    """A failed deploy does not replace the stack, so it does not steal the attribution.

    Without this the check would refuse honest verdicts, and a check that cries wolf is
    the fastest route to a promoter learning to skip it.
    """
    failed_intruder = job(
        B, "deploy-staging", "2026-08-31T22:12:46Z", "2026-08-31T22:16:48Z", "failure"
    )
    jobs = [j for j in ORIGINAL_INCIDENT if not (j.head_sha == B and j.name == MUT)] + [
        failed_intruder
    ]
    assert verdicts(jobs, A)["e2e-staging"] == "attributed"


def test_a_deploy_finishing_just_outside_the_window_is_not_overtaking() -> None:
    """Boundary control: one second after the verifier started is too late to matter."""
    late = job(B, MUT, "2026-08-31T22:12:46Z", "2026-08-31T22:16:51Z", "success")
    jobs = [j for j in ORIGINAL_INCIDENT if not (j.head_sha == B and j.name == MUT)] + [late]
    assert verdicts(jobs, A)["e2e-staging"] == "attributed"
    # ...and one second earlier it is, which is what makes the boundary real.
    on_time = job(B, MUT, "2026-08-31T22:12:46Z", "2026-08-31T22:16:50Z", "success")
    jobs = [j for j in ORIGINAL_INCIDENT if not (j.head_sha == B and j.name == MUT)] + [on_time]
    assert verdicts(jobs, A)["e2e-staging"] == "misattributed"
