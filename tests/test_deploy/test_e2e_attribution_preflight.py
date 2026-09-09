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


# ── the core#975 rename ──────────────────────────────────────────────────────────────


def test_the_composed_check_name_still_resolves_to_the_job() -> None:
    """core#975: a called workflow's check runs are `<caller job id> / <callee job id>`.

    This script's whole function is to look staging verdicts up BY NAME, so the rename
    lands directly on it. If `short_name` ever stops stripping the prefix, every lookup
    misses and the tool reports `absent` for all three jobs — which reads as *"CI has not
    run yet"*, the one outcome a promoter waits on rather than investigates.

    Both directions are asserted: the composed form must resolve, and the bare form must
    keep resolving, because runs that predate the move are exactly the evidence someone
    reaches for when something looks wrong.
    """
    from scripts.verify_e2e_attribution import MUTATION, VERIFIERS, short_name

    for bare in (MUTATION, *VERIFIERS):
        assert short_name(bare) == bare, f"a bare name must survive unchanged: {bare}"
        assert short_name(f"staging / {bare}") == bare, f"composed form not stripped: {bare}"

    # Nesting: the job that ran is the last segment, not the first.
    assert short_name("outer / staging / e2e-staging") == "e2e-staging"
    # And a name that merely contains a slash is not a composed name.
    assert short_name("build/push") == "build/push"


def test_an_unstripped_composed_name_would_be_dropped() -> None:
    """The control for the control: prove the filter really is name-sensitive.

    Without this, `test_the_composed_check_name_still_resolves_to_the_job` could pass
    against a filter that accepted anything, and the stripping would be decorative.
    """
    from scripts.verify_e2e_attribution import MUTATION, VERIFIERS

    assert "staging / e2e-staging" not in (MUTATION, *VERIFIERS)


# ── core#1174: a window nobody overtook is not the same as a reading ────────────────────
#
# `310137d0` on 2026-09-07. `e2e-sso` failed its own step 7 ("Assert staging is running THIS
# commit"), SKIPPED steps 8-15, ran ZERO SSO specs, and self-classified `wrong_build`.
# Nothing overtook its window, so the script reported `attributed` and exited 0 — an
# all-clear over a tier that had measured nothing, in the last thing a promoter reads before
# merging to `master`.
#
# The two readings were never contradicting: "was this window overtaken?" and "did this job
# produce a reading?" are different questions, and only the first was ever asked.

F = "310137d0df348f19c4db33ea62a4c44c1acc6ea1"

#: Real timings from run 34107792351. Note every job is `attributed` by the ORIGINAL rule —
#: that is the point of the fixture.
NO_READING_HEAD = [
    job(F, "deploy-staging", "2026-09-07T10:02:00Z", "2026-09-07T10:06:05Z", "success"),
    job(F, "smoke-staging", "2026-09-07T10:06:10Z", "2026-09-07T10:06:40Z", "success"),
    job(F, "e2e-staging", "2026-09-07T10:06:45Z", "2026-09-07T10:11:00Z", "success"),
    job(F, "e2e-sso", "2026-09-07T10:06:50Z", "2026-09-07T10:13:18Z", "failure"),
]


def test_the_old_rule_alone_calls_the_no_reading_head_clean() -> None:
    """The regression, stated as a test so nobody re-introduces it as a simplification.

    With no verdict classes supplied, every job is `attributed` — which is exactly what
    shipped and exactly what exited 0 over a tier that measured nothing.
    """
    assert verdicts(NO_READING_HEAD, F) == {
        "smoke-staging": "attributed",
        "e2e-staging": "attributed",
        "e2e-sso": "attributed",
    }
    assert classify(NO_READING_HEAD, F)["trustworthy"] is True


def test_a_job_that_produced_no_reading_is_refused() -> None:
    """core#1174: the fix. `wrong_build` classifies UNMEASURED, so the job did not grade."""
    result = classify(NO_READING_HEAD, F, {"e2e-sso": "UNMEASURED", "e2e-staging": "PASS"})
    assert result["jobs"]["e2e-sso"]["verdict"] == "no_verdict"
    assert result["trustworthy"] is False
    detail = result["jobs"]["e2e-sso"]["detail"]
    assert "NO reading" in detail, detail
    # The distinction is the whole finding: say it was not overtaken, or a reader repairs
    # the wrong thing by re-running a deploy that was never the problem.
    assert "not overtaken" in detail, detail


def test_the_two_verifiers_that_did_grade_stay_attributed() -> None:
    """Anti-over-fire. If a no_verdict on one tier condemned the others, the guard would red
    on every head where SSO is flaky and would be switched off within a week."""
    result = classify(NO_READING_HEAD, F, {"e2e-sso": "UNMEASURED", "e2e-staging": "PASS"})
    assert result["jobs"]["e2e-staging"]["verdict"] == "attributed"
    assert result["jobs"]["smoke-staging"]["verdict"] == "attributed"


@pytest.mark.parametrize("klass", ["UNMEASURED", "UNREADABLE"])
def test_both_no_reading_classes_refuse(klass: str) -> None:
    assert classify(NO_READING_HEAD, F, {"e2e-sso": klass})["jobs"]["e2e-sso"]["verdict"] == (
        "no_verdict"
    )


@pytest.mark.parametrize("klass", ["PASS", "FAIL"])
def test_a_real_reading_stays_attributed_even_when_it_is_red(klass: str) -> None:
    """FAIL is a READING, not an absence.

    A red that genuinely belongs to this commit is precisely what the promoter must see.
    Folding it into `no_verdict` would be this same defect pointed the other way — hiding a
    real failure behind a word that means "we do not know".
    """
    result = classify(NO_READING_HEAD, F, {"e2e-sso": klass})
    assert result["jobs"]["e2e-sso"]["verdict"] == "attributed"
    assert klass in result["jobs"]["e2e-sso"]["detail"]


def test_a_job_with_no_classifier_line_is_left_alone() -> None:
    """`smoke-staging` emits no verdict line. Measured: `verdict=<none>` on both a clean head
    and the broken one. Treating that absence as a failure would red every clean run, which
    is how a guard gets deleted — the polarity error measured three times on 2026-09-07."""
    result = classify(NO_READING_HEAD, F, {"e2e-sso": "UNMEASURED"})
    assert result["jobs"]["smoke-staging"]["verdict"] == "attributed"
    assert "<none>" in result["jobs"]["smoke-staging"]["detail"]


def test_the_clean_head_is_still_clean_with_classes_supplied() -> None:
    """The positive control. Without it, "refuses the broken head" is satisfied by a script
    that refuses everything."""
    classes = {"smoke-staging": "PASS", "e2e-staging": "PASS", "e2e-sso": "PASS"}
    assert classify(CLEAN_HEAD, E, classes)["trustworthy"] is True


def test_the_vocabulary_is_qas_and_not_a_second_definition() -> None:
    """Two independent definitions of `wrong_build` is how they drift apart — and QA fixed a
    real polarity defect in theirs on 2026-09-06 (`wrong_build` was transparent to the streak
    and could hide a spec failure). This asserts the import relationship, so a future edit
    cannot quietly re-derive the vocabulary here."""
    from scripts.e2e_tier_streak import UNMEASURED, UNREADABLE, VERDICT_CLASS
    from scripts.verify_e2e_attribution import NO_READING_CLASSES

    assert {UNMEASURED, UNREADABLE} == NO_READING_CLASSES
    # The token that caused this issue must classify as a non-reading in QA's map.
    assert VERDICT_CLASS["wrong_build"] == UNMEASURED
    assert VERDICT_CLASS["no_verdict"] == UNMEASURED
    # ...and a genuine reading must not.
    assert VERDICT_CLASS["clean"] != UNMEASURED


class TestTheCallerNamesItsTier:
    """core#1205, second half — the caller side of QA's parser fix.

    QA made `parse_verdict_line` refuse a two-tier log unless the caller names a
    tier. That is the correct shape. But this pre-flight was never updated, so it
    kept hitting the `auto` default and, the moment that fix landed, raised
    `AmbiguousVerdictError` on every `e2e-staging` log — the promotion gate crashed
    instead of reporting.

    🔑 The tier differs BY CALLER on the same log, and that is the whole lesson of
    #1205. `e2e_tier_streak.py` asks `informational`, because it measures whether
    that tier is stable enough to graduate. A promotion asks `gating`: *did the
    specs that gate a release pass?* Reading the other one is how a clean tier was
    reported to a promoter as FAIL.
    """

    def _source(self) -> str:
        return (
            Path(__file__).resolve().parents[2] / "scripts" / "verify_e2e_attribution.py"
        ).read_text(encoding="utf-8")

    def test_it_passes_a_tier_rather_than_relying_on_auto(self) -> None:
        src = self._source()
        assert "parse_verdict_line(lines, tier=" in src, (
            "the pre-flight calls parse_verdict_line without naming a tier; on a "
            "two-tier log that now raises AmbiguousVerdictError and the promotion "
            "gate crashes instead of reporting"
        )
        assert "parse_verdict_line(lines)" not in src, (
            "a bare parse_verdict_line(lines) call remains — that is the core#1205 shape"
        )

    def test_a_promotion_asks_the_gating_tier_not_the_informational_one(self) -> None:
        """The specific misread: informational is `continue-on-error` by design."""
        src = self._source()
        assert '"gating"' in src, (
            "the pre-flight does not ask for the gating tier. A promotion's question "
            "is whether the release-gating specs passed; the informational tier is "
            "continue-on-error and is red on essentially every run."
        )
        assert '"informational"' not in src, (
            "the pre-flight names the informational tier — that is the tier whose red "
            "was reported as a gating FAIL in core#1205"
        )

    def test_sso_logs_still_use_the_sso_tier(self) -> None:
        """e2e-sso emits no INFORMATIONAL_RESULT line, but naming it is still right."""
        assert '"sso" if "sso" in job.name' in self._source()
