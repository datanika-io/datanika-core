"""The graduation counter must exist somewhere other than a reviewer's head (core#1130).

`docs/QA_RULES.md` §10 says graduation is *"mechanical: 3 consecutive greens on `dev`"*. It was
not mechanical — nothing computed it. A human grepped the last N runs and counted. And the
sentence is underspecified in a way that decides the answer: **it never says what sequence
"consecutive" ranges over**, on a job whose run history is mostly runs that measured nothing.

Measured on `dev`, 2026-09-06, the nine completed `e2e-sso` runs after the SAML fix landed::

    17:23  e9e5b510  specs_failed  FAIL         1 failed, 3 skipped, 12 passed
    18:54  6a9a1d0d  wrong_build   UNMEASURED
    18:57  615cafe7  wrong_build   UNMEASURED
    19:01  88f707ac  wrong_build   UNMEASURED   3 skipped, 13 passed   <- SAML full flow PASSED
    19:13  be0bd9b9  no_verdict    UNMEASURED
    19:22  2e92cd6c  wrong_build   UNMEASURED   3 skipped, 13 passed   <- and here
    19:26  680ef967  wrong_build   UNMEASURED   3 skipped, 13 passed   <- and here
    19:32  51849052  cancelled     UNMEASURED
    19:38  df0c391c  clean         PASS         3 skipped, 13 passed   <- and here

**Seven of nine runs produced no reading at all**, and four runs in which every SSO spec passed
contribute exactly one green between them. Three readings of the same sentence:

===================  =========================================  ==================
reading              rule                                       on realistic data
===================  =========================================  ==================
calendar             3 *adjacent* runs, all green               cannot be satisfied
tally                3 greens *anywhere* in the window          already satisfied
measured             3 adjacent greens in the MEASURED subseq.  1 of 3
===================  =========================================  ==================

`tally` is the dangerous one: it graduates a tier off greens that were never adjacent, which is
the same defect as counting a phrase instead of the instruction. `calendar` is not conservative,
it is *unsatisfiable* — and an unsatisfiable bar gets lowered, which is how the tier policy's own
warning about loosening an assertion arrives from the other side.

This suite pins the `measured` reading and, more importantly, pins the two asymmetries that make
it honest:

* an **UNMEASURED** run is transparent — we know it carried no reading, so it neither advances
  nor resets;
* an **UNREADABLE** run breaks the streak — we do *not* know what it carried, and assuming it was
  not a red is the reassuring assumption.

Every predicate below is armed in-suite against a deliberately-wrong implementation. A guard
proved discriminating once by an external harness is a claim about a past session; an in-suite
arming runs every time CI does.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.e2e_tier_streak import (  # noqa: E402
    FAIL,
    INFORMATIONAL_VERDICTS,
    PASS,
    SSO_VERDICTS,
    UNMEASURED,
    UNREADABLE,
    VERDICT_CLASS,
    Reading,
    classify_verdict,
    parse_verdict_line,
    streak,
    verdict_states_in_workflow,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
CI_YML = REPO_ROOT / ".github" / "workflows" / "ci.yml"
STAGING_YML = REPO_ROOT / ".github" / "workflows" / "staging.yml"

SSO_JOB = "e2e-sso"
CLASSIFIER_STEP = "Classify what this job's result means"


# --------------------------------------------------------------------------------------
# 1. The streak itself
# --------------------------------------------------------------------------------------


class TestStreakSemantics:
    """`streak()` counts trailing greens over the MEASURED subsequence."""

    def test_three_adjacent_greens_graduate(self):
        assert streak([PASS, PASS, PASS]) == 3

    def test_a_red_resets_the_counter(self):
        # `ci.yml` already says a collision-induced red "RESETS the three-consecutive-greens
        # counter". That sentence only means anything if the count is TRAILING.
        assert streak([PASS, PASS, FAIL]) == 0

    def test_a_red_before_the_greens_does_not_reset_them(self):
        assert streak([FAIL, PASS, PASS, PASS]) == 3

    def test_unmeasured_runs_are_transparent(self):
        # The whole point. `wrong_build` says "this run cannot report on this commit" — it is
        # not a green and it is not a red, and resetting on it makes the bar unsatisfiable at
        # the measured 7-in-9 unmeasured rate.
        assert streak([PASS, UNMEASURED, PASS, UNMEASURED, PASS]) == 3

    def test_unmeasured_runs_do_not_advance_the_counter(self):
        assert streak([UNMEASURED, UNMEASURED, UNMEASURED]) == 0

    def test_a_red_still_resets_through_unmeasured_runs(self):
        # The transparency must not become a way for a red to fall out of the window.
        assert streak([PASS, PASS, FAIL, UNMEASURED, UNMEASURED]) == 0

    def test_unreadable_breaks_the_streak(self):
        # Asymmetric with UNMEASURED on purpose: we know an unmeasured run carried no reading,
        # and we do NOT know what an unreadable one carried. Treating it as transparent is
        # assuming it was not a red.
        assert streak([PASS, UNREADABLE, PASS, PASS]) == 2

    def test_the_empty_history_is_not_a_graduation(self):
        assert streak([]) == 0

    def test_the_measured_sequence_from_2026_09_06_is_one_of_three(self):
        """The real sequence, oldest -> newest. Not a fixture I invented."""
        measured = [
            FAIL,  # e9e5b510 specs_failed
            UNMEASURED,  # 6a9a1d0d wrong_build
            UNMEASURED,  # 615cafe7 wrong_build
            UNMEASURED,  # 88f707ac wrong_build   (SAML full flow passed)
            UNMEASURED,  # be0bd9b9 no_verdict
            UNMEASURED,  # 2e92cd6c wrong_build   (passed)
            UNMEASURED,  # 680ef967 wrong_build   (passed)
            UNMEASURED,  # 51849052 cancelled
            PASS,  # df0c391c clean               (passed)
        ]
        assert streak(measured) == 1, (
            "The SSO tier had exactly ONE green it could attribute to its own commit on "
            "2026-09-06, from four runs in which every SSO spec passed."
        )


class TestStreakArming:
    """Each wrong implementation a reader might reach for, shown red against these tests."""

    @staticmethod
    def _tally(classes: list[str]) -> int:
        """WRONG: counts greens anywhere. The reading that graduates on non-adjacent runs."""
        return sum(1 for c in classes if c == PASS)

    @staticmethod
    def _calendar(classes: list[str]) -> int:
        """WRONG: trailing greens over CALENDAR runs. Unsatisfiable at the measured rate."""
        n = 0
        for c in reversed(classes):
            if c != PASS:
                break
            n += 1
        return n

    @staticmethod
    def _unreadable_is_transparent(classes: list[str]) -> int:
        """WRONG: treats an unreadable run as if we knew it was not a red."""
        n = 0
        for c in reversed(classes):
            if c in (UNMEASURED, UNREADABLE):
                continue
            if c != PASS:
                break
            n += 1
        return n

    def test_the_tally_reading_is_distinguishable(self):
        seq = [PASS, FAIL, PASS, PASS]
        assert self._tally(seq) == 3, "arming: the tally reading would graduate here"
        assert streak(seq) == 2, "and the shipped rule must not"

    def test_the_calendar_reading_is_distinguishable(self):
        seq = [PASS, UNMEASURED, PASS, UNMEASURED, PASS]
        assert self._calendar(seq) == 1, "arming: calendar sees one trailing green"
        assert streak(seq) == 3, "and the shipped rule sees the three measured ones"

    def test_the_unreadable_transparency_is_distinguishable(self):
        seq = [PASS, PASS, UNREADABLE, PASS]
        assert self._unreadable_is_transparent(seq) == 3, "arming: the reassuring reading"
        assert streak(seq) == 1, "and the shipped rule stops at the run it cannot read"

    def test_control_all_three_wrong_readings_agree_with_the_right_one_on_a_clean_run(self):
        """Anti-vacuity: the armings above must not be red on *everything*.

        A discriminator that disagrees with the correct implementation on every input is
        not discriminating, it is broken — and it would make the three tests above pass for
        the wrong reason.
        """
        seq = [PASS, PASS, PASS]
        assert self._tally(seq) == 3
        assert self._calendar(seq) == 3
        assert self._unreadable_is_transparent(seq) == 3
        assert streak(seq) == 3


# --------------------------------------------------------------------------------------
# 2. The verdict vocabulary is DERIVED from the workflow, not restated here
# --------------------------------------------------------------------------------------


def _classifier_run_block() -> str:
    """The real `run:` body of `e2e-sso`'s classifier step."""
    doc = yaml.safe_load(CI_YML.read_text(encoding="utf-8"))
    job = doc["jobs"].get(SSO_JOB)
    if job is None:
        pytest.fail(f"{CI_YML} has no `{SSO_JOB}` job — this guard is pointed at nothing.")
    for step in job.get("steps", []):
        if step.get("name") == CLASSIFIER_STEP:
            return step["run"]
    pytest.fail(
        f"`{SSO_JOB}` has no step named {CLASSIFIER_STEP!r}. If it was renamed, rename it here "
        "too — do not delete this guard, it is the only thing deriving the verdict vocabulary."
    )
    raise AssertionError("unreachable")  # pragma: no cover


class TestVerdictVocabularyIsDerived:
    def test_control_every_verdict_this_script_classifies_is_still_in_the_workflow(self):
        """Anti-vacuity, and it must be DERIVED rather than a floor.

        The first version of this control asserted `len(states) >= 5`. The classifier emits
        **six**, so renaming one `STATE=` assignment left five and the control stayed green —
        it tolerated precisely the regression it existed to catch. Measured, not reasoned:
        arm 4 of `probe-1130-streak-guard-arming.py` declared RED and got GREEN.

        A number I chose is not a control. Comparing the two artifacts is.
        """
        states = verdict_states_in_workflow(_classifier_run_block())
        missing = sorted(set(SSO_VERDICTS) - states)
        assert not missing, (
            f"`scripts/e2e_tier_streak.py` classifies {missing}, which `{SSO_JOB}`'s "
            "classifier no longer emits. Either the scanner has stopped seeing the file, or "
            "a verdict state was removed and this mapping still claims it."
        )

    def test_every_state_the_classifier_can_emit_is_classified(self):
        """A new verdict must be triaged deliberately, not absorbed as UNREADABLE.

        Treating an unknown token as UNREADABLE is the *safe* runtime direction — it blocks a
        streak rather than advancing one. But silence is still wrong: whoever adds a state
        knows whether it is a measurement and this test is where they say so.
        """
        states = verdict_states_in_workflow(_classifier_run_block())
        unclassified = sorted(s for s in states if s not in VERDICT_CLASS)
        assert not unclassified, (
            f"`{SSO_JOB}`'s classifier can emit {unclassified}, which "
            "`scripts/e2e_tier_streak.py` does not classify. Decide whether each is a "
            "measurement (PASS/FAIL) or not (UNMEASURED) and add it to VERDICT_CLASS."
        )

    def test_the_informational_tier_states_are_classified_too(self):
        """`staging.yml` emits a different vocabulary for the same policy. Both directions."""
        text = STAGING_YML.read_text(encoding="utf-8")
        emitted = set(re.findall(r"INFORMATIONAL_RESULT=([a-z_]+)", text))
        assert emitted, "found no INFORMATIONAL_RESULT lines — the scanner is pointed at nothing"

        unclassified = sorted(s for s in emitted if s not in VERDICT_CLASS)
        assert not unclassified, (
            f"staging.yml emits INFORMATIONAL_RESULT={unclassified}, unclassified by "
            "`scripts/e2e_tier_streak.py`."
        )
        # The anti-vacuity half, derived the same way as the SSO one above.
        missing = sorted(set(INFORMATIONAL_VERDICTS) - emitted)
        assert not missing, (
            f"this script classifies INFORMATIONAL_RESULT={missing}, which staging.yml no "
            "longer emits — the scanner has gone blind, or a state was retired."
        )

    def test_arming_an_unclassified_state_is_red(self):
        """The predicate above must actually fire. Mutate a synthetic classifier body."""
        synthetic = "if x; then\n  STATE=degraded\nelse\n  STATE=clean\nfi\n"
        states = verdict_states_in_workflow(synthetic)
        assert "degraded" in states
        assert sorted(s for s in states if s not in VERDICT_CLASS) == ["degraded"]

    def test_arming_the_scanner_ignores_a_comparison(self):
        """`[ "$X" = "success" ]` is a read, not an assignment. It must not enter the set."""
        synthetic = 'if [ "$JOB_STATUS" = "success" ]; then STATE=clean; fi\n'
        assert verdict_states_in_workflow(synthetic) == {"clean"}


# --------------------------------------------------------------------------------------
# 3. Reading a verdict out of a run log
# --------------------------------------------------------------------------------------


class TestParseVerdictLine:
    REAL = (
        "2026-09-06T20:13:41.9769882Z SSO specs outcome: success / job status: success / "
        "verdict: clean"
    )
    # The classifier's own source is echoed into the log inside the `##[group]Run` header,
    # colour-coded and with the variables UNexpanded. Matching that is matching the script,
    # not the run.
    SOURCE_ECHO = (
        '2026-09-06T20:13:41.9656146Z \x1b[36;1mecho "SSO specs outcome: $SPECS_OUTCOME / '
        'job status: $JOB_STATUS / verdict: $STATE"\x1b[0m'
    )

    def test_it_reads_the_executed_line(self):
        assert parse_verdict_line([self.REAL]) == "clean"

    def test_it_ignores_the_echoed_script_source(self):
        assert parse_verdict_line([self.SOURCE_ECHO]) is None

    def test_the_executed_line_wins_over_the_echo(self):
        assert parse_verdict_line([self.SOURCE_ECHO, self.REAL]) == "clean"

    def test_an_empty_log_reads_as_no_verdict_not_as_a_pass(self):
        """A cancelled job's log is 0 bytes. Zero failure lines is what a clean run and a
        dead reader have in common (landing#505)."""
        assert parse_verdict_line([]) is None

    def test_a_log_without_the_line_reads_as_none(self):
        assert parse_verdict_line(["some unrelated output", "Running 16 tests"]) is None

    def test_it_reads_the_informational_result_line_too(self):
        assert parse_verdict_line(["2026-01-01T00:00:00Z INFORMATIONAL_RESULT=success"]) == (
            "success"
        )

    def test_it_ignores_the_informational_line_inside_its_own_echo(self):
        echoed = '2026-01-01T00:00:00Z \x1b[36;1m  echo "INFORMATIONAL_RESULT=success"\x1b[0m'
        assert parse_verdict_line([echoed]) is None


class TestClassifyVerdict:
    @pytest.mark.parametrize(
        ("verdict", "expected"),
        [
            ("clean", PASS),
            ("infra_only", PASS),  # specs ran and passed; the job is red for a non-test reason
            ("success", PASS),
            ("specs_failed", FAIL),
            ("failure", FAIL),
            ("wrong_build", UNMEASURED),
            ("no_verdict", UNMEASURED),
            ("cancelled", UNMEASURED),
            ("empty", UNMEASURED),
            ("unknown", UNMEASURED),
        ],
    )
    def test_the_known_vocabulary(self, verdict, expected):
        assert classify_verdict(verdict) is expected

    def test_an_unrecognised_verdict_is_unreadable_not_a_pass(self):
        assert classify_verdict("something_new") is UNREADABLE

    def test_none_is_unreadable(self):
        assert classify_verdict(None) is UNREADABLE

    def test_wrong_build_is_not_a_pass_even_though_the_specs_passed(self):
        """The four `wrong_build` runs on 2026-09-06 each carried `13 passed`.

        The classifier's own message says it: *"a green here would not have been this
        commit's green either."* A rule that counted them would have graduated the tier off
        runs that graded somebody else's build.
        """
        assert classify_verdict("wrong_build") is not PASS


# --------------------------------------------------------------------------------------
# 4. The report refuses to answer from too little data
# --------------------------------------------------------------------------------------


class TestReadingRefusesToOverclaim:
    def test_it_reports_no_data_rather_than_not_graduated(self):
        """`not graduated` from an empty read and from a real read are the same words.

        The safe direction here happens to be the same either way, but the *reason* must be
        legible: a promoter who reads "not graduated" and does not know the window was empty
        will go looking for a red that does not exist.
        """
        r = Reading.from_classes([], required=3)
        assert r.state == "no-data"
        assert not r.graduated

    def test_a_window_shorter_than_the_bar_cannot_graduate(self):
        r = Reading.from_classes([PASS, PASS], required=3)
        assert r.state == "no-data"

    def test_three_measured_greens_graduate(self):
        r = Reading.from_classes([PASS, PASS, PASS], required=3)
        assert r.graduated
        assert r.state == "graduate"

    def test_a_sparse_streak_is_flagged_rather_than_silently_graduating(self):
        """Three greens drawn from a window that measured almost nothing is a judgement.

        This is deliberately a third state and not a silent pass: the same three greens can
        mean "stable for three runs" or "the only three readings we got in a fortnight", and
        those call for different decisions by a human.
        """
        seq = [PASS] + [UNMEASURED] * 12 + [PASS, UNMEASURED, PASS]
        r = Reading.from_classes(seq, required=3)
        assert r.streak == 3
        assert r.state == "sparse"
        assert not r.graduated, "a sparse streak must not graduate on its own"

    def test_a_dense_streak_is_not_flagged_sparse(self):
        """Anti-vacuity control for the sparseness test above."""
        r = Reading.from_classes([UNMEASURED, PASS, PASS, PASS], required=3)
        assert r.state == "graduate"
        assert r.graduated

    def test_the_measurement_rate_is_reported(self):
        r = Reading.from_classes([FAIL] + [UNMEASURED] * 7 + [PASS], required=3)
        assert r.measured == 2
        assert r.total == 9
        assert r.state == "not-yet"
