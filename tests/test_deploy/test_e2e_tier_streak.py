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
    GATING_VERDICTS,
    INFORMATIONAL_VERDICTS,
    PASS,
    SSO_VERDICTS,
    UNMEASURED,
    UNREADABLE,
    VERDICT_CLASS,
    AmbiguousVerdictError,
    Reading,
    classify_verdict,
    parse_specs_outcome,
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


# --------------------------------------------------------------------------------------
# 5. `wrong_build` is a verdict about ATTRIBUTION, not about the specs (core#1151)
# --------------------------------------------------------------------------------------


class TestWrongBuildCanHideASpecFailure:
    """The bug this class exists for was in the first version of this module.

    `ci.yml`'s classifier tests attribution **before** spec outcome, deliberately::

        elif [ "$ATTRIB_OUTCOME" = "failure" ] || [ "$ATTRIB_POST_OUTCOME" = "failure" ]; then
          STATE=wrong_build
        elif [ "$SPECS_OUTCOME" = "failure" ]; then
          STATE=specs_failed

    — *"EITHER attribution check failing means this run cannot honestly report on this commit,
    whichever way the specs went."* Correct for the classifier, and it means `wrong_build`
    conflates three different spec outcomes. Measured on the four runs between the last two
    greens of 2026-09-06: two `success`, two `skipped`, and nothing stopping a `failure`.

    Treating `wrong_build` as transparent was therefore reading a verdict that records
    **attribution** as a claim about **spec outcome** — `ENGINEERING_RULES` §39.
    """

    REAL_WRONG_BUILD = (
        "2026-09-06T21:39:34Z SSO specs outcome: success / job status: failure / "
        "verdict: wrong_build"
    )
    # No such run exists in the current window, so this is synthesised -- and it is exactly
    # the case the fix is for. The control below proves it changes the VERDICT, not just the
    # parse, which is what separates a real arm from a satisfied regex.
    SYNTH_FAILED_SPECS = (
        "2026-09-06T21:39:34Z SSO specs outcome: failure / job status: failure / "
        "verdict: wrong_build"
    )

    def test_the_specs_outcome_is_recoverable_from_the_same_line(self):
        assert parse_specs_outcome([self.REAL_WRONG_BUILD]) == "success"
        assert parse_specs_outcome([self.SYNTH_FAILED_SPECS]) == "failure"

    def test_control_it_ignores_the_echoed_script_source(self):
        echoed = (
            '2026-09-06T21:39:34Z \x1b[36;1mecho "SSO specs outcome: $SPECS_OUTCOME / '
            'job status: $JOB_STATUS / verdict: $STATE"\x1b[0m'
        )
        assert parse_specs_outcome([echoed]) is None

    def test_wrong_build_with_passing_specs_is_still_transparent(self):
        """The common case must not change — seven of ten runs were unmeasured that day, and
        resetting on all of them makes the bar unsatisfiable."""
        assert classify_verdict("wrong_build", "success") is UNMEASURED

    def test_wrong_build_with_skipped_specs_is_still_transparent(self):
        assert classify_verdict("wrong_build", "skipped") is UNMEASURED

    def test_wrong_build_with_failing_specs_breaks_the_streak(self):
        """The defect. For a graduation question the property is stability, so specs failing
        anywhere in the window resets it — even when the failure is not attributable here."""
        assert classify_verdict("wrong_build", "failure") is FAIL

    def test_an_absent_specs_outcome_falls_back_to_the_verdict_alone(self):
        """`INFORMATIONAL_RESULT=` lines carry no specs field. They must keep working."""
        assert classify_verdict("wrong_build", None) is UNMEASURED
        assert classify_verdict("success", None) is PASS

    def test_the_streak_now_resets_through_a_hidden_failure(self):
        seq_before_fix = [PASS, UNMEASURED, PASS, PASS]
        assert streak(seq_before_fix) == 3, "arming: transparent, so the streak sails through"

        # the same history with the middle run correctly classified
        seq_after_fix = [PASS, FAIL, PASS, PASS]
        assert streak(seq_after_fix) == 2, "and the fix stops it at the red"

    def test_control_the_2026_09_06_graduate_is_not_affected(self):
        """Anti-regression on the live reading: none of that window's `wrong_build` runs had
        failing specs (measured — two `success`, two `skipped`), so the fix must leave the
        real verdict alone. A fix that changes an honest reading is a different bug."""
        measured = [
            PASS,  # df0c391c clean
            UNMEASURED,  # 5726b8fa no_verdict   (specs skipped)
            UNMEASURED,  # 908307c0 wrong_build  (specs success)
            UNMEASURED,  # ebb268a3 wrong_build  (specs skipped)
            UNMEASURED,  # 61f194a1 wrong_build  (specs success)
            PASS,  # b3f9761d clean
        ]
        assert streak([PASS, *measured]) == 3

    # -- the `cancelled` half of the same predicate (it was shipped untested) --------------
    #
    # `classify_verdict` covers ("wrong_build", "cancelled"), but only `wrong_build` had a
    # test. An untested clause in a predicate written to catch untested clauses is its own
    # joke; worse, a later "simplification" down to `wrong_build` alone would have gone green.
    #
    # Reachable, not hypothetical: ci.yml's classifier runs under `if: always()`, which fires
    # on cancellation, and a step that failed BEFORE the cancellation keeps `outcome: failure`.
    # So `outcome: failure / job status: cancelled / verdict: cancelled` is a line the runner
    # can really emit -- STATE=cancelled wins because it is tested first.

    REAL_SHAPE_CANCELLED = (
        "2026-09-06T21:39:34Z SSO specs outcome: failure / job status: cancelled / "
        "verdict: cancelled"
    )

    def test_cancelled_carrying_a_failure_also_breaks_the_streak(self):
        lines = [self.REAL_SHAPE_CANCELLED]
        assert parse_specs_outcome(lines) == "failure"
        assert parse_verdict_line(lines) == "cancelled"
        assert classify_verdict("cancelled", "failure") is FAIL

    def test_control_a_plain_cancellation_stays_transparent(self):
        """The common case: cancelled before the specs ran, or cancelled while green. A
        cancelled job is neither green nor red (`WORKFLOW_RULES`), so it must not reset."""
        assert classify_verdict("cancelled", "success") is UNMEASURED
        assert classify_verdict("cancelled", "skipped") is UNMEASURED
        assert classify_verdict("cancelled", "cancelled") is UNMEASURED
        assert classify_verdict("cancelled", None) is UNMEASURED


# --------------------------------------------------------------------------------------
# 6. `sparse` grades DILUTION, never LENGTH (core#1154)
# --------------------------------------------------------------------------------------


class TestSparsenessGradesDilutionNotLength:
    """`span` grows for two opposite reasons and the shipped predicate could not tell them apart.

    A streak reaches further back either because unmeasured runs sit *between* the greens (it is
    diluted -- what `sparse` means) or because there are simply *many* consecutive greens (the
    opposite). Grading on `span` therefore penalised a tier for getting healthier.

    🔑 The two tests this class replaces were both real and both discriminating -- and both held
    ``n = 3`` and varied only ``span``. A predicate with two inputs was armed against one of
    them, so every defect living in the interaction was invisible. The property that catches it
    is monotonicity, and it cannot be stated at a fixed streak length.
    """

    def test_a_dense_streak_of_any_length_graduates(self):
        """The measured case: `e2e-staging` on `dev`, 14 consecutive greens, nothing unmeasured.

        This is the densest evidence the instrument can receive. It returned `sparse` beside the
        sentence "drawn from a window that measured almost nothing", which was false.
        """
        r = Reading.from_classes([PASS] * 14, required=3)
        assert r.measured == 14
        assert r.gaps == 0, "no unmeasured run sits inside this window"
        assert r.state == "graduate"

    def test_a_streak_longer_than_the_gap_budget_is_still_graduatable(self):
        """Under `span > max_span` a streak of 11+ could never graduate however dense it was.

        The budget for unmeasured runs was `max_span - n`, which shrinks as the tier improves
        and goes negative at `n = 11`. That is not a threshold, it is a ceiling on health.
        """
        for n in (11, 14, 40):
            r = Reading.from_classes([PASS] * n, required=3)
            assert r.state == "graduate", f"a perfect streak of {n} must be able to graduate"

    def test_appending_a_green_never_moves_the_verdict_away_from_graduation(self):
        """AC3 -- monotonicity, and it is the property the fixed-`n` tests could not state.

        Measured on the real `e2e-sso` history: at 14 runs read the verdict was `graduate`; at
        15 it was `sparse`. The tier had gained a sixth consecutive green.
        """
        history = [
            UNMEASURED, UNMEASURED, UNMEASURED, UNMEASURED, PASS, PASS,
            UNMEASURED, UNMEASURED, UNMEASURED, UNMEASURED, PASS,
            UNMEASURED, PASS, PASS, PASS,
        ]  # fmt: skip
        seen_graduate = False
        for k in range(3, len(history) + 1):
            state = Reading.from_classes(history[:k], required=3).state
            if state == "graduate":
                seen_graduate = True
            elif seen_graduate:
                assert state != "sparse", (
                    f"verdict went graduate -> sparse at {k} runs read; the only thing that "
                    "changed is that the tier gained a green"
                )

    def test_a_genuinely_diluted_streak_is_still_refused(self):
        """Anti-vacuity control. The fix must not simply make everything graduate.

        `sparse` exists for "three greens across a fortnight"; that case must stay refused.
        """
        seq = [PASS] + [UNMEASURED] * 12 + [PASS, UNMEASURED, PASS]
        r = Reading.from_classes(seq, required=3)
        assert r.streak == 3
        assert r.gaps == 13
        assert r.state == "sparse"
        assert not r.graduated, "a diluted streak must not graduate on its own"

    def test_the_gap_threshold_discriminates_in_both_directions(self):
        """A threshold narrowed until it matches nothing also stops matching real dilution."""
        just_under = [PASS] + [UNMEASURED] * 7 + [PASS, PASS]
        just_over = [PASS] + [UNMEASURED] * 8 + [PASS, PASS]
        assert Reading.from_classes(just_under, required=3).state == "graduate"
        assert Reading.from_classes(just_over, required=3).state == "sparse"

    def test_the_gap_count_is_reported_so_the_two_cases_are_distinguishable(self):
        """AC4 -- `sparse` on 0 gaps and `sparse` on 13 printed the same word.

        A reader could not tell a dense streak that tripped the length ceiling from a genuinely
        diluted one. The gap count is the field that records the property the state is about.
        """
        dense = Reading.from_classes([PASS] * 14, required=3)
        diluted = Reading.from_classes([PASS] + [UNMEASURED] * 12 + [PASS, PASS], required=3)
        assert dense.gaps == 0
        assert diluted.gaps == 12
        assert dense.gaps != diluted.gaps

    def test_a_red_inside_the_window_still_resets_regardless_of_gaps(self):
        """Control: the gap rule must not rescue a streak that a FAIL has broken."""
        r = Reading.from_classes([PASS, PASS, FAIL, PASS, PASS], required=3)
        assert r.streak == 2
        assert r.state == "not-yet"

    def test_an_unreadable_run_still_breaks_the_streak_and_is_not_a_gap(self):
        """`UNMEASURED` is transparent; `UNREADABLE` is not, and must not be counted as a gap."""
        r = Reading.from_classes([PASS, PASS, UNREADABLE, PASS, PASS], required=3)
        assert r.streak == 2
        assert r.state == "not-yet"


# --------------------------------------------------------------------------------------
# 7. A verdict belongs to a TIER, and the caller must name it (core#1205)
# --------------------------------------------------------------------------------------


class TestTheVerdictIsReadPerTier:
    """`parse_verdict_line` used to try every pattern and return the last that matched.

    On an `e2e-staging` log that silently answered a question nobody asked. There was no
    gating pattern at all, so the scan fell through to `INFORMATIONAL_RESULT=` and reported
    the **informational, explicitly non-gating** tier as the gating verdict — to a promotion
    pre-flight, which printed `conclusion=success, verdict=FAIL` for a run whose own log said
    `gating step outcome: success / job status: success / verdict: clean`, 21 of 21 steps green.

    🔑 88 tests passed before and after the fix. That is the tell worth keeping: a suite that
    reads identically on both sides of a real defect did not cover it. These are the ones that
    would have.
    """

    #: A staging log carries BOTH tiers. That is the whole difficulty.
    STAGING = [
        "gating step outcome: failure / job status: failure / verdict: gating_failed",
        "INFORMATIONAL_RESULT=unknown",
    ]
    SSO_ONLY = ["SSO specs outcome: success / job status: success / verdict: clean"]

    def test_the_gating_tier_is_readable_at_all(self) -> None:
        """The pattern that did not exist. Without it every question about gating fell through."""
        assert parse_verdict_line(self.STAGING, tier="gating") == "gating_failed"

    def test_the_informational_tier_is_read_separately(self) -> None:
        assert parse_verdict_line(self.STAGING, tier="informational") == "unknown"

    def test_the_two_tiers_disagree_on_this_log_which_is_why_the_bug_existed(self) -> None:
        """The regression, stated as the property: on a real staging log the two tiers return
        DIFFERENT verdicts, so picking one silently is picking wrong half the time."""
        gating = parse_verdict_line(self.STAGING, tier="gating")
        info = parse_verdict_line(self.STAGING, tier="informational")
        assert gating != info
        assert VERDICT_CLASS[gating] == FAIL
        assert VERDICT_CLASS[info] == UNMEASURED

    def test_auto_refuses_a_log_carrying_two_tiers_rather_than_guessing(self) -> None:
        """The fix. Guessing is what core#1205 was; raising is what it becomes."""
        with pytest.raises(AmbiguousVerdictError, match="no tier was named"):
            parse_verdict_line(self.STAGING)

    def test_auto_still_works_where_it_is_unambiguous(self) -> None:
        """Measured before relying on it: `e2e-sso` logs carry **0** `INFORMATIONAL_RESULT=`
        lines (against 5 in a staging log), so an SSO log cannot be ambiguous and existing
        callers keep working."""
        assert parse_verdict_line(self.SSO_ONLY) == "clean"

    def test_an_unknown_tier_is_an_error_not_a_silent_none(self) -> None:
        """A typo'd tier returning `None` would read as 'this job printed no verdict'."""
        with pytest.raises(ValueError, match="unknown tier"):
            parse_verdict_line(self.STAGING, tier="informationnal")

    def test_control_a_log_with_neither_line_is_still_none(self) -> None:
        assert parse_verdict_line(["nothing to see"], tier="gating") is None


class TestTheGatingVocabularyIsCoupledToTheWorkflow:
    """Same two-way coupling the SSO and informational vocabularies already have.

    A third vocabulary added without it would be exactly the gap that produced core#1205: a map
    that looks like coverage and drifts from the classifier it claims to describe.
    """

    @staticmethod
    def _gating_states() -> set[str]:
        text = STAGING_YML.read_text(encoding="utf-8")
        return verdict_states_in_workflow(text)

    def test_every_state_the_gating_classifier_emits_is_classified(self) -> None:
        emitted = self._gating_states()
        known = set(VERDICT_CLASS)
        unclassified = emitted - known
        assert not unclassified, (
            f"`staging.yml` can emit {sorted(unclassified)}, which `e2e_tier_streak.py` does "
            "not classify — an unknown verdict reads as UNREADABLE and blocks a streak."
        )

    def test_every_gating_verdict_this_script_knows_is_still_in_the_workflow(self) -> None:
        """The backward direction, and the one that catches a vocabulary outliving its emitter."""
        emitted = self._gating_states()
        stale = set(GATING_VERDICTS) - emitted
        assert not stale, (
            f"`GATING_VERDICTS` names {sorted(stale)}, which `staging.yml` no longer emits."
        )

    def test_control_the_workflow_scan_is_not_empty(self) -> None:
        """An empty scan would make both assertions above vacuously true."""
        assert len(self._gating_states()) >= 4, (
            f"only {len(self._gating_states())} STATE token(s) found in staging.yml"
        )

    def test_the_gating_vocabulary_differs_from_the_sso_one_where_it_should(self) -> None:
        """They are near-identical, which is why copying one for the other is tempting and
        wrong: a spec failure is `gating_failed` here and `specs_failed` there."""
        assert "gating_failed" in GATING_VERDICTS
        assert "gating_failed" not in SSO_VERDICTS
        assert "specs_failed" in SSO_VERDICTS
        assert "specs_failed" not in GATING_VERDICTS
