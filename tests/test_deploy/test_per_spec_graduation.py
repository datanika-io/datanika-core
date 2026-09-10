"""Graduation is per SPEC; the signal was per TIER (core#1221).

`staging.yml` emitted one boolean for the whole informational tier while `docs/QA_RULES.md`
§10 graduates **per spec**. Both consequences are recorded history, not hypotheticals:

* `reflex-wire.spec.ts` had **seven** consecutive greens and read **zero**, because
  `a11y-sweep.spec.ts` arrived beside it and was legitimately red;
* while that spec stayed red, **no** spec in the tier could graduate — and §10 *requires* new
  specs to enter, so the churn that resets the counter is by design.

🔑 **A counter that resets whenever the thing it counts is added to is not a counter.**

⚠️ **This bears on core#1130's founder decision.** The `measured` reading was adopted on the
assumption that the streak measures what it claims. A per-tier signal means it did not, for
any tier holding more than one spec — the rule was sound and its *input* was not.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from scripts.e2e_tier_streak import (  # noqa: E402
    FAIL,
    INFORMATIONAL_SPEC_VERDICTS,
    LOCAL,
    PASS,
    UNMEASURED,
    UNREADABLE,
    Reading,
    classify_for_spec,
    parse_spec_verdicts,
    streak,
)

INCUMBENT = "reflex-wire.spec.ts"
NEWCOMER = "a11y-sweep.spec.ts"


def _emitter():
    """Load `e2e/scripts/informational_spec_results.py`, which is not an importable package."""
    path = REPO_ROOT / "e2e" / "scripts" / "informational_spec_results.py"
    spec = importlib.util.spec_from_file_location("informational_spec_results", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def emitter():
    return _emitter()


def _line(spec: str, verdict: str) -> str:
    return f"2026-09-10T09:00:00.0000000Z INFORMATIONAL_SPEC_RESULT={spec}:{verdict}"


# ── the defect, on the shape that produced it ────────────────────────────────────────────


class TestTheRecordedDefect:
    """Seven greens, then a newcomer arrives and is red. What does the incumbent read?"""

    @staticmethod
    def _history_per_tier() -> list[str]:
        """How it was graded: one boolean, so the newcomer's red is the incumbent's red."""
        return [PASS] * 7 + [FAIL] * 13

    @staticmethod
    def _history_per_spec() -> list[str]:
        """How it grades now: the incumbent is green in the runs where the newcomer failed."""
        per_run = [_line(INCUMBENT, "success"), _line(NEWCOMER, "failure")]
        parsed = parse_spec_verdicts(per_run)
        return [PASS] * 7 + [classify_for_spec(INCUMBENT, parsed, "failure")] * 13

    def test_the_tier_reading_destroyed_the_incumbents_streak(self) -> None:
        """The bug, stated as a measurement so the fix below has something to be better than."""
        assert streak(self._history_per_tier()) == 0
        assert Reading.from_classes(self._history_per_tier()).state == "not-yet"

    def test_the_per_spec_reading_keeps_it(self) -> None:
        """⚠️ I first asserted `== 7` here and it returned **20**. My assertion was wrong and
        the code was right, in the direction that matters: the incumbent passed in *every*
        one of those twenty runs, so its streak is the whole history — the newcomer's red was
        never its red at all. Recorded rather than quietly corrected, because "the number is
        better than I predicted" is the shape that gets rubber-stamped.
        """
        history = self._history_per_spec()
        assert streak(history) == 20, (
            "the incumbent passed in all 20 runs; the newcomer's failures were never its own"
        )
        assert Reading.from_classes(history).state == "graduate"

    def test_and_the_two_readings_actually_differ(self) -> None:
        """The control. If both readings agreed, neither test above would mean anything."""
        assert streak(self._history_per_tier()) != streak(self._history_per_spec())

    def test_the_newcomers_own_red_still_counts_against_the_newcomer(self) -> None:
        """The over-correction guard: per-spec must not make failures disappear."""
        parsed = parse_spec_verdicts([_line(INCUMBENT, "success"), _line(NEWCOMER, "failure")])
        assert classify_for_spec(NEWCOMER, parsed, "failure") == FAIL
        assert streak([PASS] * 7 + [FAIL]) == 0


# ── the asymmetry that makes the old logs readable ───────────────────────────────────────


class TestOlderLogsCarryOnlyTheTierLine:
    """A run predating the per-spec emitter carries one boolean. Its two directions do NOT
    carry the same information about one spec, and treating them symmetrically is the same
    defect one level down."""

    def test_a_green_tier_attributes_to_every_spec(self) -> None:
        """Sound: a green tier means every spec in it was green."""
        assert classify_for_spec(INCUMBENT, {}, "success") == PASS

    def test_a_red_tier_attributes_to_nothing_in_particular(self) -> None:
        """🔑 UNMEASURED, not FAIL. The line says something failed and cannot say what —
        grading it as this spec's failure is exactly how the incumbent lost seven greens."""
        assert classify_for_spec(INCUMBENT, {}, "failure") == UNMEASURED

    def test_an_empty_or_unknown_tier_is_unmeasured_as_before(self) -> None:
        for token in ("empty", "unknown"):
            assert classify_for_spec(INCUMBENT, {}, token) == UNMEASURED

    def test_an_unreadable_tier_line_stays_unreadable(self) -> None:
        assert classify_for_spec(INCUMBENT, {}, None) == UNREADABLE

    def test_the_surviving_streak_is_reported_sparse_rather_than_graduated(self) -> None:
        """🔑 The half that keeps this honest, and it fell out of core#1154 rather than
        being added here — which is the reason to trust it.

        Seven greens followed by thirteen unattributable runs is *"the streak is intact and
        nobody has measured it lately"*. That must not graduate on its own; a human decides.
        """
        history = [PASS] * 7 + [UNMEASURED] * 13
        reading = Reading.from_classes(history)
        assert reading.streak == 7
        assert reading.gaps == 13
        assert reading.state == "sparse", (
            f"got {reading.state}. A streak reaching back through 13 runs that measured "
            "nothing is not three greens across three runs."
        )


# ── a spec that did not run ──────────────────────────────────────────────────────────────


def test_a_spec_absent_from_a_run_is_unmeasured_not_failed() -> None:
    """It did not run, or did not exist yet. Neither is a failure."""
    parsed = parse_spec_verdicts([_line(NEWCOMER, "success")])
    assert classify_for_spec("did-not-exist.spec.ts", parsed, "success") == UNMEASURED


def test_a_report_with_no_specs_is_unmeasured_not_green() -> None:
    """`no_evidence` is what a crashed or misdirected run leaves behind."""
    parsed = parse_spec_verdicts([_line("<none>", "no_evidence")])
    assert parsed == {"<none>": "no_evidence"}
    assert INFORMATIONAL_SPEC_VERDICTS["no_evidence"] == UNMEASURED


def test_the_local_veto_still_wins_over_a_per_spec_green() -> None:
    """core#1232 composes with core#1221, and must: a per-spec green from a laptop is the
    false verdict with a build behind it that #1232 exists to refuse.

    Asserted at the vocabulary level here; `collect()` applies it before either classifier,
    which is the only place that can see both facts at once.
    """
    from scripts.e2e_tier_streak import CI_ENVIRONMENT, attested_environment

    log = ["2026-09-10T09:00:00.0000000Z E2E_ENVIRONMENT=local", _line(INCUMBENT, "success")]
    where = attested_environment(log)
    assert where is not None and where != CI_ENVIRONMENT
    # The per-spec line is present and green; the veto is what must decide.
    assert parse_spec_verdicts(log) == {INCUMBENT: "success"}
    assert LOCAL != PASS


# ── the emitter ──────────────────────────────────────────────────────────────────────────


class TestTheEmitter:
    @staticmethod
    def _report(files: dict[str, list[str]]) -> dict:
        """`{spec file: [test statuses]}` -> a Playwright json-reporter shaped report."""
        return {
            "suites": [
                {
                    "title": path,
                    "file": path,
                    "specs": [
                        {"title": f"t{i}", "tests": [{"status": st}]}
                        for i, st in enumerate(statuses)
                    ],
                }
                for path, statuses in files.items()
            ]
        }

    def test_one_failing_test_fails_its_whole_file(self, emitter) -> None:
        """Graduating half a file is not a thing §10 can express."""
        out = emitter.spec_results(self._report({NEWCOMER: ["expected", "unexpected"]}))
        assert out == {NEWCOMER: "failure"}

    def test_a_flaky_test_fails_its_file(self, emitter) -> None:
        """`flaky` exits 0 and produces a green run. The property a graduating spec has to
        demonstrate is stability, so for graduation it is a failure."""
        assert emitter.spec_results(self._report({NEWCOMER: ["flaky"]})) == {NEWCOMER: "failure"}

    def test_a_file_whose_every_test_skipped_produces_no_line(self, emitter) -> None:
        """A skip is in neither tier and counts toward nothing (QA_RULES §11). Silence, not
        a green — a green would let a permanently-skipped spec graduate."""
        assert emitter.spec_results(self._report({INCUMBENT: ["skipped", "skipped"]})) == {}

    def test_the_file_is_inherited_from_the_suite_when_the_spec_omits_it(self, emitter) -> None:
        """Both shapes appear in reports this repo has captured; guessing one would attribute
        a whole run to `<unknown>`."""
        report = {
            "suites": [
                {
                    "title": INCUMBENT,
                    "file": INCUMBENT,
                    "specs": [],
                    "suites": [
                        {
                            "title": "wire @informational",
                            "specs": [{"tests": [{"status": "expected"}]}],
                        }
                    ],
                }
            ]
        }
        assert emitter.spec_results(report) == {INCUMBENT: "success"}

    def test_render_is_deterministic_and_names_every_file(self, emitter) -> None:
        lines = emitter.render({NEWCOMER: "failure", INCUMBENT: "success"})
        assert lines == [
            f"INFORMATIONAL_SPEC_RESULT={NEWCOMER}:failure",
            f"INFORMATIONAL_SPEC_RESULT={INCUMBENT}:success",
        ]

    def test_an_empty_report_renders_no_evidence_not_nothing(self, emitter) -> None:
        """Printing nothing would read as *"the step did not run"*. It did; it found nothing."""
        assert emitter.render({}) == ["INFORMATIONAL_SPEC_RESULT=<none>:no_evidence"]

    def test_a_path_is_reduced_to_its_basename(self, emitter) -> None:
        assert emitter.basename("tests/reflex-wire.spec.ts") == INCUMBENT
        assert emitter.basename("tests\\\\reflex-wire.spec.ts") == INCUMBENT


# ── the two halves must agree about the vocabulary ───────────────────────────────────────


def test_every_verdict_the_emitter_can_produce_is_one_the_reader_knows(emitter) -> None:
    """Forward. A token the reader does not know is silently UNREADABLE, which blocks a
    streak forever with nothing red anywhere."""
    produced = {"success", "failure", "no_evidence"}
    unknown = sorted(produced - set(INFORMATIONAL_SPEC_VERDICTS))
    assert not unknown, f"the emitter can print {unknown}, which the reader does not classify"


def test_every_verdict_the_reader_knows_is_one_the_emitter_can_produce(emitter) -> None:
    """Backward — the anti-vacuity half, derived from the emitter's source rather than
    restated, so a vocabulary that outlives its emitter is caught."""
    source = (REPO_ROOT / "e2e" / "scripts" / "informational_spec_results.py").read_text(
        encoding="utf-8"
    )
    # Word boundary, not `"token"`: `no_evidence` is produced inside an f-string
    # (`f"{MARKER}=<none>:no_evidence"`) and carries no quotes of its own. Asserting the
    # quoted form passed on two tokens and failed on the third — a control that fires on
    # correct code, which is the direction that gets a guard deleted.
    # Tokenise instead of pattern-matching. A `` escape inside this file has now been
    # mangled twice by the tooling that wrote it, and a regex that matches nothing reports
    # EVERY token stale -- a control firing on correct code, which is how a guard gets
    # deleted. `findall` over word characters needs no escape to survive.
    words = set(re.findall("[A-Za-z_]+", source))
    stale = sorted(t for t in INFORMATIONAL_SPEC_VERDICTS if t not in words)
    assert not stale, (
        f"INFORMATIONAL_SPEC_VERDICTS names {stale}, which the emitter no longer produces"
    )


def test_control_the_emitter_and_the_reader_round_trip(emitter) -> None:
    """🔑 End to end, because the two halves passing separately is not the property.

    Emit from a report, parse the printed lines back, and classify — the path a real run
    takes through a job log.
    """
    report = TestTheEmitter._report({INCUMBENT: ["expected"], NEWCOMER: ["unexpected"]})
    lines = [
        f"2026-09-10T09:00:00.0000000Z {ln}" for ln in emitter.render(emitter.spec_results(report))
    ]
    parsed = parse_spec_verdicts(lines)
    assert classify_for_spec(INCUMBENT, parsed, "failure") == PASS
    assert classify_for_spec(NEWCOMER, parsed, "failure") == FAIL


def test_control_the_report_shape_is_the_one_the_config_asks_for() -> None:
    """The emitter reads `suites`/`specs`/`tests`. That is the `json` reporter's shape, and
    `playwright.config.ts` must actually be configured for it — a blob or html report has no
    `suites` key at all, and `spec_results` would return {} on every run while looking fine.
    """
    config = (REPO_ROOT / "e2e" / "playwright.config.ts").read_text(encoding="utf-8")
    assert '"json"' in config, (
        "playwright.config.ts no longer configures the `json` reporter. The per-spec emitter "
        "reads `suites`/`specs`/`tests`; a blob report carries `files` and no `suites`, so it "
        "would produce `no_evidence` on every run — a silent stop, not a red."
    )
