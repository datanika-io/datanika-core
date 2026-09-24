"""Ask the oracle, do not model it (core#1541).

`scripts/check_promotion_closing_refs.py` compares GitHub's own `closingIssuesReferences`
against the set the generated promotion block declares. These tests drive it with **real
captured payloads** from three named PRs rather than hand-written bodies, because a
hand-written body encodes our model of the defect, which is the one thing it cannot be used
to check (`QA_RULES` §6).

The three controls, by number
-----------------------------

============  =========================================================================
``#1526``     the narrative instance -- must flag ``1477``
``#1188``     the in-block instance -- must flag ``1130``
``#1519``     the false-positive control -- **must pass**
============  =========================================================================

`#1519` is not optional. Measured over the whole corpus, 119 of the 125 block-carrying PRs
agree with the oracle exactly; a guard that flagged them too would be refusing everything,
and the obvious repair for *"it flags everything"* is to loosen it until it permits
everything.

Why the originally-specified design is in here as a control
-----------------------------------------------------------

core#1541's acceptance criteria originally said *"it must not flag the generated block …
scan the human narrative."* `TestTheSpecifiedDesignWasBlind` implements that faithfully and
shows it **cannot see ``#1188``** -- whose keyword sits inside the block, put there by
`promotion_refs.py` rendering a promoted issue's title. It is kept, rather than deleted
along with the criterion, because a design that was withdrawn on measurement is worth being
able to re-run: without it, *"the narrative scan would have missed one"* is a sentence in a
handoff file that nothing checks.

Measured, not assumed: do code spans protect?
---------------------------------------------

Every document in this project asserts *"backticks protect; bold does not"*, and nothing had
measured it. Asked of the oracle across 309 merged PRs: a closing keyword inside a code span
appears in ``closingIssuesReferences`` **0 times out of 65**. The 65 are the generated
block's own boilerplate, which cites ``closes #272`` in backticks on every promotion -- so
the corpus supplies its own long-running natural control, and core#272 was never closed by
any of them.
"""

from __future__ import annotations

import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest

from ._workflows import job_steps

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CHECK_PATH = _REPO_ROOT / "scripts" / "check_promotion_closing_refs.py"
_GENERATOR_PATH = _REPO_ROOT / ".github" / "scripts" / "promotion_refs.py"
_FIXTURE = _REPO_ROOT / "tests" / "fixtures" / "promotion_closing_refs" / "control_prs.json"


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None, f"cannot load {path}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


check = _load(_CHECK_PATH, "check_promotion_closing_refs")
refs = _load(_GENERATOR_PATH, "promotion_refs_for_closing_check")


def _controls() -> dict[str, dict]:
    doc = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    prs = doc["prs"]
    assert set(prs) == {"1526", "1188", "1519"}, (
        "the fixture no longer carries the three named controls; a test that silently "
        "measures a different population is core#1480 one level down"
    )
    return prs


def _report(number: str):
    pr = _controls()[number]
    return check.compare(
        "datanika-io/datanika-core",
        int(number),
        pr["body"],
        frozenset(pr["closing_issues_references"]),
    )


class TestTheFixtureIsWhatItClaims:
    """Before the controls mean anything, the captured payloads have to be real."""

    def test_every_control_carries_a_generated_block(self) -> None:
        for number, pr in _controls().items():
            assert check.block_of(pr["body"]) is not None, (
                f"#{number} was captured without a promotion-refs block, so it cannot "
                f"exercise the comparison at all"
            )

    def test_every_control_carries_a_non_empty_closing_set(self) -> None:
        for number, pr in _controls().items():
            assert pr["closing_issues_references"], (
                f"#{number}'s oracle answer is empty — the capture failed and every "
                f"assertion below would pass vacuously"
            )

    def test_the_provenance_says_these_were_captured_not_written(self) -> None:
        doc = json.loads(_FIXTURE.read_text(encoding="utf-8"))
        assert "captured_utc" in doc["_provenance"]
        assert "gh api graphql" in doc["_provenance"]["how"]


class TestTheThreeNamedControls:
    def test_1526_flags_the_narrative_instance(self) -> None:
        report = _report("1526")
        assert report.verdict == "FAIL"
        assert 1477 in report.undeclared
        assert report.declared == frozenset({1507}), (
            "the block declares only the intended reference; 1477 reached the closing set "
            "from a sentence written to say the issue would NOT close"
        )

    def test_1188_flags_the_in_block_instance(self) -> None:
        report = _report("1188")
        assert report.verdict == "FAIL"
        assert 1130 in report.undeclared
        assert 1162 in report.declared, (
            "the intended reference must still be read as declared — a repair that stopped "
            "parsing the block would pass the first half of this test alone"
        )

    def test_1519_passes(self) -> None:
        report = _report("1519")
        assert report.verdict == "PASS", (
            f"the false-positive control went red: will_close={sorted(report.will_close)} "
            f"declared={sorted(report.declared)}"
        )
        assert report.exit_code == 0
        assert not report.undeclared and not report.unfired


class TestTheSpecifiedDesignWasBlind:
    """core#1541's original AC3, implemented faithfully, shown unable to see #1188.

    This is the evidence for withdrawing it. The blindness is *specific* -- the same scan
    sees #1526 perfectly -- so a reader cannot dismiss it as a broken probe.
    """

    #: GitHub's closing grammar, as the withdrawn design would have had to re-implement it.
    _GITHUB_CLOSING = re.compile(
        r"\b(?:clos(?:e|es|ed)|fix(?:|es|ed)|resolv(?:e|es|ed))\b:?[ \t]+#(\d+)", re.I
    )

    @classmethod
    def _narrative_only(cls, body: str) -> frozenset[int]:
        """ "It must not flag the generated block … scan the human narrative." Verbatim."""
        narrative = body
        if check.START in body and check.END in body:
            head, _, rest = body.partition(check.START)
            _, _, tail = rest.partition(check.END)
            narrative = head + tail
        narrative = re.sub(r"`[^`\n]*`", " ", narrative)
        return frozenset(int(n) for n in cls._GITHUB_CLOSING.findall(narrative))

    def test_the_withdrawn_scan_does_see_1526(self) -> None:
        """The positive control. Without it, the blindness below is unattributable."""
        assert 1477 in self._narrative_only(_controls()["1526"]["body"])

    def test_the_withdrawn_scan_is_blind_to_1188(self) -> None:
        assert 1130 not in self._narrative_only(_controls()["1188"]["body"]), (
            "if this ever passes, the keyword has moved out of the generated block and "
            "the withdrawn design is no longer refuted by this PR"
        )

    def test_the_keyword_really_is_inside_the_block(self) -> None:
        """Names the mechanism, so the blindness cannot be read as a regex accident."""
        block = check.block_of(_controls()["1188"]["body"])
        assert block is not None
        assert "1130" in block

    def test_the_shipped_check_flags_both(self) -> None:
        assert 1477 in _report("1526").undeclared
        assert 1130 in _report("1188").undeclared


class TestTheDeclarationParserIsNarrow:
    """It reads our own emitted line shape. Nothing else may satisfy it."""

    def test_a_code_span_in_the_blocks_own_prose_is_not_a_declaration(self) -> None:
        """Every generated block cites `closes #272` in backticks. 65 in the corpus."""
        body = (
            f"{check.START}\n"
            "### Promoted, close by hand if complete\n\n"
            "_… `WORKFLOW_RULES` §4 records landing#273, where `closes #272` on a "
            "4-of-36 partial fix retired the whole issue._\n\n"
            "- Closes #500 — a real one\n"
            f"{check.END}\n"
        )
        assert check.declared_refs(body) == frozenset({500})

    def test_a_closing_keyword_in_the_narrative_is_not_a_declaration(self) -> None:
        """Counting it would make the guard agree with the defect it exists to catch."""
        body = (
            "I will close #1477 by hand only after the deploy verifies.\n\n"
            f"{check.START}\n- Closes #1507 — the declared one\n{check.END}\n"
        )
        assert check.declared_refs(body) == frozenset({1507})

    def test_a_candidate_line_is_not_a_declaration(self) -> None:
        """`- #872 — …` carries no keyword on purpose; it closes nothing."""
        body = f"{check.START}\n- #872 — the umbrella issue · via commit abc1234\n{check.END}\n"
        assert check.declared_refs(body) == frozenset()

    def test_an_already_closed_line_is_not_a_declaration(self) -> None:
        body = f"{check.START}\n- #1477 — a title _(already closed)_ · via #1500\n{check.END}\n"
        assert check.declared_refs(body) == frozenset()

    def test_no_block_means_no_declarations(self) -> None:
        assert check.declared_refs("Closes #1 and closes #2, in prose.") == frozenset()


class TestARoundTripWithTheRealGenerator:
    """Pins the parser to `promotion_refs.py`'s emitted format without a second predicate.

    The two live in different directories and neither imports the other, so the format is a
    shared contract with nothing holding it. Driving the real generator and reading its real
    output back is what makes a change on either side go red.
    """

    @staticmethod
    def _generate(monkeypatch, capsys, commits, issues) -> str:
        log = "".join(f"{sha}\x1f{msg}\x1e" for sha, msg in commits)
        monkeypatch.setattr(refs, "run", lambda *a: log if a[:2] == ("git", "log") else "")
        monkeypatch.setattr(refs, "gh_api", lambda path: None)
        monkeypatch.setattr(refs, "gh_issue", lambda repo, num: issues.get(num, refs.ISSUE_MISSING))
        monkeypatch.setenv("REPO", "datanika-io/datanika-core")
        monkeypatch.setenv("PR_NUMBER", "0")
        monkeypatch.setenv("BASE_SHA", "aaa")
        monkeypatch.setenv("HEAD_SHA", "bbb")
        monkeypatch.setenv("DRY_RUN", "1")
        assert refs.main() == 0
        return capsys.readouterr().out

    def test_the_parser_reads_back_what_the_generator_declares(self, monkeypatch, capsys) -> None:
        out = self._generate(
            monkeypatch,
            capsys,
            commits=[("a" * 40, "[QA] A real fix (closes #500)\n")],
            issues={500: {"number": 500, "state": "open", "title": "A real issue"}},
        )
        assert check.declared_refs(out) == frozenset({500}), (
            "the generator emitted a closing line this parser did not read — the two have "
            "drifted, and the guard is now measuring nothing"
        )

    def test_a_tracking_reference_round_trips_as_not_declared(self, monkeypatch, capsys) -> None:
        """The other half. `refs #N` renders as a candidate and must close nothing."""
        out = self._generate(
            monkeypatch,
            capsys,
            commits=[("b" * 40, "[Product] Step one (refs #872)\n")],
            issues={872: {"number": 872, "state": "open", "title": "The umbrella issue"}},
        )
        assert "#872" in out, "the control: the generator did render the candidate"
        assert check.declared_refs(out) == frozenset()


class TestBothDirections:
    def test_declared_but_will_not_close_is_reported(self) -> None:
        """No natural instance in the corpus (0 of 125) — this is the synthetic arm.

        It is a real code path rather than a hypothetical: a promotion body edited by hand
        after the generator ran can leave the block describing a merge that will not happen.
        """
        body = f"{check.START}\n- Closes #500 — a real one\n{check.END}\n"
        report = check.compare("o/n", 1, body, frozenset())
        assert report.verdict == "FAIL"
        assert report.unfired == frozenset({500})
        assert not report.undeclared

    def test_agreement_is_a_pass(self) -> None:
        body = f"{check.START}\n- Closes #500 — a real one\n{check.END}\n"
        report = check.compare("o/n", 1, body, frozenset({500}))
        assert report.verdict == "PASS"
        assert report.exit_code == 0

    def test_both_directions_at_once(self) -> None:
        body = f"{check.START}\n- Closes #500 — a real one\n{check.END}\n"
        report = check.compare("o/n", 1, body, frozenset({600}))
        assert report.undeclared == frozenset({600})
        assert report.unfired == frozenset({500})


class TestTheUnmeasurableCaseIsNotAPass:
    """`QA_RULES` §31: separate *I could not read this* from *this is not here*."""

    def test_no_block_and_nothing_closing_is_a_pass(self) -> None:
        report = check.compare("o/n", 1, "A short promotion with no references.", frozenset())
        assert report.verdict == "PASS"

    def test_no_block_but_github_closes_something_is_no_verdict(self) -> None:
        report = check.compare("o/n", 1, "Narrative only, no block.", frozenset({77}))
        assert report.verdict == "NO_VERDICT"
        assert report.exit_code == 2, "2, not 0 — nothing was compared, so this is not a pass"

    def test_the_three_exit_codes_are_distinct(self) -> None:
        body = f"{check.START}\n- Closes #500 — x\n{check.END}\n"
        codes = {
            check.compare("o/n", 1, body, frozenset({500})).exit_code,
            check.compare("o/n", 1, body, frozenset({600})).exit_code,
            check.compare("o/n", 1, "no block", frozenset({77})).exit_code,
        }
        assert codes == {0, 1, 2}


class TestTheReportNamesItsPopulation:
    def test_the_render_prints_both_sets_and_the_verdict(self) -> None:
        body = f"{check.START}\n- Closes #1507 — x\n{check.END}\n"
        text = check.render(check.compare("o/n", 1526, body, frozenset({1477, 1507})))
        assert "1477" in text and "1507" in text
        assert "FAIL" in text
        assert "generated block present" in text

    def test_the_no_verdict_message_says_it_is_not_a_pass(self) -> None:
        text = check.render(check.compare("o/n", 1, "no block", frozenset({77})))
        assert "NOTHING COULD BE COMPARED" in text
        assert "not a pass" in text

    def test_the_message_offers_backticks_and_refuses_escaping(self) -> None:
        body = f"{check.START}\n- Closes #1507 — x\n{check.END}\n"
        text = check.render(check.compare("o/n", 1526, body, frozenset({1477, 1507})))
        assert "Do NOT escape the hash" in text
        assert "backticks" in text.lower()
        for bad in ("&#35;", "%23", "&num;"):
            assert bad not in text, f"the message must never suggest {bad}"

    def test_the_message_does_not_tell_anyone_to_be_careful(self) -> None:
        """Sibling of `check_closing_keyword_intent.py`'s own rule. In every measured
        instance the author knew and said so beside the reference."""
        body = f"{check.START}\n- Closes #1507 — x\n{check.END}\n"
        text = check.render(check.compare("o/n", 1526, body, frozenset({1477, 1507}))).lower()
        for scold in ("be careful", "be more careful", "pay attention", "carelessness"):
            assert scold not in text


class TestItIsWiredWhereItClaimsToBe:
    """Asserted on the PARSED workflow, not on raw text: a path named in a comment
    satisfies a grep (`QA_RULES` §24a), and this file's whole subject is text that is read
    by a parser rather than by a person."""

    def test_the_script_exists(self) -> None:
        assert _CHECK_PATH.is_file()

    def test_a_step_in_the_promotion_job_runs_it(self) -> None:
        runs = [str(s.get("run", "")) for s in job_steps("refs")]
        invoking = [r for r in runs if "scripts/check_promotion_closing_refs.py" in r]
        assert invoking, (
            "no step in the `refs` job invokes the check. A guard nothing runs is the "
            "defect it exists to catch, one level up."
        )

    def test_it_runs_after_the_generator(self) -> None:
        """Ordering is the load-bearing part: before it, the block does not exist yet."""
        runs = [str(s.get("run", "")) for s in job_steps("refs")]
        generator = next(i for i, r in enumerate(runs) if "promotion_refs.py" in r)
        checker = next(i for i, r in enumerate(runs) if "check_promotion_closing_refs.py" in r)
        assert checker > generator, (
            "the check reads the body the generator writes; running it first measures a "
            "body with no block and reports NO_VERDICT on every promotion"
        )

    def test_the_workflow_fires_on_edited(self) -> None:
        """A promotion body is routinely edited after opening. Without `edited` this check
        only ever sees the body as it was at `opened`."""
        import yaml

        doc = yaml.safe_load(
            (_REPO_ROOT / ".github" / "workflows" / "promotion-pr-refs.yml").read_text(
                encoding="utf-8"
            )
        )
        # `on` is parsed as the boolean True by YAML 1.1. Accept either spelling rather
        # than assuming which one this parser hands back.
        trigger = doc.get("on", doc.get(True))
        assert "edited" in trigger["pull_request"]["types"]

    def test_the_workflow_says_the_check_is_advisory(self) -> None:
        """`master` has no required checks and promotions use `--admin`. A write-up
        claiming this is gated would be false, and the next reader would rely on it."""
        text = (_REPO_ROOT / ".github" / "workflows" / "promotion-pr-refs.yml").read_text(
            encoding="utf-8"
        )
        assert "ADVISORY BY CONSTRUCTION" in text
        assert "--admin" in text


class TestTheOracleIsAskedRatherThanModelled:
    def test_the_checker_contains_no_closing_keyword_grammar(self) -> None:
        """The point of the corrected design. A keyword list here would be a model of
        GitHub's parser, and a model is the one thing it cannot be used to check."""
        source = _CHECK_PATH.read_text(encoding="utf-8")
        code = "\n".join(line for line in source.splitlines() if not line.lstrip().startswith("#"))
        # The docstring legitimately discusses keywords; the compiled patterns are what
        # would constitute a second parser.
        compiled = re.findall(r"re\.compile\((.*?)\)", code, re.DOTALL)
        for pattern in compiled:
            assert "resolv" not in pattern and "fix" not in pattern, (
                f"this looks like a re-implementation of GitHub's closing grammar: {pattern!r}"
            )

    def test_it_asks_for_the_oracle_by_name(self) -> None:
        assert "closingIssuesReferences" in _CHECK_PATH.read_text(encoding="utf-8")

    def test_it_states_what_the_oracle_does_not_answer(self) -> None:
        """A reader who takes this check's reading AFTER a merge would otherwise conclude
        the link fired. Two of the three flagged PRs are merged with the issue still open.

        Asserting the PRESENCE of the caveat, not the absence of an overclaim -- a guard
        written the other way is satisfied by deleting the whole paragraph (`QA_RULES` §24a
        / `WORKFLOW_RULES` §4). The live fact itself is deliberately not pinned: it would go
        red the day somebody closes one of those issues.
        """
        doc = _CHECK_PATH.read_text(encoding="utf-8")
        assert "links now" in doc
        assert "not a record of what fired" in doc
        assert "runs BEFORE the merge" in doc

    def test_a_failed_lookup_is_not_a_pass(self, monkeypatch) -> None:
        monkeypatch.setattr(check, "fetch_closing_refs", lambda repo, pr: None)
        monkeypatch.setattr(check, "fetch_body", lambda repo, pr: "whatever")
        assert check.main(["--repo", "o/n", "--pr", "1"]) == 2

    def test_a_failed_body_read_is_not_a_pass(self, monkeypatch) -> None:
        monkeypatch.setattr(check, "fetch_closing_refs", lambda repo, pr: frozenset())
        monkeypatch.setattr(check, "fetch_body", lambda repo, pr: None)
        assert check.main(["--repo", "o/n", "--pr", "1"]) == 2

    def test_the_cli_exits_1_on_a_real_disagreement(self, monkeypatch) -> None:
        pr = _controls()["1526"]
        monkeypatch.setattr(
            check, "fetch_closing_refs", lambda repo, n: frozenset(pr["closing_issues_references"])
        )
        monkeypatch.setattr(check, "fetch_body", lambda repo, n: pr["body"])
        assert check.main(["--repo", "datanika-io/datanika-core", "--pr", "1526"]) == 1

    def test_the_cli_exits_0_on_the_false_positive_control(self, monkeypatch) -> None:
        pr = _controls()["1519"]
        monkeypatch.setattr(
            check, "fetch_closing_refs", lambda repo, n: frozenset(pr["closing_issues_references"])
        )
        monkeypatch.setattr(check, "fetch_body", lambda repo, n: pr["body"])
        assert check.main(["--repo", "datanika-io/datanika-core", "--pr", "1519"]) == 0


@pytest.mark.parametrize("number", ["1526", "1188", "1519"])
def test_every_named_control_is_actually_exercised(number: str) -> None:
    """core#1480: an instrument must establish its subject is in what it measured."""
    report = _report(number)
    assert report.block_present
    assert report.will_close, f"#{number} contributed no oracle answer to any assertion"
