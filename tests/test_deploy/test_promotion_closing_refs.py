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

🚨 **All three are PROMOTIONS, and that is a limit on what they can prove.**
``closingIssuesReferences`` is empty **by construction** on a PR that does not target the
default branch, so these three would behave identically whether or not the script checked its
own population — and the first version did not, returning ``PASS`` / exit ``0`` for every
feature PR in the repository. ``TestTheOracleIsBlindOffTheDefaultBranch`` is that half, and it
exists because a set of controls drawn from one population cannot tell you about another.

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
    """All three named controls are PROMOTIONS, so `master` is both their base and the
    default branch. That is not incidental -- see `TestTheOracleIsBlindOffTheDefaultBranch`,
    which is the half these three cannot speak for."""
    pr = _controls()[number]
    return check.compare(
        "datanika-io/datanika-core",
        int(number),
        pr["body"],
        frozenset(pr["closing_issues_references"]),
        "master",
        "master",
    )


def _cmp(body: str, will_close, base: str = "main", default: str = "main"):
    """`compare` with the population arguments spelled out, for the synthetic cases."""
    return check.compare("o/n", 1, body, frozenset(will_close), base, default)


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
        report = _cmp(body, set())
        assert report.verdict == "FAIL"
        assert report.unfired == frozenset({500})
        assert not report.undeclared

    def test_agreement_is_a_pass(self) -> None:
        body = f"{check.START}\n- Closes #500 — a real one\n{check.END}\n"
        report = _cmp(body, {500})
        assert report.verdict == "PASS"
        assert report.exit_code == 0

    def test_both_directions_at_once(self) -> None:
        body = f"{check.START}\n- Closes #500 — a real one\n{check.END}\n"
        report = _cmp(body, {600})
        assert report.undeclared == frozenset({600})
        assert report.unfired == frozenset({500})


class TestTheUnmeasurableCaseIsNotAPass:
    """`QA_RULES` §31: separate *I could not read this* from *this is not here*."""

    def test_no_block_and_nothing_closing_is_a_pass(self) -> None:
        report = _cmp("A short promotion with no references.", set())
        assert report.verdict == "PASS"

    def test_no_block_but_github_closes_something_is_no_verdict(self) -> None:
        report = _cmp("Narrative only, no block.", {77})
        assert report.verdict == "NO_VERDICT"
        assert report.exit_code == 2, "2, not 0 — nothing was compared, so this is not a pass"

    def test_the_three_exit_codes_are_distinct(self) -> None:
        body = f"{check.START}\n- Closes #500 — x\n{check.END}\n"
        codes = {
            _cmp(body, {500}).exit_code,
            _cmp(body, {600}).exit_code,
            _cmp("no block", {77}).exit_code,
        }
        assert codes == {0, 1, 2}


class TestTheReportNamesItsPopulation:
    def test_the_render_prints_both_sets_and_the_verdict(self) -> None:
        body = f"{check.START}\n- Closes #1507 — x\n{check.END}\n"
        text = check.render(_cmp(body, {1477, 1507}))
        assert "1477" in text and "1507" in text
        assert "FAIL" in text
        assert "generated block present" in text

    def test_the_no_verdict_message_says_it_is_not_a_pass(self) -> None:
        text = check.render(_cmp("no block", {77}))
        assert "NOTHING COULD BE COMPARED" in text
        assert "not a pass" in text

    def test_the_message_offers_backticks_and_refuses_escaping(self) -> None:
        body = f"{check.START}\n- Closes #1507 — x\n{check.END}\n"
        text = check.render(_cmp(body, {1477, 1507}))
        assert "Do NOT escape the hash" in text
        assert "backticks" in text.lower()
        for bad in ("&#35;", "%23", "&num;"):
            assert bad not in text, f"the message must never suggest {bad}"

    def test_the_message_does_not_tell_anyone_to_be_careful(self) -> None:
        """Sibling of `check_closing_keyword_intent.py`'s own rule. In every measured
        instance the author knew and said so beside the reference."""
        body = f"{check.START}\n- Closes #1507 — x\n{check.END}\n"
        text = check.render(_cmp(body, {1477, 1507})).lower()
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


class _NoSubprocess:
    """Stands in for the `subprocess` module inside the checker, and refuses.

    Installed on `check` itself rather than on the real `subprocess` module, so nothing
    outside this file's own subject is affected.
    """

    @staticmethod
    def run(*_args, **_kwargs):
        raise AssertionError(
            "the checker shelled out to the real `gh`. Every test in this class patches "
            "`check.fetch_pr_facts`; if the real one still runs, the patch is not reaching "
            "the code under test (core#1593) and the test is measuring the live API."
        )


class TestTheOracleIsAskedRatherThanModelled:
    @pytest.fixture(autouse=True)
    def _no_network(self, monkeypatch) -> None:
        """🚨 The invariant this class had no way to state, and four of its five CLI-boundary
        tests violated it silently (core#1593).

        `await_reparse` took `fetch=fetch_pr_facts` as a DEFAULT ARGUMENT, which binds the
        function object at definition time. `monkeypatch.setattr(check, "fetch_pr_facts", …)`
        rebinds the module ATTRIBUTE, so it never reached the call, and `main` went to the
        network on every one of these tests.

        What that cost, measured on run 36137656373: in a CI job with no `GH_TOKEN` the real
        lookup failed, `main` correctly returned 2, and

        * `exits_1_on_a_real_disagreement` and `exits_0_on_the_false_positive_control` went
          **red** -- the visible half;
        * `a_failed_lookup_is_not_a_pass` and `exits_2_on_a_pr_the_oracle_cannot_see` stayed
          **green while asserting exit 2 for a reason that was not theirs** -- the half that
          would never have been noticed. The second of those is the CLI-boundary test for the
          population check, so the population check had no CLI coverage at all.

        Locally the same four passed *because* the live call succeeded, which is a green
        attached to the wrong mechanism.

        A unit test's verdict must not depend on a token, and this fixture is what makes that
        structural instead of remembered.
        """
        monkeypatch.setattr(check, "subprocess", _NoSubprocess)

    def test_a_patch_of_the_module_attribute_reaches_the_cli(self, monkeypatch) -> None:
        """The seam must be resolved at CALL time. Asserts the PRESENCE of the right thing --
        that the patched fetch was actually *called* -- rather than the absence of a network
        call, which `_no_network` covers from the other side.

        Red against the unfixed checker: the default-argument binding sends `main` to the real
        `fetch_pr_facts`, `_NoSubprocess.run` fires, and `calls` stays empty.
        """
        calls: list[int] = []

        def _fetch(repo: str, n: int):
            calls.append(n)
            return ("master", "master", "", frozenset())

        monkeypatch.setattr(check, "fetch_pr_facts", _fetch)
        assert check.main(["--repo", "datanika-io/datanika-core", "--pr", "1519"]) == 0
        assert calls == [1519], (
            "main() did not call the patched `check.fetch_pr_facts`. The injection point is a "
            "default argument on `await_reparse`, bound at definition time; resolve it inside "
            "the function body instead."
        )

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
        monkeypatch.setattr(check, "fetch_pr_facts", lambda repo, pr: None)
        assert check.main(["--repo", "o/n", "--pr", "1"]) == 2

    def test_the_cli_exits_1_on_a_real_disagreement(self, monkeypatch) -> None:
        pr = _controls()["1526"]
        monkeypatch.setattr(
            check,
            "fetch_pr_facts",
            lambda repo, n: (
                "master",
                "master",
                pr["body"],
                frozenset(pr["closing_issues_references"]),
            ),
        )
        assert check.main(["--repo", "datanika-io/datanika-core", "--pr", "1526"]) == 1

    def test_the_cli_exits_0_on_the_false_positive_control(self, monkeypatch) -> None:
        pr = _controls()["1519"]
        monkeypatch.setattr(
            check,
            "fetch_pr_facts",
            lambda repo, n: (
                "master",
                "master",
                pr["body"],
                frozenset(pr["closing_issues_references"]),
            ),
        )
        assert check.main(["--repo", "datanika-io/datanika-core", "--pr", "1519"]) == 0

    def test_the_cli_exits_2_on_a_pr_the_oracle_cannot_see(self, monkeypatch) -> None:
        """The whole point of the population check, at the CLI boundary."""
        monkeypatch.setattr(
            check,
            "fetch_pr_facts",
            lambda repo, n: ("master", "dev", "Closes #1541\nCloses #1543", frozenset()),
        )
        assert check.main(["--repo", "datanika-io/datanika-core", "--pr", "1552"]) == 2


class TestTheOracleIsBlindOffTheDefaultBranch:
    """core#1541, second correction: `closingIssuesReferences` is empty BY CONSTRUCTION on a
    PR that does not target the default branch.

    Found by Infra mid-build, re-derived here with controls in the same invocation before
    being acted on. The three named controls above are all promotions, so **they cannot
    speak for this half at all** -- they would behave identically whether or not this guard
    existed, which is precisely the shape of blindness this issue is about.

    Measured on the real API 2026-09-24::

        landing #674   base dev     default main     MERGED  totalCount=0   title declares a closure
        core    #1552  base dev     default master   OPEN    totalCount=0   body declares TWO
        core    #1519  base master  default master   MERGED  totalCount=2   <- positive control
        landing #676   base main    default main     MERGED  totalCount=1   <- positive control

    The two controls are not decoration: Infra's first reading of this returned `0` with a
    positive control that **also** returned `0`, so the zero had measured nothing.
    """

    def test_a_non_default_base_is_no_verdict_not_a_pass(self) -> None:
        report = _cmp("no block at all", set(), base="dev", default="master")
        assert report.verdict == "NO_VERDICT"
        assert report.reason == "not-default-base"
        assert report.exit_code == 2

    def test_this_is_the_measured_regression_core_1552(self) -> None:
        """Before the fix, THIS returned PASS and exit 0 — on a body declaring two closures.

        The body is the shape of PR #1552's own: two line-initial closing declarations, no
        generated block, and an oracle that returns nothing because the base is `dev`.
        """
        body = "Two complementary changes.\n\nCloses #1541\nCloses #1543\n"
        report = _cmp(body, set(), base="dev", default="master")
        assert report.verdict != "PASS", (
            "a feature PR reads as a clean promotion — this is the defect, and it shipped "
            "in the first version of this script"
        )
        assert report.exit_code == 2

    def test_the_population_check_outranks_the_block_check(self) -> None:
        """A `dev` PR that somehow carries a block is still unmeasurable, not a comparison."""
        body = f"{check.START}\n- Closes #500 — x\n{check.END}\n"
        report = _cmp(body, set(), base="dev", default="master")
        assert report.reason == "not-default-base"

    def test_control_the_same_body_on_the_default_branch_is_measured(self) -> None:
        """Without this, the refusals above are equally explained by a guard that refuses
        everything — and the obvious repair for that is to loosen it until it permits
        everything."""
        body = f"{check.START}\n- Closes #500 — x\n{check.END}\n"
        report = _cmp(body, {500}, base="master", default="master")
        assert report.oracle_applies
        assert report.verdict == "PASS"

    def test_the_two_no_verdict_causes_are_distinguishable(self) -> None:
        """`QA_RULES` §31 rule 2: a single 'unmeasured' word covers both, and the one it
        picks is the one nobody acts on."""
        blind = _cmp("x", set(), base="dev", default="master")
        no_block = _cmp("Narrative only.", {77}, base="main", default="main")
        assert blind.verdict == no_block.verdict == "NO_VERDICT"
        assert blind.reason != no_block.reason

    def test_the_message_says_it_is_a_fact_about_the_branch(self) -> None:
        text = check.render(_cmp("Closes #1541", set(), base="dev", default="master"))
        assert "CANNOT SEE THIS PR" in text
        assert "fact about the branch" in text
        assert "check_closing_keyword_intent.py" in text, (
            "a refusal must name the instrument that DOES apply to a feature PR, or it "
            "reads as 'nothing checks this'"
        )

    def test_the_report_prints_the_base_and_the_default(self) -> None:
        """§31 rule 1: report the population next to the verdict, always."""
        text = check.render(_cmp("x", set(), base="dev", default="master"))
        assert "base branch" in text
        assert "dev" in text and "master" in text
        assert "oracle applies here" in text


@pytest.mark.parametrize("number", ["1526", "1188", "1519"])
def test_every_named_control_is_actually_exercised(number: str) -> None:
    """core#1480: an instrument must establish its subject is in what it measured."""
    report = _report(number)
    assert report.block_present
    assert report.will_close, f"#{number} contributed no oracle answer to any assertion"


class TestTheOracleIsStaleForABodyTheStepJustWrote:
    """core#1575 - the check asked GitHub about a body it had not reparsed, and FAILED on
    every promotion.

    `promotion-pr-refs.yml` writes the generated block with `gh pr edit` in the step
    immediately before this one. GitHub recomputes `closingIssuesReferences` asynchronously,
    so the first look returns nothing while the block declares six issues - which the script
    scored as `DECLARED BUT WILL NOT CLOSE`. Measured 2 of 2 failures on the runs that
    actually contained the step; all 98 greens in the workflow's tally predate it.

    The controls here drive the SAME subject with the wait on and off. A version that merely
    waits and then agrees is indistinguishable, from a green alone, from one that stopped
    looking, so every test below pairs the fixed behaviour against the pre-fix behaviour
    rather than asserting the fixed one alone.
    """

    BLOCK = f"narrative\n{check.START}\n- Closes #1548 - a\n- Closes #1549 - b\n{check.END}\n"

    def _fetch_sequence(self, answers):
        """A fetcher returning a different closing set on each successive look."""
        seq = list(answers)
        calls = {"n": 0}

        def fetch(_repo, _pr):
            i = min(calls["n"], len(seq) - 1)
            calls["n"] += 1
            return ("master", "master", self.BLOCK, frozenset(seq[i]))

        return fetch, calls

    def test_the_stale_first_look_is_what_produced_the_failure(self) -> None:
        """PRE-FIX BEHAVIOUR, pinned: one look at an empty oracle is a FAIL.

        This is the control for everything below. If it ever stops being a FAIL, the tests
        that follow are comparing the fix against nothing.
        """
        report = _cmp(self.BLOCK, set(), base="master", default="master")
        assert report.verdict == "FAIL"
        assert report.unfired == frozenset({1548, 1549})

    def test_the_wait_returns_as_soon_as_the_oracle_answers(self) -> None:
        """Empty on look 1, correct on look 2 - the measured shape. No FAIL, one gap slept."""
        fetch, calls = self._fetch_sequence([set(), {1548, 1549}])
        slept: list[float] = []
        facts, waited = check.await_reparse(
            "o/n", 1, wait_seconds=300, poll_seconds=15, fetch=fetch, sleep=slept.append
        )
        assert calls["n"] == 2, "it must look again, not give up on the first empty answer"
        assert waited.answered_on_look == 2
        assert not waited.exhausted
        assert slept == [15], "exactly one gap between two looks"

        _d, _b, body, will_close = facts
        report = check.compare(
            "o/n", 1, body, will_close, "master", "master", reparse_exhausted=waited.exhausted
        )
        assert report.verdict == "PASS", "the block was right; the check was early"

    def test_an_already_current_oracle_costs_zero_seconds(self) -> None:
        """A re-run, or the post-merge invocation: the wait must not tax the case that does
        not need it."""
        fetch, calls = self._fetch_sequence([{1548, 1549}])
        slept: list[float] = []
        _facts, waited = check.await_reparse(
            "o/n", 1, wait_seconds=300, poll_seconds=15, fetch=fetch, sleep=slept.append
        )
        assert calls["n"] == 1
        assert waited.answered_on_look == 1
        assert slept == [], "it slept on a body nobody had just rewritten"

    def test_an_exhausted_wait_is_no_verdict_and_not_a_pass(self) -> None:
        """The failure direction that matters: exhaustion must not become a green.

        A check that waits and then reports success is worse than the bug it replaced.
        """
        fetch, _calls = self._fetch_sequence([set()])
        facts, waited = check.await_reparse(
            "o/n", 1, wait_seconds=30, poll_seconds=10, fetch=fetch, sleep=lambda _s: None
        )
        assert waited.exhausted
        _d, _b, body, will_close = facts
        report = check.compare(
            "o/n", 1, body, will_close, "master", "master", reparse_exhausted=True
        )
        assert report.verdict == "NO_VERDICT"
        assert report.reason == "oracle-silent-after-wait"
        assert report.exit_code == 2, "2 is 'nothing could be compared', never 0"

    def test_a_mangled_block_is_still_a_finding_after_the_wait(self) -> None:
        """THE CONTROL core#1575 ASKS FOR BY NAME: seen failing on a deliberately mangled
        block, not merely passing once the timing is fixed.

        GitHub has answered here - it names a DIFFERENT set - so this is not the timing case,
        and the wait must not launder it into an unmeasurable.
        """
        report = check.compare(
            "o/n",
            1,
            self.BLOCK,
            frozenset({1548, 9999}),
            "master",
            "master",
            reparse_exhausted=True,
        )
        assert report.verdict == "FAIL", (
            "a block declaring an issue GitHub will not close is the core#1541 defect, and "
            "reparse_exhausted must not suppress it"
        )
        assert report.unfired == frozenset({1549})
        assert report.undeclared == frozenset({9999})

    def test_the_real_defect_axis_is_not_in_the_stopping_condition(self) -> None:
        """`will_close - declared` - GitHub closes something the block never mentions - is
        the defect core#1541 exists to catch. No amount of waiting may hide it.

        Driven with the oracle naming an UNDECLARED issue from the first look: the poll stops
        immediately (every declared ref is linked) and the finding survives.
        """
        fetch, calls = self._fetch_sequence([{1548, 1549, 4242}])
        facts, waited = check.await_reparse(
            "o/n", 1, wait_seconds=300, poll_seconds=15, fetch=fetch, sleep=lambda _s: None
        )
        assert calls["n"] == 1, "declared is fully linked, so there is nothing to wait for"
        _d, _b, body, will_close = facts
        report = check.compare(
            "o/n", 1, body, will_close, "master", "master", reparse_exhausted=waited.exhausted
        )
        assert report.verdict == "FAIL"
        assert report.undeclared == frozenset({4242})

    def test_disabling_the_wait_restores_the_old_fail(self) -> None:
        """`--wait-seconds 0` must not convert a real finding into an unmeasurable.

        `reparse_exhausted` means *I waited and learned nothing*. A caller that never waited
        has established nothing of the kind, so the old verdict stands - otherwise opting out
        of the wait would silently downgrade every mangled block to exit 2.
        """
        report = check.compare(
            "o/n", 1, self.BLOCK, frozenset(), "master", "master", reparse_exhausted=False
        )
        assert report.verdict == "FAIL"

    def test_an_unreadable_subject_stops_the_poll_rather_than_retrying_it(self) -> None:
        """A failed lookup is not a pending reparse. Folding the two would spend the whole
        window re-asking a question that already errored."""
        facts, waited = check.await_reparse(
            "o/n",
            1,
            wait_seconds=300,
            poll_seconds=15,
            fetch=lambda _r, _p: None,
            sleep=lambda _s: None,
        )
        assert facts is None
        assert waited.looks == 1

    def test_the_refusal_names_the_mechanism_and_refuses_the_wrong_remedy(self) -> None:
        """The pre-fix message told the promoter to re-run the generator - which had been
        right all along. A diagnosis that sends you to repair the wrong thing is worse than
        none, at the one moment somebody is deciding whether to trust the closure set.
        """
        text = check.render(
            check.compare(
                "o/n", 1, self.BLOCK, frozenset(), "master", "master", reparse_exhausted=True
            )
        )
        assert "NEVER ANSWERED" in text
        assert "Do NOT re-run the generator" in text
        assert "closingIssuesReferences" in text

    def test_the_workflow_actually_passes_a_wait(self) -> None:
        """A fix nothing invokes is this bug one level up and looks identical to a fix."""
        runs = " ".join(str(s.get("run", "")) for s in job_steps("refs"))
        assert "check_promotion_closing_refs.py" in runs
        assert "--wait-seconds" in runs, (
            "the script grew a wait and the workflow still calls it without one, so "
            "production keeps the pre-fix behaviour while the tests stay green"
        )
