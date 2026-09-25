"""The promotion-refs generator's own logic, tested (core#635).

`.github/scripts/promotion_refs.py` decides which issues a `dev -> master`
promotion closes. It had **no tests at all**, and it carried a bug that made it
fail in the dangerous direction.

`GET /repos/{repo}/commits/{sha}/pulls` returns every pull whose branch
*contains* a commit, not the one that *introduced* it. Every open feature branch
cut from `dev` therefore comes back for every commit already on `dev`. The
generator filtered only previous promotion PRs, so an open branch's closing
keywords were attributed to the promotion — caught on PR #634, which claimed
`Closes #608 ... via #633` while #633 was open and one commit ahead of `dev`.

Why this is worth tests rather than care: the automation exists *because*
hand-enumeration leaked, and it leaked **open** — six issues left open in a day
(WORKFLOW_RULES.md section 8). This bug made it leak **closed**. An issue left
open gets re-triaged by the next person to read the board; an issue wrongly
closed does not get re-triaged by anyone.

With five departments merging into `dev` several times an hour, an open feature
branch at promotion time is the normal case, not a rare one.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MODULE_PATH = _REPO_ROOT / ".github" / "scripts" / "promotion_refs.py"


def _load():
    name = "promotion_refs"
    spec = importlib.util.spec_from_file_location(name, _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


refs = _load()


def _pull(number=1, merged=True, base="dev", title="", body=""):
    return {
        "number": number,
        "merged_at": "2026-08-30T09:44:00Z" if merged else None,
        "base": {"ref": base},
        "title": title,
        "body": body,
    }


def test_the_script_exists_where_the_workflow_invokes_it():
    """`promotion-pr-refs.yml` runs this path by string; a rename 404s silently."""
    assert _MODULE_PATH.is_file(), f"{_MODULE_PATH} is missing"
    workflow = (_REPO_ROOT / ".github" / "workflows" / "promotion-pr-refs.yml").read_text(
        encoding="utf-8"
    )
    assert ".github/scripts/promotion_refs.py" in workflow


class TestIntroducedTheCommit:
    def test_an_open_branch_that_merely_contains_the_commit_is_skipped(self):
        """core#635 exactly: PR #633, open, branched from dev, carrying `closes #608`."""
        assert not refs.introduced_the_commit(_pull(number=633, merged=False))

    def test_a_merged_feature_pr_counts(self):
        assert refs.introduced_the_commit(_pull(number=631, merged=True, base="dev"))

    def test_a_previous_promotion_pr_is_still_skipped(self):
        """The merge check does NOT subsume this: a promotion PR is merged too."""
        pull = _pull(number=621, merged=True, base="master")
        assert not refs.introduced_the_commit(pull)

    def test_the_promotion_pr_being_written_is_skipped(self):
        """Open, base master -- must fail both filters, not just one."""
        assert not refs.introduced_the_commit(_pull(number=634, merged=False, base="master"))

    @pytest.mark.parametrize("base", ["dev", "main", "some-feature"])
    def test_any_non_master_base_is_fine_once_merged(self, base):
        assert refs.introduced_the_commit(_pull(merged=True, base=base))

    def test_a_missing_base_object_does_not_explode(self):
        """The API shape is not ours; a missing key must not crash the promotion."""
        assert refs.introduced_the_commit({"number": 1, "merged_at": "2026-08-30T09:44:00Z"})

    def test_merged_at_absent_is_treated_as_not_merged(self):
        """Absent and null must behave identically -- fail closed, not open."""
        assert not refs.introduced_the_commit({"number": 1, "base": {"ref": "dev"}})


class TestFindRefs:
    """The keyword parser. `refs #N` must NOT close; `closes #N` must."""

    def test_closing_keywords_are_found(self):
        assert refs.find_refs("[Infra] Fix the thing (closes #622)", "") == {622}

    def test_a_bare_refs_does_not_close(self):
        """core#612 wrote `refs`, deliberately, because its scope was wider."""
        assert refs.find_refs("[QA] Graduate image-probe (refs #602)", "") == set()

    def test_a_plain_issue_mention_does_not_close(self):
        assert refs.find_refs("See #615 for background", "") == set()

    def test_the_body_is_searched_as_well_as_the_subject(self):
        assert 628 in refs.find_refs("[Infra] Watchdog", "Some prose.\n\ncloses #628\n")


class TestAPullRequestTitleDeclaresOnlyInTrailingPosition:
    """core#1554 — prose in a PR title became a closing declaration.

    `find_refs` scans its *subject* whole. That is right for a **commit message** and
    wrong for a **PR title**, and the difference is not a matter of taste:

    **GitHub does not parse a PR title.** Measured 2026-09-24 over 711 merged PRs across
    both repos. `closingIssuesReferences` can only answer for the 311 based on a *default*
    branch — the oracle's domain (core#1541) — and in that population **188** PRs whose
    BODY declared an issue have it in the oracle (the positive control, without which the
    next sentence is not a reading), while the **one** PR that declared a number only in
    its TITLE closed **nothing**:

        core#308  base=master  closingIssuesReferences: []
        "[Infra] Promote dev -> master (QA restore-verification fixes #300/#301/#302)"

    Two consequences, pointing opposite ways:

    * the title harvest is **load-bearing** — since GitHub ignores titles, this script is
      the only thing that turns a source PR's ``(closes #N)`` into a real closure in the
      promotion body. Removing it leaks issues open, the WORKFLOW_RULES section 8 defect
      this automation exists to prevent;
    * and it **manufactures** closures from prose, because nothing else would have.

    So the title narrows to the declaration position our convention writes, and the commit
    subject does not. GitHub scans commit messages itself, so harvesting one agrees with
    what will happen anyway; narrowing it would make this script *under*-report a closure
    that fires regardless — the invisible direction.

    Measured on the same corpus, discharging the acceptance criterion's warning that a
    narrowed rule might "harvest nothing and silently empty the closing set": **63** titles
    harvest under the shipped whole-title grep and **63** under this rule. 0 lost, 0 gained.

    There is therefore **no live instance in 711 PRs** — the defect is latent. The prose
    fixture below is consequently a *real* title of ours rather than one invented to fail.
    """

    # Real merged-PR titles, with provenance, so the population these controls speak for is
    # the population the generator actually meets (round-16 lesson: controls drawn from one
    # population cannot speak for another).
    #
    # ⚠️ Every expected value is a LITERAL. Computing it with the function under test is
    # satisfied by that function doing nothing — WORKFLOW_RULES section 4, core#1543.
    REAL_CONVENTION = [
        (  # core#1244
            "[QA] Lint the directory that decides whether a promotion may proceed (closes #1237)",
            {1237},
        ),
        (  # core#1250
            "[Product] The audit page can finally say what changed (closes #694)",
            {694},
        ),
        (  # landing#664 -- a capitalised keyword, which the corpus really contains
            "[Growth] Stop pointing readers at an in-app byte figure no screen shows (Closes #663)",
            {663},
        ),
        (  # core#308 -- three numbers, one parenthetical; only the keyword-qualified one counts
            "[Infra] Promote dev -> master (QA restore-verification fixes #300/#301/#302)",
            {300},
        ),
        (  # landing#674
            "[Engineering] /docs/runs and /api/reference describe the Cancel control "
            "that shipped (closes #672)",
            {672},
        ),
    ]

    # The title of core#1162, verbatim. Our PR titles take exactly this shape.
    PROSE_TITLE = (
        '[QA] A commit saying "Does not close #1130" CLOSED #1130 — the keyword parser has '
        "no negation, and it took an unanswered founder decision off the board"
    )

    def test_the_real_convention_still_declares(self):
        """Half one. If this goes red, the narrowing has emptied the closing set."""
        assert self.REAL_CONVENTION, "anti-vacuity: an empty fixture list passes every loop below"
        for title, expected in self.REAL_CONVENTION:
            assert refs.find_pr_refs(title, "") == expected, title

    def test_prose_in_a_title_declares_nothing(self):
        """Half two, in the same class as half one, so neither can be satisfied alone."""
        assert refs.find_pr_refs(self.PROSE_TITLE, "") == set()

    def test_a_keyword_in_a_non_trailing_parenthetical_is_not_a_declaration(self):
        """A parenthetical that is not the title's tail is prose like any other."""
        title = "[QA] (closes #1) was the old convention, now we do X"
        assert refs.find_pr_refs(title, "") == set()

    def test_a_commit_subject_is_deliberately_left_scanning_whole(self):
        """The asymmetry IS the fix, which is why `find_refs` was not simply edited.

        Identical text, the other call site. GitHub closes from a commit message, so
        reporting it is honest; refusing to report it would hide a closure that happens.
        """
        assert refs.find_refs(self.PROSE_TITLE, "") == {1130}

    def test_the_body_half_is_untouched(self):
        """A line-initial declaration still counts; mid-sentence prose still does not."""
        assert refs.find_pr_refs("[QA] no refs in this title", "Closes #77") == {77}
        prose = "I will close #77 by hand later"
        assert refs.find_pr_refs("[QA] no refs in this title", prose) == set()

    def test_tracking_refs_are_deliberately_not_narrowed(self):
        """Scoped out on purpose, and recorded so the omission is a decision.

        A tracking reference is rendered **without** a keyword, as a candidate a human
        reviews, so a false one costs a review line rather than a closure. Narrowing it
        buys nothing and would have to be argued separately.
        """
        title = "[Product] Rule the routes (refs #1534, refs #1311)"
        assert refs.find_tracking_refs(title, "") == {1534, 1311}


class TestFindTrackingRefs:
    """The non-closing half (core#1040). `refs #N` is a real signal and used to be
    invisible: core's generator matched closing keywords only, so a `refs`-only commit
    produced no line anywhere and nothing was red."""

    @pytest.mark.parametrize(
        "subject",
        [
            "[Product] Widen the empty state (refs #872)",
            "[Product] Widen the empty state (part of #872)",
            "[Product] Widen the empty state (towards #872)",
            "[Engineering] Step A (addresses #872)",
            "[Engineering] Step A (implements #872)",
        ],
    )
    def test_the_tracking_keywords_are_found(self, subject):
        assert refs.find_tracking_refs(subject, "") == {872}

    def test_a_closing_keyword_is_not_a_tracking_reference(self):
        """The two sets must stay disjoint, or an issue lands in both lists."""
        assert refs.find_tracking_refs("[Infra] Fix it (closes #622)", "") == set()

    def test_a_bare_mention_is_not_a_tracking_reference(self):
        """A mention is not a declaration -- the same asymmetry find_refs relies on."""
        assert refs.find_tracking_refs("See #615 for background", "") == set()

    def test_the_body_is_searched_for_line_initial_declarations(self):
        assert 628 in refs.find_tracking_refs("[Infra] Watchdog", "Some prose.\n\nrefs #628\n")

    def test_prose_about_tracking_keywords_mid_sentence_does_not_count(self):
        """Same false-positive class the closing parser already closed."""
        body = "The generator harvested refs #142 out of its own body, which was wrong."
        assert refs.find_tracking_refs("", body) == set()


class TestRepoAliases:
    """`core#1014` inside datanika-core is not a cross-repo reference. Found by
    rehearsing against a real promotion, where three such mentions were rendered under
    'referenced in another repository'."""

    def test_core(self):
        assert refs.repo_aliases("datanika-io/datanika-core") == {"datanika-core", "core"}

    def test_landing(self):
        assert refs.repo_aliases("datanika-io/datanika-landing") == {
            "datanika-landing",
            "landing",
        }


class TestFindCrossRepoRefs:
    """Cross-repo references exist so a commit that referenced something ELSEWHERE is
    distinguishable from one that referenced nothing -- two states that were identical
    before core#1040, because both patterns require whitespace before `#` and so matched
    `refs cloud#151` not at all."""

    MINE = {"datanika-core", "core"}

    def test_a_keyword_qualified_cross_repo_ref_is_found(self):
        got = refs.find_cross_repo_refs(
            "[Engineering] Let it hold NULL (refs cloud#151)", "", self.MINE
        )
        assert got == {"cloud#151"}

    def test_a_self_qualified_reference_is_not_cross_repo(self):
        """`core#1014` is this repo. Rendering it as another repository's is a
        confident wrong line, which is worse than no line."""
        assert (
            refs.find_cross_repo_refs("Found while measuring refs core#1014", "", self.MINE)
            == set()
        )

    def test_a_bare_cross_repo_mention_does_not_count(self):
        """Measured on the real batch: an unkeyworded pattern harvested `cloud#164`
        and `cloud#165` out of a body that cited them as already-shipped background,
        and reported the batch fully accounted. A flattering number, and false."""
        body = "Two readers are already shipped: check_schedule_quota (cloud#165) and cloud#164."
        assert refs.find_cross_repo_refs("", body, self.MINE) == set()

    def test_a_closing_keyword_on_another_repo_still_counts_as_cross_repo(self):
        assert refs.find_cross_repo_refs("Fix the page (closes cloud#164)", "", self.MINE) == {
            "cloud#164"
        }


class TestRunUsesUtf8:
    """Without `encoding="utf-8"`, `text=True` decodes with the platform codec. On this
    Windows box that is cp1251, so every em dash in an issue title comes back as
    mojibake -- reproduced live: the pre-change script rendered `вЂ”` where the shipped
    block has `—`. Invisible on the ubuntu runner, visible in exactly the local DRY_RUN
    rehearsal a promoter would run before a promotion."""

    def test_subprocess_is_called_with_an_explicit_encoding(self, monkeypatch):
        seen = {}

        class _Result:
            returncode = 0
            stdout = ""
            stderr = ""

        def fake_run(args, **kwargs):
            seen.update(kwargs)
            return _Result()

        monkeypatch.setattr(refs.subprocess, "run", fake_run)
        refs.run("git", "log")
        assert seen.get("encoding") == "utf-8", (
            "run() must pin the decode codec; the platform default mangles issue titles"
        )


class TestTheBodyAccountsForEveryCommit:
    """core#1040's core claim: the block must be able to say **I could not tell**.

    Drives `main()` end to end with DRY_RUN, stubbing only the two I/O seams, and
    asserts on the rendered block rather than on internal state -- the block is what a
    promoter reads and what GitHub parses.
    """

    @staticmethod
    def _drive(monkeypatch, capsys, commits, pulls_by_sha, issues):
        """commits: list of (sha, message). Returns the rendered block.

        `issues` maps number -> the issue dict, or `refs.ISSUE_MISSING` for a number
        the repository does not have, or `None` for a lookup that failed. A number
        **absent** from the dict is `ISSUE_MISSING`, which is the honest stub: a
        fixture repo containing no such issue is a repo where that number 404s. That
        default is load-bearing — modelling it as `None` would have every test
        silently exercise the API-failure path instead.
        """
        log = "".join(f"{sha}\x1f{msg}\x1e" for sha, msg in commits)

        def fake_run(*args):
            if args[:2] == ("git", "log"):
                return log
            return ""  # `gh pr view` -- irrelevant under DRY_RUN

        def fake_gh_api(path):
            if "/pulls" in path:
                return pulls_by_sha.get(path.rsplit("/", 2)[-2], [])
            return None

        def fake_gh_issue(repo, num):
            return issues.get(num, refs.ISSUE_MISSING)

        monkeypatch.setattr(refs, "run", fake_run)
        monkeypatch.setattr(refs, "gh_api", fake_gh_api)
        monkeypatch.setattr(refs, "gh_issue", fake_gh_issue)
        monkeypatch.setenv("REPO", "datanika-io/datanika-core")
        monkeypatch.setenv("PR_NUMBER", "0")
        monkeypatch.setenv("BASE_SHA", "aaa")
        monkeypatch.setenv("HEAD_SHA", "bbb")
        monkeypatch.setenv("DRY_RUN", "1")
        rc = refs.main()
        out = capsys.readouterr().out
        return rc, out

    def test_a_commit_referencing_nothing_is_named_not_dropped(self, monkeypatch, capsys):
        rc, out = self._drive(
            monkeypatch,
            capsys,
            commits=[
                ("a" * 40, "[Infra] Comment-only cleanup\n\nNo issue here.\n"),
                ("b" * 40, "[QA] Real fix (closes #500)\n"),
            ],
            pulls_by_sha={},
            issues={500: {"number": 500, "state": "open", "title": "A real issue"}},
        )
        assert rc == 0
        assert "I could not tell" in out
        assert "aaaaaaa" in out, "the unaccounted commit's sha must appear in the body"
        assert "Comment-only cleanup" in out, "and its subject, so it can be judged"
        assert "1 of 2" in out

    def test_a_refs_only_batch_produces_candidates_rather_than_an_empty_body(
        self, monkeypatch, capsys
    ):
        rc, out = self._drive(
            monkeypatch,
            capsys,
            commits=[("c" * 40, "[Product] Step one (refs #872)\n")],
            pulls_by_sha={},
            issues={872: {"number": 872, "state": "open", "title": "The umbrella issue"}},
        )
        assert rc == 0
        assert "Promoted, close by hand if complete" in out
        assert "#872" in out
        assert "1 of 1" in out

    def test_a_candidate_line_carries_no_closing_keyword(self, monkeypatch, capsys):
        """The safety property. GitHub parses the raw body, so a keyword on a candidate
        line would close an issue this promotion did not finish -- landing#273 again."""
        rc, out = self._drive(
            monkeypatch,
            capsys,
            commits=[("d" * 40, "[Product] Step one (refs #872)\n")],
            pulls_by_sha={},
            issues={872: {"number": 872, "state": "open", "title": "The umbrella issue"}},
        )
        block = out.split("<!-- promotion-refs:start -->", 1)[1]
        candidates = block.split("### Promoted, close by hand if complete", 1)[1]
        candidates = candidates.split("###", 1)[0]
        for line in candidates.splitlines():
            if line.startswith("- "):
                assert not refs.KEYWORD.search(line), f"candidate line would close: {line!r}"

    def test_a_cross_repo_reference_is_reported_and_counts_as_accounted(self, monkeypatch, capsys):
        rc, out = self._drive(
            monkeypatch,
            capsys,
            commits=[("e" * 40, "[Engineering] Hold NULL (refs cloud#151)\n")],
            pulls_by_sha={},
            issues={},
        )
        assert rc == 0
        assert "not closable from here" in out
        assert "cloud#151" in out
        assert "1 of 1" in out
        assert "I could not tell" not in out

    def test_coverage_states_the_suppressed_already_closed_references(self, monkeypatch, capsys):
        """Rehearsing the real promotion produced 'Coverage: 3 of 3' beside TWO visible
        entries, because the third commit's `refs #904` resolved to a closed issue. An
        unexplainable count reads as a bug, and next time it is one nobody will look."""
        rc, out = self._drive(
            monkeypatch,
            capsys,
            commits=[
                ("f" * 40, "[Infra] Something (refs #904)\n"),
                ("0" * 40, "[QA] Visible work (refs #905)\n"),
            ],
            pulls_by_sha={},
            issues={
                904: {"number": 904, "state": "closed", "title": "Long since done"},
                905: {"number": 905, "state": "open", "title": "Still open"},
            },
        )
        assert "2 of 2" in out
        assert "#905" in out, "the open candidate renders"
        assert "#904" not in out, "the closed one does not -- re-listing it is noise"
        assert "already-closed issue" in out, (
            "but the coverage line must explain why 2 accounted shows 1 visible entry"
        )


class TestAReferenceThatDoesNotResolve:
    """landing#493: a parsed reference that resolves to nothing used to vanish.

    `landing 36048435` said `refs #676`. No such issue exists in `datanika-landing`
    (the highest is 486; the author almost certainly meant `core#676`). The generator
    parsed the reference -- so the commit counted as *accounted for* -- and then
    dropped the line at `if not isinstance(issue, dict): continue`. The body therefore
    looked complete while one promoted commit was spoken for nowhere.

    Both directions are defects, which is why there are three states and not two:
    dropping it silently is what happened, and calling a transient API failure a typo
    would print a false accusation into a promotion body.
    """

    _drive = staticmethod(TestTheBodyAccountsForEveryCommit._drive)

    def test_a_nonexistent_number_is_named_rather_than_dropped(self, monkeypatch, capsys):
        rc, out = self._drive(
            monkeypatch,
            capsys,
            commits=[
                ("3" * 40, "[Product] Spec: the sub-processor register (refs #676)\n"),
                ("4" * 40, "[QA] Real work (refs #905)\n"),
            ],
            pulls_by_sha={},
            issues={905: {"number": 905, "state": "open", "title": "Still open"}},
        )
        assert rc == 0
        assert "does not resolve in this repository" in out
        assert "#676" in out, "the mistyped number must appear, or the typo stays invisible"
        assert "3333333" in out, "and the commit that carried it, so it can be traced"

    def test_the_coverage_line_says_so_rather_than_reading_as_clean(self, monkeypatch, capsys):
        """A commit whose only reference is unresolvable still counts as accounted --
        it IS named, in the new section. Without a sentence in the coverage line the
        number reads as if nothing were wrong, which is the defect one level up."""
        rc, out = self._drive(
            monkeypatch,
            capsys,
            commits=[("5" * 40, "[Product] Spec (refs #676)\n")],
            pulls_by_sha={},
            issues={},
        )
        assert "1 of 1" in out
        assert "did not resolve to an issue here" in out

    def test_a_block_is_written_even_when_every_reference_is_unresolvable(
        self, monkeypatch, capsys
    ):
        """The exact landing#492 shape. Before the fix this fell through the
        'none resolve to open issues; body unchanged' early return and rendered
        NOTHING -- an empty body for a promotion with a mistyped reference in it."""
        rc, out = self._drive(
            monkeypatch,
            capsys,
            commits=[("6" * 40, "[Product] Spec (refs #676)\n")],
            pulls_by_sha={},
            issues={},
        )
        assert rc == 0
        assert refs.START in out, "no block at all is how the reference stayed invisible"
        assert "#676" in out

    def test_the_unresolvable_line_closes_nothing(self, monkeypatch, capsys):
        """Safety property, same as the candidate list. GitHub parses the raw body, so
        a keyword here would close whatever issue that number happens to hit."""
        rc, out = self._drive(
            monkeypatch,
            capsys,
            commits=[("7" * 40, "[Product] Spec (closes #676)\n")],
            pulls_by_sha={},
            issues={},
        )
        block = out.split(refs.START, 1)[1]
        for line in block.splitlines():
            if line.startswith("- `#676`"):
                assert not refs.KEYWORD.search(line), f"would close: {line!r}"
                break
        else:
            raise AssertionError("no line for #676 was rendered at all")

    def test_a_failed_lookup_is_not_reported_as_a_typo(self, monkeypatch, capsys):
        """The other direction. `None` means 'we could not check', and saying
        'does not resolve' there accuses a perfectly good reference."""
        rc, out = self._drive(
            monkeypatch,
            capsys,
            commits=[("8" * 40, "[Infra] Something (refs #500)\n")],
            pulls_by_sha={},
            issues={500: None},
        )
        assert "could not be checked" in out
        assert "does not resolve in this repository" not in out

    def test_a_number_that_is_a_pull_request_is_named_too(self, monkeypatch, capsys):
        """`#N` resolving to a PR was also a bare `continue`. Same silent drop."""
        rc, out = self._drive(
            monkeypatch,
            capsys,
            commits=[("9" * 40, "[Infra] Something (refs #400)\n")],
            pulls_by_sha={},
            issues={
                400: {
                    "number": 400,
                    "state": "open",
                    "title": "A pull request",
                    "pull_request": {"url": "..."},
                }
            },
        )
        assert "resolves to a pull request, not an issue" in out
        assert "#400" in out


class TestGhIssueSeparates404FromFailure:
    """The seam that makes three states possible, tested against gh's real shapes.

    Measured 2026-09-04 against the live API rather than assumed:

        existing issue     -> rc 0, stdout is the issue JSON
        nonexistent issue  -> rc 1, stdout {"message":"Not Found",...,"status":"404"}
                                    stderr `gh: Not Found (HTTP 404)`

    `run()` discarded stdout on a non-zero exit, which is *why* the two were
    indistinguishable: the only machine-readable status lives in the body it threw
    away.
    """

    @staticmethod
    def _with(monkeypatch, rc, stdout, stderr=""):
        monkeypatch.setattr(refs, "run_capture", lambda *a: (rc, stdout, stderr))

    def test_a_real_issue_comes_back_as_a_dict(self, monkeypatch):
        self._with(monkeypatch, 0, '{"number": 486, "state": "open", "title": "x"}')
        got = refs.gh_issue("datanika-io/datanika-landing", 486)
        assert isinstance(got, dict) and got["number"] == 486

    def test_a_404_body_is_issue_missing(self, monkeypatch):
        self._with(
            monkeypatch,
            1,
            '{"message":"Not Found","documentation_url":"https://x","status":"404"}',
            "gh: Not Found (HTTP 404)",
        )
        assert refs.gh_issue("datanika-io/datanika-landing", 676) is refs.ISSUE_MISSING

    def test_a_404_with_an_unparseable_body_still_reads_as_missing(self, monkeypatch):
        """Falls back to stderr, because neither output shape is a contract."""
        self._with(monkeypatch, 1, "not json at all", "gh: Not Found (HTTP 404)")
        assert refs.gh_issue("datanika-io/datanika-landing", 676) is refs.ISSUE_MISSING

    def test_any_other_failure_is_none_not_missing(self, monkeypatch):
        """A rate limit, a network drop or a bad token must NOT render as a typo."""
        self._with(monkeypatch, 1, "", "error connecting to api.github.com")
        assert refs.gh_issue("datanika-io/datanika-core", 500) is None

    def test_a_rate_limit_is_not_a_404(self, monkeypatch):
        self._with(
            monkeypatch,
            1,
            '{"message":"API rate limit exceeded","status":"403"}',
            "gh: API rate limit exceeded (HTTP 403)",
        )
        assert refs.gh_issue("datanika-io/datanika-core", 500) is None

    def test_a_success_that_is_not_an_issue_object_is_none(self, monkeypatch):
        """Belt and braces: rc 0 with a body that has no `number` is not an issue."""
        self._with(monkeypatch, 0, '{"message": "something else"}')
        assert refs.gh_issue("datanika-io/datanika-core", 500) is None


class TestABorrowedStringCannotInjectAClosingReference:
    """core#1543. This generator renders text nobody here wrote, and it can carry a keyword.

    The block quotes each promoted issue's **title** and each unaccounted commit's
    **subject**. A title of the form `… close #N …` therefore becomes a live closing
    reference for an issue nobody promoted — written by the tooling, not by a person.

    ⚠️ **These are unit assertions over the rendered string, which is the cheap
    approximation** (core#1543 AC3). The end-to-end oracle is `closingIssuesReferences`, and
    what it says is recorded in `neutralise`'s docstring: across 309 merged PRs a closing
    keyword inside a code span appears in a closing set **0 times out of 65**.
    """

    #: The real title of core#1162, verbatim from the API. This exact string is what PR
    #: #1188 rendered into its block, and it is why GitHub linked #1130 as a closing
    #: reference that no human wrote.
    REAL_TITLE = (
        '[QA] A commit saying "Does not close #1130" CLOSED #1130 — the keyword parser '
        "has no negation, and it took an unanswered founder decision off the board"
    )

    def test_the_arming_control_this_title_really_does_carry_two_references(self):
        """Without this, every assertion below could pass against a harmless title."""
        found = {int(n) for n in refs.KEYWORD.findall(self.REAL_TITLE)}
        assert found == {1130}, (
            "the real title no longer carries a closing reference, so it can no longer "
            "demonstrate the defect — re-derive the instance before trusting this class"
        )
        assert len(refs.KEYWORD.findall(self.REAL_TITLE)) == 2, (
            "both `close #1130` and `CLOSED #1130` fire; a repair that silences one is not a repair"
        )

    def _line(self, monkeypatch, capsys, title, state="open"):
        log = f"{'a' * 40}\x1f[Infra] Promote (closes #1162)\n\x1e"
        monkeypatch.setattr(refs, "run", lambda *a: log if a[:2] == ("git", "log") else "")
        monkeypatch.setattr(refs, "gh_api", lambda path: None)
        monkeypatch.setattr(
            refs,
            "gh_issue",
            lambda repo, num: {"number": num, "state": state, "title": title},
        )
        monkeypatch.setenv("REPO", "datanika-io/datanika-core")
        monkeypatch.setenv("PR_NUMBER", "0")
        monkeypatch.setenv("BASE_SHA", "aaa")
        monkeypatch.setenv("HEAD_SHA", "bbb")
        monkeypatch.setenv("DRY_RUN", "1")
        assert refs.main() == 0
        out = capsys.readouterr().out
        return next(ln for ln in out.splitlines() if ln.startswith("- Closes #1162"))

    #: An INDEPENDENT code-span stripper, deliberately not built from `neutralise`.
    #:
    #: 🔑 The first version of the assertions below removed the title by computing
    #: `line.replace(refs.neutralise(TITLE), " ")` — i.e. it asked the function under test
    #: where its own output was. With `neutralise` mutated to a no-op the expected value
    #: became the raw title, the raw title was of course present, and **every one of these
    #: tests stayed green against the pre-fix code.** Found by the arming pass; reading them
    #: did not find it, because each assertion is locally correct.
    #:
    #: A test that derives its expectation from the thing in doubt is satisfied by that
    #: thing doing nothing. This regex is the cheap markdown approximation (core#1543 AC3);
    #: the oracle is `closingIssuesReferences`, and `neutralise`'s docstring carries what it
    #: said.
    _CODE_SPAN = re.compile(r"(`+).*?\1", re.DOTALL)

    @classmethod
    def _outside_code_spans(cls, text):
        return cls._CODE_SPAN.sub(" ", text)

    def test_control_the_stripper_can_see_both_kinds_of_text(self):
        """§24a's lesson: without this, narrowing the stripper until it matches nothing
        would 'fix' any false positive and leave every assertion below vacuous."""
        assert self._outside_code_spans("keep `drop` keep") == "keep   keep"
        assert self._outside_code_spans("no spans here") == "no spans here"
        assert "inner`tick" not in self._outside_code_spans("``inner`tick`` after")
        assert "after" in self._outside_code_spans("``inner`tick`` after")

    def test_the_intended_reference_still_fires(self, monkeypatch, capsys):
        """AC2's first half. A repair that neutralised the whole line passes only this."""
        assert "Closes #1162" in self._line(monkeypatch, capsys, self.REAL_TITLE)

    def test_the_title_contributes_no_live_reference(self, monkeypatch, capsys):
        """AC2's second half, and the two must hold together.

        Strip every code span from the rendered line with a stripper that knows nothing
        about `neutralise`, then scan what is left. The only live reference remaining must
        be the one this promotion actually declares.
        """
        line = self._line(monkeypatch, capsys, self.REAL_TITLE)
        outside = self._outside_code_spans(line)
        assert {int(n) for n in refs.KEYWORD.findall(outside)} == {1162}
        assert "1130" not in outside, "the borrowed title still reaches GitHub's parser"

    def test_the_title_is_still_readable(self, monkeypatch, capsys):
        """The false-positive control. Neutralising by deleting would pass the test above
        and destroy the one thing the block exists to show a promoter."""
        line = self._line(monkeypatch, capsys, self.REAL_TITLE)
        assert "unanswered founder decision off the board" in line
        assert "[QA] A commit saying" in line

    def test_a_candidate_line_is_neutralised_too(self, monkeypatch, capsys):
        """`refs #N` renders a title in a different branch of the same function."""
        log = f"{'b' * 40}\x1f[Product] Step one (refs #872)\n\x1e"
        monkeypatch.setattr(refs, "run", lambda *a: log if a[:2] == ("git", "log") else "")
        monkeypatch.setattr(refs, "gh_api", lambda path: None)
        monkeypatch.setattr(
            refs,
            "gh_issue",
            lambda repo, num: {"number": num, "state": "open", "title": self.REAL_TITLE},
        )
        for key, value in {
            "REPO": "datanika-io/datanika-core",
            "PR_NUMBER": "0",
            "BASE_SHA": "aaa",
            "HEAD_SHA": "bbb",
            "DRY_RUN": "1",
        }.items():
            monkeypatch.setenv(key, value)
        assert refs.main() == 0
        line = next(ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("- #872"))
        assert not refs.KEYWORD.search(self._outside_code_spans(line))

    def _drive(self, monkeypatch, capsys, log, issue):
        monkeypatch.setattr(refs, "run", lambda *a: log if a[:2] == ("git", "log") else "")
        monkeypatch.setattr(refs, "gh_api", lambda path: None)
        monkeypatch.setattr(refs, "gh_issue", lambda repo, num: dict(issue, number=num))
        for key, value in {
            "REPO": "datanika-io/datanika-core",
            "PR_NUMBER": "0",
            "BASE_SHA": "aaa",
            "HEAD_SHA": "bbb",
            "DRY_RUN": "1",
        }.items():
            monkeypatch.setenv(key, value)
        assert refs.main() == 0
        return capsys.readouterr().out

    def test_an_already_closed_line_is_neutralised_too(self, monkeypatch, capsys):
        """The other branch of the same `if`. Both render a title; both borrow it."""
        out = self._drive(
            monkeypatch,
            capsys,
            f"{'c' * 40}\x1f[Infra] Promote (closes #1162)\n\x1e",
            {"state": "closed", "title": self.REAL_TITLE},
        )
        line = next(ln for ln in out.splitlines() if ln.startswith("- #1162"))
        assert "already closed" in line
        assert not refs.KEYWORD.search(self._outside_code_spans(line))

    def test_an_unaccounted_commit_subject_is_neutralised(self, monkeypatch, capsys):
        """The fourth render site — and reaching it exposed a sharper point.

        core#1543's body names titles only. A commit **subject** is borrowed text by the
        same argument and reaches the block by the same route, in the *"I could not tell"*
        section.

        🔑 Getting here required the URL form, and the reason is the finding: a subject
        containing `close #N` is *parsed* by this generator, so it counts as accounted and
        never reaches that section at all. What lands there is precisely what the
        generator's own regex could **not** read — and GitHub's grammar is wider than it
        is. GitHub closes on `closes <issue URL>`; `KEYWORD` requires a literal `#`, so it
        sees nothing. **The commits this script understands least are the ones whose raw
        text it prints most verbatim.**
        """
        subject = (
            "[Infra] Tidy up — this does not close "
            "https://github.com/datanika-io/datanika-core/issues/1130"
        )
        out = self._drive(
            monkeypatch,
            capsys,
            f"{'d' * 40}\x1f{subject}\n\x1e{'e' * 40}\x1f[QA] Real (closes #500)\n\x1e",
            {"state": "open", "title": "A real issue"},
        )
        assert "I could not tell" in out, (
            "the control: this subject must be UNACCOUNTED, or the assertion below is "
            "reading a line from a different section"
        )
        line = next(ln for ln in out.splitlines() if ln.startswith("- `ddddddd`"))
        assert "1130" in line, "the subject must still be shown to the promoter, not stripped"
        assert "issues/1130" not in self._outside_code_spans(line)

    def test_the_generators_parser_is_narrower_than_githubs(self):
        """States the gap above as its own assertion, so it cannot be read as incidental."""
        url_form = "closes https://github.com/datanika-io/datanika-core/issues/1130"
        assert not refs.KEYWORD.search(url_form)
        assert not refs.TRACKING.search(url_form)
        assert not refs.CROSS_REPO.search(url_form)


class TestNeutralise:
    """The fence has to survive the strings this project actually writes."""

    def test_an_ordinary_title_round_trips(self):
        assert refs.neutralise("A plain title") == "`A plain title`"

    def test_a_title_containing_backticks_gets_a_longer_fence(self):
        """We write these constantly — `--since` did not bound this capture, and so on."""
        out = refs.neutralise("`--since` did not bound this")
        assert out.startswith("``") and out.endswith("``")
        assert "`--since`" in out, "the inner span must survive intact"

    def test_a_title_that_starts_with_a_backtick_is_padded(self):
        out = refs.neutralise("`code` first")
        assert out.startswith("`` ") and out.endswith(" ``")

    def test_the_fence_always_exceeds_the_longest_run(self):
        for n in range(1, 6):
            inner = "`" * n
            out = refs.neutralise(f"x{inner}y")
            fence = out[: len(out) - len(out.lstrip("`"))]
            assert len(fence) > n, f"fence {fence!r} cannot contain a run of {n}"

    def test_a_newline_cannot_break_out_of_the_span(self):
        """A blank line ends the paragraph and would end the span with it."""
        assert "\n" not in refs.neutralise("first\n\nsecond")

    def test_empty_stays_empty(self):
        assert refs.neutralise("") == ""

    def test_it_never_escapes_the_hash(self):
        """The measured non-repair: an entity for `#` contains a hash and digits of its
        own, so it silences the closure AND plants a reference to an unrelated issue."""
        out = refs.neutralise("close #1130")
        for bad in ("&#35;", "&#x23;", "&num;", "%23"):
            assert bad not in out
        assert "#1130" in out, "the number must survive verbatim inside the span"
