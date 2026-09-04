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
