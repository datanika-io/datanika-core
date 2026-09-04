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
        """commits: list of (sha, message). Returns the rendered block."""
        log = "".join(f"{sha}\x1f{msg}\x1e" for sha, msg in commits)

        def fake_run(*args):
            if args[:2] == ("git", "log"):
                return log
            return ""  # `gh pr view` -- irrelevant under DRY_RUN

        def fake_gh_api(path):
            if "/pulls" in path:
                return pulls_by_sha.get(path.rsplit("/", 2)[-2], [])
            if "/issues/" in path:
                return issues.get(int(path.rsplit("/", 1)[-1]))
            return None

        monkeypatch.setattr(refs, "run", fake_run)
        monkeypatch.setattr(refs, "gh_api", fake_gh_api)
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
