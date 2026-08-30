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
