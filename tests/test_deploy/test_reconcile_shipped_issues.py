"""The post-promotion reconciler's own logic, tested (core#773).

`.github/scripts/reconcile_shipped_issues.py` decides which OPEN issues are already live in
production, and labels them. It exists because `promotion_refs.py` only ever sees *closing*
keywords, while almost every commit here writes `refs #N` on purpose (WORKFLOW_RULES §4).

Two properties carry the whole design, and both are tested by watching them fail:

1. **It must not fire on a foreign issue number.** `landing#406` and
   `datanika-io/datanika-cloud#119` both appear in core commit messages. Reading either as a
   core number labels an unrelated issue with a claim about a deploy that never touched it.

2. **A run that measured nothing must not report success.** This is the specific defect the
   whole change is a response to: on 2026-08-31 the landing promotion generated an *empty*
   references block and its workflow still reported `success`, which is indistinguishable
   from the workflow never having run. So an empty scan, an empty parse and an empty issue
   list each exit non-zero, and each is asserted below.

The workflow-name test is not boilerplate. `workflow_run` matches the upstream workflow by
its `name:` string; a typo there means this never fires, and a workflow that never fires
produces exactly the same evidence as a repo with nothing to reconcile.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MODULE_PATH = _REPO_ROOT / ".github" / "scripts" / "reconcile_shipped_issues.py"
_WORKFLOW = _REPO_ROOT / ".github" / "workflows" / "post-promotion-reconcile.yml"
_CD_WORKFLOW = _REPO_ROOT / ".github" / "workflows" / "deploy-pointer.yml"


def _load():
    name = "reconcile_shipped_issues"
    spec = importlib.util.spec_from_file_location(name, _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


rec = _load()


def test_the_script_exists_where_the_workflow_invokes_it():
    assert _MODULE_PATH.is_file()
    assert "reconcile_shipped_issues.py" in _WORKFLOW.read_text(encoding="utf-8")


def test_the_workflow_run_trigger_names_the_cd_workflow_exactly():
    """A mismatch here is silent: the reconciler simply never runs, and reports nothing."""
    cd_name = None
    for line in _CD_WORKFLOW.read_text(encoding="utf-8").splitlines():
        if line.startswith("name:"):
            cd_name = line.split("name:", 1)[1].strip().strip("\"'")
            break
    assert cd_name, "deploy-pointer.yml has no top-level name:"
    workflow = _WORKFLOW.read_text(encoding="utf-8")
    assert f'workflows: ["{cd_name}"]' in workflow, (
        f"post-promotion-reconcile.yml must listen for {cd_name!r}; a typo fires nothing"
    )


def test_label_description_fits_githubs_limit():
    """GitHub rejects a label description over 100 chars with HTTP 422.

    Found by running this for real, not by the dry run: the description was 111 characters,
    the label was never created, all 13 `gh issue edit` calls failed with "not found", and
    the script still exited 0 and commented on the promotion PR saying they were labelled.
    """
    assert len(rec.LABEL_DESC) <= rec.LABEL_DESC_MAX == 100, len(rec.LABEL_DESC)


def test_a_label_that_cannot_be_created_stops_the_run(monkeypatch, tmp_path, capsys):
    """Fail closed. A comment claiming issues are labelled, when they are not, is worse
    than no comment: it is a false record on the one artifact people go back and read."""
    monkeypatch.setenv("REPO", "datanika-io/datanika-core")
    monkeypatch.setenv("DEPLOY_SHA", "deadbeef")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(rec, "scan_commits", lambda ref: ({704: [("abc12345", "[Infra] x")]}, 700))
    monkeypatch.setattr(
        rec, "open_issues", lambda repo: [{"number": 704, "title": "t", "labels": []}]
    )
    monkeypatch.setattr(rec, "ensure_label", lambda repo: False)

    def _explode(*args, **kwargs):  # pragma: no cover - must never be reached
        raise AssertionError("must not label or comment when the label is missing")

    monkeypatch.setattr(rec, "run", _explode)
    assert rec.main() == 1
    assert "could not be created" in capsys.readouterr().out


def test_the_checkout_is_not_shallow():
    """A shallow checkout scans a truncated range and under-reports."""
    assert "fetch-depth: 0" in _WORKFLOW.read_text(encoding="utf-8")


def test_it_queues_rather_than_cancelling():
    """Labelling is a mutation; WORKFLOW_RULES §2 says mutations queue."""
    workflow = _WORKFLOW.read_text(encoding="utf-8")
    assert "cancel-in-progress: false" in workflow


class TestReferenceExtraction:
    def test_a_subject_ref_is_found(self):
        assert rec.refs_in_commit("[Infra] Do the thing (refs #704)", "") == {704}

    def test_the_core_qualified_subject_form_is_found(self):
        assert rec.refs_in_commit("[QA] Commit the QA rules to the repo (core#718)", "") == {718}

    def test_a_keyword_ref_in_the_body_is_found(self):
        assert rec.refs_in_commit("[Engineering] Title", "Body text.\n\nrefs #659") == {659}

    def test_a_bare_mention_in_the_body_is_not_a_claim_of_authorship(self):
        """Prose mentions history constantly; only a keyword position claims the work."""
        assert rec.refs_in_commit("[Infra] Title", "same family as #602, see the writeup") == set()

    @pytest.mark.parametrize(
        "text",
        [
            "landing#406",
            "datanika-landing#406",
            "datanika-io/datanika-cloud#119",
            "cloud#119",
        ],
    )
    def test_a_foreign_reference_is_never_read_as_a_core_number(self, text):
        assert rec.refs_in_commit(f"[Infra] Title ({text})", f"refs {text}") == set()

    def test_a_foreign_reference_beside_a_local_one_keeps_only_the_local(self):
        got = rec.refs_in_commit(
            "[Product] Quickstart (refs #736)", "the landing half is landing#406"
        )
        assert got == {736}

    def test_a_fenced_block_is_not_scanned(self):
        body = "Example:\n\n```\nrefs #999\n```\n\nrefs #12"
        assert rec.refs_in_commit("[Infra] Title", body) == {12}

    def test_several_refs_in_one_commit_all_count(self):
        assert rec.refs_in_commit("[Eng] T", "refs #659\nrefs #686\nrefs #700") == {659, 686, 700}

    def test_a_merge_subject_names_a_pull_request_not_an_issue(self):
        """116 of master's 706 commits are these, and #678 is a PR, never an issue."""
        assert rec.refs_in_commit("Merge pull request #678 from datanika-io/dev", "") == set()
        assert rec.refs_in_commit("Merge branch 'dev' into master", "") == set()

    def test_a_merge_commit_body_is_still_scanned(self):
        """A promotion merge carries the generated `Closes #N` in its body; keep those."""
        got = rec.refs_in_commit("Merge pull request #678 from datanika-io/dev", "Closes #737")
        assert got == {737}


class TestDeptTag:
    @pytest.mark.parametrize(
        "subject,expected",
        [
            ("[Infra] Something", "Infra"),
            ("[QA] Something", "QA"),
            ("Merge pull request #601 from datanika-io/dev", "unlabelled"),
        ],
    )
    def test_dept_is_taken_from_the_subject_tag(self, subject, expected):
        assert rec.dept_of(subject) == expected


class TestARunThatMeasuredNothingFails:
    """Each of these would otherwise exit 0 and look exactly like 'nothing to reconcile'."""

    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch, tmp_path):
        monkeypatch.setenv("REPO", "datanika-io/datanika-core")
        monkeypatch.setenv("PROD_REF", "master")
        monkeypatch.setenv("DEPLOY_SHA", "deadbeef")
        monkeypatch.chdir(tmp_path)

    def test_zero_commits_scanned_is_a_failure(self, monkeypatch, capsys):
        """Isolated from the empty-parse guard on purpose.

        The obvious spelling — `({}, 0)` — is caught by *either* guard, so removing the
        shallow-checkout check leaves the test green. Verified by mutation: it passed
        against a build with `if scanned == 0` deleted. A non-empty reference map with a
        zero commit count is unreachable in production and is exactly what discriminates.
        """
        monkeypatch.setenv("DRY_RUN", "1")
        monkeypatch.setattr(
            rec, "scan_commits", lambda ref: ({704: [("abc12345", "[Infra] x")]}, 0)
        )
        monkeypatch.setattr(
            rec, "open_issues", lambda repo: [{"number": 704, "title": "t", "labels": []}]
        )
        assert rec.main() == 1
        assert "scanned 0 commits" in capsys.readouterr().out

    def test_zero_references_parsed_is_a_failure(self, monkeypatch):
        monkeypatch.setattr(rec, "scan_commits", lambda ref: ({}, 700))
        monkeypatch.setattr(rec, "open_issues", lambda repo: [{"number": 1, "title": "t"}])
        assert rec.main() == 1

    def test_an_empty_issue_list_is_treated_as_a_failed_query(self, monkeypatch):
        monkeypatch.setattr(
            rec, "scan_commits", lambda ref: ({704: [("abc12345", "[Infra] x")]}, 700)
        )
        monkeypatch.setattr(rec, "open_issues", lambda repo: [])
        assert rec.main() == 1

    def test_a_healthy_run_with_nothing_new_succeeds(self, monkeypatch):
        monkeypatch.setattr(
            rec, "scan_commits", lambda ref: ({704: [("abc12345", "[Infra] x")]}, 700)
        )
        monkeypatch.setattr(
            rec,
            "open_issues",
            lambda repo: [{"number": 999, "title": "not shipped", "labels": []}],
        )
        assert rec.main() == 0

    def test_the_dept_comes_from_a_tagged_commit_not_whichever_came_first(
        self, monkeypatch, capsys
    ):
        """An untagged commit must not report the whole group as 'unlabelled'.

        Real case: #598's first referencing commit on master is a merge commit, so the
        summary attributed an Infra issue to no department at all.
        """
        monkeypatch.setenv("DRY_RUN", "1")
        monkeypatch.setattr(
            rec,
            "scan_commits",
            lambda ref: (
                {598: [("00000000", "an untagged commit"), ("1f9c269c", "[Infra] Alert debounce")]},
                700,
            ),
        )
        monkeypatch.setattr(
            rec, "open_issues", lambda repo: [{"number": 598, "title": "pager", "labels": []}]
        )
        assert rec.main() == 0
        assert "[Infra]" in capsys.readouterr().out

    def test_dry_run_reports_without_mutating(self, monkeypatch, capsys):
        monkeypatch.setenv("DRY_RUN", "1")
        monkeypatch.setattr(
            rec, "scan_commits", lambda ref: ({704: [("abc12345", "[Infra] x")]}, 700)
        )
        monkeypatch.setattr(
            rec,
            "open_issues",
            lambda repo: [{"number": 704, "title": "celery scrape", "labels": []}],
        )

        def _explode(*args, **kwargs):  # pragma: no cover - must never be reached
            raise AssertionError("dry run must not call gh")

        monkeypatch.setattr(rec, "ensure_label", _explode)
        monkeypatch.setattr(rec, "run", _explode)
        assert rec.main() == 0
        assert "would label #704" in capsys.readouterr().out


def test_the_comment_names_the_issues_and_the_label_query():
    body = rec.build_comment(
        [{"number": 704, "title": "Celery scrape", "sha": "abc12345", "dept": "Infra"}],
        "master",
        "d068d0fa2eb2",
    )
    assert rec.START in body and rec.END in body
    assert "#704" in body and "Infra" in body
    assert rec.LABEL in body
    assert "Nothing here has been closed" in body
