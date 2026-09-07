"""A strict xfail must not outlive the issue it pins (core#1025, second half).

The audit itself is `scripts/strict_xfail_issue_audit.py`. This file pins the three properties
that decide whether it is worth having:

1. it goes **red** when a strict xfail names a closed issue;
2. it goes **green** on the current tree;
3. it **refuses to answer** rather than reading clean whenever it measured nothing — an empty
   census, an unreadable API, an unattributable marker.

Property 3 is the whole point. core#896 was closed COMPLETED while four strict xfails named it,
and every signal available at the time agreed it was fine. A check that reported "0 closed
issues" because it could not reach GitHub would have been one more of those.

⚠️ **Every test here injects its own reader.** The audit takes `read` as a parameter precisely
so this file never touches the network: a network-reading guard in the `test` job is the thing
core#1025 forbids, and mocking `subprocess` instead would assert the shape of a `gh` command
rather than the behaviour of the check.

🚨 **The historical demonstration core#1025 asks for is no longer available as written, and that
is recorded rather than worked around.** Its acceptance says to arm the check by pointing it at
**core#896**, "a real historical case that needs no synthetic fixture". Measured 2026-09-07:
core#896 is **OPEN, stateReason REOPENED**. So the issue chosen as a known-closed control has
stopped carrying the property it was chosen for, and using it would have produced a green arm
that proves nothing — *a control that does not carry the suspected cause cannot exonerate
anything* (`WORKFLOW_RULES`). The arming below uses an injected CLOSED state instead, which is
deterministic and cannot rot the same way.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.strict_xfail_issue_audit import (  # noqa: E402
    REPO_FOR_PREFIX,
    CouldNotMeasureError,
    audit,
    repo_for,
    tokens_to_markers,
)
from tests.test_deploy.test_strict_xfail_reasons_name_an_issue import (  # noqa: E402
    Marker,
    applied_markers,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "strict-xfail-audit.yml"


def all_open(_token: str) -> tuple[str, str | None, str]:
    return ("OPEN", None, "a live issue")


def all_closed(_token: str) -> tuple[str, str | None, str]:
    return ("CLOSED", "COMPLETED", "an issue somebody closed")


def unreadable(token: str) -> tuple[str, str | None, str]:
    raise CouldNotMeasureError(f"could not read {token}: gh exited 1")


# ======================================================================================
# 1. The finding it exists to make
# ======================================================================================


class TestItCatchesAClosedIssueStillPinned:
    def test_a_closed_issue_with_a_live_strict_xfail_is_a_finding(self) -> None:
        """core#896's shape: the issue reads closed, the marker is still in the tree."""
        markers = [Marker("tests/test_services/test_x.py", None, frozenset({"core#896"}), False)]
        findings = audit(markers, all_closed)
        assert len(findings) == 1
        assert findings[0].token == "core#896"
        assert findings[0].markers == ("tests/test_services/test_x.py",)

    def test_the_finding_names_every_marker_pinning_that_issue(self) -> None:
        """Four markers named core#896. A finding that names one sends you to fix a quarter."""
        markers = [
            Marker("tests/a.py", None, frozenset({"core#896"}), False),
            Marker("tests/b.py", None, frozenset({"core#896"}), False),
            Marker("tests/c.py", "entry", frozenset({"core#896"}), True),
        ]
        (finding,) = audit(markers, all_closed)
        assert finding.markers == ("tests/a.py", "tests/b.py", "tests/c.py[entry]")
        assert "tests/c.py[entry]" in finding.render()

    def test_an_open_issue_is_not_a_finding(self) -> None:
        markers = [Marker("tests/a.py", None, frozenset({"core#648"}), False)]
        assert audit(markers, all_open) == []

    @pytest.mark.parametrize("state", ["closed", "Closed", "CLOSED"])
    def test_the_state_comparison_is_case_insensitive(self, state: str) -> None:
        """`gh` returns `CLOSED`; the GraphQL API returns `closed`. A case-sensitive compare
        would silently find nothing against one of them."""
        markers = [Marker("tests/a.py", None, frozenset({"core#1"}), False)]
        assert len(audit(markers, lambda _t: (state, None, "t"))) == 1


# ======================================================================================
# 2. It refuses to answer rather than reading clean — the reason it is worth having
# ======================================================================================


class TestItRefusesToReportCleanWhenItMeasuredNothing:
    def test_an_empty_census_raises_rather_than_returning_no_findings(self) -> None:
        """A scanner that finds nothing reports no closed issues, which is a healthy tree's
        output. That is the defect this whole check is about, one level up."""
        with pytest.raises(CouldNotMeasureError, match="census is empty"):
            audit([], all_open)

    def test_markers_that_name_no_issue_raise_rather_than_reading_clean(self) -> None:
        """Nine markers and zero tokens is not a clean audit — it is an unattributable tree."""
        markers = [Marker("tests/a.py", None, frozenset(), False)]
        with pytest.raises(CouldNotMeasureError, match="none names an issue"):
            audit(markers, all_open)

    def test_an_unreadable_issue_raises_rather_than_counting_as_open(self) -> None:
        """A rate limit, a 404 or a permissions failure must not render as `not closed`."""
        markers = [Marker("tests/a.py", None, frozenset({"core#648"}), False)]
        with pytest.raises(CouldNotMeasureError, match="measured nothing"):
            audit(markers, unreadable)

    def test_one_unreadable_issue_poisons_the_whole_run(self) -> None:
        """Partial coverage reported as a pass is how a green comes to mean nothing.

        The readable half here is genuinely clean; the run is still refused, because "3 of 4
        issues are open" is not the question the check was asked.
        """

        def flaky(token: str) -> tuple[str, str | None, str]:
            if token == "cloud#160":
                raise CouldNotMeasureError("could not read cloud#160: private repo, no token")
            return ("OPEN", None, "t")

        markers = [
            Marker("tests/a.py", None, frozenset({"core#648"}), False),
            Marker("tests/b.py", None, frozenset({"cloud#160"}), True),
        ]
        with pytest.raises(CouldNotMeasureError, match="could not read 1 of 2"):
            audit(markers, flaky)

    def test_a_closed_issue_is_still_reported_when_everything_is_readable(self) -> None:
        """Control for the two tests above: the refusal must be caused by unreadability, not by
        the audit having become unable to return a finding at all."""
        markers = [
            Marker("tests/a.py", None, frozenset({"core#648"}), False),
            Marker("tests/b.py", None, frozenset({"core#896"}), False),
        ]
        found = audit(
            markers, lambda t: ("CLOSED", None, "t") if t == "core#896" else ("OPEN", None, "t")
        )
        assert [f.token for f in found] == ["core#896"]


# ======================================================================================
# 3. Token -> repository. A `cloud#N` sent to core 404s, and a swallowed 404 is a false green
# ======================================================================================


class TestTokenRouting:
    def test_core_and_cloud_tokens_route_to_different_repositories(self) -> None:
        assert repo_for("core#648") == "datanika-io/datanika-core"
        assert repo_for("cloud#160") == "datanika-io/datanika-cloud"
        assert repo_for("core#1") != repo_for("cloud#1")

    @pytest.mark.parametrize("bad", ["#648", "core648", "landing#1", "core#", "core#x", ""])
    def test_an_unroutable_token_raises_rather_than_defaulting_to_core(self, bad: str) -> None:
        """Defaulting would send every unknown prefix to a repo that does not contain it, and a
        404 read as `not closed` is the reassuring failure again."""
        with pytest.raises(CouldNotMeasureError):
            repo_for(bad)

    def test_every_prefix_the_census_emits_is_routable(self) -> None:
        """The live coupling: a new prefix in a reason must not silently become unauditable."""
        prefixes = {t.split("#", 1)[0] for m in applied_markers(REPO_ROOT) for t in m.tokens}
        assert prefixes, "no issue tokens in the census — the resolver has gone blind"
        unroutable = prefixes - set(REPO_FOR_PREFIX)
        assert not unroutable, (
            f"the strict-xfail census emits {sorted(unroutable)} tokens and "
            "scripts/strict_xfail_issue_audit.py has no repository for them, so the audit "
            "would skip every marker naming one"
        )


# ======================================================================================
# 4. The real tree — green today, and the census is genuinely non-empty
# ======================================================================================


class TestAgainstTheRealTree:
    def test_the_real_tree_has_no_unattributed_markers_and_audits_green(self) -> None:
        """AC2. Uses an injected all-open reader: whether those issues are open today is a fact
        about GitHub and belongs in the scheduled job, not in the `test` gate."""
        markers = applied_markers(REPO_ROOT)
        assert audit(markers, all_open) == []

    def test_control_the_real_tree_would_go_red_if_its_issues_were_closed(self) -> None:
        """AC1/AC3 arming, on the REAL census rather than a fixture.

        Without this, the green above is equally consistent with `audit` having become unable
        to report anything at all.
        """
        markers = applied_markers(REPO_ROOT)
        findings = audit(markers, all_closed)
        tokens = {f.token for f in findings}
        assert tokens == set(tokens_to_markers(markers)), (
            "closing every issue the tree names must produce a finding for every one of them"
        )
        assert len(tokens) >= 2, "too few distinct issues for this arming to mean much"

    def test_control_the_census_covers_both_static_and_table_driven_markers(self) -> None:
        markers = applied_markers(REPO_ROOT)
        assert any(not m.via_table for m in markers), "no static marker in the census"
        assert any(m.via_table for m in markers), "no table-driven marker in the census"


# ======================================================================================
# 5. The TIER. core#1025's hard requirement: a network read may not red-light a PR
# ======================================================================================


class TestTheWorkflowRunsInASafeTier:
    @staticmethod
    def _wf() -> dict:
        assert WORKFLOW.exists(), f"{WORKFLOW} is missing"
        # `on:` is parsed by PyYAML 1.1 rules as the boolean True. Read both spellings rather
        # than assuming, or this whole class silently asserts nothing.
        doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
        assert isinstance(doc, dict)
        return doc

    def _triggers(self) -> dict:
        doc = self._wf()
        triggers = doc.get("on", doc.get(True))
        assert isinstance(triggers, dict), f"could not read the trigger block: {triggers!r}"
        return triggers

    def test_it_is_scheduled(self) -> None:
        assert "schedule" in self._triggers()

    def test_it_can_be_dispatched(self) -> None:
        """`workflow_dispatch --ref <branch>` is the only way to exercise this before the
        promotion that starts the schedule."""
        assert "workflow_dispatch" in self._triggers()

    @pytest.mark.parametrize("forbidden", ["pull_request", "pull_request_target", "merge_group"])
    def test_it_never_runs_on_a_pull_request(self, forbidden: str) -> None:
        """core#1025's tier requirement, stated as the property rather than as a ban on a word.

        A network read on a PR trigger makes an unrelated branch red because somebody closed an
        issue. That is not a flaky test — it is a check whose verdict is about the world rather
        than about the diff.
        """
        assert forbidden not in self._triggers()

    def test_a_could_not_measure_exit_is_not_allowed_to_pass(self) -> None:
        """Exit 2 means the tree was not audited. The step must turn that into a failure.

        Asserted as branch-then-action rather than as a token: `WORKFLOW_RULES` measured three
        guards that survived a mutation disabling the branch while leaving its body intact.
        """
        body = WORKFLOW.read_text(encoding="utf-8")
        lines = [ln.strip() for ln in body.splitlines() if not ln.strip().startswith("#")]
        try:
            i = next(i for i, ln in enumerate(lines) if 'if [ "$rc" -eq 2 ]' in ln)
        except StopIteration:  # pragma: no cover
            pytest.fail("the step does not branch on the could-not-measure exit code at all")
        window = lines[i : i + 5]
        assert any(ln.startswith("exit 1") for ln in window), (
            "the could-not-measure branch exists but does not fail the step within it; "
            f"saw {window!r}"
        )

    def test_it_asserts_a_positive_artifact_rather_than_the_absence_of_errors(self) -> None:
        """`WORKFLOW_RULES`: assert what would be present had the thing happened."""
        body = WORKFLOW.read_text(encoding="utf-8")
        assert "strict xfail markers applied" in body, (
            "the step does not check for the audit's own census line, so a run that printed "
            "nothing at all would pass"
        )

    def test_control_the_trigger_reader_can_see_a_trigger(self) -> None:
        """Anti-vacuity: if `_triggers()` returned `{}` every forbidden-trigger test above would
        pass while asserting nothing."""
        assert set(self._triggers()) >= {"schedule", "workflow_dispatch"}
