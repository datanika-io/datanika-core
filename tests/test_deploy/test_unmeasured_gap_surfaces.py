"""An absence of measurement must surface somewhere other than a non-required job's log.

core#1256. `e2e-sso` produced **no reading** on five consecutive commits and CI concluded
`success` on every one. Nothing was broken: core#876's guard refused to grade someone else's
build after a newer push redeployed staging out from under the verifier. **The defect is that
nobody sees the accumulation.**

`e2e-sso` is **not a required check** — read from the protection API, not from memory — and the
SSO tier is an Enterprise-only auth path, on the surface this project already shipped a P0 auth
bypass on.

Measured over 50 runs on `dev`:

```
23 of 50 produced no reading (46%)
consecutive-UNMEASURED run lengths: [1, 1, 2, 2, 5, 5, 7]
```

🔑 **The trigger is push CADENCE, not carelessness**, so a convention cannot fix it. And the
threshold is not taste: that distribution has a clean break — nothing at 3 or 4 — so `>=3` and
`>=4` both fire on exactly the 5, 5 and 7 and neither fires on the 1s and 2s. **3** is chosen
because it is not a new magic number: it is the graduation bar, so a tier unmeasured three runs
in a row could not have graduated anything even if every spec passed.

⚠️ **Not made a required check, deliberately.** That blocks every merge during any normal burst
with no PR at fault — the `image-cve` mistake.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from scripts.e2e_tier_streak import (  # noqa: E402
    FAIL,
    LOCAL,
    PASS,
    UNMEASURED,
    UNREADABLE,
    Reading,
)

U, P, F = UNMEASURED, PASS, FAIL
THRESHOLD = 3

WATCHDOG = REPO_ROOT / ".github" / "workflows" / "scheduled-workflow-watchdog.yml"


# ── the measurement ──────────────────────────────────────────────────────────────────────


class TestTheGapIsCounted:
    def test_the_observed_five_gap(self) -> None:
        r = Reading.from_classes([F, U, U, U, U, U, F, P])
        assert r.longest_unmeasured == 5
        assert r.trailing_unmeasured == 0, "the gap had ended by the time it was read"

    def test_a_gap_that_is_still_open(self) -> None:
        """`trailing_` is *are we blind right now*; `longest_` is *did we go blind at all*."""
        r = Reading.from_classes([P, P, U, U, U])
        assert r.trailing_unmeasured == 3
        assert r.longest_unmeasured == 3

    def test_a_daily_check_needs_longest_not_trailing(self) -> None:
        """🔑 The gap that already closed is the one nobody notices.

        The real incident had recovered by the time anyone looked: `FAIL` at 14:06 ended it.
        A watchdog reading only `trailing_` would have seen 0 and said nothing.
        """
        recovered = [F, U, U, U, U, U, F, P, P]
        r = Reading.from_classes(recovered)
        assert r.trailing_unmeasured == 0
        assert r.longest_unmeasured >= THRESHOLD, "a check on trailing alone would miss this"


class TestOnlyTheSILENTClassCounts:
    """UNMEASURED is transparent to the streak by design — that is why it accumulates
    invisibly. UNREADABLE and LOCAL already **block**, so a run of them is loud already.
    Counting them here would double-report a signal that is not silent."""

    def test_unreadable_is_not_counted(self) -> None:
        assert Reading.from_classes([P, UNREADABLE, UNREADABLE, UNREADABLE]).longest_unmeasured == 0

    def test_local_is_not_counted(self) -> None:
        assert Reading.from_classes([P, LOCAL, LOCAL, LOCAL]).longest_unmeasured == 0

    def test_control_they_do_still_block_the_streak(self) -> None:
        """The reason they need no second alarm — asserted, not assumed."""
        assert Reading.from_classes([P, P, P, UNREADABLE]).streak == 0
        assert Reading.from_classes([P, P, P, LOCAL]).streak == 0


# ── the threshold, against the measured distribution ─────────────────────────────────────


@pytest.mark.parametrize(
    ("name", "classes", "should_fire"),
    [
        ("isolated single", [P, P, U, P, P], False),
        ("two in a row", [P, U, U, P, P], False),
        ("the observed 5-gap", [F, U, U, U, U, U, F], True),
        ("the observed 7-gap", [P, U, U, U, U, U, U, U, P], True),
        ("nothing blind at all", [P, P, F, P], False),
    ],
)
def test_the_threshold_matches_the_measured_distribution(
    name: str, classes: list[str], should_fire: bool
) -> None:
    """Ordinary runs must not fire; the observed blackouts must. Both directions, or the
    number is a preference rather than a threshold."""
    fires = Reading.from_classes(classes).longest_unmeasured >= THRESHOLD
    assert fires is should_fire, f"{name}: fired={fires}, expected {should_fire}"


def test_control_the_threshold_discriminates_at_all() -> None:
    """A threshold that answers the same way to every input measures nothing."""
    ordinary = Reading.from_classes([P, U, U, P]).longest_unmeasured >= THRESHOLD
    blackout = Reading.from_classes([P, U, U, U, U, U, P]).longest_unmeasured >= THRESHOLD
    assert ordinary is False and blackout is True


def test_control_a_neighbouring_threshold_is_not_equivalent() -> None:
    """If 2 and 3 behaved identically the choice would be arbitrary. 2 fires on the ordinary
    'two in a row', which is core#876's guard working correctly and must stay quiet."""
    two_in_a_row = [P, U, U, P, P]
    assert Reading.from_classes(two_in_a_row).longest_unmeasured >= 2
    assert not Reading.from_classes(two_in_a_row).longest_unmeasured >= THRESHOLD


# ── the surfacing ────────────────────────────────────────────────────────────────────────


def _workflow_doc() -> dict:
    import yaml

    return yaml.safe_load(WATCHDOG.read_text(encoding="utf-8"))


def _step_by_name(prefix: str) -> dict:
    """Address a step by its name, not by position — a step inserted above must not
    silently repoint every assertion at a different step."""
    return next(
        s
        for s in _workflow_doc()["jobs"]["e2e-measurement-gap"]["steps"]
        if str(s.get("name", "")).startswith(prefix)
    )


def _strip_comments(shell: str) -> str:
    """Drop whole-line `#` comments.

    Not a shell parser: a `#` inside a quoted string survives, which is the safe
    direction — it can only make an assertion stricter, never vacuous.
    """
    return "\n".join(line for line in shell.splitlines() if not line.lstrip().startswith("#"))


def _shell_of(step_id: str) -> str:
    """The EXECUTABLE shell of one step, addressed by `id`, with comments removed.

    🚨 core#1260. The first version of the assertions below joined every step's `run` into
    one string and searched it. Two ways that stops discriminating, both of which have now
    happened in this repo:

    * a **comment** containing the expression satisfies the search — Infra's sibling guard
      matched the comment a PR added *explaining why the gate moved*, so it passed across
      the exact edit it existed to notice;
    * a **second job** appended to the file supplies its own copy, so the assertion stays
      green even if the step it is about loses the property entirely.

    Addressing by `id` closes the second; stripping comments closes the first. There is no
    AST for shell-inside-YAML, so `test_control_the_extractor_rejects_a_comment_only_match`
    is what stands in for one.
    """
    step = next(
        s for s in _workflow_doc()["jobs"]["e2e-measurement-gap"]["steps"] if s.get("id") == step_id
    )
    return _strip_comments(str(step.get("run", "")))


class TestItSurfacesSomewhereThatFires:
    @staticmethod
    def _workflow() -> dict:
        return _workflow_doc()

    def test_the_gap_job_lives_in_a_workflow_that_already_fires(self) -> None:
        """🔑 Not a new scheduled workflow.

        A new one is a new thing that can silently never fire — which is the OTHER half of
        core#1256, and putting the detector in one would be building the same defect one level
        up. This file fires daily and is itself watched by the job beside it.
        """
        wf = self._workflow()
        assert "e2e-measurement-gap" in wf["jobs"]
        assert "watchdog" in wf["jobs"], "the gap job must ride the watchdog's proven schedule"
        triggers = wf[True] if True in wf else wf["on"]
        assert "schedule" in triggers

    def test_the_reader_step_cannot_fail_the_job_silently(self) -> None:
        """`continue-on-error` on the reader, so the issue-filing step gets to run and decide.
        Without it the job goes red and the finding never becomes an issue anyone reads —
        which is the same non-required-log grave this issue is about."""
        step = next(
            s
            for s in self._workflow()["jobs"]["e2e-measurement-gap"]["steps"]
            if s.get("id") == "gap"
        )
        assert step.get("continue-on-error") is True

    def test_a_crash_and_a_finding_are_different_signals(self) -> None:
        """core#691, one level down. A crash is also non-zero, so the job asserts a
        POSITIVE artifact — the summary line the reader always prints — before believing
        a finding."""
        body = _shell_of("gap")
        assert "broken.txt" in body, "no separate channel for 'it could not run'"
        assert "grep -q" in body and "unmeasured" in body, (
            "nothing asserts the reader actually produced its summary line, so a crash "
            "and a gap would be one signal"
        )

    def test_not_graduated_is_not_a_finding(self) -> None:
        """Exit 1 means *not yet three greens*, the normal resting state of an
        informational tier. Filing on it would page every day and the guard would be
        switched off within a week."""
        body = _shell_of("gap")
        assert '= "2"' in body, "the job must key on exit 2 specifically, not on any non-zero exit"

    def test_the_filing_gate_covers_the_step_never_running(self) -> None:
        """core#1260, the shape Infra repaired one level up in this same file.

        `== 'failure'` is true only when the step RAN and returned non-zero. If checkout
        or setup-python dies, the outcome is EMPTY, the filing step is SKIPPED, and the
        job ends red having filed nothing — byte-identical to a correct detection,
        because that path also ends red.
        """
        gate = str(_step_by_name("File an issue").get("if", ""))
        assert "!= 'success'" in gate, (
            f"gate is {gate!r}. An equality gate misses the case where the step NEVER "
            "RAN, which is the case that looks exactly like a correct detection."
        )
        assert "cancelled()" in gate, (
            "a cancelled run is neither green nor red and carries no steps in the API; "
            "filing on it would manufacture a finding out of an absence"
        )

    def test_a_run_that_produced_no_artifacts_is_not_reported_as_a_finding(self) -> None:
        """The case the widened gate introduces: the filing step now also runs when the
        reader never executed, so there are no artifacts at all. Filing a blindness issue
        then would be a fabricated finding, and `cat out-*.txt` would print nothing while
        looking like a report."""
        body = _strip_comments(str(_step_by_name("File an issue").get("run", "")))
        assert "! -f blind.txt" in body and "! -f broken.txt" in body, (
            "the filing step does not distinguish 'no artifacts at all' from a finding"
        )


def test_control_the_extractor_rejects_a_comment_only_match() -> None:
    """🔑 The anti-vacuity assertion for the extractor itself.

    Product hit the substring-check defect three times in one day — the third inside the
    guard written to prevent the first two — and only mutation exposed any of them. There
    is no AST for shell-inside-YAML, so this stands in for one: prove the extractor cannot
    be satisfied by a comment, and prove stripping comments does not eat the real line.
    """
    commented = '  # if [ "$rc" = "2" ]; then echo blind; fi\n  echo hello'
    assert '= "2"' not in _strip_comments(commented), (
        "a commented-out gate satisfied the extractor; every assertion built on it would "
        "then be green against code that does not contain the gate at all"
    )

    live = '  if [ "$rc" = "2" ]; then echo blind; fi'
    assert '= "2"' in _strip_comments(live), (
        "stripping comments must not strip the real line — that would make every "
        "assertion vacuously FALSE instead of vacuously true, which is the failure that "
        "gets a guard deleted rather than trusted"
    )


def test_control_the_workflow_file_is_the_one_that_is_watched() -> None:
    """Anti-vacuity: if this file were renamed, every assertion above would pass against a
    workflow nothing schedules."""
    assert WATCHDOG.exists()
    text = WATCHDOG.read_text(encoding="utf-8")
    assert "cron:" in text, "the host workflow no longer carries a schedule"
