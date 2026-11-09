"""Attribution trailers must be refused BEFORE the commit reaches origin (core#1385).

The founder's standing ruling (WORKFLOW_RULES, top block) is that no commit or PR body carries a
`Co-Authored-By` or `Claude-Session` trailer. Every department reported an **armed** check on
every commit all week, with genuine controls -- a planted trailer returned 1, the real commit
returned 0 -- and five commits still reached shared branches carrying one.

🔑 **The pattern was never the defect. The placement was.** Engineering measured its own breach
and named it: the scan ran *after* publishing. An armed check that runs after the artifact leaves
is a post-mortem with good manners. The pre-push hook is the only thing in this project that sees
a commit before `origin` does.

⚠️ **Payloads here are BUILT, never written as literal trailer lines**, exactly as
``test_closing_keyword_intent.py`` builds its closing keywords. This file, the guard, and the
commit that ships them are all documents *about* the trailers -- the highest-risk carriers there
are. A literal in a test file is harmless; keeping the habit intact is what makes the habit
survive into the commit message, where it is not.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.check_attribution_trailers import (  # noqa: E402
    ARTIFACTS,
    CONTROL_SAMPLES,
    TRAILER,
    findings,
    render,
    summarise,
    summarise_blank,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "check_attribution_trailers.py"
HOOK = REPO_ROOT / "scripts" / "hooks" / "pre-push"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "attribution.yml"

#: Built, never written as prose. See the module docstring.
COAUTH = "Co-Authored" + "-By"
SESSION = "Claude" + "-Session"


def msg(subject: str, *body: str) -> str:
    return "\n\n".join([subject, *body])


def _yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _on(doc: dict) -> dict:
    """PyYAML resolves the bare key ``on`` to the boolean ``True`` (YAML 1.1). GitHub does not."""
    raw = doc.get("on", doc.get(True))
    return raw if isinstance(raw, dict) else {}


def _step_bodies(path: Path) -> list[dict]:
    return [s for job in (_yaml(path).get("jobs") or {}).values() for s in (job.get("steps") or [])]


def _run_blocks(path: Path) -> list[str]:
    """Only what a shell actually executes. A checker named in a comment gates nothing."""
    return [str(s["run"]) for s in _step_bodies(path) if s.get("run")]


# ======================================================================================
# 1. Controls first — the pattern must be shown able to FIRE, not merely to stay quiet
# ======================================================================================


class TestTheDetectorDiscriminates:
    def test_control_the_pattern_sees_a_planted_coauthor_trailer(self) -> None:
        assert len(findings(msg("[Infra] x", f"{COAUTH}: A B <a@b.c>"))) == 1

    def test_control_the_pattern_sees_a_planted_session_trailer(self) -> None:
        assert len(findings(msg("[Infra] x", f"{SESSION}: https://example.test/s/1"))) == 1

    def test_control_a_clean_message_reads_zero(self) -> None:
        """The reading in doubt. It means nothing without the two controls above it --
        a grep returning 0 is exactly what a broken pattern also returns."""
        assert findings(msg("[Infra] Ordinary work (refs #1)", "A body with no trailers.")) == []

    def test_both_trailers_in_one_message_are_both_reported(self) -> None:
        m = msg("[Infra] x", f"{COAUTH}: A B <a@b.c>\n{SESSION}: https://example.test/s/1")
        assert len(findings(m)) == 2


class TestItDoesNotFireOnProseAboutTheRule:
    """🚨 The false positive that would get this guard switched off.

    The commits most likely to *discuss* the trailers are the ones implementing this gate. A
    guard that refuses a commit message explaining the rule is a guard people route around.
    The predicate is structural: a trailer is a line-anchored ``Key: value`` with a value.
    """

    def test_a_backticked_mention_is_not_a_trailer(self) -> None:
        assert findings(msg("[Infra] x", f"Never add `{COAUTH}` lines to a commit.")) == []

    def test_a_mid_line_mention_is_not_a_trailer(self) -> None:
        assert findings(msg("[Infra] x", f"The {SESSION} line is refused by the hook.")) == []

    def test_a_key_with_no_value_is_not_a_trailer(self) -> None:
        """A heading-like line naming the key is prose, not an attribution."""
        assert findings(msg("[Infra] x", f"{COAUTH}:")) == []

    def test_a_list_item_naming_the_key_is_not_a_trailer(self) -> None:
        assert findings(msg("[Infra] x", f"- {COAUTH}: refused, see the hook")) == []


# ======================================================================================
# 2. The verdict must carry the control's reading BESIDE it
# ======================================================================================


class TestTheVerdictCarriesItsOwnControl:
    """🔑 The requirement that makes a clean reading mean something at the moment it is read.

    Knowing a failure mode confers no immunity; a mechanism does. What saved this project's
    vacuous-check incident was printing the population size on the line above the answer. A
    reader of the hook's output must be able to see, without leaving the terminal, that the
    scanner could have found something and that it looked at a non-zero number of commits.
    """

    def test_the_clean_summary_states_both_control_readings(self) -> None:
        text = summarise(scanned=3, found=0)
        assert "controls" in text.lower()
        for label in (COAUTH, SESSION):
            assert label in text, f"the verdict does not show the {label} control's reading"
        assert "1" in text, "the control readings are not shown as numbers"

    def test_the_clean_summary_states_the_population_size(self) -> None:
        text = summarise(scanned=7, found=0)
        assert "7" in text, "the verdict does not say how many commits it read"

    def test_a_zero_population_says_so_rather_than_reading_as_clean(self) -> None:
        """🚨 Scanning nothing and scanning something clean must not print the same thing.
        A range that selected no commits measured nothing."""
        empty = summarise(scanned=0, found=0)
        clean = summarise(scanned=4, found=0)
        assert empty != clean
        assert "0 commits" in empty or "no commits" in empty.lower()

    def test_control_the_samples_really_do_trip_the_pattern(self) -> None:
        """Anti-vacuity for the summary: the numbers it prints must be measured, not typed.

        If ``CONTROL_SAMPLES`` ever stopped matching, ``summarise`` would print `0` beside a
        clean verdict -- which is the honest output, and this test is what makes that visible
        rather than leaving the guard quietly disarmed.
        """
        assert CONTROL_SAMPLES, "there are no control samples; the verdict's control is fiction"
        for sample in CONTROL_SAMPLES:
            assert TRAILER.search(sample), f"control sample no longer trips the pattern: {sample!r}"


# ======================================================================================
# 3. The refusal text
# ======================================================================================


class TestTheRefusal:
    @staticmethod
    def _text() -> str:
        return render(findings(msg("[Infra] x", f"{COAUTH}: A B <a@b.c>")), "abc1234")

    def test_it_names_the_line_and_the_key(self) -> None:
        t = self._text()
        assert "line 3" in t
        assert COAUTH in t

    def test_it_names_the_ruling_rather_than_scolding(self) -> None:
        t = self._text()
        assert "founder" in t.lower()
        for scold in ("be careful", "you should have", "double-check"):
            assert scold not in t.lower(), f"the message scolds: {scold!r}"

    def test_it_says_the_harness_notice_does_not_override_the_ruling(self) -> None:
        """The author is usually complying with a harness instruction that claims to supersede
        the project rule. A refusal that does not address that leaves them with two orders."""
        assert "harness" in self._text().lower()

    def test_every_byte_of_the_refusal_is_ascii(self) -> None:
        """The cp1251 lesson, from the closing-keyword guard on this same hook: ``render()`` is
        called ONLY on the refusal path, so a non-ASCII byte there crashes the guard at exactly
        the moment it has news, printing a traceback instead of the message."""
        offenders = sorted({hex(ord(c)) for c in self._text() if ord(c) > 127})
        assert not offenders, f"the refusal carries non-ASCII codepoints {offenders}"

    def test_control_the_ascii_check_can_see_a_non_ascii_character(self) -> None:
        assert [hex(ord(c)) for c in "a⚠b" if ord(c) > 127] == ["0x26a0"]


# ======================================================================================
# 4. It runs end to end, and the hook actually invokes and ACTS ON it
# ======================================================================================


class TestItRunsAndIsWired:
    def _run(self, message: str, tmp_path: Path) -> subprocess.CompletedProcess:
        f = tmp_path / "msg.txt"
        f.write_bytes(message.encode("utf-8"))
        return subprocess.run(  # noqa: S603
            [sys.executable, str(SCRIPT), "--message-file", str(f)],
            capture_output=True,
            check=False,
        )

    def test_exit_1_on_a_trailer(self, tmp_path: Path) -> None:
        p = self._run(msg("[Infra] x", f"{SESSION}: https://example.test/s/1"), tmp_path)
        assert p.returncode == 1
        assert b"REFUSED" in p.stdout

    def test_exit_0_on_a_clean_message(self, tmp_path: Path) -> None:
        p = self._run(msg("[Infra] x", "Nothing to see."), tmp_path)
        assert p.returncode == 0, p.stdout.decode("utf-8", "replace")

    def test_an_empty_but_readable_range_exits_2_and_is_not_a_pass(self) -> None:
        """🚨 The message said "this measured NOTHING" and the exit code said "clean".

        Found by RUNNING the `merge_group` branch's command shape rather than asserting its
        text: `--range <sha>..<sha>` printed *"read 0 commits -- this measured NOTHING"* and
        returned **0**. In the hook that is survivable, because a human reads the message and
        the hook has a `-eq 2` warning branch waiting for it. **In a workflow the exit code
        is the only consumer**, so the step went green on a scan of nothing — which is the
        exact defect this whole file is about, shipped inside the fix for it.

        ``test_a_zero_population_says_so_rather_than_reading_as_clean`` above compares the
        two *messages* and passes either way. Nothing asserted the code. That is the gap.
        """
        p = subprocess.run(  # noqa: S603
            [sys.executable, str(SCRIPT), "--range", "HEAD..HEAD"],
            capture_output=True,
            check=False,
            cwd=str(REPO_ROOT),
        )
        assert b"NOTHING" in p.stdout, p.stdout
        assert p.returncode == 2, (
            "an empty range scanned nothing and said so, then exited 0 -- so every consumer "
            "that reads the code rather than the text scores it as clean"
        )

    def test_an_unreadable_range_exits_2_and_is_not_a_pass(self) -> None:
        p = subprocess.run(  # noqa: S603
            [sys.executable, str(SCRIPT), "--range", "no-such-ref..also-not-real"],
            capture_output=True,
            check=False,
            cwd=str(REPO_ROOT),
        )
        assert p.returncode == 2, (
            "an unreadable range measured nothing; exit 0 there is a check that passes "
            "whenever it is broken"
        )

    def test_the_pre_push_hook_actually_invokes_it(self) -> None:
        """A guard nothing runs is the original defect one level up, and looks identical to a
        fix. This is the assertion that separates 'the scan exists' from 'the scan gates'."""
        lines = [
            ln.strip()
            for ln in HOOK.read_text(encoding="utf-8").splitlines()
            if not ln.strip().startswith("#")
        ]
        assert any("check_attribution_trailers.py" in ln for ln in lines), (
            "the pre-push hook does not invoke the trailer guard"
        )

    def test_the_hook_acts_on_the_result_rather_than_only_running_it(self) -> None:
        lines = [ln.strip() for ln in HOOK.read_text(encoding="utf-8").splitlines()]
        idx = next(
            i
            for i, ln in enumerate(lines)
            if "check_attribution_trailers.py" in ln and not ln.startswith("#")
        )
        window = "\n".join(lines[idx : idx + 14])
        assert "exit 1" in window, f"the hook runs the guard but does not fail on it:\n{window}"

    def test_the_guard_carries_no_exemption(self) -> None:
        src = SCRIPT.read_text(encoding="utf-8")
        for exemption in ("ALLOWLIST", "EXEMPT", "skip_if", "if author"):
            assert exemption not in src, f"the guard carries an exemption: {exemption}"

    def test_it_runs_before_the_path_gate_can_skip_anything(self) -> None:
        """A trailer has nothing to do with which files changed.

        `should-run-tests.sh` can legitimately skip pytest for a docs-only push -- and a
        docs-only commit carries a trailer just as easily as a code one. The guard must sit
        above that gate in the hook, or the cheapest commits are the unchecked ones.
        """
        text = HOOK.read_text(encoding="utf-8")
        guard_at = text.index("check_attribution_trailers.py")
        gate_at = text.index("should-run-tests.sh")
        assert guard_at < gate_at, (
            "the trailer guard runs after the path-aware test gate, so a docs-only push can "
            "reach origin unscanned"
        )


# ======================================================================================
# 5. The OTHER artifact the ruling names: a PR body. A commit-range gate cannot see one.
# ======================================================================================


class TestEveryArtifactTheRulingNamesHasACarrier:
    """🔑 The assertion that was missing, and it is an INVARIANT rather than an instance.

    The ruling names two artifacts: a commit message and a PR body. Both this file's
    docstring and the module's said so -- *"no commit or PR body carries a..."* -- while
    only the commit half had anything enforcing it. **A guard asserting a scope it does
    not have reads exactly like coverage** for as long as nobody asks which file enforces
    which half.

    Measured on ``dev`` @ ``437d5854`` before this class existed: the checker was invoked
    by ``scripts/hooks/pre-push`` and by nothing else; ``grep -rn`` over ``.github/``
    returned **0**, with the same search shape finding
    ``check_promotion_closing_refs`` in ``promotion-pr-refs.yml`` as the control. The
    ``--stdin`` mode was already there and nothing fed it.

    So the fix was to build the missing carrier, not to narrow the claim. This asserts
    *a mechanism per artifact*, which stays true however either mechanism is implemented
    -- and goes red the moment a third artifact is added to the ruling with nothing
    behind it.
    """

    def test_the_checker_knows_exactly_the_artifacts_the_ruling_names(self) -> None:
        assert set(ARTIFACTS) == {"commit", "pr-body"}, (
            "the ruling names a commit message and a PR body; the checker's artifact table "
            "must name the same two, because the remedy it prints depends on which one"
        )

    def test_the_commit_carrier_is_the_pre_push_hook(self) -> None:
        assert "check_attribution_trailers.py" in HOOK.read_text(encoding="utf-8")

    def test_the_pr_body_carrier_exists_at_all(self) -> None:
        assert WORKFLOW.exists(), (
            f"{WORKFLOW.relative_to(REPO_ROOT)} is absent, so nothing mechanical reads a PR "
            "body. A pre-push hook is handed a commit range and a PR body is not in one, so "
            "the hook cannot be this carrier however it is configured."
        )

    def test_the_pr_body_carrier_invokes_the_checker_on_the_body(self) -> None:
        """Presence of the right thing, not absence of a wrong word (§5b rule 3).

        A workflow that merely *mentions* the checker satisfies nothing. The three things
        that make it a carrier are the script, the mode that accepts a body, and the
        artifact selector that makes the refusal print the PR-body remedy instead of
        sending the author to rebase a commit that is not the problem.
        """
        run_blocks = _run_blocks(WORKFLOW)
        assert any("check_attribution_trailers.py" in b for b in run_blocks), (
            "the workflow names the checker only in prose; no `run:` step executes it"
        )
        body_step = next((b for b in run_blocks if "--artifact pr-body" in b), None)
        assert body_step is not None, (
            "no step scans with `--artifact pr-body`, so either the body is not scanned or "
            "its refusal prints the commit remedy (`git commit --amend`) to someone whose "
            "commits are fine"
        )
        assert "--stdin" in body_step, "the body must reach the checker through --stdin"
        assert "PR_BODY" in body_step, "the step does not read the body it was handed"


class TestThePrBodyCarrierIsWiredSAFELY:
    """The three ways this workflow could be wrong without ever going red."""

    def test_it_triggers_on_edited_not_only_on_opened(self) -> None:
        """The reflex trailer is typed when the body is WRITTEN, and a body is routinely
        rewritten after opening. Without ``edited`` the first save is the only one read,
        and a trailer added in the second is never seen by anything."""
        doc = _yaml(WORKFLOW)
        types = ((_on(doc).get("pull_request") or {}).get("types")) or []
        assert "edited" in types, f"pull_request types are {types!r}; a rewritten body is unread"

    def test_the_body_reaches_the_step_through_env_never_through_run_interpolation(self) -> None:
        """🚨 A PR body is attacker-controlled text on a PUBLIC repo.

        ``${{ github.event.pull_request.body }}`` inside a ``run:`` script is a
        command-injection sink -- the expression is substituted into the shell source
        before bash sees it, so a body containing a backtick or ``$(...)`` executes on the
        runner. Through ``env:`` it is data.
        """
        for block in _run_blocks(WORKFLOW):
            assert "pull_request.body" not in block, (
                "the PR body is interpolated into a `run:` script, which executes it:\n" + block
            )
        assert "PR_BODY: ${{ github.event.pull_request.body }}" in WORKFLOW.read_text(
            encoding="utf-8"
        ), "the body does not reach the step through env: at all"

    def test_control_the_injection_detector_can_see_an_interpolated_body(self) -> None:
        """Anti-vacuity for the test above: it asserts an absence, so it has to be shown
        able to find the thing it bans. A detector that matches nothing passes on a
        workflow that does exactly what it forbids."""
        planted = 'run: |\n  echo "${{ github.event.pull_request.body }}"\n'
        assert "pull_request.body" in planted

    def test_each_event_scans_the_artifact_that_event_actually_carries(self) -> None:
        """A ``merge_group`` payload has no pull request, so it has no body.

        The workflow must trigger on ``merge_group`` (invariant 1 of
        ``test_merge_queue_triggers``: without it a required version of this check
        produces no run for a queue entry, and the entry is ejected after 30 minutes with
        nothing red). The honest way to satisfy that is not a no-op green on the event
        that governs the merge -- it is to scan the artifact that *does* exist there, the
        entry's commit range. So each branch is gated on its own event, and neither
        reports on the other's artifact.
        """
        doc = _yaml(WORKFLOW)
        assert "merge_group" in _on(doc), "a queue entry would get no run from this workflow"
        body = next(b for b in _step_bodies(WORKFLOW) if "--artifact pr-body" in str(b.get("run")))
        rng = next(b for b in _step_bodies(WORKFLOW) if "--range" in str(b.get("run")))
        assert "pull_request" in str(body.get("if")), "the body scan is not gated to its own event"
        assert "merge_group" in str(rng.get("if")), "the range scan is not gated to its own event"


# ======================================================================================
# 6. The PR-body scan, end to end — and the failure mode a hook does not have
# ======================================================================================


class TestThePrBodyScanDiscriminates:
    @staticmethod
    def _run(body: str) -> subprocess.CompletedProcess:
        return subprocess.run(  # noqa: S603
            [
                sys.executable,
                str(SCRIPT),
                "--stdin",
                "--artifact",
                "pr-body",
                "--label",
                "PR #1 body",
                "--context",
                "attribution.yml",
            ],
            input=body.encode("utf-8"),
            capture_output=True,
            check=False,
        )

    def test_a_planted_trailer_in_a_body_exits_1(self) -> None:
        p = self._run("Some PR body.\n\n" + f"{COAUTH}: A B <a@b.c>\n")
        assert p.returncode == 1, p.stdout.decode("utf-8", "replace")
        assert b"REFUSED" in p.stdout

    def test_a_clean_body_exits_0(self) -> None:
        """The reading in doubt. It means nothing without the test above it, in the same
        class, driven through the same entry point."""
        p = self._run("Some PR body with no trailers.\n\nrefs #1\n")
        assert p.returncode == 0, p.stdout.decode("utf-8", "replace")

    def test_a_blank_body_exits_2_rather_than_reading_as_clean(self) -> None:
        """🚨 The failure mode a workflow has and a hook does not.

        The hook derives its own input from ``git log``. A workflow is *handed* the body by
        the platform, so a step that silently receives an empty string is
        indistinguishable from a PR whose body is clean -- and ``exit 0`` cannot tell a
        reader which they have. One is a finding about the plumbing; the other is a pass.

        This is not new policy: it is the module's existing rule for an empty range
        (*"a range that selected no commits measured nothing"*) applied to the other input
        it accepts.
        """
        for blank in ("", "\n", "   \n\t\n"):
            p = self._run(blank)
            assert p.returncode == 2, (
                f"a blank body ({blank!r}) exited {p.returncode}; a check that passes when it "
                "receives nothing passes precisely when it is broken"
            )
            assert b"NOTHING" in p.stdout, p.stdout

    def test_the_hooks_own_verdict_still_names_the_hook(self) -> None:
        """The other end of the same property, and it exists because a mutation found the gap.

        Parameterising ``context`` creates two ways to get it wrong: the workflow's verdict
        can claim to be the hook's, and the hook's can stop saying where it ran. A mutation
        of the *default* survived the test below -- correctly, because the workflow always
        passes ``--context`` explicitly, so the default is on a path that test never drives.
        """
        assert "pre-push" in summarise(scanned=1, found=0)

    def test_the_verdict_names_where_it_ran_and_what_it_read(self) -> None:
        """``pre-push`` used to be hardcoded in the summary, so a workflow's output claimed
        to be a hook's. *Where* a check ran is the first thing a reader needs when deciding
        whether a clean line covers the artifact they care about."""
        p = self._run("A clean body.\n")
        out = p.stdout.decode("utf-8", "replace")
        assert "attribution.yml" in out, out
        assert "pre-push" not in out, f"the workflow's verdict claims to be the hook's:\n{out}"
        assert "PR body" in out, out


class TestTheRefusalSendsThePrBodyAuthorSomewhereThatEXISTS:
    @staticmethod
    def _text() -> str:
        found = findings(msg("A PR body", f"{COAUTH}: A B <a@b.c>"))
        return render(found, "PR #1 body", artifact="pr-body")

    def test_it_offers_a_pr_edit_rather_than_a_rebase(self) -> None:
        """Telling the author of a PR body to run ``git commit --amend`` sends them to
        rewrite a commit that is not the problem -- and on a branch already pushed, that is
        a force-push for nothing."""
        t = self._text()
        assert "gh pr edit" in t, t
        for wrong in ("git commit --amend", "git rebase -i"):
            assert wrong not in t, f"the PR-body refusal prescribes {wrong!r}:\n{t}"

    def test_the_commit_refusal_still_offers_the_commit_remedy(self) -> None:
        """The other half of the same control: a change that fixed the PR-body remedy by
        deleting the remedy block would pass the test above."""
        t = render(findings(msg("[QA] x", f"{COAUTH}: A B <a@b.c>")), "abc1234")
        assert "git commit --amend" in t
        assert "gh pr edit" not in t

    def test_it_names_the_artifact_it_found_the_trailer_in(self) -> None:
        assert "PR body" in self._text()

    def test_every_byte_of_the_pr_body_refusal_is_ascii(self) -> None:
        """``render`` is reached ONLY on the refusal path, so a non-ASCII byte in the new
        branch crashes the guard at exactly the moment it has news. The existing ASCII test
        covers the commit branch only, and a second branch through the same function is a
        second chance to ship that bug."""
        offenders = sorted({hex(ord(c)) for c in self._text() if ord(c) > 127})
        assert not offenders, f"the PR-body refusal carries non-ASCII codepoints {offenders}"

    def test_every_byte_of_the_blank_verdict_is_ascii(self) -> None:
        text = summarise_blank(label="PR #1 body", context="attribution.yml")
        offenders = sorted({hex(ord(c)) for c in text if ord(c) > 127})
        assert not offenders, f"the blank verdict carries non-ASCII codepoints {offenders}"

    def test_the_blank_verdict_is_distinguishable_from_a_clean_one(self) -> None:
        blank = summarise_blank(label="PR #1 body", context="attribution.yml")
        clean = summarise(scanned=1, found=0, context="attribution.yml", noun="PR body")
        assert blank != clean
        assert "NOTHING" in blank and "NOTHING" not in clean
