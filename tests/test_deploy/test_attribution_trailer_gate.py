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

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.check_attribution_trailers import (  # noqa: E402
    CONTROL_SAMPLES,
    TRAILER,
    findings,
    render,
    summarise,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "check_attribution_trailers.py"
HOOK = REPO_ROOT / "scripts" / "hooks" / "pre-push"

#: Built, never written as prose. See the module docstring.
COAUTH = "Co-Authored" + "-By"
SESSION = "Claude" + "-Session"


def msg(subject: str, *body: str) -> str:
    return "\n\n".join([subject, *body])


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
