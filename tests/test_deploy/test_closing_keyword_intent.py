"""A commit must not close an issue its subject only `refs` (core#1162).

The guard is `scripts/check_closing_keyword_intent.py`. This file pins the properties that decide
whether it is worth having, and the controls are as important as the findings: two heuristics for
this same defect were proposed and **withdrawn by their own authors** after measurement, because
each over-fired on correct commits.

⚠️ **Payloads here are built from `KW + " #" + n`, never written as literal prose.** The reason is
specific rather than fussy: this bug's most reliable carrier is documentation *about* it. Four
separate times in one day someone nearly re-fired the parser inside a document describing it — a
promotion body, a handoff, a reopen comment, and a comment on the issue itself. File contents are
not parsed by GitHub, so a literal here would be harmless; building them keeps the habit intact
where it is not harmless, which is every commit message and PR body that cites this work.

🚨 **No surface is exempt.** A guard that skipped commits "about the parser" would skip exactly
the commits most likely to carry the payload. `test_a_commit_about_this_very_defect_still_fires`
pins that.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scripts.check_closing_keyword_intent import (  # noqa: E402
    CLOSING,
    findings,
    normalise,
    render,
    subject_of,
    weak_refs,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "check_closing_keyword_intent.py"
HOOK = REPO_ROOT / "scripts" / "hooks" / "pre-push"

#: Built, never written as prose. See the module docstring.
CLOSE = "clos" + "es"
NOT_CLOSE = "does not clos" + "e"


def msg(subject: str, *body: str) -> str:
    return "\n\n".join([subject, *body])


# ======================================================================================
# 1. The four measured instances. Every one had a weak keyword in the subject.
# ======================================================================================


class TestTheRealInstances:
    """Reconstructed in the shape the real messages had — same structure, `<n>` numbers."""

    @pytest.mark.parametrize(
        ("label", "body"),
        [
            ("an explicit denial", "Does not clos" + "e #900. The runtime floor is open."),
            ("a denial mid-sentence", "The rule is correct and " + NOT_CLOSE + " #900: it"),
            # ca3fc58f: NOT a denial. Ordinary prose in which a noun precedes a number.
            ("prose, no negation at all", "refuted the fix #900 itself proposes"),
            ("the landing case", "Does not clos" + "e #900 — the remaining captures are the work"),
        ],
    )
    def test_each_measured_shape_is_caught(self, label: str, body: str) -> None:
        found = findings(msg("[QA] Something useful (refs #900)", body))
        assert len(found) == 1, label
        assert found[0].issue == "900"

    def test_the_prose_case_is_why_a_negation_wordlist_cannot_work(self) -> None:
        """`refuted the fix <ref>` contains no negation. A 40-char negation window scored 67%
        recall precisely because it cannot see this one, and it is a real historical instance."""
        body = "refuted the fix #900 itself proposes"
        assert "not" not in body.lower().replace("proposes", "")
        assert len(findings(msg("[QA] x (refs #900)", body))) == 1


# ======================================================================================
# 2. Controls. Infra's four, verbatim in behaviour — the guard must stay quiet on these
# ======================================================================================


class TestItStaysQuietOnCorrectCommits:
    def test_a_genuine_closure_is_not_flagged(self) -> None:
        """Subject declares `closes`; intent and effect agree. This is the ordinary path and
        flagging it is the failure mode that gets a guard switched off."""
        assert findings(msg(f"[QA] Fix the thing ({CLOSE} #900)", "Body text.")) == []

    def test_markdown_between_the_keyword_and_the_ref_is_not_a_closure(self) -> None:
        """GitHub requires them adjacent. A looser separator produced 5 false positives on 400
        commits, two of them on commits *explaining* they were not closing something."""
        assert (
            findings(msg("[QA] x (refs #900)", "Also: ruled **not resolved**. #900 is open.")) == []
        )

    def test_a_refs_only_commit_is_not_flagged(self) -> None:
        assert findings(msg("[QA] x (refs #900)", "This touches it and does not finish it.")) == []

    def test_a_closing_keyword_naming_a_different_issue_is_not_flagged(self) -> None:
        """A commit may legitimately `refs` one issue and close another."""
        assert findings(msg("[QA] x (refs #900)", f"{CLOSE} #901")) == []

    def test_a_subject_with_no_issue_reference_at_all_is_not_flagged(self) -> None:
        assert findings(msg("[QA] A tooling tidy-up", f"{CLOSE} #900")) == []

    def test_the_word_closed_with_no_reference_after_it_is_not_flagged(self) -> None:
        assert (
            findings(msg("[QA] x (refs #900)", "The issue was closed last week by someone.")) == []
        )

    def test_a_subject_that_both_refs_and_closes_the_same_issue_is_flagged(self) -> None:
        """The contradiction is the predicate, wherever in the message it sits."""
        assert len(findings(f"[QA] x (refs #900) — {CLOSE} #900")) == 1

    @pytest.mark.parametrize("prefix", ["core", "landing", "cloud"])
    def test_the_house_prose_form_word_hash_n_is_not_a_closing_reference(self, prefix: str) -> None:
        """🚨 The only two false positives in 400 commits, and they were settled EMPIRICALLY.

        A first draft accepted `word#N`. That is this project's prose convention for naming an
        issue in another repository — GitHub's cross-repo syntax is `owner/repo#N`, so a bare
        `core#N` is plain text and closes nothing. Rather than reason about it, I read the two
        flagged issues' timelines: **both were closed BY HAND, no commit attributed.** GitHub had
        demonstrably not acted on `fixes core#<n>` or `Closes core#<n>`.

        A guard firing here would flag correct commits written in the house style — the
        over-firing that got two earlier heuristics withdrawn.
        """
        assert findings(msg("[QA] x (refs #900)", f"{CLOSE} {prefix}#900 item 1")) == []

    def test_a_fully_qualified_cross_repo_reference_is_a_closing_reference(self) -> None:
        """Control for the test above: the tightening must not lose the form GitHub does act on.
        Without this, `word#N` and `owner/repo#N` could both be rejected and the suite would
        still be green."""
        body = f"{CLOSE} datanika-io/datanika-core#900"
        assert len(findings(msg("[QA] x (refs #900)", body))) == 1


# ======================================================================================
# 3. Escaping RE-POINTS the parser. The finding that makes normalisation load-bearing
# ======================================================================================


class TestEscapingIsNotADisarm:
    @pytest.mark.parametrize("esc", ["&#35;", "&#035;", "&#x23;", "&num;", "%23"])
    def test_an_escaped_hash_is_folded_back_before_the_grammar_runs(self, esc: str) -> None:
        body = "Does not clos" + "e " + esc + "900."
        assert len(findings(msg("[QA] x (refs #900)", body))) == 1, (
            f"{esc} hid the reference; escaping must not disarm the check"
        )

    def test_escaping_does_two_different_things_and_only_one_was_predicted(self) -> None:
        """🚨 Measured, and it CORRECTS the finding as it was handed to me.

        The report was *"escaping does not disarm the parser — it re-points it"*. Measured on the
        real strings, escaping does **both**, to two different scans, in opposite directions:

        * the **closing-keyword** grammar stops matching entirely — the `&` sits between the
          keyword and the hash, so an escaped denial closes nothing **and evades this guard**;
        * a **bare-reference** scan — which is what the promotion-refs tooling runs — reads the
          entity's own `#` + digits and returns an **unrelated issue**.

        So the conclusion survives and the mechanism is sharper: escaping is not a repair. It
        silences the closure while planting a reference to an issue nobody meant, in the one
        document (a promotion body) whose generated ref list people trust *because* it is
        mechanical.
        """
        raw = "Does not clos" + "e &#35;900."

        # 1. the closing grammar: escaped -> nothing at all; folded -> the real reference
        assert [m.group(2) for m in CLOSING.finditer(raw)] == [], (
            "escaped, the closing grammar matches nothing — which is why folding must come first"
        )
        assert [m.group(2) for m in CLOSING.finditer(normalise(raw))] == ["900"]

        # 2. a bare-reference scan: escaped -> an UNRELATED issue; folded -> the real one
        bare = re.compile(r"(?<![A-Za-z0-9_/-])#(\d+)")
        assert bare.findall(raw) == ["35"], "the entity's own hash names a different issue"
        assert bare.findall(normalise(raw)) == ["900"]

    def test_normalising_is_what_stops_an_escape_evading_this_guard(self) -> None:
        """The consequence of the test above, stated as the property the guard needs."""
        escaped = msg("[QA] x (refs #900)", "Does not clos" + "e &#35;900.")
        assert len(findings(escaped)) == 1, "an escaped denial must not slip past"

    def test_the_error_message_never_recommends_escaping(self) -> None:
        """An author who follows a 'just escape it' hint retires an unrelated issue."""
        text = render(findings(msg("[QA] x (refs #900)", "Does not clos" + "e #900.")), "x")
        assert "escape" in text.lower(), "it must warn about escaping"
        for bad in ("&#35;", "&num;", "%23", "use an entity", "escape the hash instead"):
            assert bad not in text, f"the message suggests {bad!r} as a remedy"


# ======================================================================================
# 4. The message is aimed at the mechanism, not at the author
# ======================================================================================


class TestTheMessageIsNotAboutDiscipline:
    @staticmethod
    def _text() -> str:
        return render(findings(msg("[QA] x (refs #900)", "Does not clos" + "e #900.")), "abc123")

    def test_it_names_the_line_and_the_phrase(self) -> None:
        t = self._text()
        assert "line 3" in t
        assert "Does not clos" + "e #900." in t

    def test_it_offers_a_rewrite_rather_than_an_instruction_to_be_careful(self) -> None:
        t = self._text().lower()
        assert "paraphrase" in t and "elide" in t
        for scold in ("be careful", "you should have", "carelessness is", "double-check"):
            assert scold not in t, f"the message scolds: {scold!r}"

    def test_it_says_the_author_did_nothing_wrong(self) -> None:
        """In every measured instance the author knew and said so beside the reference. A guard
        aimed at discipline is aimed at the wrong thing."""
        assert "carelessness" in self._text().lower()

    def test_it_states_the_deliberate_exception(self) -> None:
        assert "closes" in self._text()


# ======================================================================================
# 5. No surface is exempt — the highest-risk documents are documents about this bug
# ======================================================================================


class TestNoExemptions:
    def test_a_commit_about_this_very_defect_still_fires(self) -> None:
        """Four times in one day someone nearly re-fired the parser inside a document about it.
        An exemption for 'commits about the parser' would skip exactly those."""
        m = msg(
            "[QA] Guard the closing-keyword parser (refs #900)",
            "Explaining the bug: a message reading 'does not clos" + "e #900' closes it anyway.",
        )
        assert len(findings(m)) == 1

    def test_the_guard_does_not_special_case_any_path_or_author(self) -> None:
        src = SCRIPT.read_text(encoding="utf-8")
        for exemption in ("skip_if", "ALLOWLIST", "EXEMPT", "--no-verify", "if author"):
            assert exemption not in src, f"the guard carries an exemption: {exemption}"


# ======================================================================================
# 6. Arming — the predicate must be shown able to stay silent AND able to fire
# ======================================================================================


class TestTheHelpersDiscriminate:
    def test_weak_refs_reads_a_run_of_references(self) -> None:
        """core#1168's shape: `refs #A, #B`. Missing the second is a silent half-coverage."""
        assert weak_refs("[Product] x (refs #519, #508)") == {"519", "508"}

    def test_weak_refs_is_empty_on_a_closing_subject(self) -> None:
        assert weak_refs(f"[QA] x ({CLOSE} #900)") == set()

    def test_subject_of_takes_only_the_first_line(self) -> None:
        assert subject_of("first\n\nsecond") == "first"

    def test_normalise_is_a_no_op_on_ordinary_text(self) -> None:
        """A normaliser that rewrote ordinary text would manufacture references."""
        plain = "Does not clos" + "e #900. Nothing to fold."
        assert normalise(plain) == plain


# ======================================================================================
# 7. The guard runs, end to end, and is wired into the hook
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

    def test_exit_1_on_a_contradiction(self, tmp_path: Path) -> None:
        p = self._run(msg("[QA] x (refs #900)", "Does not clos" + "e #900."), tmp_path)
        assert p.returncode == 1
        assert b"REFUSED" in p.stdout

    def test_exit_0_on_a_genuine_closure(self, tmp_path: Path) -> None:
        p = self._run(msg(f"[QA] x ({CLOSE} #900)", "Body."), tmp_path)
        assert p.returncode == 0, p.stdout.decode("utf-8", "replace")

    def test_an_unreadable_range_exits_2_and_is_not_a_pass(self) -> None:
        """A range that cannot be read measured nothing. Exit 0 there would be a check that
        passes whenever it is broken."""
        p = subprocess.run(  # noqa: S603
            [sys.executable, str(SCRIPT), "--range", "no-such-ref..no-such-ref-either"],
            capture_output=True,
            check=False,
            cwd=str(REPO_ROOT),
        )
        assert p.returncode == 2

    def test_the_pre_push_hook_actually_invokes_it(self) -> None:
        """A guard nothing runs is the original bug one level up, and looks identical to a fix."""
        hook = HOOK.read_text(encoding="utf-8")
        lines = [ln.strip() for ln in hook.splitlines() if not ln.strip().startswith("#")]
        assert any("check_closing_keyword_intent.py" in ln for ln in lines), (
            "the pre-push hook does not invoke the guard"
        )

    def test_the_hook_acts_on_the_result_rather_than_only_running_it(self) -> None:
        """Assert branch-then-action: three guards in this repo survived a mutation that
        disabled the branch while leaving its body intact."""
        lines = [ln.strip() for ln in HOOK.read_text(encoding="utf-8").splitlines()]
        idx = next(
            i
            for i, ln in enumerate(lines)
            if "check_closing_keyword_intent.py" in ln and not ln.startswith("#")
        )
        window = "\n".join(lines[idx : idx + 14])
        assert "exit 1" in window, f"the hook runs the guard but does not fail on it:\n{window}"
