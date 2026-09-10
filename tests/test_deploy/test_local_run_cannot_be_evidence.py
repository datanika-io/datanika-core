"""A local E2E run must be unable to produce anything the readers take as evidence (core#1232).

Staging is the only place the E2E suite can run today, and contending for it is what produced
*"7 of 10 runs measured nothing"* and cost a round on core#1209. Per-agent local stacks remove
that contention — and introduce a failure worse than the one they remove.

🚨 **A local green that can advance a streak is a false verdict with a build behind it**, which
is harder to disbelieve than a false verdict with nothing behind it. Staging runs the real
image behind real Apache with real blue/green; a local stack runs a different image on a
different network, and has been measured at ~1.9x CI for reasons unrelated to the code
(core#961). Everything about a local run can be green while nothing about it describes
production.

The two readers — `scripts/e2e_tier_streak.py` (graduation) and
`scripts/verify_e2e_attribution.py` (the promotion pre-flight) — decide by **matching a text
pattern**. The line says *what* a tier concluded; it has never said *where the tier ran*. That
was safe only while the sole producer was a workflow step and the sole consumer path was
`gh run view --log`. Local runs end that, so this file installs the missing half.

Two guards, failing closed in opposite directions — **neither is sufficient alone**:

* **on presence** — `E2E_ENVIRONMENT=<anything but staging>` in a log is a VETO. It is read
  before any verdict line and beats one that appears *after* it saying `clean`. Local stacks
  make that ordering ordinary: the harness prints where it ran, then the suite prints its
  result.
* **on absence** — nothing outside `.github/workflows/` may *print* a verdict marker at all. A
  silent log therefore cannot have come from a local runner, which is what lets the veto stay
  silent about the entire pre-core#1232 history instead of reddening it.

⚠️ Measured before it was written, because an over-firing guard gets switched off (core#1162
put a proposed one at 29% precision; core#1168's at 40.2%): across **848 tracked files** the
three markers appear in **8**, of which **3** are under an executable path and all three are
comments. The allowlist below is that measurement, not a guess.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from scripts.e2e_tier_streak import (  # noqa: E402
    CI_ENVIRONMENT,
    ENVIRONMENT_MARKER,
    LOCAL,
    LOCAL_VERDICT,
    LOCAL_VERDICTS,
    PASS,
    Reading,
    attested_environment,
    classify_verdict,
    environment_attestation,
    parse_verdict_line,
    streak,
)
from scripts.verify_e2e_attribution import Job, classify  # noqa: E402

#: The literal prefix each tier's classifier prints. **Prefixes, not the parsed patterns.**
#: `parse_verdict_line`'s regexes require a literal token where the shell source has `$STATE`,
#: so the workflow's own `echo` does not match them — only the executed log line does. A static
#: guard built on the parsed pattern would scan the repo, find nothing, and pass forever.
MARKERS = {
    "gating": "gating step outcome:",
    "sso": "SSO specs outcome:",
    "informational": "INFORMATIONAL_RESULT=",
    # core#1221: graduation is per spec, so there is now a per-spec marker too. Registering
    # it here is not paperwork — an unregistered marker is one a local runner may print
    # freely, which is this file's whole subject.
    "informational_spec": "INFORMATIONAL_SPEC_RESULT=",
}

#: Emitters that legitimately print a marker from OUTSIDE `.github/workflows/`.
#:
#: The workflow exclusion below assumed every emitter lived in a workflow. core#1221 broke
#: that assumption on purpose: the per-spec verdict is computed by reading a JSON report,
#: which is a program, and a program parked in a `run:` block is a recipe in prose
#: (core#1197). So the exclusion is now a NAMED set — and
#: :func:`test_every_sanctioned_emitter_is_actually_invoked_by_a_workflow` gives it a
#: property rather than leaving it an allowlist: an emitter no workflow runs is a script
#: that can print evidence and never does, which is indistinguishable from a local runner.
SANCTIONED_EMITTERS = frozenset({"e2e/scripts/informational_spec_results.py"})

#: Files that may mention a marker, measured 2026-09-09. Two emit them; the rest parse, test or
#: document them. Adding an entry is deliberate — which is the property, not the paperwork.
MARKER_ALLOWLIST = frozenset(
    {
        ".github/workflows/ci.yml",  # emits: SSO specs outcome
        ".github/workflows/staging.yml",  # emits: gating + INFORMATIONAL_RESULT
        "scripts/e2e_tier_streak.py",  # the parser
        "tests/test_deploy/test_e2e_tier_streak.py",  # its tests
        "tests/test_deploy/test_image_probe.py",  # asserts exactly one owner
        "tests/test_deploy/test_local_run_cannot_be_evidence.py",  # this file
        "docs/QA_RULES.md",  # the tier policy
        "e2e/tests/a11y-sweep.spec.ts",  # comment: read the marker, not the tick
        "e2e/tests/golden-path.spec.ts",  # comment: how it graduated
        "e2e/scripts/informational_spec_results.py",  # emits the per-spec marker (core#1221)
    }
)

#: The steps that legitimately produce a marker. The anti-vacuity control: a scanner that finds
#: none of these is broken, and a broken scanner passes this file forever.
REAL_EMITTERS = (
    (".github/workflows/staging.yml", "gating step outcome:"),
    (".github/workflows/staging.yml", "INFORMATIONAL_RESULT="),
    (".github/workflows/ci.yml", "SSO specs outcome:"),
)

#: Something that prints, in any of the languages a runner here could be written in.
PRINTERS = ("console.log", "echo ", "echo(", "print(", "printf", "Write-Host")

#: Where a developer-runnable thing lives. `tests/` is excluded **deliberately and this is the
#: one exclusion that could gut the guard**: the parser's own corpus is made of captured log
#: lines, several of which are the echoed shell source and therefore contain `echo "SSO specs
#: outcome: ..."` as DATA. The first run of this file fired on exactly those three lines. So
#: the exclusion is real, and `test_control_the_predicate_still_fires_on_a_runner` is what
#: stops it being a hole — the predicate is a pure function and is driven with a synthetic
#: offender, not merely observed to be quiet on today's tree.
EXECUTABLE_ROOTS = ("e2e/", "scripts/", "deploy/")


def _prints_a_marker(rel: str, line: str) -> bool:
    """Would this line, in this file, PRINT a verdict marker?

    Mentioning one in a comment is harmless. Printing one is what makes a local run
    indistinguishable from a staging run the moment its output is captured.
    """
    if rel.startswith(".github/workflows/") or rel.startswith("tests/"):
        return False
    if rel in SANCTIONED_EMITTERS:
        return False
    if not rel.startswith(EXECUTABLE_ROOTS):
        return False
    return any(m in line for m in MARKERS.values()) and any(p in line for p in PRINTERS)


GREEN_GATING = (
    "2026-09-09T12:00:00.0000000Z gating step outcome: success / job status: success / "
    "verdict: clean"
)


def _tracked_files() -> list[str]:
    out = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "ls-files"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    if len(out) < 100:
        raise RuntimeError(
            f"`git ls-files` returned {len(out)} paths. This scan is only meaningful over the "
            "whole tree; a short list is a broken harness, not a clean repo."
        )
    return out


def _marker_hits() -> dict[str, list[tuple[str, str]]]:
    """`file -> [(marker, line)]` for every tracked file carrying any marker."""
    hits: dict[str, list[tuple[str, str]]] = {}
    for rel in _tracked_files():
        path = REPO_ROOT / rel
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except (OSError, IsADirectoryError):
            continue
        found = [
            (marker, line.strip())
            for marker in MARKERS.values()
            for line in text.splitlines()
            if marker in line
        ]
        if found:
            hits[rel] = found
    return hits


# ── the static half: fails closed on ABSENCE of an attestation ──────────────────────────


def test_control_the_scanner_finds_the_real_emitters() -> None:
    """Run first. A scanner that sees nothing passes every assertion below it."""
    hits = _marker_hits()
    for rel, marker in REAL_EMITTERS:
        assert any(m == marker for m, _ in hits.get(rel, [])), (
            f"the scanner did not find {marker!r} in {rel}, which emits it. Every other "
            "assertion in this file is vacuous until this one passes."
        )


def test_no_tracked_file_outside_the_allowlist_mentions_a_verdict_marker() -> None:
    """The measurement, held. 8 files at the time of writing; a 9th is a decision."""
    hits = _marker_hits()
    stray = sorted(set(hits) - MARKER_ALLOWLIST)
    assert not stray, (
        f"{len(stray)} file(s) outside the allowlist carry an E2E verdict marker: {stray}. "
        "The two readers decide by matching this text, and they cannot tell where it was "
        "produced. If this is a local test runner, give it its own vocabulary — never this "
        "one. If it is documentation, add it to MARKER_ALLOWLIST in the same commit and say "
        "which it is."
    )


def test_nothing_outside_the_ci_workflows_prints_a_verdict_marker() -> None:
    """🔑 The one aimed at the local runner that does not exist yet.

    Mentioning a marker in a comment is harmless. **Printing one is what makes a local run
    indistinguishable from a staging run** the moment that output is captured anywhere — a
    pasted log, an uploaded artifact, a workflow step that shells out to the runner.

    Measured 0 at the time of writing, with every marker already present in two `e2e/` specs
    as prose. So this is not a hypothetical boundary being drawn around today's behaviour: it
    is the exact line between what those files do and what a runner would do.
    """
    offenders = [
        f"{rel}: {line[:110]}"
        for rel, found in _marker_hits().items()
        for _marker, line in found
        if _prints_a_marker(rel, line)
    ]
    assert not offenders, (
        "these print an E2E verdict marker from outside CI:\n  " + "\n  ".join(offenders) + "\n"
        "A local run that prints this is producing evidence the graduation reader and the "
        "promotion pre-flight cannot tell from staging's. Call "
        "`environment_attestation('local')` instead — that is vetoed by construction."
    )


def test_control_the_predicate_still_fires_on_a_runner_that_prints_one() -> None:
    """🔑 The positive control for the `tests/` exclusion above.

    Driven with the exact shape a local runner would have, so "the tree is quiet" is a
    measurement rather than a consequence of having excluded everything.
    """
    assert _prints_a_marker(
        "scripts/run-e2e-local.sh",
        'echo "gating step outcome: success / job status: success / verdict: clean"',
    )
    assert _prints_a_marker(
        "e2e/scripts/local-stack.mjs", 'console.log("INFORMATIONAL_RESULT=success")'
    )
    assert _prints_a_marker(
        "deploy/local/run.sh", 'printf "SSO specs outcome: ok / job status: ok / verdict: clean"'
    )


def test_control_the_predicate_ignores_the_fixture_that_made_it_fire() -> None:
    """The three lines the first run of this file flagged, verbatim in shape.

    They are captured log text in the parser's own corpus — the echoed shell source, which is
    the thing `parse_verdict_line` has to be able to reject. A guard that forbids them forbids
    testing the parser.
    """
    fixture = '2026-09-06T20:13:41Z [36;1mecho "SSO specs outcome: $SPECS_OUTCOME / '
    assert not _prints_a_marker("tests/test_deploy/test_e2e_tier_streak.py", fixture)
    # ...and it is still not allowed to appear in an unlisted file at all:
    assert "tests/test_deploy/test_e2e_tier_streak.py" in MARKER_ALLOWLIST


def test_every_sanctioned_emitter_is_actually_invoked_by_a_workflow() -> None:
    """🔑 What makes SANCTIONED_EMITTERS a property rather than an allowlist.

    A script that can print graduation evidence and that no workflow runs is a script whose
    output can only ever have come from a person's machine — which is precisely the thing
    this file refuses. Same shape as `test_server_script_coverage.py`: an installer nothing
    invokes is the bug one level up and looks identical to a fix.
    """
    workflows = chr(10).join(
        wf.read_text(encoding="utf-8", errors="replace")
        for wf in (REPO_ROOT / ".github" / "workflows").glob("*.yml")
    )
    for rel in sorted(SANCTIONED_EMITTERS):
        assert (REPO_ROOT / rel).exists(), (
            f"{rel} is sanctioned to print a marker and does not exist"
        )
        name = rel.rsplit("/", 1)[-1]
        assert name in workflows, (
            f"{rel} may print a verdict marker but no workflow invokes it. Either wire it "
            "into CI or take it off SANCTIONED_EMITTERS — a marker only a human can produce "
            "is the definition of what this file exists to reject."
        )


def test_the_local_token_is_emitted_by_no_workflow() -> None:
    """Its defining property, asserted — not an exemption from the anti-vacuity control.

    The three tier vocabularies are pinned in both directions against the workflows that emit
    them. `LOCAL_VERDICTS` is pinned the **opposite** way: no CI classifier may ever produce
    this token, because a token CI can emit is a token a local runner can copy.
    """
    for wf in sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml")):
        text = wf.read_text(encoding="utf-8", errors="replace")
        assert LOCAL_VERDICT not in text, (
            f"{wf.name} contains {LOCAL_VERDICT!r}. This token exists to mean 'not CI'; a "
            "workflow that emits it makes the distinction unrecoverable."
        )
    assert LOCAL_VERDICTS == {LOCAL_VERDICT: LOCAL}


# ── the runtime half: fails closed on PRESENCE of a foreign attestation ──────────────────


class TestTheVeto:
    def test_a_local_attestation_produces_the_local_verdict(self) -> None:
        log = ["2026-09-09T12:00:00.0000000Z E2E_ENVIRONMENT=local", GREEN_GATING]
        assert attested_environment(log) == "local"
        assert parse_verdict_line(log, tier="gating") == LOCAL_VERDICT
        assert classify_verdict(LOCAL_VERDICT) == LOCAL

    def test_the_veto_beats_a_clean_verdict_printed_after_it(self) -> None:
        """🚨 The ordering control, and the reason this is a veto rather than a fallback.

        A harness prints where it ran, and *then* the suite prints its result — so the green
        line is the later one in every real local log. Resolving by "last match wins" is
        core#1205 in a new costume, and it would resolve this the wrong way every time.
        """
        log = ["2026-09-09T12:00:00.0000000Z E2E_ENVIRONMENT=local", GREEN_GATING]
        assert log.index(GREEN_GATING) > 0, "the arming needs the green line to come second"
        assert classify_verdict(parse_verdict_line(log, tier="gating")) == LOCAL

    def test_any_environment_that_is_not_the_ci_one_is_vetoed(self) -> None:
        """Fails closed on a value nobody has thought of, not just on the string `local`."""
        for env in ("local", "docker-desktop", "laptop", "worktree", "wsl2", "unknown"):
            log = [f"2026-09-09T12:00:00.0000000Z E2E_ENVIRONMENT={env}", GREEN_GATING]
            assert classify_verdict(parse_verdict_line(log, tier="gating")) == LOCAL, env

    def test_control_the_ci_environment_is_not_vetoed(self) -> None:
        log = [
            f"2026-09-09T12:00:00.0000000Z E2E_ENVIRONMENT={CI_ENVIRONMENT}",
            GREEN_GATING,
        ]
        assert classify_verdict(parse_verdict_line(log, tier="gating")) == PASS

    def test_control_a_log_with_no_attestation_reads_exactly_as_before(self) -> None:
        """Every log written before core#1232 is silent, and must stay a normal reading.

        Grading silence as LOCAL would red the entire history — and it is not what silence
        means. `test_nothing_outside_the_ci_workflows_prints_a_verdict_marker` is what covers
        silence; this asserts the veto does not overreach into it.
        """
        assert attested_environment([GREEN_GATING]) is None
        assert classify_verdict(parse_verdict_line([GREEN_GATING], tier="gating")) == PASS


# ── the two callers, opposite correct answers ────────────────────────────────────────────


class TestALocalRunCannotAdvanceTheStreak:
    def test_local_breaks_the_trailing_streak(self) -> None:
        """By construction, not by a special case: `streak` skips only UNMEASURED."""
        assert streak([PASS, PASS, LOCAL, PASS]) == 1

    def test_three_greens_and_a_local_run_do_not_graduate(self) -> None:
        assert Reading.from_classes([PASS, PASS, PASS, LOCAL]).state == "not-yet"

    def test_a_local_run_cannot_be_the_third_green(self) -> None:
        assert Reading.from_classes([PASS, PASS, LOCAL]).state == "not-yet"

    def test_control_three_staging_greens_still_graduate(self) -> None:
        """The other half of the property. A guard that refuses everything is not a guard."""
        assert Reading.from_classes([PASS, PASS, PASS]).state == "graduate"


class TestALocalRunCannotPassThePreflight:
    @staticmethod
    def _jobs() -> list[Job]:
        """One commit, its own deploy, three verifiers that ran entirely inside its window."""
        sha = "abc1234"
        deploy_start, deploy_end = "2026-09-09T00:00:00Z", "2026-09-09T00:05:00Z"
        job_start, job_end = "2026-09-09T00:06:00Z", "2026-09-09T00:20:00Z"
        return [
            Job(1, sha, "deploy-staging", deploy_start, deploy_end, "success"),
            Job(1, sha, "smoke-staging", job_start, job_end, "success"),
            Job(1, sha, "e2e-staging", job_start, job_end, "success"),
            Job(1, sha, "e2e-sso", job_start, job_end, "success"),
        ]

    def test_a_local_verdict_is_not_attributed(self) -> None:
        out = classify(self._jobs(), "abc1234", {"e2e-staging": LOCAL, "e2e-sso": PASS})
        assert out["jobs"]["e2e-staging"]["verdict"] == "local_run"
        assert out["trustworthy"] is False

    def test_it_is_reported_separately_from_no_verdict(self) -> None:
        """The promoter's next move differs, so the two must not share a name.

        `no_verdict` says *go and get a reading*. `local_run` says *a reading exists and it is
        of the wrong machine* — the more dangerous of the two, because there is a passing build
        behind it.
        """
        out = classify(self._jobs(), "abc1234", {"e2e-staging": LOCAL})
        assert "different machine" in out["jobs"]["e2e-staging"]["detail"]
        assert out["jobs"]["e2e-staging"]["verdict"] != "no_verdict"

    def test_control_the_same_jobs_with_a_staging_verdict_pass(self) -> None:
        """🔑 Identical jobs. The ONLY difference is the class, and the answers are opposite."""
        out = classify(self._jobs(), "abc1234", {"e2e-staging": PASS, "e2e-sso": PASS})
        assert out["jobs"]["e2e-staging"]["verdict"] == "attributed"
        assert out["trustworthy"] is True


def test_the_two_readers_agree_about_what_local_means() -> None:
    """One vocabulary, two consumers. A class only one of them knows is worse than neither."""
    from scripts.verify_e2e_attribution import LOCAL_CLASS

    assert LOCAL_CLASS == LOCAL, (
        f"the pre-flight calls it {LOCAL_CLASS!r} and the streak reader calls it {LOCAL!r}. "
        "core#1205 was two readers disagreeing about which tier a verdict belonged to; this "
        "would be two readers disagreeing about whether it belonged to production at all."
    )


@pytest.mark.parametrize("tier", ["gating", "informational", "sso"])
def test_the_veto_applies_to_every_tier(tier: str) -> None:
    """A tier the veto skips is a tier a local run can still graduate."""
    log = [
        "2026-09-09T12:00:00.0000000Z E2E_ENVIRONMENT=local",
        GREEN_GATING,
        "2026-09-09T12:00:01.0000000Z INFORMATIONAL_RESULT=success",
        "2026-09-09T12:00:02.0000000Z SSO specs outcome: success / job status: success / "
        "verdict: clean",
    ]
    assert classify_verdict(parse_verdict_line(log, tier=tier)) == LOCAL


# ── the emitter and the parser are one string ────────────────────────────────────────────


class TestTheAttestationRoundTrips:
    """🔑 The property that survives a rename.

    A local runner that hardcodes `E2E_ENVIRONMENT=local` keeps printing it after the marker is
    renamed here — and the veto stops firing, silently, while the runner, the suite and CI all
    stay green. So the emitter is a function in the same module as the parser, and these assert
    the round trip rather than the literal.
    """

    def test_what_the_helper_emits_is_what_the_veto_reads(self) -> None:
        line = environment_attestation("local")
        assert attested_environment([f"2026-09-09T12:00:00.0000000Z {line}"]) == "local"
        log = [f"2026-09-09T12:00:00.0000000Z {line}", GREEN_GATING]
        assert classify_verdict(parse_verdict_line(log, tier="gating")) == LOCAL

    def test_the_ci_environment_round_trips_too_and_is_not_vetoed(self) -> None:
        """Both directions from one helper, or it only proves the refusing half."""
        line = environment_attestation(CI_ENVIRONMENT)
        log = [f"2026-09-09T12:00:00.0000000Z {line}", GREEN_GATING]
        assert attested_environment([f"2026-09-09T12:00:00.0000000Z {line}"]) == CI_ENVIRONMENT
        assert classify_verdict(parse_verdict_line(log, tier="gating")) == PASS

    def test_it_refuses_a_value_the_parser_could_not_read_back(self) -> None:
        """A marker the veto cannot read is a local run that looks like a staging one —
        produced by the very call that was supposed to prevent it."""
        for bad in ("Local", "local stack", "local!", "", "LOCAL"):
            with pytest.raises(ValueError, match="cannot be read back"):
                environment_attestation(bad)

    def test_the_marker_name_is_not_duplicated_in_the_pattern(self) -> None:
        """The anti-drift control: one literal, interpolated into the regex."""
        source = (REPO_ROOT / "scripts" / "e2e_tier_streak.py").read_text(encoding="utf-8")
        occurrences = source.count(f'"{ENVIRONMENT_MARKER}"')
        assert occurrences == 1, (
            f"`{ENVIRONMENT_MARKER}` is written as a bare literal {occurrences} times. Two "
            "copies is how the emitter and the parser drift apart, which disarms the veto "
            "without reddening anything."
        )
