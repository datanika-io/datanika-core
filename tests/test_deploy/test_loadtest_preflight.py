"""The load test's OPERATOR-SIDE preflight must exist and must discriminate (core#778).

``scripts/loadtest/run.sh`` runs **on the production box** and its preflight is box-shaped:
staging ``/healthz``, production ``/healthz``, an E2E container census, ``load1``. Those are
necessary and they are not sufficient, because the box cannot see GitHub.

🔑 **The measured finding this file guards.** run.sh's census asks *"is an E2E suite running
right now"*; a 25-minute ladder needs *"will one START during my run"*. Those differ, and they
differ worst in the gap between a ``dev`` run's unit jobs finishing and its staging jobs
starting — in that window the census reads a confident zero while a staging deploy is minutes
away. Measured twice on two sessions: E2E containers **0**, staging ``Up 3 hours``, ``load1
0.46``, and an in-flight ``dev`` run whose staging jobs did not exist yet. **All four box-side
checks would have PASSED and staging would have been rebuilt underneath the run.**

⚠️ **What makes this file worth having is the mutation tests, not the presence tests.** The way
a guard like this fails is by being quietly simplified — a comparison flipped, a gate deleted,
a control dropped — and every one of those leaves it *passing*. So the assertions below drive
the script's own ``--self-check`` against populations the repository does not currently
contain, and then **mutate a copy and require it to go red**.

🚨 Two project rules are load-bearing here and are asserted rather than trusted:

* *A control must be able to fail in the direction of your conclusion.* A guard that refuses
  **everything** is not discriminating — and the obvious repair for "it refuses everything" is
  to loosen it, which is how a guard becomes maximally unsafe while looking cautious. So a
  clear board **must** produce ``PASS``, and that is tested first.
* *An instrument that cannot see part of its population reports that part as clean.* Each gate
  reads a filtered population it grades **and** an unfiltered one that must be non-empty, so a
  broken query refuses instead of reading as quiet. Measured on 2026-09-23: a merge-queue
  GraphQL call with a doubled owner returned an ``errors`` array, printed nothing, and read
  exactly like an empty queue.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
LOADTEST = ROOT / "scripts" / "loadtest"
PREFLIGHT = LOADTEST / "preflight.sh"
RUNNER = LOADTEST / "run.sh"
README = LOADTEST / "README.md"

BASH = shutil.which("bash")
needs_bash = pytest.mark.skipif(BASH is None, reason="bash unavailable on this host")


def _self_check(script: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [BASH, str(script), "--self-check"],
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_the_preflight_is_in_the_repository():
    """It is the half run.sh structurally cannot do, so it cannot live in a session's head."""
    assert PREFLIGHT.is_file(), (
        "scripts/loadtest/preflight.sh is missing. The operator-side gates were hand-evaluated "
        "twice and written down twice; coordinator rule 9 says the instrument ships."
    )
    assert PREFLIGHT.stat().st_size > 2000, "too small to be the real thing"


@needs_bash
def test_the_self_check_passes_as_shipped():
    """The shipped script must agree with its own table. This is the cheap regression net."""
    r = _self_check(PREFLIGHT)
    assert r.returncode == 0, f"--self-check failed as shipped:\n{r.stdout}\n{r.stderr}"
    assert "all 10 cases correct" in r.stdout, r.stdout


@needs_bash
def test_a_clear_board_passes_and_each_gate_refuses_on_its_own():
    """The discrimination requirement, read off the self-check's own output.

    A guard that answers every population the same way has measured nothing. Requiring a
    ``PASS`` line *and* a distinct refusal per gate is what separates a gate from a wall.
    """
    out = _self_check(PREFLIGHT).stdout
    assert "ok    PASS" in out, (
        "the self-check never exercises a clear board, so nothing proves this guard can ever "
        "say yes — a guard that always refuses is one careless repair away from permitting "
        "everything"
    )
    for gate in ("G1", "G2", "G3", "G4"):
        assert f"REFUSE:{gate}" in out, f"{gate} is never exercised by the self-check"


@needs_bash
@pytest.mark.parametrize(
    "old,new,label",
    [
        ('[ "$runs"  -gt 0 ]', '[ "$runs"  -lt 0 ]', "G1 comparison inverted"),
        ('[ "$queue" -gt 0 ]', '[ "$queue" -lt 0 ]', "G2 comparison inverted"),
        ('[ "$prs"   -gt 0 ]', '[ "$prs"   -lt 0 ]', "G3 comparison inverted"),
        ('[ "$cooled" -eq 0 ]', '[ "$cooled" -eq 9 ]', "G4 comparison broken"),
    ],
)
def test_breaking_a_gate_turns_the_self_check_red(tmp_path, old, new, label):
    """Anti-vacuity. A check never seen failing is not evidence.

    ⚠️ The mutation is applied to a **copy**, and the source string is asserted to be present
    first. *"The test did not catch it"* and *"the change never reached the file"* produce
    identical output, and only one of them is a finding.
    """
    src = PREFLIGHT.read_text(encoding="utf-8")
    assert old in src, f"mutation anchor {old!r} not found — this test is measuring nothing"

    mutant = tmp_path / "preflight.sh"
    mutant.write_text(src.replace(old, new, 1), encoding="utf-8")

    r = _self_check(mutant)
    assert r.returncode == 25, (
        f"{label}: the self-check still passed. A gate whose comparison can be inverted "
        f"without anything going red is decoration.\n{r.stdout}"
    )


@needs_bash
def test_deleting_a_gate_outright_turns_the_self_check_red(tmp_path):
    """The simplification that a comparison-flip test would miss: removing the line."""
    src = PREFLIGHT.read_text(encoding="utf-8")
    marker = "REFUSE:G3:$prs open PR"
    assert marker in src, "anchor missing — this test is measuring nothing"

    kept = [ln for ln in src.splitlines(keepends=True) if marker not in ln]
    assert len(kept) < len(src.splitlines(keepends=True)), "nothing was removed"

    mutant = tmp_path / "preflight.sh"
    mutant.write_text("".join(kept), encoding="utf-8")
    r = _self_check(mutant)
    assert r.returncode == 25, (
        "removing the G3 gate entirely left the self-check green.\n" + r.stdout
    )


@needs_bash
def test_an_unreadable_count_refuses_rather_than_counting_as_zero(tmp_path):
    """`a guard that errors is not a guard` — run.sh's own defect 6, one level up.

    A failed API call yields an empty string or a sentinel, and an empty string compared
    numerically is the shape that reads as a quiet repository.
    """
    out = _self_check(PREFLIGHT).stdout
    assert "unreadable in-flight-run count" in out
    assert "unreadable merge-queue count" in out
    assert "unreadable open-PR count" in out
    assert "unreadable cooldown state" in out


@pytest.mark.parametrize("gate", ["G1", "G2", "G3", "G4"])
def test_every_gate_carries_a_positive_control(gate):
    """A filtered zero beside an unreadable population is not evidence.

    Asserted as the PRESENCE of a named control failure path, per WORKFLOW_RULES §4 — a script
    that merely stopped mentioning the hazard would satisfy a ban and fails this.
    """
    src = PREFLIGHT.read_text(encoding="utf-8")
    assert f"{gate} control failed" in src, (
        f"{gate} has no control-failure path, so a broken query for it reads as clean"
    )


def test_the_refusal_exit_codes_are_distinct():
    """An operator wrapping this in a script must be able to tell the gates apart."""
    src = PREFLIGHT.read_text(encoding="utf-8")
    for gate, code in (("G1", "20"), ("G2", "21"), ("G3", "22"), ("G4", "23")):
        assert f"{gate}) exit {code}" in src, f"{gate} has no distinct exit code"
    assert "exit 24" in src, "no distinct exit code for an unreadable instrument"
    assert "exit 25" in src, "no distinct exit code for a failed self-check"


def test_the_real_path_has_no_env_backdoor():
    """The self-check must not be reachable by an environment variable.

    A guard with an injection point is a guard a stray variable disarms, and the disarmed
    state looks identical to the armed one.
    """
    src = PREFLIGHT.read_text(encoding="utf-8")
    assert "--self-check" in src, "no self-check mode at all"
    for backdoor in ("PREFLIGHT_FAKE", "FORCE_PASS", "SKIP_PREFLIGHT", "PREFLIGHT_OVERRIDE"):
        assert backdoor not in src, f"{backdoor} is an override; there must be none"


def test_the_preflight_states_what_it_cannot_promise():
    """An over-claiming guard is worse than none.

    It cannot see the future — a department can merge ninety seconds from now. The script has
    to say so, because the next operator's confidence is calibrated by this text and by
    nothing else.
    """
    src = PREFLIGHT.read_text(encoding="utf-8")
    assert "does not remove it" in src, (
        "the script does not state the residual risk, so a PASS reads as a guarantee it cannot give"
    )


def test_the_readme_sends_the_operator_through_the_preflight_first():
    """A gate nothing invokes is the original bug one level up, and looks identical to a fix."""
    text = README.read_text(encoding="utf-8")
    assert "preflight.sh" in text, (
        "the README's Run-it section does not name preflight.sh, so the documented path "
        "still goes straight to run.sh — which is the state this instrument was built to end"
    )
    assert text.index("preflight.sh") < text.index("run.sh --keys"), (
        "preflight.sh is mentioned only after the run command; the order is the instruction"
    )


def test_run_sh_keeps_its_own_box_side_preflight():
    """The two are complements. Neither replaces the other, and shipping this one must not
    tempt anyone into thinning the other — the box-side checks see things GitHub cannot."""
    src = RUNNER.read_text(encoding="utf-8")
    for needed in ("healthz", "load1", "docker ps"):
        assert needed in src, (
            f"run.sh no longer performs its own {needed} check. The operator-side preflight "
            f"runs minutes earlier and from another machine; it is not a substitute."
        )
