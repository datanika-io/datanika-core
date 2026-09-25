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

import re
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


# ── G4's READING, not just its comparison (core#1580) ─────────────────────────────────────
#
# 🔑 Why these exist as a separate block. The mutation test above flips `[ "$cooled" -eq 0 ]`
# and proves the *decision* discriminates. It could never have found core#1580, because
# ``verdict()`` takes ``cooled`` as an **argument** — the reading that produces it sat entirely
# outside the only self-checked part of the script.
#
# The defect: G4 read ``.workflow_runs[0].updated_at`` from a listing the API sorts by
# ``created_at`` descending. ``[0]`` is the most recently *started* completed run; the gate asks
# when one last *finished*. ``[0]``'s ``updated_at`` is never later than the true maximum, so the
# age was never smaller than the real one — **it failed open.** Measured 2026-09-25: 12 of 12
# samples disagreed with the honest maximum, and on the live invocation the honest age was 7
# minutes against a 10-minute cooldown. It went unnoticed only because G1 refused for an
# unrelated reason, which is the shape where one gate covers for another.
#
# ⚠️ So these assertions drive the **reading**, and the load-bearing one is the *divergence*
# case: the fixed reading and the old one must answer the measured listing DIFFERENTLY. A
# population where they agree proves nothing, which is why the script keeps ``first_finish``
# rather than deleting it.


@needs_bash
def test_the_self_check_covers_the_reading_and_not_only_the_decision():
    """The boundary move is the fix. Assert it is actually inside the self-check."""
    out = _self_check(PREFLIGHT).stdout
    assert "the cooldown READING must pick the latest finish, not the first row" in out, (
        "the self-check does not drive the reading at all, so core#1580's class is still "
        "outside the self-checked part — which is the whole defect, not the sort order"
    )
    assert "the readings differ as they must" in out, (
        "nothing compares the fixed reading against the old one, so a revert to first-row "
        "semantics would be caught by no assertion that states the relationship"
    )
    # the two sides of the measured instant, both printed so a reader can see the direction
    assert "PERMITS - the bug" in out and "REFUSES - the fix" in out, out


@needs_bash
@pytest.mark.parametrize(
    "old,new,label",
    [
        (
            r'if [ -z "$best" ] || [ "$ts" \> "$best" ]; then best="$ts"; fi',
            r'if [ -z "$best" ]; then best="$ts"; fi',
            "newest_finish reverted to first-row semantics — core#1580 itself",
        ),
        (
            r'if [ -z "$best" ] || [ "$ts" \> "$best" ]; then best="$ts"; fi',
            r'if [ -z "$best" ] || [ "$ts" \< "$best" ]; then best="$ts"; fi',
            "newest_finish picks the earliest finish instead of the latest",
        ),
        (
            '    case "$st" in (completed) ;; (*) continue ;; esac\n'
            '    [ -n "$ts" ] || continue\n'
            '    if [ -z "$best" ]',
            '    [ -n "$ts" ] || continue\n    if [ -z "$best" ]',
            "newest_finish stops filtering on status, so a running run counts as a finish",
        ),
        (
            'cooled=0; [ "$age" -ge "$cd" ] && cooled=1',
            'cooled=1; [ "$age" -ge "$cd" ] && cooled=0',
            "the cooldown comparison inverted",
        ),
        (
            '[ -n "$iso" ] || { echo "ERR:no completed dev run readable',
            '[ -n "$iso" ] || { echo "1 9999"; :; } || { echo "ERR:no completed dev run readable',
            "an absent reading counted as cooled instead of refusing",
        ),
    ],
)
def test_breaking_the_cooldown_reading_turns_the_self_check_red(tmp_path, old, new, label):
    """Anti-vacuity for the reading. Every one of these was seen red before shipping.

    ⚠️ The anchor is asserted present first. *"The test did not catch it"* and *"the mutation
    never reached the file"* produce identical output, and only one of them is a finding — this
    bit while writing these very cases: a multi-line anchor missed, and the first reading of
    that was a guess about line endings that measurement refuted (the file is LF).
    """
    src = PREFLIGHT.read_text(encoding="utf-8")
    assert old in src, f"mutation anchor {old!r} not found — this case is measuring nothing"

    mutant = tmp_path / "preflight.sh"
    mutant.write_text(src.replace(old, new, 1), encoding="utf-8")
    assert mutant.read_text(encoding="utf-8") != src, "the mutation changed nothing"

    r = _self_check(mutant)
    assert r.returncode == 25, (
        f"{label}: the self-check still passed. This is the class core#1580 was, and it stayed "
        f"invisible for exactly this reason.\n{r.stdout}"
    )


@needs_bash
def test_a_population_where_the_readings_agree_is_reported_as_measuring_nothing(tmp_path):
    """Drive the divergence assertion with the population it exists to catch.

    Rule 10's inverse: a control that cannot fail is not a control. If ``first_finish`` is made
    identical to ``newest_finish`` the two readings agree, and the self-check must say so rather
    than reporting a comparison that compared a value with itself.
    """
    lines = PREFLIGHT.read_text(encoding="utf-8").splitlines(keepends=True)
    start = next((i for i, ln in enumerate(lines) if ln.startswith("first_finish() {")), None)
    assert start is not None, (
        "first_finish is gone — the divergence control has no old reading to compare against"
    )
    end = next(i for i in range(start, len(lines)) if lines[i].rstrip("\n") == "}")

    stub = ["first_finish() {\n", "  newest_finish\n", "}\n"]
    mutant = tmp_path / "preflight.sh"
    mutant.write_text("".join(lines[:start] + stub + lines[end + 1 :]), encoding="utf-8")
    r = _self_check(mutant)
    assert r.returncode == 25, (
        "the two readings were made identical and the self-check still passed, so the "
        "divergence case is satisfied by comparing a value with itself\n" + r.stdout
    )
    assert "measuring nothing" in r.stdout, r.stdout


def test_the_live_g4_query_does_not_select_a_single_element():
    """Assert the PRESENCE of the right shape, per WORKFLOW_RULES §4.

    A ban on ``[0]`` would be satisfied by a comment explaining why ``[0]`` is wrong, and by
    deleting the gate. What must hold is positive: the call asks for a population, and the
    choice of element is made by the function the self-check drives.
    """
    src = PREFLIGHT.read_text(encoding="utf-8")
    g4 = src.split("── G4:")[1]
    assert "per_page=100" in g4, (
        "G4's query no longer asks for a population. `per_page=1` starves the maximum of "
        "anything to maximise over, which reinstates core#1580 while every line below it "
        "still reads as fixed"
    )
    assert "| newest_finish" in g4, (
        "the live path no longer routes its reading through newest_finish, so the reading the "
        "self-check drives and the reading the operator gets are different code"
    )
    assert "G4 control failed" in g4, "G4 lost its control-failure path"


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


# The RUN command, not today's arguments. `run.sh --keys` was the original anchor, which made
# this guard red on a correct change: documenting the defaults invocation (core#1556 fixed them,
# so `bash scripts/loadtest/run.sh` is now the documented call) removed the literal and the
# assertion raised ValueError. WORKFLOW_RULES §5a — assert the invariant, not the instance.
RUN_INVOCATION = re.compile(r"bash\s+scripts/loadtest/run\.sh")
PREFLIGHT_INVOCATION = re.compile(r"\bpreflight\.sh\b")


def test_the_readme_sends_the_operator_through_the_preflight_first():
    """A gate nothing invokes is the original bug one level up, and looks identical to a fix."""
    text = README.read_text(encoding="utf-8")
    pre = PREFLIGHT_INVOCATION.search(text)
    run = RUN_INVOCATION.search(text)
    assert pre, (
        "the README's Run-it section does not name preflight.sh, so the documented path "
        "still goes straight to run.sh — which is the state this instrument was built to end"
    )
    assert run, "the README no longer shows how to invoke run.sh at all"
    assert pre.start() < run.start(), (
        "preflight.sh is mentioned only after the run command; the order is the instruction"
    )


def test_the_ordering_guard_can_fail_and_survives_a_change_of_arguments():
    """Driven with both populations, plus the change that broke the previous anchor.

    🔴 The old assertion indexed the literal ``run.sh --keys``. That is a snapshot: it pinned
    the *arguments* as a stand-in for *the run command*, so documenting the defaults invocation
    — the correct outcome of core#1556 — made it raise instead of pass. The repointed pattern
    must accept either form and must still refuse the wrong order.
    """
    right = "First: bash scripts/loadtest/preflight.sh\nThen: bash scripts/loadtest/run.sh\n"
    wrong = "Just run: bash scripts/loadtest/run.sh\n...later, preflight.sh exists too.\n"
    with_args = (
        "First: bash scripts/loadtest/preflight.sh\n"
        "Then: bash scripts/loadtest/run.sh --keys 300 --stages '5:120s'\n"
    )

    for label, text in (("correct order", right), ("with arguments", with_args)):
        pre = PREFLIGHT_INVOCATION.search(text)
        run = RUN_INVOCATION.search(text)
        assert pre and run and pre.start() < run.start(), f"{label} must pass"

    pre = PREFLIGHT_INVOCATION.search(wrong)
    run = RUN_INVOCATION.search(wrong)
    assert pre and run and pre.start() > run.start(), (
        "the guard must still refuse a README that sends the operator to run.sh first — "
        "otherwise it passes on the very state it exists to prevent"
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
