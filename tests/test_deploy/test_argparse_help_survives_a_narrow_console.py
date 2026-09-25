"""`--help` must survive a console codec we do not control (core#1585).

`scripts/e2e_tier_streak.py --help` exited **1** on the dev host, printing 1,898 bytes and never
reaching the options block. argparse encodes `description` to the console codec; that module passed
`__doc__`, the docstring carries 🚨, and the console here is `cp1251`. So the failure was not
*"no help"* — it was **help that ends before the flags**, and truncation reads as the end of the
list.

🔑 **Why that mattered more than a broken `--help` usually would.** [core#1567] left
`e2e_tier_streak`'s default window reproducible only as far as two looks can establish, and the
mitigation it leaves in force — in `QA_RULES` §10 and on the [core#720] thread — is *"pin `--since`
on every graduation reading."* **The one command an agent would run to discover that flag was the
one that died.**

── What this guard covers, and what it does not ──────────────────────────────────────────────────

**It runs the real program.** It does not model argparse, and that is deliberate: a structural
assertion (*"no parser passes `__doc__` as `description`"*) catches the instance that was filed and
misses the same defect arriving through `epilog=` or through any single argument's `help=` string.
Running `--help` catches all three. Same reasoning as core#1575's *"ask GitHub what a promotion will
close instead of modelling its parser."*

**The population is derived, not listed.** `git ls-files` over three directories, filtered to files
that actually construct an `ArgumentParser`. A literal list cannot fail for a file nobody thought
of — which is exactly how this defect's own issue under-counted it (see below).

🔴 **core#1585's own scope was wrong, and it was mine.** The issue said *"every `scripts/*.py` whose
module docstring carries an emoji"*, and prescribed a guard over `scripts/*.py`. Measured
2026-09-25, two independent instruments agreeing (an AST pass over `ArgumentParser(description=…)`
and a real `--help` run under both codecs): the population is **6 files across 3 directories** —

    scripts/e2e_tier_streak.py                  scripts/slo_report.py
    scripts/strict_xfail_issue_audit.py         .github/scripts/cross_tree_import_gate.py
    e2e/scripts/assert_sso_coverage.py          e2e/scripts/informational_spec_results.py

A `scripts/*.py` guard would have covered **3 of 6** and read green over the other three — among
them `e2e/scripts/informational_spec_results.py`, which emits the graduation marker, and
`.github/scripts/cross_tree_import_gate.py`, which decides whether an image may build. *An issue's
population count inherits the defect of the instrument that produced it* (coordinator rule 33).

⚠️ **The gate codec is `cp1251`, not `ascii`, and the difference is measured rather than chosen for
convenience.** Under `PYTHONIOENCODING=ascii`, five files still fail — but the entire residue is
**one EM DASH (U+2014) and one SECTION SIGN (U+00A7)** in argument `help=` strings, both of which
`cp1251` encodes and both of which render on every console this project runs on. Gating on `ascii`
would demand removing em dashes from help text: red on correct code, which `WORKFLOW_RULES` §5a
forbids. **So this is the population this guard does NOT cover: `--help` on a strictly 7-bit
console.** Stated with its numbers so the next reader does not infer completeness from a green.

── The two controls, and why both are needed ─────────────────────────────────────────────────────

An `rc == 0` assertion alone is satisfied by a program that prints a prologue and stops, which is
the exact shape of the defect. So each subject must also be seen to **reach its options block**,
and that half needs its own control — hence `test_a_prologue_that_exits_zero_is_not_accepted`
beside `test_an_emoji_description_is_rejected`. One without the other is how a guard comes to pass
by gutting its own assertion.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

#: Directories holding first-party command-line programs. Asserted non-empty below, so a rename
#: that empties one reds instead of silently shrinking the population.
SEARCHED_DIRS = ("scripts", ".github/scripts", "e2e/scripts")

#: The codec the gate drives. See the docstring: `ascii` is stricter than the invariant needs.
GATE_CODEC = "cp1251"

#: Tracked Python under SEARCHED_DIRS that constructs no ArgumentParser, and is therefore not a
#: subject. Recorded as a count rather than a list — the assertion that matters is that every
#: tracked file is classified as one or the other, with nothing falling through.
_NOT_A_CLI_PROGRAM = "constructs no ArgumentParser"


def _tracked_python() -> list[Path]:
    """Every tracked `*.py` under SEARCHED_DIRS.

    `git ls-files` rather than a glob: it answers *"what does this repository ship"* and cannot
    vary with a stale `.venv`, local build output, or another session's untracked scratch.
    ⚠️ It also cannot see a file being ADDED in this very commit — `git add` first.
    """
    out = subprocess.run(
        ["git", "ls-files", "--", *SEARCHED_DIRS],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [REPO_ROOT / line for line in out.splitlines() if line.endswith(".py")]


def _constructs_argument_parser(path: Path) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
            if name == "ArgumentParser":
                return True
    return False


def _classify() -> tuple[list[Path], list[Path]]:
    """Split the tracked population into subjects and non-subjects. Nothing falls through."""
    subjects, others = [], []
    for p in _tracked_python():
        (subjects if _constructs_argument_parser(p) else others).append(p)
    return subjects, others


def _run_help(path: Path, codec: str) -> tuple[int, str]:
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = codec
    proc = subprocess.run(
        [sys.executable, str(path), "--help"],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        timeout=120,
    )
    return proc.returncode, (proc.stdout + proc.stderr).decode("utf-8", "replace")


def _reached_the_options_block(out: str) -> bool:
    """argparse always lists `-h, --help` in its options block.

    Its presence is therefore the cheapest available proof that rendering got PAST the description
    — which is precisely where core#1585 died. Absence means the program printed a prologue.
    """
    return "-h" in out and "--help" in out


def _defect(path: Path, codec: str) -> str | None:
    """Return a human-readable defect for `path` under `codec`, or None if it is clean."""
    rc, out = _run_help(path, codec)
    if rc != 0:
        tail = out.strip().splitlines()[-1:] or ["(no output)"]
        return f"exited {rc} under PYTHONIOENCODING={codec}; last line: {tail[0][:160]}"
    if not _reached_the_options_block(out):
        return (
            f"exited 0 under PYTHONIOENCODING={codec} but printed no options block "
            f"({len(out)} bytes) - help that ends before the flags"
        )
    return None


SUBJECTS, NON_SUBJECTS = _classify()


def _population_note() -> str:
    """Every failure message names what was searched AND what was not. Rule 26."""
    return (
        f"\n  POPULATION SEARCHED    : {len(SUBJECTS)} programs with an ArgumentParser, under "
        f"{', '.join(SEARCHED_DIRS)}"
        f"\n  CLASSIFIED NON-SUBJECT : {len(NON_SUBJECTS)} tracked .py ({_NOT_A_CLI_PROGRAM})"
        f"\n  NOT COVERED BY THIS GATE: --help on a strictly 7-bit console; the gate codec is "
        f"{GATE_CODEC}. See this module's docstring for the measured residue."
    )


def test_the_searched_directories_are_all_present_and_non_empty() -> None:
    """A rename that empties a directory must red, not silently shrink the population.

    Without this, `git mv scripts/ tools/` leaves every assertion below iterating an empty list and
    passing - the blind-spot shape this whole file exists to refuse.
    """
    for d in SEARCHED_DIRS:
        assert (REPO_ROOT / d).is_dir(), (
            f"{d} is gone; repoint SEARCHED_DIRS rather than deleting it"
        )
    assert len(SUBJECTS) >= 15, (
        f"only {len(SUBJECTS)} CLI programs discovered, which is fewer than the 19 measured on "
        f"2026-09-25 - the population derivation is probably broken, not the repository"
        + _population_note()
    )


def test_every_tracked_python_is_classified() -> None:
    """Subjects + non-subjects must account for the whole tracked population."""
    assert len(SUBJECTS) + len(NON_SUBJECTS) == len(_tracked_python())


@pytest.mark.parametrize("script", SUBJECTS, ids=lambda p: str(p.relative_to(REPO_ROOT)))
def test_help_survives_a_narrow_console(script: Path) -> None:
    """Every first-party CLI program's `--help` must exit 0 AND reach its options block.

    Driven with the codec forced, because a UTF-8 runner asserts nothing here: CI is UTF-8 and
    this defect only ever appeared on a narrow console.
    """
    problem = _defect(script, GATE_CODEC)
    assert problem is None, (
        f"{script.relative_to(REPO_ROOT)}: {problem}"
        f"\n  Remedy: stop passing __doc__ (or any emoji-carrying string) as `description`, "
        f"`epilog`, or an argument's `help=`. The docstring is for a reader of the file; these "
        f"strings are for a terminal whose codec we do not control." + _population_note()
    )


def test_the_flag_the_1567_mitigation_depends_on_is_discoverable() -> None:
    """core#1585 AC1, on its exact subject: `--since` must be findable from the tool itself.

    This is narrower than the class gate above on purpose. The class gate proves `--help` renders;
    this proves the one flag a standing mitigation depends on is actually *in* what rendered.
    """
    rc, out = _run_help(REPO_ROOT / "scripts" / "e2e_tier_streak.py", GATE_CODEC)
    assert rc == 0, f"--help exited {rc} under {GATE_CODEC}"
    assert "--since" in out, (
        "`--since` is absent from e2e_tier_streak --help. While core#1567 is open the standing "
        "mitigation is to pin it on every graduation reading, and this is the command an agent "
        "runs to find it." + _population_note()
    )


# ── Controls. Each drives the checker with a subject built to fail, in the same run. ──────────────


def _write_probe(tmp_path: Path, name: str, body: str) -> Path:
    p = tmp_path / name
    p.write_text(body, encoding="utf-8")
    return p


def _probe_source(description_literal: str) -> str:
    return (
        "import argparse\n"
        "def main():\n"
        f"    ap = argparse.ArgumentParser(description={description_literal})\n"
        '    ap.add_argument("--since")\n'
        "    ap.parse_args()\n"
        "main()\n"
    )


def test_an_emoji_description_is_rejected(tmp_path: Path) -> None:
    """POSITIVE CONTROL for the rc half: the fixed shape passes, the pre-fix shape still fails.

    Both halves in one test, because a control that only shows the bad case passing-to-failing
    cannot tell a working gate from one that refuses everything (coordinator rule 10's inverse).
    """
    broken = _write_probe(
        tmp_path, "broken_cli.py", _probe_source('"\\U0001f6a8 emoji in the description"')
    )
    assert _defect(broken, GATE_CODEC) is not None, (
        "the gate accepted an emoji-carrying description - it cannot detect core#1585 at all"
    )

    fixed = _write_probe(tmp_path, "fixed_cli.py", _probe_source('"plain ascii description"'))
    assert _defect(fixed, GATE_CODEC) is None, (
        "the gate refused a correctly-fixed program, so it refuses everything and discriminates "
        "nothing - one careless repair away from permitting the defect"
    )


def test_a_prologue_that_exits_zero_is_not_accepted(tmp_path: Path) -> None:
    """POSITIVE CONTROL for the options-block half, which `rc == 0` alone cannot provide.

    core#1585's output was 1,898 bytes of prologue - enough to look like output. A gate asserting
    only the exit status is satisfied by exactly that, so this drives the checker with a program
    that exits 0 and prints a prologue with no flag list.
    """
    prologue_only = _write_probe(
        tmp_path,
        "prologue_cli.py",
        'import sys\nprint("usage: probe_cli [options]")\nprint("A tool that never lists flags.")\n'
        "sys.exit(0)\n",
    )
    problem = _defect(prologue_only, GATE_CODEC)
    assert problem is not None and "options block" in problem, (
        "a program that exits 0 having printed only a prologue was accepted; the rc assertion is "
        f"carrying this gate on its own. got: {problem}"
    )
