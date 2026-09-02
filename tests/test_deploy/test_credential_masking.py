"""Decoded credentials must be masked before they reach `$GITHUB_ENV` (core#943).

`nightly-connector-smoke.yml` decodes a base64 bundle and appends each `KEY=VALUE` line to
`$GITHUB_ENV`. Actions masks values it has **registered**; it registered the bundle
(`QA_CONNECTOR_CREDENTIALS`), not the individual values decoded out of it — those are
different strings manufactured at runtime. Once a value is in `$GITHUB_ENV` it joins the job
environment, and the runner prints the `env:` map in the `##[group]Run …` header of every
subsequent step. So each credential was echoed in cleartext several times per run.

**Nothing reported the transition** — no warning, no annotation, no failed step. That is why
this needs a guard rather than care: the step already carried considered notes about core#270
and core#827, written by someone paying attention, and the hole is structural. Same family as
core#646, where a value crossing a boundary under a name its consumer did not read stopped
being the thing the system was protecting.

Two halves are asserted, and neither implies the other:

1. every `$GITHUB_ENV` write of decoded bundle material is preceded by a mask, in **both**
   materialization steps;
2. the mask has a documented length floor — masking `1` (this bundle carries
   `DATANIKA_CONNECTOR_SMOKE=1`) would redact every digit `1` in the log and produce a run
   nobody can triage. A "safer" workflow that is unreadable is not the fix.

The masking function is also executed below, because "the file contains the string
`::add-mask::`" is satisfied by a helper that never fires.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "nightly-connector-smoke.yml"
SOURCE = WORKFLOW.read_text("utf-8")

GITHUB_ENV_WRITE = re.compile(r'^\s*echo "\$line" >> "\$GITHUB_ENV"', re.MULTILINE)
MASK_CALL = re.compile(r'^\s*mask_value "\$\{line#\*=\}"', re.MULTILINE)


def _bash() -> str:
    exe = shutil.which("bash")
    if exe is None:  # pragma: no cover
        pytest.fail("bash not found; this suite must not silently stop testing the workflow")
    return exe


def test_the_workflow_still_writes_decoded_lines_to_github_env():
    # Anti-vacuity. If the materialization steps are ever restructured, every assertion
    # below becomes trivially true against a file that no longer does this at all.
    assert len(GITHUB_ENV_WRITE.findall(SOURCE)) == 2, (
        "expected exactly two $GITHUB_ENV writes of decoded credential lines "
        "(QA_CONNECTOR_CREDENTIALS and QA_WAVE1_CREDENTIALS)"
    )


def test_every_decoded_value_is_masked_before_it_reaches_github_env():
    """AC1 + AC2 — both bundles, and the mask must come BEFORE the write."""
    masks = [m.start() for m in MASK_CALL.finditer(SOURCE)]
    writes = [m.start() for m in GITHUB_ENV_WRITE.finditer(SOURCE)]
    assert len(masks) == len(writes) == 2
    for mask_at, write_at in zip(masks, writes, strict=True):
        assert mask_at < write_at, (
            "mask_value must run before the $GITHUB_ENV write — masking afterwards leaves "
            "the value already printed in the next step's env block"
        )


def test_the_helper_is_defined_in_both_steps():
    # Each `run:` block is its own shell, so a single definition would leave the second
    # step calling an undefined function. Under `bash -e` that aborts the step, which is
    # at least loud — but the point of the guard is that it never gets that far.
    assert SOURCE.count("mask_value() {") == 2


def test_the_length_floor_is_documented_and_real():
    assert re.search(r'\[ "\$\{#v\}" -ge 8 \]', SOURCE), "expected a length floor of 8"
    # The floor is a judgement with a cost on both sides; it must carry its reason.
    assert "DATANIKA_CONNECTOR_SMOKE=1" in SOURCE


def _mask(value: str) -> str:
    """Run the REAL helper, lifted verbatim out of the workflow."""
    match = re.search(r"( *)mask_value\(\) \{\n(.*?)\n\s*\}", SOURCE, re.DOTALL)
    assert match, "could not lift mask_value() out of the workflow — parser broken, not the file"
    body = "\n".join(line.strip() for line in match.group(2).splitlines())
    script = f'mask_value() {{\n{body}\n}}\nmask_value "$1"\n'
    proc = subprocess.run(
        [_bash(), "-c", script, "bash", value],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env={**os.environ},
    )
    assert proc.returncode == 0, proc.stderr
    return proc.stdout


def test_a_credential_shaped_value_is_masked():
    assert _mask("sk_test_51QhRt7Kx9mNpQwErTyUi").strip() == (
        "::add-mask::sk_test_51QhRt7Kx9mNpQwErTyUi"
    )


def test_a_short_flag_value_is_not_masked():
    # `DATANIKA_CONNECTOR_SMOKE=1`. Masking this redacts every `1` in the log.
    assert _mask("1") == ""
    assert _mask("true") == ""


def test_the_floor_is_exercised_in_both_directions():
    # Seven characters out, eight in — the boundary itself, not a value near it.
    assert _mask("1234567") == ""
    assert _mask("12345678").strip() == "::add-mask::12345678"


def test_the_guard_can_fail():
    """Negative control against the REAL workflow bytes, not a fixture.

    Removing one of the two mask calls must be caught. A control written from the same
    mental model as the check agrees with the check including where the check is wrong.
    """
    anchor = '            mask_value "${line#*=}"\n'
    assert SOURCE.count(anchor) == 2, "anchor did not match twice — the control would be inert"
    broken = SOURCE.replace(anchor, "", 1)

    masks = [m.start() for m in MASK_CALL.finditer(broken)]
    writes = [m.start() for m in GITHUB_ENV_WRITE.finditer(broken)]
    assert len(writes) == 2
    assert len(masks) == 1, "the mutation must remove exactly one mask call"
    # This is what the real assertion above would see: counts no longer agree.
    assert len(masks) != len(writes)


def test_the_floor_guard_can_fail():
    broken = SOURCE.replace('[ "${#v}" -ge 8 ]', '[ "${#v}" -ge 0 ]')
    assert '[ "${#v}" -ge 8 ]' not in broken
    match = re.search(r"( *)mask_value\(\) \{\n(.*?)\n\s*\}", broken, re.DOTALL)
    body = "\n".join(line.strip() for line in match.group(2).splitlines())
    proc = subprocess.run(
        [_bash(), "-c", f'mask_value() {{\n{body}\n}}\nmask_value "$1"\n', "bash", "1"],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env={**os.environ},
    )
    # With the floor removed, the unreadable-log failure mode returns.
    assert proc.stdout.strip() == "::add-mask::1"
