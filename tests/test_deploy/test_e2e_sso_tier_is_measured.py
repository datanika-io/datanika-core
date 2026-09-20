"""The SSO tier's env gates must be supplied by the step that runs the specs (core#1099).

`e2e/tests/sso-{oidc,saml}.spec.ts` put their whole `describe` behind

```ts
const SSO_GATE = process.env.DATANIKA_E2E_SSO_AUTHENTIK !== "1";
test.skip(() => SSO_GATE, "Requires DATANIKA_E2E_SSO_AUTHENTIK=1 + Authentik container");
```

so if that variable is anything but the exact string `"1"`, **all nine Authentik specs skip,
Playwright exits 0, and `e2e-sso`'s classifier emits `clean` — "SSO specs passed and the job is
green."** The five ungated `sso-edge-cases` specs carry the run and pass on the runner, so the
tally reads `0 failed / 11 skipped / 5 passed` and the colour is indistinguishable from a real
pass. Measured on the real spec files, four arms, core#1099.

That green matters more than it looks: `e2e-sso` has been red on every push for weeks for
core#830, so a green from this job is the most anticipated signal in the pipeline — and this
route produces exactly that colour having tested no IdP path at all.

⚠️ **The `no_verdict` Telegram alert cannot catch it.** That step exists because *"an absent
verdict on a job that is normally red is the easiest thing in this pipeline to misread as a
fix"* — but it is gated on `steps.verdict.outputs.state == 'no_verdict'`, which is the `else`
branch for `SPECS_OUTCOME` being `skipped`. This route makes it `success`.

**What this file does and does not close.** It pins the *agreement* between the two artifacts:
the variable and the value the specs compare against are derived from the spec sources, what
the step supplies is derived from `ci.yml`, and neither is restated here — so a rename or a
deletion on **either** side goes red. It does **not** reach the residual, which is a `success`
from a run that executed zero IdP specs by some route nobody has thought of; that needs the
step to read its own tally and is `ci.yml`'s owner's call (core#1099 carries the ask).

🔑 **The gate this file does NOT guard, and why — the measurement that narrowed the fix.**
The other way to empty this suite is dropping `DATANIKA_E2E_SLOW`, which
`playwright.config.ts`'s `grepInvert: /@slow/` reads. That one is **self-caught**: all three
SSO spec files are `@slow`, so Playwright collects nothing and exits **1** with
`Error: No tests found.` — measured, not assumed. `e2e-sso` therefore does not need
`e2e-staging`'s `Assert the @slow specs were actually collected` step for that reason. One gate
is self-guarding and the other is not, and only measuring both said which. Guarding the
self-caught one here would have been a redundant check standing in front of a real signal
(cloud#176).

**The requirement is POSITIVE — the step must supply the gate — never a ban on some spelling.**
A negative assertion is satisfied by the corrected artifact's own denial, and by deleting the
line entirely (`WORKFLOW_RULES.md` §4).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CI = REPO_ROOT / ".github" / "workflows" / "ci.yml"
SPEC_DIR = REPO_ROOT / "e2e" / "tests"

SSO_JOB = "e2e-sso"
SPECS_STEP = "Run SSO specs"

#: `const SSO_GATE = process.env.DATANIKA_E2E_SSO_AUTHENTIK !== "1";`
#: The value is captured, not assumed — the specs compare against `"1"` today, and a guard
#: that hardcoded that would go green if the spec changed to `"true"` while `ci.yml` did not.
_GATE_DECL = re.compile(
    r"""^\s*const\s+(?P<name>\w+)\s*=\s*process\.env\.(?P<var>[A-Za-z0-9_]+)\s*"""
    r"""!==\s*["'](?P<value>[^"']*)["']\s*;""",
    re.M,
)


@dataclass(frozen=True)
class Gate:
    """One `process.env.X !== "v"` gate that actually skips specs."""

    spec: str
    const: str
    var: str
    value: str


def _sso_spec_files() -> list[Path]:
    return sorted(SPEC_DIR.glob("sso-*.spec.ts"))


def gates_in(source: str, spec_name: str) -> list[Gate]:
    """Gate declarations in one spec file that are *used* to skip something.

    The `test.skip(` usage check is load-bearing: a `const X = process.env.Y !== "1"` that
    nothing consults would otherwise satisfy every assertion below while gating nothing, and a
    guard satisfied by a dead read is the shape it exists to catch.
    """
    found: list[Gate] = []
    for m in _GATE_DECL.finditer(source):
        const = m.group("name")
        # ⚠️ Bounded by `;` (statement end), NOT by `)`. The real idiom is
        # `test.skip(() => SSO_GATE, "…")`, so a `[^)]*` window stops at the arrow
        # function's own closing paren and matches nothing — which made this scanner
        # return `[]` against the real tree on its first run. The positive control
        # below is what caught it; without that control the whole file would have gone
        # green while guarding nothing.
        used = re.search(
            r"\.skip\s*\([^;]{0,300}?\b" + re.escape(const) + r"\b",
            source,
        )
        if used:
            found.append(
                Gate(spec=spec_name, const=const, var=m.group("var"), value=m.group("value"))
            )
    return found


def all_gates() -> list[Gate]:
    out: list[Gate] = []
    for path in _sso_spec_files():
        out.extend(gates_in(path.read_text(encoding="utf-8"), path.name))
    return out


def sso_specs_step_env() -> dict[str, str]:
    """The `env:` of the step that actually runs the SSO specs, out of the real `ci.yml`."""
    doc = yaml.safe_load(CI.read_text(encoding="utf-8"))
    jobs = doc.get("jobs") or {}
    assert SSO_JOB in jobs, (
        f"the {SSO_JOB!r} job vanished from ci.yml — this guard is now testing nothing. "
        "If it was renamed, rename it here rather than deleting the guard."
    )
    for step in jobs[SSO_JOB].get("steps") or []:
        if step.get("name") == SPECS_STEP:
            return {str(k): str(v) for k, v in (step.get("env") or {}).items()}
    pytest.fail(
        f"no step named {SPECS_STEP!r} in the {SSO_JOB!r} job. A guard that cannot find its "
        "target passes silently, so this is a failure rather than a skip."
    )


def unsupplied(gates: list[Gate], env: dict[str, str]) -> list[str]:
    """THE PREDICATE. Gates the step fails to supply at the value the specs demand.

    Module-level on purpose, so the arming tests below exercise the same code path the real
    assertion does. A control written *beside* a check keeps passing when the check's own
    transform is removed.
    """
    problems: list[str] = []
    for gate in gates:
        actual = env.get(gate.var)
        if actual is None:
            problems.append(
                f"{gate.spec}: gates on ${gate.var}, which `{SPECS_STEP}` does not set at all "
                f"— every spec behind {gate.const} would skip and the step would still exit 0"
            )
        elif actual != gate.value:
            problems.append(
                f"{gate.spec}: gates on ${gate.var} == {gate.value!r}, but `{SPECS_STEP}` sets "
                f"it to {actual!r} — the comparison is a string equality, so this skips "
                "everything just as surely as omitting it"
            )
    return problems


# ---------------------------------------------------------------------------
# Positive control first: a scanner that finds nothing is indistinguishable from
# a clean tree, and would make every assertion below vacuously true.
# ---------------------------------------------------------------------------


def test_control_the_gate_scanner_finds_the_real_gates() -> None:
    specs = _sso_spec_files()
    assert len(specs) >= 3, (
        f"expected the three sso-*.spec.ts files, found {[p.name for p in specs]}. If the SSO "
        "specs moved, point this guard at them — do not let it scan an empty directory."
    )

    gates = all_gates()
    assert gates, (
        "no env gate found in any SSO spec file. Either the gating idiom changed, or this "
        "scanner has stopped matching it. Both make the assertions below vacuous, so this is "
        "the failure that must fire first."
    )
    assert {g.spec for g in gates} >= {"sso-oidc.spec.ts", "sso-saml.spec.ts"}, (
        "the two wholly-gated IdP spec files are the ones this guard exists for; the scanner "
        f"found gates only in {sorted({g.spec for g in gates})}."
    )


def test_the_gate_is_read_from_the_spec_not_restated_here() -> None:
    """Both halves of the comparison come out of the source, so a change on either side moves.

    This is the assertion that makes the guard survive a rename: nothing in this file names
    `DATANIKA_E2E_SSO_AUTHENTIK` or `"1"`.
    """
    gates = all_gates()
    for gate in gates:
        assert gate.var.startswith("DATANIKA_"), (
            f"{gate.spec} gates on ${gate.var}, which is not one of ours. A spec gated on a "
            "third-party or ambient variable cannot be guaranteed by our own workflow."
        )
        assert gate.value != "", (
            f"{gate.spec}: ${gate.var} is compared against the empty string, so the gate is "
            "open whenever the variable is unset — which is not a gate."
        )


# ---------------------------------------------------------------------------
# The assertion.
# ---------------------------------------------------------------------------


def test_every_sso_gate_is_supplied_by_the_step_that_runs_the_specs() -> None:
    problems = unsupplied(all_gates(), sso_specs_step_env())
    assert not problems, (
        "the SSO tier can report a PASS having executed none of its IdP specs (core#1099):\n  "
        + "\n  ".join(problems)
        + "\n\nPlaywright exits 0 when every collected test skips, so `Run SSO specs` reports "
        "`success`, the classifier emits `clean`, and the job goes green having tested no SSO "
        "path. The `no_verdict` alert cannot fire — it keys on the specs step NOT being "
        "`success`."
    )


# ---------------------------------------------------------------------------
# In-suite arming. A guard proved discriminating once by an external harness is a
# claim about a past session; this runs every time CI does.
# ---------------------------------------------------------------------------


def test_arming_the_predicate_is_red_when_the_step_drops_a_gate() -> None:
    gates = all_gates()
    env = sso_specs_step_env()
    for gate in gates:
        crippled = {k: v for k, v in env.items() if k != gate.var}
        problems = unsupplied(gates, crippled)
        assert any(gate.var in p for p in problems), (
            f"removing ${gate.var} from `{SPECS_STEP}` left the predicate silent. This is the "
            "exact mutation the guard exists to catch."
        )


def test_arming_the_predicate_is_red_when_the_step_supplies_the_wrong_value() -> None:
    """The likelier accident: `"1"` written as `"true"`, or as an unquoted YAML `1`.

    Both are *plausible* edits and both close the gate silently, because the spec compares
    strings. An unquoted `1` in YAML is an int, which is why `sso_specs_step_env()` stringifies
    — and `str(1) == "1"`, so that particular spelling is in fact harmless. The wrong-word
    spelling is not.
    """
    gates = all_gates()
    env = sso_specs_step_env()
    for gate in gates:
        wrong = {**env, gate.var: gate.value + "x"}
        problems = unsupplied(gates, wrong)
        assert any(gate.var in p for p in problems), (
            f"setting ${gate.var} to a value the specs do not accept left the predicate "
            "silent. String equality is the whole gate."
        )


def test_arming_the_predicate_is_green_on_the_real_pair() -> None:
    """The control that stops the two arming tests above passing by a predicate that always
    fails. Narrowing a check until nothing satisfies it is a worse bug than the one it fixed,
    and a silent one."""
    assert unsupplied(all_gates(), sso_specs_step_env()) == []


def test_arming_the_scanner_ignores_a_gate_nothing_skips_on() -> None:
    """A dead `process.env` read must not be mistaken for a gate.

    Without the `test.skip(` usage check, a leftover constant would be reported as a gate and
    this guard would start demanding `ci.yml` supply a variable that governs nothing — a false
    red, which is how a guard gets deleted.
    """
    dead = 'const UNUSED_GATE = process.env.DATANIKA_E2E_NOT_A_GATE !== "1";\n'
    assert gates_in(dead, "synthetic.spec.ts") == []

    live = dead + 'test.skip(() => UNUSED_GATE, "reason");\n'
    found = gates_in(live, "synthetic.spec.ts")
    assert [g.var for g in found] == ["DATANIKA_E2E_NOT_A_GATE"], (
        "the scanner must still recognise a gate once something skips on it — a matcher "
        "narrowed until it finds nothing is the failure this control exists to catch."
    )


# ---------------------------------------------------------------------------
# core#1130 AC5 — the residual this file's docstring names is CLOSED. Pinned here
# so it cannot come undone quietly.
# ---------------------------------------------------------------------------
#
# 🔑 **How this section came to exist, because the method is the point.** It was written as a
# `xfail(strict=True)` ratchet, on the docstring's word that the residual was open and on
# core#1130's measurement that `PLAYWRIGHT_JSON_OUTPUT_NAME` had **0 hits** in the job. It went
# `XPASS(strict)` on the first run. The AC had shipped in between, and a ratchet without `strict`
# would have sat here asserting a closed gap was open — the exact shape core#1130 was filed about,
# in a guard written to prevent it. **An open issue is not evidence the work is undone**
# (`WORKFLOW_RULES` §4), and the `strict` is what said so.

#: The step that reads the Playwright report and decides whether the IdP specs executed.
COVERAGE_STEP_ID = "sso_coverage"
#: The classifier's env key carrying that step's outcome, and the state it must produce.
COVERAGE_OUTCOME_VAR = "COVERAGE_OUTCOME"


class HarnessError(Exception):
    """A setup invariant failed — never an `AssertionError`, so no marker can absorb it."""


def _sso_steps() -> list[dict]:
    doc = yaml.safe_load(CI.read_text(encoding="utf-8"))
    jobs = (doc or {}).get("jobs") or {}
    if SSO_JOB not in jobs:
        raise HarnessError(f"{SSO_JOB!r} is not in ci.yml — repoint this guard, do not delete it")
    steps = jobs[SSO_JOB].get("steps") or []
    if len(steps) < 5:
        raise HarnessError(f"{SSO_JOB!r} has {len(steps)} steps; that is not the real job")
    return steps


def _step_by_id(step_id: str) -> dict:
    for step in _sso_steps():
        if step.get("id") == step_id:
            return step
    raise HarnessError(f"no step with id={step_id!r} in {SSO_JOB!r}")


def test_control_the_job_reader_finds_the_classifier() -> None:
    """Run first. Every assertion below is vacuous if this cannot read the job."""
    verdict = _step_by_id("verdict")
    assert "STATE=clean" in str(verdict.get("run") or ""), (
        "the classifier no longer emits STATE=clean — this section is reading the wrong step"
    )


def test_a_step_reads_the_playwright_report_and_counts_what_executed() -> None:
    """core#1130 AC1-AC4: the tier reports how many IdP specs it actually EXECUTED.

    `steps.sso_specs.outcome` is a **process exit code**, and Playwright exits 0 when every
    collected test skips. So the count has to come from the report, not from the exit status.
    """
    run = str(_step_by_id(COVERAGE_STEP_ID).get("run") or "")
    assert "assert_sso_coverage.py" in run, (
        f"the {COVERAGE_STEP_ID!r} step no longer runs the coverage assertion"
    )
    report = str(_step_by_id("sso_specs").get("env", {}).get("PLAYWRIGHT_JSON_OUTPUT_NAME") or "")
    assert report, "the specs step no longer writes a JSON report, so nothing can count executions"
    assert report in run, (
        f"the coverage step does not read {report!r}, the report the specs step writes — "
        "so whatever it counts, it is not this run"
    )
    assert (REPO_ROOT / "e2e" / "scripts" / "assert_sso_coverage.py").is_file()


def test_the_coverage_result_reaches_the_verdict() -> None:
    """core#1130 AC5. **This is the half that a named step does not give you.**

    The coverage step is `continue-on-error: true` **on purpose** — that is what lets its
    *outcome* be `failure` while the job continues far enough for the classifier to read it. The
    consequence is that the step cannot fail the job by itself, so if the classifier ignored it,
    the assertion would be a **reporter rather than a gate**: loud in the log, absent from the
    verdict, and green.
    """
    verdict = _step_by_id("verdict")
    env = {str(k): str(v) for k, v in (verdict.get("env") or {}).items()}
    assert COVERAGE_OUTCOME_VAR in env, (
        f"the classifier does not receive the {COVERAGE_STEP_ID!r} step's outcome at all, so a "
        "run that executed zero IdP specs can still be classified `clean` (core#1130 AC5)"
    )
    assert COVERAGE_STEP_ID in env[COVERAGE_OUTCOME_VAR], env[COVERAGE_OUTCOME_VAR]

    run = str(verdict.get("run") or "")
    assert f"${COVERAGE_OUTCOME_VAR}" in run, (
        f"{COVERAGE_OUTCOME_VAR} is supplied to the classifier and never read — an unread input "
        "is the shape of a check that reports instead of gating"
    )
    # The branch must reach a NON-green state, and must be tested before `clean` can be emitted.
    branch = run.index(f"${COVERAGE_OUTCOME_VAR}")
    assert run.index("STATE=clean") > branch, (
        "the coverage branch is tested AFTER `clean` is decided, so it cannot prevent one"
    )
    tail = run[branch : run.index("STATE=clean")]
    assert "no_verdict" in tail or "STATE=specs_failed" in tail, (
        "the coverage branch does not lead to a non-green state; AC5 asks for `no_verdict` or red"
    )
    # 🔑 The branch must fire on the value a FAILED step's outcome actually takes. Without this,
    # retyping `= "failure"` to anything else disarms the gate and leaves every assertion above
    # green — measured, on this guard, before it shipped. `outcome` is one of
    # success|failure|cancelled|skipped, so requiring `failure` pins the contract rather than a
    # spelling somebody chose (`WORKFLOW_RULES` §4).
    assert '"failure"' in tail, (
        f'the {COVERAGE_STEP_ID!r} branch never compares against "failure", so it cannot fire '
        "when the coverage assertion fails — the gate is present and unreachable"
    )


def test_the_floor_is_checked_in_rather_than_derived_from_the_spec_files() -> None:
    """A floor computed from the files present falls as they are deleted — and agrees with it."""
    source = (REPO_ROOT / "e2e" / "scripts" / "assert_sso_coverage.py").read_text(encoding="utf-8")
    assert "SSO_IDP_EXECUTED_FLOOR = " in source, (
        "the executed floor is no longer a checked-in constant, so deleting an IdP spec file "
        "would lower the bar it is measured against (core#1130's Route B)"
    )
