"""[core#1301] The overage soak must be unable to report success having executed nothing.

`overage-e2e-nightly.yml` calls itself the last gate before the overage-charge cutover, and
its only verdict was the exit code of `npx playwright test` -- which is **0 when every
collected test skips**. Its own header has said so in prose since it was written:

    # A "green" run where all 3 tests SKIPPED is NOT a passing soak night.

Nothing enforced it. A rule with no mechanism holds only while somebody happens to check.

⚠️ **The fixtures here are REAL Playwright reports, not hand-written JSON.** Each was produced
by running the real reporter:

* `report-all-skipped.json` -- the REAL `overage-charge-cycle.spec.ts`, run with the GATE env
  removed and everything else mirroring the nightly job. Output: `3 skipped`, `exit 0`. This
  is the vacuous green itself, captured, not simulated.
* `report-2passed-1accounted.json` and `report-unaccounted-skip.json` -- a throwaway spec
  mirroring the real describe title and all three test titles exactly, with the same in-body
  `test.skip(true, ...)`, run from a subdirectory so the report's file BASENAME is
  `overage-charge-cycle.spec.ts`. The mixed 2-passed shape cannot be produced locally from the
  real spec, which needs staging plus Paddle sandbox credentials.

That distinction is the point of `test_control_*` below: a guard driven only by JSON somebody
typed is a guard asserting its author's model of the reporter. These assert against what the
reporter actually emits -- including that Playwright records a skip's reason in
`tests[].annotations[]`, which is the fact the whole "name the skip" half rests on.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
E2E = REPO_ROOT / "e2e"
SPEC = E2E / "tests" / "overage-charge-cycle.spec.ts"
SCRIPT = E2E / "scripts" / "assert_overage_coverage.py"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "overage-e2e-nightly.yml"
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "overage_coverage"

ALL_SKIPPED = FIXTURES / "report-all-skipped.json"
MIXED_OK = FIXTURES / "report-2passed-1accounted.json"
UNACCOUNTED = FIXTURES / "report-unaccounted-skip.json"

#: A top-level `test(` in a spec file. `test.skip(` does not match -- the `.` is not `(`.
_TEST = re.compile(r"^\s*test\(", re.M)

#: An issue citation of the `core#NNNN` shape. Deliberately NOT a GitHub auto-link form:
#: `word#N` is inert, which is what a code comment wants.
_ISSUE = re.compile(r"core#(\d+)")

#: 🚨 The REASON STRING of an in-body `test.skip(true, "...")` — not the file around it.
#:
#: The first version of this scanned the whole spec text, and the mutation harness caught it
#: **unarmed**: reverting the skip to the dead `core#361` left the test green, because the
#: explanatory comment added directly above it also says `core#1302`. That is this project's
#: recorded "a substring check over a whole file is satisfied by a COMMENT" trap, walked into
#: by the person who wrote the rule down. The reason string is the thing Playwright records
#: and the thing an operator reads, so it is the thing to assert on.
_SKIP_REASON = re.compile(r"test\.skip\(\s*true\s*,\s*[\"']([^\"']*)[\"']")


def _module():
    spec = importlib.util.spec_from_file_location("assert_overage_coverage", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def cov():
    return _module()


def _spec_text() -> str:
    return SPEC.read_text(encoding="utf-8")


def _run(cov, report: Path, capsys) -> tuple[int, str]:
    code = cov.main([str(report)])
    return code, capsys.readouterr().out


# ── controls first: the fixtures must really carry the shapes claimed ────────────────────


def test_control_the_real_spec_exists_and_parses_to_three_tests() -> None:
    """Anti-vacuity for every count below. If the spec is gone or the counter reads zero,
    the floor arithmetic is satisfiable by nothing at all."""
    assert SPEC.exists(), "the spec is gone — the floor below is now about nothing"
    assert len(_TEST.findall(_spec_text())) == 3


def test_control_the_fixtures_are_real_playwright_reports() -> None:
    """Each must carry the reporter's own `stats` block. A hand-written stub would not."""
    for path in (ALL_SKIPPED, MIXED_OK, UNACCOUNTED):
        assert path.exists(), f"{path.name} missing"
        report = json.loads(path.read_text(encoding="utf-8"))
        stats = report.get("stats")
        assert stats and "expected" in stats and "skipped" in stats, (
            f"{path.name} has no reporter stats block — it is not a real report"
        )


@pytest.mark.parametrize(
    ("fixture", "executed", "skipped"),
    [
        ("report-all-skipped.json", 0, 3),
        ("report-2passed-1accounted.json", 2, 1),
        ("report-unaccounted-skip.json", 1, 2),
    ],
)
def test_control_each_fixture_carries_the_shape_it_is_named_for(
    cov, fixture: str, executed: int, skipped: int
) -> None:
    """The three arms must actually differ. Two fixtures that classify identically would let
    every assertion below pass while testing one case."""
    summary = cov.classify(json.loads((FIXTURES / fixture).read_text(encoding="utf-8")))
    assert summary["collected"] == 3
    assert summary["executed"] == executed
    assert len(summary["skipped"]) == skipped


def test_control_playwright_really_records_a_skip_reason(cov) -> None:
    """🔑 The 'name the skip' half rests entirely on this being true of the real reporter.

    The list reporter — which is what the run log shows — prints a skipped test's title and
    drops the reason. The JSON reporter carries it. If that ever stopped being so, the guard
    would silently print `(Playwright recorded no reason)` forever and still pass.
    """
    summary = cov.classify(json.loads(ALL_SKIPPED.read_text(encoding="utf-8")))
    reasons = [row["reason"] for row in summary["skipped"]]
    assert all(reasons), "the reporter recorded no skip reason — the guard's display is blind"
    # The all-skipped fixture skipped at the describe-level GATE, so the recorded reason is
    # the GATE's. That is exactly the diagnostic an operator needs on a vacuous-green night.
    assert any("DATANIKA_E2E_OVERAGE_CHARGE" in r for r in reasons)
    assert all(row["where"] for row in summary["skipped"]), "no file:line recorded"


# ── the guard refuses what it must ───────────────────────────────────────────────────────


def test_refuses_the_all_skipped_run(cov, capsys) -> None:
    """THE case. This fixture is the vacuous green captured from the real spec: the run the
    job reported `success` for, with `playwright exit=0`."""
    code, out = _run(cov, ALL_SKIPPED, capsys)
    assert code == 2, "a run that executed nothing was graded a pass"
    assert "0 executed / 3 skipped / 3 collected" in out
    assert "REFUSED" in out


def test_the_all_skipped_run_is_diagnosed_as_a_no_op_not_as_a_bad_skip(cov, capsys) -> None:
    """🔑 Ordering, and the guard's own first run is what earned this assertion.

    A wholly skipped run satisfies BOTH "nothing executed" and "a skip is unaccounted" — the
    two other specs skipped at the GATE and are not in ACCOUNTED_SKIPS. The first version
    graded it as UNACCOUNTED, which is true and useless: it sends the reader hunting a badly
    written skip when the real cause is a missing PADDLE_SANDBOX_* secret.

    So the message must name the no-op and the GATE, and must NOT lead with the skip triage.
    """
    _, out = _run(cov, ALL_SKIPPED, capsys)
    assert "executed NOTHING" in out
    assert "PADDLE_SANDBOX_" in out, "the message does not name the actual likely cause"
    refusal = out[out.index("REFUSED") :]
    assert "UNACCOUNTED" not in refusal, (
        "the refusal leads with skip triage — that is the less informative true statement"
    )


def test_refuses_an_unaccounted_skip(cov, capsys) -> None:
    """core#1130 AC3: a skip is either executed or named. This one is neither."""
    code, out = _run(cov, UNACCOUNTED, capsys)
    assert code == 2
    assert "UNACCOUNTED" in out
    assert "no charge when usage under included" in out


def test_refuses_a_missing_report(cov, capsys, tmp_path) -> None:
    """An absent report is not 'every spec passed'; it is no reading at all."""
    code, out = _run(cov, tmp_path / "nope.json", capsys)
    assert code == 2
    assert "no reading at all" in out.lower() or "NO report" in out


def test_refuses_a_report_from_some_other_run(cov, capsys, tmp_path) -> None:
    """A report containing no overage specs cannot vouch for the overage soak — and an empty
    result from a filtered read is also what a broken filter returns."""
    other = tmp_path / "other.json"
    other.write_text(
        json.dumps(
            {
                "stats": {"expected": 9, "skipped": 0},
                "suites": [
                    {
                        "file": "sso-oidc.spec.ts",
                        "specs": [{"title": "x", "tests": [{"status": "expected"}]}],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    code, out = _run(cov, other, capsys)
    assert code == 2
    assert "no specs from overage-charge-cycle.spec.ts" in out


# ── and passes what it must — a guard that cannot pass is not a gate ─────────────────────


def test_passes_the_real_mixed_run_and_names_the_accounted_skip(cov, capsys) -> None:
    """The shape every soak night has actually produced since 2026-07-21: 2 passed, 1 skipped.

    ⚠️ This arm is as load-bearing as the refusals. A guard that fires on correct work is how
    a guard gets deleted, and this is the state the job is in on a good night.
    """
    code, out = _run(cov, MIXED_OK, capsys)
    assert code == 0, f"the guard refused a legitimately green soak night:\n{out}"
    assert "2 executed / 1 skipped / 3 collected" in out
    assert "Paddle 4xx response marks Charge failed with reason" in out
    assert "accounted:" in out
    # The reason must come from the RUN, not be restated by the guard.
    assert "reported :" in out


# ── the accounted set and the floor stay honest ──────────────────────────────────────────


def test_the_floor_plus_the_accounted_skips_equals_the_real_test_count(cov) -> None:
    """⚠️ Adding an entry to ACCOUNTED_SKIPS gives up real coverage of the charge path, so it
    must lower the floor in the same commit. This assertion is what forces both edits into one
    diff instead of letting an exemption quietly absorb the slack."""
    real = len(_TEST.findall(_spec_text()))
    assert cov.OVERAGE_EXECUTED_FLOOR + len(cov.ACCOUNTED_SKIPS) == real, (
        f"{len(cov.ACCOUNTED_SKIPS)} accounted skip(s) + floor "
        f"{cov.OVERAGE_EXECUTED_FLOOR} != {real} tests in {SPEC.name}"
    )


def test_the_floor_branch_refuses_a_partial_run(cov, capsys, tmp_path) -> None:
    """The floor's own refusal branch, driven directly.

    ⚠️ **Synthetic input, and labelled as such.** With today's constants this branch is
    UNREACHABLE from a real report: 3 tests, 1 accounted skip, floor 2 — so any run with one
    executed spec has an *unaccounted* skip and is refused one check earlier. It becomes
    reachable the moment a second exemption is added, which is exactly when a floor that has
    never executed would be discovered not to work.

    So this is defence-in-depth being proven to function, not a case seen in the wild. The
    three fixtures above are the real ones; this one is hand-built on purpose and says so.
    """
    only_accounted = next(iter(cov.ACCOUNTED_SKIPS))
    report = tmp_path / "partial.json"
    report.write_text(
        json.dumps(
            {
                "stats": {"expected": 1, "skipped": 1},
                "suites": [
                    {
                        "file": "overage-charge-cycle.spec.ts",
                        "specs": [
                            {"title": "cycle: seed", "tests": [{"status": "expected"}]},
                            {"title": only_accounted, "tests": [{"status": "skipped"}]},
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    code, out = _run(cov, report, capsys)
    assert code == 2, "a partial run met the floor"
    assert "1 specs executed, floor is 2" in out


def test_the_floor_is_not_derived_from_the_spec_file(cov) -> None:
    """core#1130's Route B. A floor computed from the tests present falls as they are deleted
    and agrees with the attack, so the constant must be a literal someone has to retype."""
    source = SCRIPT.read_text(encoding="utf-8")
    assignment = re.search(r"^OVERAGE_EXECUTED_FLOOR\s*=\s*(.+)$", source, re.M)
    assert assignment, "the floor constant is gone"
    assert assignment.group(1).strip().isdigit(), (
        "the floor is computed rather than checked in — it now agrees with the attack"
    )


def test_every_accounted_skip_names_a_test_that_actually_exists(cov) -> None:
    """An entry for a title no test has exempts nothing and hides the slack it absorbed."""
    text = _spec_text()
    for title in cov.ACCOUNTED_SKIPS:
        assert title in text, f"ACCOUNTED_SKIPS names {title!r}, which is in no test in the spec"


def test_the_skip_reason_and_the_accounted_entry_cite_the_same_issue(cov) -> None:
    """🔑 The defect this whole PR is about: the skip's reason cited core#361, CLOSED
    2026-07-20 and never about this test, so the exit condition was dead for ~8 weeks while
    the skip fired on all 54 scheduled runs.

    Asserted as a POSITIVE coupling rather than a ban on the old number. A rule shaped 'must
    not say 361' is satisfied by deleting the citation entirely, and would go red on a correct
    future renumbering; this one goes red whenever the two halves stop naming one issue.

    ⚠️ Scoped to the skip's REASON STRING, not the file. See `_SKIP_REASON` — the whole-file
    version of this assertion was measurably unarmed.
    """
    reasons = _SKIP_REASON.findall(_spec_text())
    assert reasons, "no in-body `test.skip(true, ...)` found — this test now asserts nothing"

    in_reason = set(_ISSUE.findall(" ".join(reasons)))
    in_guard = set(_ISSUE.findall("\n".join(cov.ACCOUNTED_SKIPS.values())))
    assert in_guard, "the accounted entry cites no issue — it has no exit condition"
    assert in_reason, "the skip's reason cites no issue — it has no exit condition"
    assert in_guard & in_reason, (
        f"the guard's exit condition {sorted(in_guard)} and the skip's own reason "
        f"{sorted(in_reason)} name different issues — the two halves have drifted apart"
    )


# ── and the workflow actually RUNS it ────────────────────────────────────────────────────


def test_the_workflow_invokes_the_guard() -> None:
    """🚨 The load-bearing one. A guard nothing runs is this bug one level up and looks
    identical to a fix — the same shape as `deploy/server/` shipping correct content to a path
    no workflow read (core#747)."""
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["overage-e2e"]["steps"]
    invoking = [s for s in steps if SCRIPT.name in str(s.get("run", ""))]
    assert invoking, f"no step in {WORKFLOW.name} runs {SCRIPT.name}"

    step = invoking[0]
    assert step.get("if") == "always()", (
        "the guard must not be skippable — on the green path it is the only thing that can "
        "tell a pass from a no-op"
    )
    assert not step.get("continue-on-error"), (
        "nothing downstream reads this run, so a swallowed red here is the defect the guard "
        "was built to catch"
    )


def test_the_failure_filer_is_keyed_on_the_coverage_step_too() -> None:
    """A step-keyed filer that names only the soak step files NOTHING when the coverage
    assertion refuses — and a vacuous soak night is precisely what needs filing.

    ⚠️ This pairs with `test_ci_autofiler_trigger.py`, which forced the step-keying in the
    first place: adding the `always()` coverage step made the job-keyed filer unable to tell
    a failing test from a failing assertion (core#873). That guard checks the filer is
    step-keyed; this one checks it is keyed on the RIGHT steps. Satisfying the first by
    keying on `soak` alone would pass there and silently drop half the alerts.
    """
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["overage-e2e"]["steps"]
    filer = next(s for s in steps if "gh issue create" in str(s.get("run", "")))
    cond = str(filer.get("if", ""))

    assert "steps.coverage.outcome" in cond, (
        "the filer ignores the coverage assertion — an all-skipped night would redden the "
        "job and file nothing"
    )
    assert "steps.soak.outcome" in cond, "the filer ignores the spec run itself"
    assert cond.strip().startswith("always()"), (
        "a condition with no leading status function is implicitly success()-gated, so it "
        "can never fire on a real failure (core#873's own proposed fix had this bug)"
    )
    assert "== 'failure'" not in cond, (
        "an equality gate on a step outcome misses 'never ran' — a skipped step's outcome "
        "is empty, not 'failure' (core#1260)"
    )


def test_the_guard_reads_the_report_the_test_step_writes() -> None:
    """The emitter and the reader must name ONE string. A report written to one path and read
    from another produces 'does not exist' — which this guard refuses correctly, but for a
    reason that would send the operator hunting the secrets instead of the typo."""
    workflow = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    steps = workflow["jobs"]["overage-e2e"]["steps"]

    written = {
        s["env"]["PLAYWRIGHT_JSON_OUTPUT_NAME"]
        for s in steps
        if isinstance(s.get("env"), dict) and "PLAYWRIGHT_JSON_OUTPUT_NAME" in s["env"]
    }
    assert len(written) == 1, f"expected exactly one report path, got {written}"
    report = written.pop()

    reader = next(s for s in steps if SCRIPT.name in str(s.get("run", "")))
    assert report in reader["run"], (
        f"the test step writes {report} but the guard reads {reader['run']!r}"
    )
