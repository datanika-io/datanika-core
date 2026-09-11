"""The SSO tier must count what it executed (core#1130 AC1-AC4).

`e2e-sso`'s classifier reads `steps.sso_specs.outcome` — a **process exit code** — and Playwright
exits 0 when every collected test skips. So a run that executed no IdP spec has always been
indistinguishable from one in which all of them passed, and [#1099] exists because this job is one
fixture line from going green for the first time.

Two routes the PR-time guard (`test_e2e_sso_tier_is_measured.py`) cannot block, which is what AC2
requires the demonstration to use:

* **Route A, live now.** `sso-oidc.spec.ts` skips `OIDC JIT provisioning` on
  `!process.env.DATANIKA_E2E_API_KEY_ORG_A` — not the `process.env.X !== "v"` shape that scanner
  matches. **The tier has always reported on 8 of its 9 IdP specs**, and the absent one asserts an
  authorization property.
* **Route B.** Delete `sso-oidc.spec.ts` and `npx playwright test sso-` still collects the ungated
  edge-case specs, which pass. Exit 0 -> `clean`.

⚠️ **The floor is a checked-in constant, deliberately not derived from the spec files**, because a
floor computed from the files present falls as they are deleted and agrees with Route B. This file
is what keeps the constant honest, so lowering it is a visible act at PR time.
"""

from __future__ import annotations

import importlib.util
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
E2E_TESTS = REPO_ROOT / "e2e" / "tests"
SCRIPT = REPO_ROOT / "e2e" / "scripts" / "assert_sso_coverage.py"
CI = REPO_ROOT / ".github" / "workflows" / "ci.yml"

#: A top-level `test(` in a spec file.
_TEST = re.compile(r"^\s*test\(", re.M)


def _module():
    spec = importlib.util.spec_from_file_location("assert_sso_coverage", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def cov():
    return _module()


def _count(name: str) -> int:
    return len(_TEST.findall((E2E_TESTS / name).read_text(encoding="utf-8")))


# ── controls first ───────────────────────────────────────────────────────────────────────


def test_control_the_idp_spec_files_exist_and_carry_tests(cov) -> None:
    """Anti-vacuity. If the files are gone or the counter reads zero, every arithmetic
    assertion below is satisfiable by nothing at all."""
    for name in cov.IDP_SPEC_FILES:
        path = E2E_TESTS / name
        assert path.exists(), f"{name} is gone — the floor below is now about nothing"
        assert _count(name) > 0, f"{name} parsed to 0 tests; the counter is broken, not the file"


def test_control_the_edge_cases_are_deliberately_not_in_the_idp_set(cov) -> None:
    """🔑 The exclusion IS Route B. `sso-edge-cases.spec.ts` is ungated and passes on the
    runner, so counting it would let a deleted IdP file hide behind it — the floor would still
    be met by specs that need no IdP at all."""
    assert "sso-edge-cases.spec.ts" not in cov.IDP_SPEC_FILES
    assert (E2E_TESTS / "sso-edge-cases.spec.ts").exists()


# ── the floor is honest ──────────────────────────────────────────────────────────────────


def test_the_floor_plus_the_accounted_skips_equals_the_real_idp_spec_count(cov) -> None:
    """The constant cannot drift from reality without someone changing this number too.

    ⚠️ Adding an entry to `ACCOUNTED_SKIPS` lowers real coverage of an Enterprise-only auth
    path, so it must lower the floor in the same commit — and this assertion is what forces
    both edits into one diff instead of letting a skip quietly absorb the slack.
    """
    real = sum(_count(name) for name in cov.IDP_SPEC_FILES)
    assert cov.SSO_IDP_EXECUTED_FLOOR + len(cov.ACCOUNTED_SKIPS) == real, (
        f"{len(cov.ACCOUNTED_SKIPS)} accounted skip(s) + floor "
        f"{cov.SSO_IDP_EXECUTED_FLOOR} != {real} IdP specs on disk. Either a spec was added "
        "and the floor was not raised, or one was removed and the floor was not lowered."
    )


def test_every_accounted_skip_names_a_spec_that_actually_exists(cov) -> None:
    """A recorded exemption for a test nobody has is an exemption that hides nothing and
    silently absorbs one unit of the floor."""
    titles = "\n".join(
        (E2E_TESTS / name).read_text(encoding="utf-8") for name in cov.IDP_SPEC_FILES
    )
    for title in cov.ACCOUNTED_SKIPS:
        assert title in titles, f"ACCOUNTED_SKIPS names {title!r}, which is in no IdP spec file"


def test_every_accounted_skip_records_a_reason_worth_reading(cov) -> None:
    """AC3 is *the reason recorded*, not *the skip listed*. A one-word reason is a listing."""
    for title, why in cov.ACCOUNTED_SKIPS.items():
        assert len(why) > 60, f"{title!r}'s reason is too short to be a reason: {why!r}"
        assert "core#1130" in why, f"{title!r}'s reason does not point at the tracking issue"


# ── the routes AC2 requires ──────────────────────────────────────────────────────────────


def _report(files: dict[str, list[tuple[str, str]]]) -> dict:
    return {
        "suites": [
            {
                "title": name,
                "file": name,
                "specs": [{"title": t, "tests": [{"status": st}]} for t, st in specs],
            }
            for name, specs in files.items()
        ]
    }


def _run(payload: dict) -> int:
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "results-sso.json"
        path.write_text(json.dumps(payload), encoding="utf-8")
        return subprocess.run(
            [sys.executable, str(SCRIPT), str(path)], capture_output=True, text=True
        ).returncode


def _full(cov) -> dict[str, list[tuple[str, str]]]:
    """A report in which every IdP spec executed, plus the edge cases."""
    out: dict[str, list[tuple[str, str]]] = {}
    for name in cov.IDP_SPEC_FILES:
        out[name] = [(f"{name}#{i}", "expected") for i in range(_count(name))]
    out["sso-edge-cases.spec.ts"] = [("edge", "expected")]
    return out


def test_control_a_full_run_passes(cov) -> None:
    """A floor that refuses everything is not a floor."""
    assert _run(_report(_full(cov))) == 0


def test_route_b_a_deleted_idp_spec_file_is_refused(cov) -> None:
    """The edge cases still pass and Playwright still exits 0. This is what the job could not
    see."""
    files = _full(cov)
    files.pop(cov.IDP_SPEC_FILES[0])
    assert _run(_report(files)) == 2


def test_route_a_an_unaccounted_skip_is_refused(cov) -> None:
    """A gate the PR-time scanner's shape cannot match — the live one is exactly this."""
    files = _full(cov)
    first = cov.IDP_SPEC_FILES[0]
    files[first] = [(t, "skipped") if i == 0 else (t, s) for i, (t, s) in enumerate(files[first])]
    assert _run(_report(files)) == 2


def test_the_1099_case_every_idp_spec_skips(cov) -> None:
    """The case the issue is named for: zero IdP specs executed, exit 0, previously `clean`."""
    files = _full(cov)
    for name in cov.IDP_SPEC_FILES:
        files[name] = [(t, "skipped") for t, _ in files[name]]
    assert _run(_report(files)) == 2


def test_a_missing_report_is_refused_not_treated_as_a_pass(cov) -> None:
    """No report is no reading. Reporting a pass there is the defect one level up."""
    out = subprocess.run(
        [sys.executable, str(SCRIPT), "no-such-report.json"], capture_output=True, text=True
    )
    assert out.returncode == 2
    assert "no reading" in out.stdout.lower()


# ── the wiring, so the script is not merely present ──────────────────────────────────────


class TestTheJobActuallyRunsIt:
    """A checker nothing invokes is [core#747] one level up and looks identical to a fix."""

    @staticmethod
    def _job() -> dict:
        return yaml.safe_load(CI.read_text(encoding="utf-8"))["jobs"]["e2e-sso"]

    def test_the_specs_step_writes_a_report(self) -> None:
        step = next(s for s in self._job()["steps"] if s.get("id") == "sso_specs")
        assert "results-sso.json" in str(step.get("env")), (
            "the run writes no JSON report, so the coverage step has nothing to read and "
            "refuses on every run"
        )

    def test_the_coverage_step_exists_and_cannot_kill_the_job(self) -> None:
        step = next(s for s in self._job()["steps"] if s.get("id") == "sso_coverage")
        assert step.get("continue-on-error") is True, (
            "without continue-on-error the job dies red before the classifier attaches a "
            "verdict, which is a red with no reading — the thing this tier is about"
        )
        assert "assert_sso_coverage.py" in str(step.get("run"))

    def test_the_classifier_reads_it_before_deciding_clean(self) -> None:
        """🔑 Order is the assertion. After `specs_failed`, because a real red is more
        informative than *we could not tell*; before `clean`, because an exit-0 run that
        executed nothing is precisely what this job could not see."""
        verdict = next(s for s in self._job()["steps"] if s.get("id") == "verdict")
        assert "COVERAGE_OUTCOME" in str(verdict.get("env")), "the classifier cannot see it"
        body = str(verdict.get("run"))
        assert "COVERAGE_OUTCOME" in body
        assert (
            body.index("specs_failed") < body.index("COVERAGE_OUTCOME") < body.index("STATE=clean")
        ), "the coverage branch is in the wrong place in the if-chain"
