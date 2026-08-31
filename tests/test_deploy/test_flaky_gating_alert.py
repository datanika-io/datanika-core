"""core#757 — a flaky GATING spec must alert and file, not produce a silent green.

## The defect this guards

`e2e/playwright.config.ts` sets ``retries: process.env.CI ? 1 : 0``. A gating spec
that fails then passes on retry is ``flaky``; Playwright exits **0**; the job
concludes ``success``; and every alert in ``ci.yml`` is ``if: failure()``. So an
intermittent product bug produced a green run, no Telegram alert and no issue.

Observed on `de00365` (run 33328874370): ``1 flaky / 16 skipped / 45 passed``,
conclusion ``success``, no `e2e-failure` issue — the auto-filed series #698 →
#740 → #751 skips it. That commit was promoted and is an ancestor of `master`.

`docs/QA_RULES.md` §12 already forbade retrying an assertion. The rule was
written; the pipeline did not implement it.

## Provenance of the fixtures — read this before trusting them

``de00365-uploaded-report.json`` is the **real artifact**, extracted byte-identical
from the `playwright-report` uploaded by run 33328874370. It is used in
`test_the_real_de00365_artifact_is_no_evidence_not_clean`.

The *flaky* fixtures are faithful reconstructions in Playwright's JSON-reporter
schema, not captures — and the reason is itself a finding. The real run could not
have produced one: no JSON reporter was configured (this PR adds it), and the
uploaded HTML report was **clobbered** by the three ``npx playwright test --list``
invocations that run after the tests in the "Assert the @slow specs were actually
collected" step. That is why the genuine artifact reads ``total: 0, flaky: 0,
ok: true`` for a run that had one flaky gating spec, with a ``duration`` of 61 ms
and a ``startTime`` 4.3 minutes after the golden-path test began.

Both halves of that are fixed here: the detector reads a path outside
``playwright-report/``, and it runs before the ``--list`` steps.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DETECTOR = REPO_ROOT / "e2e" / "scripts" / "detect_flaky_gating.py"
CI_YML = REPO_ROOT / ".github" / "workflows" / "ci.yml"
PW_CONFIG = REPO_ROOT / "e2e" / "playwright.config.ts"
REAL_ARTIFACT = REPO_ROOT / "tests" / "test_data" / "playwright" / "de00365-uploaded-report.json"

# The real failure signature from de00365, kept verbatim so the fixture is
# recognisable as the run it reconstructs.
GOLDEN_PATH_TITLE = "new user signs up, wires CSV → DuckDB, runs it, and sees rows land"
REAL_ERROR = (
    'run for "qa golden upload 1788115956097" did not reach a terminal state '
    'within 150000ms (last seen: status="(no run row appeared)", rows=0).'
)


def _load_detector():
    spec = importlib.util.spec_from_file_location("detect_flaky_gating", DETECTOR)
    assert spec and spec.loader, f"cannot import the detector at {DETECTOR}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def detector():
    assert DETECTOR.exists(), (
        f"{DETECTOR} is missing — core#757's detector is what makes a flaky "
        f"gating spec visible. Without it the pipeline is back to a silent green."
    )
    return _load_detector()


def _spec(title: str, status: str, *, tags: list[str] | None = None) -> dict:
    """One spec in Playwright's JSON-reporter schema."""
    results = (
        [{"status": "failed", "retry": 0, "error": {"message": REAL_ERROR}}]
        if status == "flaky"
        else []
    )
    results.append({"status": "passed", "retry": 1 if status == "flaky" else 0})
    return {
        "title": title,
        "ok": status != "unexpected",
        "tags": tags or [],
        "tests": [
            {
                "expectedStatus": "passed",
                "projectName": "chromium",
                "status": status,
                "results": results,
            }
        ],
    }


def _report(*, describe: str, specs: list[dict], file_title: str = "golden-path.spec.ts") -> dict:
    """A Playwright JSON report with one file suite and one describe suite."""
    return {
        "config": {},
        "suites": [
            {
                "title": file_title,
                "file": file_title,
                "specs": [],
                "suites": [{"title": describe, "specs": specs}],
            }
        ],
        "errors": [],
        "stats": {"expected": len(specs), "unexpected": 0, "flaky": 0, "skipped": 0},
    }


# ---------------------------------------------------------------------------
# 1. The defect: a flaky GATING spec must be detected.
# ---------------------------------------------------------------------------


def test_flaky_gating_spec_is_detected(detector):
    """The de00365 shape: one gating spec failed, retried, passed. Run was green."""
    report = _report(
        describe="golden path @slow",
        specs=[_spec(GOLDEN_PATH_TITLE, "flaky", tags=["@slow"])],
    )
    result = detector.analyse(report)

    assert result["status"] == "flaky", (
        "A gating spec that failed and passed on retry must be reported as flaky. "
        "This is exactly the run that concluded `success` and filed nothing."
    )
    assert len(result["flaky_gating"]) == 1
    assert GOLDEN_PATH_TITLE in result["flaky_gating"][0]


def test_flaky_gating_spec_is_detected_when_nested_deeper(detector):
    """The marker check walks the whole ancestry, not just the immediate parent."""
    report = {
        "suites": [
            {
                "title": "golden-path.spec.ts",
                "specs": [],
                "suites": [
                    {
                        "title": "golden path @slow",
                        "specs": [],
                        "suites": [
                            {"title": "inner", "specs": [_spec(GOLDEN_PATH_TITLE, "flaky")]}
                        ],
                    }
                ],
            }
        ]
    }
    assert detector.analyse(report)["status"] == "flaky"


# ---------------------------------------------------------------------------
# 2. The permissive control (QA_RULES §3): a healthy run must stay green.
#    A detector that fires on everything passes test 1 and is worthless.
# ---------------------------------------------------------------------------


def test_a_fully_passing_run_is_clean(detector):
    report = _report(
        describe="golden path @slow",
        specs=[_spec(GOLDEN_PATH_TITLE, "expected"), _spec("another spec", "expected")],
    )
    result = detector.analyse(report)

    assert result["status"] == "clean", "A run with no flaky specs must not alert."
    assert result["flaky_gating"] == []


def test_a_skipped_spec_does_not_alert(detector):
    report = _report(describe="golden path @slow", specs=[_spec(GOLDEN_PATH_TITLE, "skipped")])
    assert detector.analyse(report)["status"] == "clean"


# ---------------------------------------------------------------------------
# 3. The discriminating control: informational specs do NOT hold the gate, so a
#    flaky one is not news. A detector that fires on both is the same defect one
#    level up — it would make the informational tier gate-like by the back door.
# ---------------------------------------------------------------------------


def test_a_flaky_informational_spec_does_not_alert(detector):
    report = _report(
        describe="overage soak @informational",
        specs=[_spec("charges an overage after the cycle rolls", "flaky")],
    )
    result = detector.analyse(report)

    assert result["status"] == "clean", (
        "An informational spec does not hold the gate (tier policy §5), so a "
        "flaky one must not alert. Firing on both tiers is the same defect one "
        "level up."
    )
    assert result["flaky_gating"] == []
    assert len(result["flaky_informational"]) == 1


def test_gating_and_informational_are_separated_in_one_report(detector):
    """Both tiers in one run: only the gating half may alert."""
    report = {
        "suites": [
            {
                "title": "golden-path.spec.ts",
                "specs": [],
                "suites": [
                    {"title": "golden path @slow", "specs": [_spec(GOLDEN_PATH_TITLE, "flaky")]},
                    {"title": "soak @informational", "specs": [_spec("soak spec", "flaky")]},
                ],
            }
        ]
    }
    result = detector.analyse(report)
    assert result["status"] == "flaky"
    assert len(result["flaky_gating"]) == 1
    assert len(result["flaky_informational"]) == 1


# ---------------------------------------------------------------------------
# 4. The trap this whole ticket is about: an EMPTY report is not a clean one.
#    Uses the genuine uploaded artifact from the de00365 run.
# ---------------------------------------------------------------------------


def test_the_real_de00365_artifact_is_no_evidence_not_clean(detector):
    """The real uploaded report says `flaky: 0, ok: true` for a run that had one.

    It was clobbered by the trailing `playwright test --list` calls. If the
    detector called this "clean" it would reproduce core#757 one level up:
    a green that would look identical had the thing failed.
    """
    assert REAL_ARTIFACT.exists(), f"missing captured artifact {REAL_ARTIFACT}"
    report = json.loads(REAL_ARTIFACT.read_text(encoding="utf-8"))

    # Provenance guard: if someone replaces the fixture with a populated report,
    # this test silently stops testing the thing it was written for.
    assert report["stats"]["total"] == 0 and report["stats"]["ok"] is True, (
        "The fixture must remain the real clobbered artifact: total=0, ok=true."
    )

    result = detector.analyse(report)
    assert result["status"] == "no-evidence", (
        "An empty report must be 'no-evidence', never 'clean'. The real artifact "
        "asserts ok:true with zero tests; treating that as a pass is the defect."
    )


def test_an_unreadable_report_is_no_evidence(detector, tmp_path):
    missing = tmp_path / "does-not-exist.json"
    assert detector.load(missing) is None
    rc = detector.main([str(missing)])
    assert rc == 0, "the detector is not the gate — it must not fail the job itself"


# ---------------------------------------------------------------------------
# 5. Wiring. A perfect detector nothing calls is not a fix.
# ---------------------------------------------------------------------------


def test_playwright_config_emits_a_json_report():
    """Without a JSON reporter the detector has nothing to read — and would
    silently report `no-evidence` forever, which is a different silent failure."""
    cfg = PW_CONFIG.read_text(encoding="utf-8")
    assert '"json"' in cfg or "'json'" in cfg, (
        "e2e/playwright.config.ts must configure the json reporter; the detector reads its output."
    )


def test_json_report_is_written_outside_the_clobbered_report_folder():
    """`playwright-report/` is regenerated by the trailing `--list` calls.

    The de00365 artifact is the proof. A JSON report written in there would be
    destroyed the same way, and the detector would read an empty file.
    """
    cfg = PW_CONFIG.read_text(encoding="utf-8")
    # Find the json reporter's outputFile.
    assert "outputFile" in cfg, "the json reporter needs an explicit outputFile"
    for line in cfg.splitlines():
        if "outputFile" in line and "json" in cfg[: cfg.index(line) + len(line)]:
            assert "playwright-report/" not in line, (
                "The JSON report must not live in playwright-report/ — that folder "
                "is regenerated by the `playwright test --list` calls in the assert "
                "step, which is how the de00365 artifact ended up empty."
            )


def test_ci_runs_the_detector_and_alerts_on_flaky_gating():
    ci = CI_YML.read_text(encoding="utf-8")

    assert "detect_flaky_gating.py" in ci, (
        "ci.yml must run e2e/scripts/detect_flaky_gating.py after the gating run."
    )

    e2e_job = ci[ci.index("\n  e2e-staging:") :]
    e2e_job = e2e_job[: e2e_job.index("\n  e2e-sso:")]

    assert "detect_flaky_gating.py" in e2e_job, "the detector must run in the e2e-staging job"

    # The alert and the filer must both be reachable on a flaky-but-green run,
    # i.e. guarded by the detector's own output rather than by failure().
    assert "flaky_gate" in e2e_job, (
        "the detector step needs id `flaky_gate` so later steps can branch on it"
    )
    assert e2e_job.count("steps.flaky_gate.outputs.status == 'flaky'") >= 2, (
        "both the Telegram alert and the issue filer must fire on a flaky gating "
        "run. Guarding only one of them recreates half of core#757."
    )


def test_the_detector_runs_before_the_list_calls_that_clobber_reports():
    """Ordering is load-bearing, not cosmetic."""
    ci = CI_YML.read_text(encoding="utf-8")
    e2e_job = ci[ci.index("\n  e2e-staging:") :]
    e2e_job = e2e_job[: e2e_job.index("\n  e2e-sso:")]

    # Match the STEP DECLARATION, not the phrase (QA_RULES §7). The step name is
    # also quoted inside the detector step's own comment, so a bare-phrase search
    # finds that comment first and reports the ordering backwards — which is how
    # the first version of this test failed against correct wiring.
    detector_at = e2e_job.index("- name: Detect flaky gating specs")
    list_assert_at = e2e_job.index("- name: Assert the @slow specs were actually collected")

    assert detector_at < list_assert_at, (
        "The detector must run BEFORE the step whose `playwright test --list` "
        "calls regenerate the report folder. de00365 is what happens otherwise."
    )
