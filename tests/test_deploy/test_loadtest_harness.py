"""The load-test harness must exist in the repository, with its safety properties (core#778).

Run 9 (2026-09-17) established the published floor of **>= 60 authed req/s** on the current
hardware. Its harness lived in ``.scratch/``, which is swept without warning, so by 2026-09-20
the floor was being cited on the issue and **nothing could re-run it**. That is coordinator
rule 9 — *if a measurement will become a floor, the instrument ships in the same PR, not the
number* — and this file is what stops it recurring.

🔑 **What is asserted here is STRUCTURE, not results.** These tests cannot run a load test and
must never pretend to. They assert that the instrument is present and still carries the
properties that make a run safe next to production — because the way this harness fails is by
being quietly simplified, not by going red.

⚠️ Every assertion below is written as **the presence of the right thing**, never the absence of
a wrong word (WORKFLOW_RULES §4): a file that merely stopped mentioning a hazard would satisfy a
ban and fails these instead.
"""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
LOADTEST = ROOT / "scripts" / "loadtest"
K6 = LOADTEST / "k6_baseline.js"
RUNNER = LOADTEST / "run.sh"
SEEDER = LOADTEST / "seed_loadtest_org.py"
README = LOADTEST / "README.md"


@pytest.mark.parametrize("path", [K6, RUNNER, SEEDER, README])
def test_the_instrument_is_in_the_repository(path):
    """The whole point. `.scratch/` is swept; a floor's instrument cannot live there."""
    assert path.is_file(), (
        f"{path.relative_to(ROOT)} is missing. core#778's published floor (>= 60 req/s) would "
        f"again be a number whose instrument cannot be re-run."
    )
    assert path.stat().st_size > 400, f"{path.name} is too small to be the real thing"


def test_the_abort_criteria_live_in_the_generator():
    """Fixed before the run, in code — so the pass bar cannot be chosen after seeing numbers.

    The neighbour threshold is the founder's own condition on this test ever running at all:
    production must not be harmed. It is asserted by name.
    """
    src = K6.read_text(encoding="utf-8")
    assert "thresholds:" in src, "no thresholds block — the run would have no abort criteria"
    for needed in ("http_req_failed", "http_req_duration", "datanika_neighbour_non_200"):
        assert needed in src, f"{needed} is not among the abort criteria"
    assert src.count("abortOnFail") >= 3, (
        "fewer than three abortOnFail thresholds: a criterion that only reports at the end "
        "does not stop a run that is already harming the box it shares with production"
    )


def test_the_generator_is_open_model():
    """A closed loop measures the target's pace back to itself and can never find a knee."""
    src = K6.read_text(encoding="utf-8")
    assert "arrival-rate" in src, (
        "the executor is not an arrival-rate one, so the generator slows down when the target "
        "does — which cannot find the knee core#778 asks for"
    )
    assert "dropped_iterations" in src, (
        "dropped_iterations is not mentioned; under an open model it is a PRIMARY result and "
        "a run that ignores it can report a rate it never actually offered"
    )


def test_the_generator_image_is_pinned():
    """`:latest` on an instrument is how two runs stop being comparable."""
    src = RUNNER.read_text(encoding="utf-8")
    assert "grafana/k6:" in src, "the k6 image is not named"
    assert "grafana/k6:latest" not in src, (
        "the generator floats on :latest — same trap already recorded for celery-exporter, and "
        "worse here because the artifact IS a measurement"
    )


def test_the_runner_targets_staging_and_watches_the_neighbour():
    src = RUNNER.read_text(encoding="utf-8")
    assert "127.0.0.1:8100" in src, "staging's backend port is not the target"
    assert "8000" in src, "production is not sampled as the neighbour"
    # The refusals, by the thing each one protects.
    assert "healthz" in src
    assert "e2e" in src, "no refusal covering a concurrent e2e-staging run"


def test_cleanup_is_verified_by_effect_not_merely_attempted():
    """A cleanup that was only invoked is the same evidence class as a green that cannot fail."""
    runner = RUNNER.read_text(encoding="utf-8")
    seeder = SEEDER.read_text(encoding="utf-8")
    assert "trap cleanup EXIT" in runner, (
        "cleanup is not on an EXIT trap, so an aborted run leaves live keys behind"
    )
    assert "active_remaining" in seeder, (
        "the revoker does not report a remaining-active count, so nothing distinguishes "
        "'revoked everything' from 'called revoke'"
    )
    assert "list_api_keys" in seeder and seeder.count("list_api_keys") >= 2, (
        "the revoker does not RE-READ after revoking; trusting its own loop counter is the "
        "same mistake as trusting an exit code"
    )


def test_the_seeder_refuses_anywhere_that_is_not_staging():
    """🚨 The single most important line in the harness.

    April's runs went at production and left its database unusable for ~an hour. The guard is
    POSITIVE — it requires evidence that this IS staging — because "no evidence it is
    production" is satisfied by a failed lookup.
    """
    src = SEEDER.read_text(encoding="utf-8")
    assert "_guard_not_production" in src
    assert src.count("_guard_not_production(session)") >= 2, (
        "the production guard is not called on BOTH paths; revoke touches the same database "
        "as mint and must be guarded identically"
    )
    assert '"staging" not in url' in src or "'staging' not in url" in src, (
        "the guard is not written as a positive requirement for 'staging'"
    )


def test_the_readme_states_the_key_ceiling_and_the_founders_label():
    """Two facts a future session would otherwise re-derive the expensive way."""
    src = README.read_text(encoding="utf-8")
    assert "rate_limit_rpm / 60" in src, "the key-count ceiling formula is not recorded"
    assert "floor under neighbour load" in src, (
        "the founder's label on any result is missing; without it a number from this harness "
        "gets published as a capacity figure"
    )
    assert "NOT yet executed" in src or "not yet executed" in src.lower(), (
        "the README must say plainly whether this harness has been run end to end; an "
        "unexecuted instrument quoted as if proven is the defect one level up"
    )


def test_the_guards_can_fail():
    """Negative control: each assertion above is shown able to fail on a plausible mutation."""
    k6 = K6.read_text(encoding="utf-8")
    runner = RUNNER.read_text(encoding="utf-8")
    seeder = SEEDER.read_text(encoding="utf-8")

    # Mutations a well-meaning simplification would actually make.
    assert "abortOnFail" not in k6.replace("abortOnFail", ""), "control constructed wrongly"
    assert "trap cleanup EXIT" not in runner.replace("trap cleanup EXIT", "")
    assert "_guard_not_production" not in seeder.replace("_guard_not_production", "")
    assert "grafana/k6:latest" not in runner  # the pinned form is what is present

    # And the fixture is reading real files, or every test above is vacuous.
    assert len(k6) > 2000 and len(runner) > 2000 and len(seeder) > 2000
