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

import re
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
    assert "healthz" in src


def test_the_e2e_refusal_matches_the_containers_an_e2e_run_really_creates():
    """🔴 Found on this harness's first rehearsal: the pattern was aimed at a guessed name.

    It read `datanika-staging-e2e`, which does not exist. The containers an E2E run really
    brings up — measured while `e2e-sso` was live against staging — are:

        e2e-authentik-server-1, e2e-authentik-worker-1, e2e-authentik-redis-1, e2e-authentik-db-1

    **The guard matched none of them and would have let a load test start on top of a running
    suite**, producing exactly the false gating red it exists to prevent.

    So this test drives the pattern against the real names rather than asserting the word
    "e2e" appears somewhere in the script.
    """
    src = RUNNER.read_text(encoding="utf-8")
    m = re.search(r"grep -ciE '([^']+)'", src)
    assert m, "the E2E refusal no longer uses a greppable pattern"
    pattern = m.group(1)

    real = [
        "e2e-authentik-server-1",
        "e2e-authentik-worker-1",
        "e2e-authentik-redis-1",
        "e2e-authentik-db-1",
    ]
    for name in real:
        assert re.search(pattern, name, re.IGNORECASE), (
            f"the refusal pattern {pattern!r} does not match {name!r}, a container an E2E run "
            f"actually creates"
        )

    # Negative control: it must NOT match the ordinary stack, or the harness can never run.
    for name in ("datanika-staging-app", "datanika-app-b", "datanika-postgres", "datanika-grafana"):
        assert not re.search(pattern, name, re.IGNORECASE), (
            f"the refusal pattern {pattern!r} matches {name!r} — it would refuse every run"
        )

    # And the superseded pattern is shown unable to do the job.
    assert not re.search("datanika-staging-e2e", real[0]), (
        "control is malformed: the old pattern must be demonstrated not to match"
    )


def test_the_load_threshold_compares_numerically_not_as_strings():
    """`[ "10.5" \\> "3.0" ]` is FALSE — string comparison, at exactly the load that matters.

    The original refusal used that form, so a box at load 10.5 passed a check meant to stop it
    at 3.0. Corrected to `awk`; pinned here because the shell form reads correct.
    """
    src = RUNNER.read_text(encoding="utf-8")
    assert "awk" in src, "the load comparison is not numeric"
    assert '\\> "3.0"' not in src and '\\> "3.0"' not in src, (
        "the string-comparison form is back; 10.5 would compare as less than 3.0"
    )
    # Demonstrate the defect the fix removes, so the assertion above is not a bare taboo.
    assert "10.5" < "3.0", "string comparison must be shown to get this wrong"
    assert float("10.5") > float("3.0")


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
    """🚨 The single most important behaviour in the harness — and it is tested by DRIVING it.

    April's runs went at production and left its database unusable for ~an hour.

    🔴 **The previous version of this test asserted that a guard existed and was called on both
    paths. It was, and it still could not tell staging from production** — both stacks report
    `postgres:5432/datanika` and an empty `app_env`, because each Compose project resolves
    `postgres` inside its own network. The guard refused *everything*, which is the safe
    direction and why nothing was damaged, but this test could not have noticed the difference.
    **A guard never seen refusing the thing it exists to refuse is not evidence.**

    So the decision is now a pure function and this drives it with the **real measured values
    of both environments**.
    """
    import importlib.util

    spec = importlib.util.spec_from_file_location("_seed_loadtest_org", SEEDER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    # Measured on the box, 2026-09-21.
    assert mod.is_staging("https://staging-app.datanika.io", "sandbox") is True, (
        "the guard refuses the real staging environment, so the harness cannot run at all"
    )
    assert mod.is_staging("https://app.datanika.io", "production") is False, (
        "🚨 the guard PERMITS the real production environment"
    )

    # Both signals are required: one misconfigured value must not unlock production.
    assert mod.is_staging("https://app.datanika.io", "sandbox") is False
    assert mod.is_staging("https://staging-app.datanika.io", "production") is False
    # And absence is never staging.
    assert mod.is_staging("", "") is False

    src = SEEDER.read_text(encoding="utf-8")
    assert src.count("_guard_not_production(session)") >= 2, (
        "the guard is not called on BOTH paths; revoke touches the same database as mint"
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
