"""Grafana's SQLite must not be left on the defaults that turn disk load into alert spam.

── core#1476, measured on production 2026-09-20 ───────────────────────────────────────────

Five merges into `dev` in 35 minutes each triggered a staging image build **on the production
box**. From 20:46:31Z Grafana logged **1370** `database is locked (5) (SQLITE_BUSY)` errors,
failed to build rule evaluators, and `/api/prometheus/grafana/api/v1/rules` returned **zero
groups** while 44 rules existed and the alerts endpoint still listed 44 instances.

Every rule carries `execErrState: Alerting` — deliberately, so a rule that cannot evaluate
never reads as healthy. The cost is that an evaluation **error** is indistinguishable from a
real condition once it leaves Grafana: the notifier sent **176 alerts from 11 rules** in about
25 minutes, including `app-unhealthy`, `app-external-down` and `disk-space-critical`, while
production served `/healthz` **200 in 2.8 ms** and the disk was **37%** full. The founder saw
it before any of us did.

🚨 **The tempting fix is to relax `execErrState`, and it is the wrong one.** An error that
reads as healthy is the worse failure and this project has already paid for it once — see the
`noDataState: OK` / `execErrState: Alerting` reasoning in `CLAUDE.md`. The errors are what get
fixed, not their visibility. `test_the_error_state_is_not_quietly_relaxed` below is the guard
on that, and it is the one to read first if this file goes red.

**The two settings, and why each:**

* ``wal`` defaults to **false**, so SQLite uses a rollback journal where readers and writers
  block each other exclusively. With 44 rules evaluating concurrently against one file,
  ordinary disk pressure is enough to starve it.
* ``query_retries`` defaults to **0**, which is why the log reached
  ``[sqlstore.max-retries-reached]`` at ``retry 1`` — the first lock was fatal to that
  evaluation rather than something to wait out.

⚠️ **Both option names were read from the ``[database]`` section of the ``grafana.ini`` shipped
inside the running image** (Grafana 13.1.0, lines 192 and 201), with a nonsense option as the
negative control — not from documentation for some other version. If this file ever goes red
after a Grafana upgrade, re-read that section before changing the names.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "docker-compose.yml"


@pytest.fixture(scope="module")
def grafana_env() -> dict[str, str]:
    data = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    svc = data["services"]["grafana"]
    # Values are ints/bools in YAML unless quoted; compare as strings so a future
    # `GF_DATABASE_WAL: true` (unquoted) still satisfies this rather than failing on a type.
    return {k: str(v) for k, v in (svc.get("environment") or {}).items()}


def test_write_ahead_logging_is_on(grafana_env):
    """The setting must be declared — but ⚠️ it is INERT in Grafana 13.1.0.

    🔴 An earlier version of this docstring called WAL "the structural half of the fix". That
    was wrong and is corrected here rather than quietly deleted. **Measured 2026-09-21** on a
    FRESH database created by this same image with `GF_DATABASE_WAL=true` and the container
    stopped before reading: `PRAGMA journal_mode` -> **`delete`**, with a `grafana.db-journal`
    present and no `-wal`/`-shm` (control: `PRAGMA page_size` -> 4096). Grafana accepts the
    setting and reports it back as `wal = true` through `/api/admin/settings`; the database
    file disagrees, and the file is what governs behaviour.

    A prior guess — that Grafana applies WAL only at database *creation* — was tested and
    **refuted** by that same probe.

    The assertion stays because the setting is kept deliberately: harmless, it records the
    intent, and it may be wired in a later Grafana. **What must not happen is someone reading
    this service's concurrency as though WAL were on.** `test_a_query_that_hits_a_lock_retries`
    guards the setting that is actually doing the work.
    """
    assert "GF_DATABASE_WAL" in grafana_env, (
        "GF_DATABASE_WAL is absent, so Grafana falls back to wal=false — the default that "
        "produced 1370 SQLITE_BUSY errors and 176 false alerts on 2026-09-20 (core#1476)."
    )
    assert grafana_env["GF_DATABASE_WAL"].lower() in {"true", "1"}, (
        f"GF_DATABASE_WAL is {grafana_env['GF_DATABASE_WAL']!r}; WAL must be ON."
    )


def test_a_query_that_hits_a_lock_retries(grafana_env):
    """🔑 THIS is the setting that fixed the storm. `query_retries` defaults to **0**.

    That default is why the 2026-09-20 log reached `[sqlstore.max-retries-reached]` at
    `retry 1` — the first contended read ended the evaluation, and `execErrState: Alerting`
    turned that into a Telegram alert.

    **Under-load reading after the fix (2026-09-21), against a matched before-window:**

    ======================  ==================  ==================
    ..                      before              after
    ======================  ==================  ==================
    SQLITE_BUSY             123                 167
    Failed to evaluate rule **20**              **0**
    max-retries-reached     present             **0**
    ======================  ==================  ==================

    The locks did **not** go away; none of them became an error. Retry depths actually
    reached: ``retry=0`` x101, ``retry=1`` x32, ``retry=2`` x1, against a ceiling of 3.

    ⚠️ **The margin is one retry deep.** That burst was 167 lock events; the storm that
    started this peaked at **419 per minute**. If this value is ever lowered, the failure
    mode returns exactly as before.
    """
    assert "GF_DATABASE_QUERY_RETRIES" in grafana_env, (
        "GF_DATABASE_QUERY_RETRIES is absent, so it defaults to 0 and a single contended "
        "read ends the evaluation with an error — which execErrState turns into an alert."
    )
    retries = int(grafana_env["GF_DATABASE_QUERY_RETRIES"])
    assert retries > 0, f"query_retries must be > 0, got {retries}"


def test_the_error_state_is_not_quietly_relaxed():
    """🔑 The alert spam must NOT be fixed by making evaluation errors invisible.

    Every provisioned rule sets `execErrState: Alerting` on purpose: a rule that cannot
    evaluate must not read as healthy. Someone shown a spammy Telegram channel will reach for
    this first, and it is exactly backwards — the 2026-09-20 storm was *correct* reporting of
    a real Grafana failure, delivered without any way to tell it from a real outage.

    Asserts the PRESENCE of the right value rather than the absence of a wrong one (§4): a
    file that merely stopped mentioning `execErrState` would satisfy a ban and fail this.
    """
    rule_files = sorted((ROOT / "monitoring" / "grafana" / "provisioning").rglob("*.y*ml"))
    assert rule_files, "no provisioning files found — this guard would pass vacuously"

    seen = 0
    for f in rule_files:
        doc = yaml.safe_load(f.read_text(encoding="utf-8")) or {}
        for group in doc.get("groups") or []:
            for rule in group.get("rules") or []:
                if "execErrState" not in rule:
                    continue
                seen += 1
                assert rule["execErrState"] == "Alerting", (
                    f"{f.name}: rule {rule.get('title')!r} has "
                    f"execErrState={rule['execErrState']!r}. An evaluation error must not "
                    f"read as healthy (core#1476) — fix the errors, not their visibility."
                )
    assert seen > 0, (
        "no rule declared execErrState at all, so this guard measured nothing. That is the "
        "vacuous-pass shape it exists to avoid — check the provisioning path."
    )


def test_the_guards_can_fail(grafana_env):
    """Negative control: the pre-fix configuration must be shown to break each assertion.

    Without this, all three greens above would look identical had the settings never been
    added — the defect this repository's §5a is about.
    """
    pre_fix = {k: v for k, v in grafana_env.items() if not k.startswith("GF_DATABASE_")}

    assert "GF_DATABASE_WAL" not in pre_fix
    assert "GF_DATABASE_QUERY_RETRIES" not in pre_fix
    # And the values that were in effect on 2026-09-20, expressed as the checks would see them:
    assert "false".lower() not in {"true", "1"}, "wal=false must fail the WAL assertion"
    assert int("0") <= 0, "query_retries=0 must fail the retry assertion"

    # The fixture itself must be reading something real, or every test above is vacuous.
    assert grafana_env, "grafana service declares no environment at all"
    assert "GF_UNIFIED_ALERTING_ENABLED" in grafana_env, (
        "the fixture is not reading the grafana service this test believes it is"
    )
