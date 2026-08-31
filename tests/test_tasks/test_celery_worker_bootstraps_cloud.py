"""The Celery worker must subscribe the cloud plugin's hooks. [core#772]

``init_cloud`` / ``bootstrap_cloud`` were called from exactly one place —
``datanika/datanika.py``, the Reflex app module, which
``services/_register_hooks.py``'s own docstring already notes *"is never
imported by the Celery worker."* Core's own hooks were centralised and called
from ``celery_app.py`` for that reason; the plugin's never were.

Every run executes in the worker (``run_upload_task.delay`` &c. from the UI,
the API and the scheduler — there is no synchronous path), and
``run.before_execute`` / ``run.*_completed`` are emitted inside those tasks. So
with no cloud subscriber in that process, **no run quota was enforced and no
usage was metered** — the V2 byte cap and the V1 ``runs_included`` cap alike.

These tests run in a **subprocess** and stub ``datanika_cloud`` into
``sys.modules``, for two reasons. Core must not depend on cloud (core's own
venv has no ``datanika_cloud``), and ``celery_app`` has module-level side
effects that make an in-process reload unreliable. The stub means this asserts
the *call*, which is the thing that was missing; cloud's suite asserts the real
handlers land (``tests/test_billing_tick.py`` and the worker probe there).

⚠️ Deliberately **not** a source-scan. ``tests/test_app_plugin_init.py`` uses
one for the ordering constraint it checks, and core#738 is the standing lesson
that a guard matching one spelling stays green on every other. A test that
greps for ``bootstrap_cloud`` in ``celery_app.py`` would pass on a call sitting
inside ``if False:``.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent

_SCRIPT = """
import sys, types

calls = []

if {install_stub}:
    pkg = types.ModuleType("datanika_cloud")
    pkg.__path__ = []
    plugin = types.ModuleType("datanika_cloud.plugin")
    plugin.bootstrap_cloud = lambda: calls.append("bootstrap_cloud")
    pkg.plugin = plugin
    sys.modules["datanika_cloud"] = pkg
    sys.modules["datanika_cloud.plugin"] = plugin

try:
    import datanika.tasks.celery_app  # noqa: F401
except Exception as exc:
    print("IMPORT_ERROR", type(exc).__name__)
else:
    print("CALLS", len(calls))
"""


def _run(edition: str, *, install_stub: bool = True) -> str:
    script = textwrap.dedent(_SCRIPT).format(install_stub=install_stub)
    proc = subprocess.run(
        [sys.executable, "-c", script],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=180,
        env={
            **_clean_env(),
            "DATANIKA_EDITION": edition,
        },
    )
    lines = [ln for ln in proc.stdout.splitlines() if ln.startswith(("CALLS", "IMPORT_ERROR"))]
    assert lines, f"probe produced no verdict.\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    return lines[-1]


def _clean_env() -> dict[str, str]:
    """A child env without inherited ``GIT_*``.

    ``GIT_DIR`` is exported to hooks without ``GIT_WORK_TREE``, so a child
    process shelling out to git writes metadata to the wrong place. Nothing
    here runs git, but the suite is run from the pre-push hook and stripping
    it is the standing rule for anything that spawns a child.
    """
    import os

    return {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}


class TestWorkerBootstrapsTheCloudPlugin:
    def test_cloud_edition_bootstraps_the_plugin(self):
        assert _run("cloud") == "CALLS 1"

    def test_core_edition_does_not(self):
        """Negative control.

        A ``celery_app`` that bootstrapped unconditionally would satisfy the
        test above while breaking every self-hosted core install, which has no
        ``datanika_cloud`` to import.
        """
        assert _run("core") == "CALLS 0"

    def test_bootstrap_runs_exactly_once(self):
        """``bootstrap_cloud`` is not idempotent in cloud releases before the
        paired change (cloud#129 adds ``_on_once``), and ``model_runs``
        metering is deliberately not deduplicated — so a second subscription is
        a double-count on a billing dimension. Assert on the count, not on
        truthiness."""
        assert _run("cloud") == "CALLS 1"

    def test_missing_plugin_under_cloud_edition_fails_loudly(self):
        """``DATANIKA_EDITION=cloud`` with no plugin installed must raise, not
        degrade.

        ``datanika/datanika.py`` already imports the plugin unguarded under
        that edition, so the web process fails the same way and the
        misconfiguration surfaces in both. A worker that silently started
        without metering is the entire defect this issue is about: a silent
        fallback on missing config is what made it invisible for months.
        """
        assert _run("cloud", install_stub=False) == "IMPORT_ERROR ModuleNotFoundError"


class TestBillingTickTask:
    """Core's beat-scheduled announcer for cloud's overage path (cloud#129).

    ``charge_cycle_overages`` / ``emit_charge_incoming_notices`` are plain
    functions in cloud, in no beat schedule and registered with no Celery app,
    so nothing in production ever called them. Core cannot import cloud to
    schedule them, so it announces an event on the hour and cloud subscribes.
    """

    def test_task_is_registered_under_a_stable_name(self):
        from datanika.tasks.billing_tasks import billing_tick_task

        assert billing_tick_task.name == "datanika.billing_tick"

    def test_beat_schedule_carries_it(self):
        from datanika.tasks.celery_app import celery_app

        entries = celery_app.conf.beat_schedule
        names = {e["task"] for e in entries.values()}
        assert "datanika.billing_tick" in names, (
            f"billing tick is not scheduled; beat would never run it. Scheduled: {names}"
        )

    def test_maintenance_is_still_scheduled(self):
        """Negative control: a schedule that replaced rather than extended."""
        from datanika.tasks.celery_app import celery_app

        names = {e["task"] for e in celery_app.conf.beat_schedule.values()}
        assert "datanika.run_maintenance" in names

    def test_task_module_is_included_so_the_worker_registers_it(self):
        """Beat sends by name; the worker must have imported the module or the
        message is rejected as an unregistered task."""
        from datanika.tasks.celery_app import celery_app

        assert "datanika.tasks.billing_tasks" in celery_app.conf.include

    def test_announcing_reaches_subscribers_and_returns_their_report(self):
        from datanika import hooks
        from datanika.tasks.billing_tasks import billing_tick_task

        seen = []

        def _subscriber(*, context=None, **_kwargs):
            seen.append(context)
            if context is not None:
                context["charge_cycle_overages"] = {"issued": 1}

        hooks.on("billing.hourly_tick", _subscriber)
        try:
            result = billing_tick_task()
        finally:
            hooks.off("billing.hourly_tick", _subscriber)

        assert len(seen) == 1
        assert result == {"charge_cycle_overages": {"issued": 1}}

    def test_a_failing_subscriber_does_not_kill_the_tick(self):
        """``announce`` semantics, not ``emit``.

        core#456: ``run.*_completed`` used ``emit``, so one raising handler
        starved every handler behind it — including cloud's byte metering. A
        billing tick with several subscribers must not inherit that.
        """
        from datanika import hooks
        from datanika.tasks.billing_tasks import billing_tick_task

        reached = []

        def _boom(**_kwargs):
            raise RuntimeError("subscriber exploded")

        def _after(*, context=None, **_kwargs):
            reached.append(True)

        hooks.on("billing.hourly_tick", _boom)
        hooks.on("billing.hourly_tick", _after)
        try:
            billing_tick_task()
        finally:
            hooks.off("billing.hourly_tick", _boom)
            hooks.off("billing.hourly_tick", _after)

        assert reached == [True]

    def test_no_subscriber_is_not_an_error(self):
        """Core edition, or cloud promoted after core. Returns an empty report
        rather than raising."""
        from datanika.tasks.billing_tasks import billing_tick_task

        assert billing_tick_task() == {}


@pytest.mark.parametrize("edition", ["cloud", "core"])
def test_probe_itself_reaches_a_verdict(edition):
    """Guard on the harness. A subprocess that died before printing would make
    every assertion above fail with a confusing message; ``_run`` asserts a
    verdict line exists, and this pins that it does for both editions."""
    assert _run(edition).startswith(("CALLS", "IMPORT_ERROR"))
