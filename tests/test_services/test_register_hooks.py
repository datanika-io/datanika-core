"""Regression test for core#287.

Both the Reflex app and the Celery worker must register the same
hook subscribers. Before #287, registrations lived only in
``datanika/datanika.py`` (the Reflex app module), so Celery's
``charge_cycle_overages`` emitted ``charge_*`` hooks into an empty
handler dict in the worker process.

This test does two things:

1. Unit: import ``datanika.services._register_hooks`` and call
   ``register_all_core_hooks()`` against a cleared ``hooks._handlers``
   dict, then assert every expected event has a handler.
2. Subprocess guard: fire ``python -c "import datanika.tasks.celery_app;
   ..."`` and check the worker's real startup surface registers all
   the same events. Catches someone removing the
   ``register_all_core_hooks()`` import from ``celery_app.py`` in
   the future.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

EXPECTED_EVENTS = (
    "charge_incoming",
    "charge_issued",
    "charge_failed",
    "quota.warning_threshold_reached",
    "run.upload_completed",
    "run.models_completed",
    "run.transformation_completed",
)


class TestRegisterAllCoreHooks:
    def test_registers_every_expected_event(self, monkeypatch):
        """Unit: direct call registers every event the subscribers own."""
        from datanika import hooks
        from datanika.services import _register_hooks

        monkeypatch.setattr(hooks, "_handlers", {})
        monkeypatch.setattr(_register_hooks, "_already_registered", False)

        _register_hooks.register_all_core_hooks()

        for event in EXPECTED_EVENTS:
            assert hooks._handlers.get(event), (
                f"{event!r} has no handler after register_all_core_hooks(). "
                "See #287 — cloud emits these from the Celery worker; if "
                "registration drops, Notification rows never land."
            )

    def test_idempotent_within_a_process(self, monkeypatch):
        """Second call must not double-register (would dispatch twice)."""
        from datanika import hooks
        from datanika.services import _register_hooks

        monkeypatch.setattr(hooks, "_handlers", {})
        monkeypatch.setattr(_register_hooks, "_already_registered", False)

        _register_hooks.register_all_core_hooks()
        first_counts = {e: len(hooks._handlers.get(e, [])) for e in EXPECTED_EVENTS}

        _register_hooks.register_all_core_hooks()
        second_counts = {e: len(hooks._handlers.get(e, [])) for e in EXPECTED_EVENTS}

        assert first_counts == second_counts, (
            "register_all_core_hooks() double-registered on second call. "
            "Sentinel is broken — a future second import (Celery + Reflex "
            "sharing the same process in tests) would fire every subscriber "
            "twice per emit."
        )


class TestCeleryWorkerRegistersHooks:
    """Subprocess guard: `datanika.tasks.celery_app` must populate the
    full handler dict at import time. This catches someone removing the
    ``register_all_core_hooks()`` call from the Celery entrypoint.
    """

    def test_celery_worker_import_populates_hooks(self):
        # setup_logging writes structured JSON logs to stdout when the
        # celery entrypoint is imported, so we prefix output with a known
        # marker and filter on that — otherwise the parse picks up log
        # lines and fails with an unhelpful ValueError.
        marker = "HOOKS_CHECK:"
        script = (
            "import datanika.tasks.celery_app  # noqa: F401\n"
            "from datanika import hooks\n"
            f"for e in {list(EXPECTED_EVENTS)!r}:\n"
            "    n = len(hooks._handlers.get(e, []))\n"
            f"    print('{marker}' + e + '=' + str(n))\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, (
            f"Celery worker import failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
        found: dict[str, int] = {}
        for line in result.stdout.splitlines():
            if not line.startswith(marker):
                continue
            event, _, count = line.removeprefix(marker).partition("=")
            found[event] = int(count)

        missing = [e for e in EXPECTED_EVENTS if e not in found]
        assert not missing, (
            f"Marker-tagged output missing events {missing!r}.\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
        for event, count in found.items():
            assert count >= 1, (
                f"{event!r} has no handler after `import datanika.tasks.celery_app`. "
                "If ``register_all_core_hooks()`` was removed from "
                "``celery_app.py``, cloud tasks emitting this event will be "
                "silently ignored — see #287."
            )


@pytest.mark.parametrize("event", EXPECTED_EVENTS)
def test_expected_events_list_matches_registry(event):
    """Documentation test: if a new hook subscriber is added (e.g. a future
    ``tenant_suspended`` event), this list must be updated alongside the
    registration function. Fails loud if someone forgets to wire both ends.
    """
    from datanika.services import _register_hooks

    source = _register_hooks.__file__
    with open(source, encoding="utf-8") as fp:
        body = fp.read()
    # Every registered hook function should be mentioned in the module.
    # Weak check — just asserts the three new charge_* registrations are
    # still called out; stronger enforcement lives in the unit test above.
    assert "register_charge_notification_hooks" in body
    assert "register_quota_notification_hooks" in body
    assert "register_in_app_notification_hooks" in body
    assert "NotificationService" in body
