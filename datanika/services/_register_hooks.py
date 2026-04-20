"""Idempotent one-shot registration for all core hook handlers (#287).

``datanika.hooks._handlers`` is a module-level dict — one instance per
Python process. Registrations don't cross process boundaries. Before
this module, all ``register_*_notification_hooks()`` calls lived in
``datanika/datanika.py`` (the Reflex app module), which is never
imported by the Celery worker — so cloud's ``charge_cycle_overages``
could emit ``charge_*`` hooks all day long into an empty handler dict.

This module centralises registration. Both entrypoints call
``register_all_core_hooks()``:

- ``datanika/datanika.py`` — Reflex app (web process).
- ``datanika/tasks/celery_app.py`` — Celery worker + beat processes.

The function is **idempotent** via a module-level sentinel; calling it
twice from the same process is a no-op, so if someone later wires it
into a third entrypoint there's no double-registration risk.
"""

from __future__ import annotations

_already_registered = False


def register_all_core_hooks() -> None:
    """Subscribe all core-side hook handlers. Idempotent per process."""
    global _already_registered
    if _already_registered:
        return

    # Import inside the function to avoid import cycles at module load and
    # to keep the Celery worker's cold-start surface minimal.
    from datanika.services.charge_notification_hooks import (
        register_charge_notification_hooks,
    )
    from datanika.services.in_app_notification_hooks import (
        register_in_app_notification_hooks,
    )
    from datanika.services.notification_service import (
        NotificationService,
        register_hooks,
    )
    from datanika.services.quota_notification_hooks import (
        register_quota_notification_hooks,
    )

    # External-channel dispatch (Slack/Telegram/webhook/email) on
    # ``run.{upload,models,transformation}_completed``.
    register_hooks(NotificationService())
    # In-app Notification rows on run completion.
    register_in_app_notification_hooks()
    # Quota warning — in-app + external dispatch.
    register_quota_notification_hooks()
    # V2 P5 Option B — cycle-boundary charge events.
    register_charge_notification_hooks()

    _already_registered = True
