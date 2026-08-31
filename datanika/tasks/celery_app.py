from celery import Celery

from datanika.config import settings
from datanika.logging_config import setup_logging
from datanika.services._register_hooks import register_all_core_hooks

setup_logging(debug=settings.debug)

# Register every core-side hook subscriber in this process. Without this,
# cloud tasks running in the Celery worker (e.g. ``charge_cycle_overages``
# emitting ``charge_*`` events) would fire into an empty handler dict and
# the user-facing Notification rows would silently never land. See #287.
register_all_core_hooks()

celery_app = Celery(
    "datanika",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
)

# Register Prometheus metrics for task execution
from datanika.services.metrics import setup_celery_metrics  # noqa: E402

setup_celery_metrics(celery_app)

celery_app.conf.include = [
    "datanika.tasks.upload_tasks",
    "datanika.tasks.transformation_tasks",
    "datanika.tasks.pipeline_tasks",
    "datanika.tasks.email_tasks",
    "datanika.tasks.maintenance_tasks",
    "datanika.tasks.billing_tasks",
]

# Schedule hourly maintenance task via Celery Beat
celery_app.conf.beat_schedule = {
    "hourly-maintenance": {
        "task": "datanika.run_maintenance",
        "schedule": 3600.0,  # every hour
    },
    # cloud#129. Cloud's overage functions describe themselves as hourly and
    # were in no schedule at all, so no overage could ever have been billed.
    # This announces `billing.hourly_tick`; cloud subscribes. Its own entry
    # rather than a line inside `run_maintenance`, so a maintenance failure
    # (which raises `MaintenanceError` by design) cannot silently skip billing,
    # and so the tick has its own Celery result to read back.
    "hourly-billing-tick": {
        "task": "datanika.billing_tick",
        "schedule": 3600.0,  # every hour
    },
}

# --- Cloud plugin hooks in the worker process (core#772) ---------------------
#
# `bootstrap_cloud()` is the plugin's app-free phase-1 entry point. It was
# called from `datanika/datanika.py` alone — the Reflex app module, which
# `services/_register_hooks.py` already documents as never being imported by
# the Celery worker. That is why core's own hooks are registered above; the
# plugin's never were.
#
# Every run executes here (`run_upload_task.delay` &c. from the UI, the API and
# the scheduler — there is no synchronous path) and `run.before_execute` /
# `run.*_completed` are emitted inside those tasks. Without this call the
# worker had zero cloud subscribers, so no run quota was enforced and no usage
# was metered at all — the V2 byte cap and the V1 `runs_included` cap alike.
#
# Deliberately last in this module: `datanika_cloud.plugin` imports Reflex UI
# state, which can import back into `datanika.tasks.*`, and by this point
# `celery_app` is fully defined.
#
# ⚠️ Requires a cloud tree where `bootstrap_cloud()` is idempotent (cloud#129
# adds `_on_once`). The Reflex web process imports BOTH `datanika.datanika` and
# this module, so it now reaches the call twice; against an older cloud tree
# that double-subscribes every handler, and `model_runs` metering is
# deliberately not deduplicated. **Promote cloud before core.**
#
# No try/except: `DATANIKA_EDITION=cloud` with no plugin installed is a
# misconfiguration, and `datanika/datanika.py` already raises on it. A worker
# that starts quietly without metering is precisely the failure this fixes.
if settings.datanika_edition == "cloud":
    from datanika_cloud.plugin import bootstrap_cloud  # noqa: E402

    bootstrap_cloud()
