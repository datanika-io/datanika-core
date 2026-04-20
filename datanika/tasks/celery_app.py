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
]

# Schedule hourly maintenance task via Celery Beat
celery_app.conf.beat_schedule = {
    "hourly-maintenance": {
        "task": "datanika.run_maintenance",
        "schedule": 3600.0,  # every hour
    },
}
