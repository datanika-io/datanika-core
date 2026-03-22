from celery import Celery

from datanika.config import settings

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
