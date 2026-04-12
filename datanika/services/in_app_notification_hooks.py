"""Hook handlers that create in-app notifications on run completion.

Subscribes to the same hook events as the external notification
service (``notification_service.py``) but creates ``Notification``
records in the DB instead of dispatching to Slack/email/etc.
"""

import logging

from datanika import hooks
from datanika.models.notification import NotificationType
from datanika.services.in_app_notification_service import InAppNotificationService

logger = logging.getLogger(__name__)

_svc = InAppNotificationService()


def _on_run_completed(
    session,
    org_id: int,
    run_id: int = 0,
    status: str = "",
    error_message: str | None = None,
    target_type: str = "run",
    target_id: int = 0,
    table_count: int = 0,
    **_kw,
) -> None:
    if status == "failed":
        ntype = NotificationType.RUN_FAILED
        title = f"Run #{run_id} failed"
        message = error_message or "Pipeline run failed"
    else:
        ntype = NotificationType.RUN_SUCCEEDED
        title = f"Run #{run_id} succeeded"
        message = None

    try:
        _svc.create(
            session,
            org_id,
            ntype,
            title=title,
            resource_type=target_type or "run",
            resource_id=run_id,
            message=message,
        )
    except Exception:
        logger.warning("Failed to create in-app notification for run %s", run_id, exc_info=True)


def register_in_app_notification_hooks() -> None:
    hooks.on("run.upload_completed", _on_run_completed)
    hooks.on("run.models_completed", _on_run_completed)
    hooks.on("run.transformation_completed", _on_run_completed)
