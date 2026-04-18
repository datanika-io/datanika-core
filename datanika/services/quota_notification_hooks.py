"""Quota warning notification hook — creates in-app notifications and
dispatches to user-configured channels when a subscription crosses
80% of any quota dimension.

Subscribes to the ``quota.warning_threshold_reached`` event emitted by
the cloud plugin's BillingService. Pure subscriber — does nothing in
open-source-only deployments because the event is never emitted there.
"""

import logging

from sqlalchemy.orm import Session

from datanika import hooks
from datanika.models.notification import NotificationType
from datanika.services.in_app_notification_service import InAppNotificationService
from datanika.ui.state.base_state import get_sync_session

logger = logging.getLogger(__name__)

_METRIC_LABELS = {
    "model_runs": "model runs",
    "bytes_processed": "GB processed",
    "connections": "connections",
    "schedules": "schedules",
    "seats": "seats",
}

_svc = InAppNotificationService()


def _on_quota_warning(
    *,
    org_id: int,
    metric: str,
    used: int,
    limit: int,
    plan_name: str,
    **_kw,
) -> None:
    """Handler for ``quota.warning_threshold_reached``.

    Creates a Notification row in the in-app notification center and
    dispatches to the org's configured external notification channels
    (``quota_warning`` event). The cloud emitter already debounces per
    ``(org, metric, billing_period)`` so this handler never double-fires
    within a period.
    """
    metric_label = _METRIC_LABELS.get(metric, metric)
    title = f"Quota warning: 80% of {metric_label} used"
    message = f"You've used {used:,} of {limit:,} {metric_label} on your {plan_name} plan."

    session = get_sync_session()
    try:
        _svc.create(
            session,
            org_id,
            NotificationType.QUOTA_WARNING,
            title=title,
            resource_type="quota",
            resource_id=0,  # no single resource — quota warning is org-level
            message=message,
        )
        _dispatch_external(
            session,
            org_id,
            metric=metric,
            metric_label=metric_label,
            used=used,
            limit=limit,
            plan_name=plan_name,
        )
        session.commit()
    except Exception:  # noqa: BLE001 — never let notification failures escape emit()
        session.rollback()
        logger.exception(
            "Failed to record quota warning notification for org %d (metric=%s)",
            org_id,
            metric,
        )
    finally:
        session.close()


def _dispatch_external(
    session: Session,
    org_id: int,
    *,
    metric: str,
    metric_label: str,
    used: int,
    limit: int,
    plan_name: str,
) -> None:
    """Dispatch to configured Slack/Telegram/webhook/email channels."""
    from datanika.services.notification_service import NotificationService

    svc = NotificationService()
    payload = {
        "metric": metric,
        "metric_label": metric_label,
        "used": used,
        "limit": limit,
        "plan_name": plan_name,
        "pct": round(used / limit * 100, 1) if limit else 0,
    }
    svc.notify(session, org_id, "quota_warning", payload)


def register_quota_notification_hooks() -> None:
    """Subscribe to ``quota.warning_threshold_reached``."""
    hooks.on("quota.warning_threshold_reached", _on_quota_warning)
