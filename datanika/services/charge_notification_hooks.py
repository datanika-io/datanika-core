"""Charge notification hooks — V2 P5 Option B (core#249).

Subscribes to the three cycle-boundary charge events emitted by the cloud
plugin's ``datanika_cloud/billing/tasks.py``:

- ``charge_incoming``: ~24h before cycle close, customer has crossed
  ``plan.bytes_included`` → warn them so they can top up or adjust usage
  before the charge fires.
- ``charge_issued``: Paddle accepted the one-off charge; invoice receipt.
- ``charge_failed``: API retries exhausted (after CHARGE_MAX_RETRIES);
  ops alert + customer email asking to contact support.

Same shape as ``quota_notification_hooks.py`` — creates in-app
Notification rows and dispatches to user-configured external channels.
Handlers are pure subscribers: in an open-source build with no cloud
plugin loaded, these events never emit and the module is inert.
"""

import logging

from sqlalchemy.orm import Session

from datanika import hooks
from datanika.models.notification import NotificationType
from datanika.services.in_app_notification_service import InAppNotificationService
from datanika.ui.state.base_state import get_sync_session

logger = logging.getLogger(__name__)

_svc = InAppNotificationService()


def _format_money(amount_cents: int, currency: str = "USD") -> str:
    symbol = "$" if currency == "USD" else currency + " "
    return f"{symbol}{amount_cents / 100:.2f}"


def _dispatch(session: Session, org_id: int, event: str, payload: dict) -> None:
    """Send to configured Slack/Telegram/webhook/email channels."""
    from datanika.services.notification_service import NotificationService

    NotificationService().notify(session, org_id, event, payload)


def _with_session(fn):
    """Open a session, run ``fn(session)``, commit/rollback/close.

    Swallows all exceptions so notification delivery failures can't break
    the emit path — the event source is a Celery task, we don't want a
    transient notification-service error to fail the charge task too.
    """

    def _wrapped(**kw):
        session = get_sync_session()
        try:
            fn(session, **kw)
            session.commit()
        except Exception:  # noqa: BLE001
            session.rollback()
            logger.exception(
                "Failed to handle charge notification (event=%s, org_id=%s)",
                fn.__name__,
                kw.get("org_id"),
            )
        finally:
            session.close()

    return _wrapped


@_with_session
def _on_charge_incoming(
    session: Session,
    *,
    org_id: int,
    subscription_id: int,
    amount_cents: int,
    currency: str = "USD",
    metric: str = "bytes_processed",
    **_kw,
) -> None:
    title = f"Upcoming overage charge: {_format_money(amount_cents, currency)}"
    message = (
        f"Your usage this cycle will add {_format_money(amount_cents, currency)} "
        f"to your next invoice. Charge fires at cycle close."
    )
    _svc.create(
        session,
        org_id,
        NotificationType.CHARGE_INCOMING,
        title=title,
        resource_type="subscription",
        resource_id=subscription_id,
        message=message,
    )
    _dispatch(
        session,
        org_id,
        "charge_incoming",
        {
            "amount_cents": amount_cents,
            "currency": currency,
            "metric": metric,
        },
    )


@_with_session
def _on_charge_issued(
    session: Session,
    *,
    org_id: int,
    subscription_id: int,
    charge_id: int,
    amount_cents: int,
    currency: str = "USD",
    metric: str = "bytes_processed",
    **_kw,
) -> None:
    title = f"Overage charge: {_format_money(amount_cents, currency)}"
    message = (
        f"A one-off charge of {_format_money(amount_cents, currency)} "
        f"has been added to your subscription for this cycle's usage."
    )
    _svc.create(
        session,
        org_id,
        NotificationType.CHARGE_ISSUED,
        title=title,
        resource_type="charge",
        resource_id=charge_id,
        message=message,
    )
    _dispatch(
        session,
        org_id,
        "charge_issued",
        {
            "amount_cents": amount_cents,
            "currency": currency,
            "metric": metric,
            "charge_id": charge_id,
        },
    )


@_with_session
def _on_charge_failed(
    session: Session,
    *,
    org_id: int,
    subscription_id: int,
    charge_id: int,
    amount_cents: int,
    currency: str = "USD",
    metric: str = "bytes_processed",
    attempts: int = 0,
    last_error: str = "",
    **_kw,
) -> None:
    title = "Overage charge failed — action needed"
    message = (
        f"We couldn't collect {_format_money(amount_cents, currency)} for this cycle "
        f"after {attempts} attempts. Please contact support or check your payment method."
    )
    _svc.create(
        session,
        org_id,
        NotificationType.CHARGE_FAILED,
        title=title,
        resource_type="charge",
        resource_id=charge_id,
        message=message,
    )
    _dispatch(
        session,
        org_id,
        "charge_failed",
        {
            "amount_cents": amount_cents,
            "currency": currency,
            "metric": metric,
            "charge_id": charge_id,
            "attempts": attempts,
            "last_error": last_error,
        },
    )


def register_charge_notification_hooks() -> None:
    """Subscribe to the 3 cycle-boundary charge events."""
    hooks.on("charge_incoming", _on_charge_incoming)
    hooks.on("charge_issued", _on_charge_issued)
    hooks.on("charge_failed", _on_charge_failed)
