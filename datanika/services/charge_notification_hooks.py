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
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika import hooks
from datanika.models.notification import Notification, NotificationType
from datanika.services.in_app_notification_service import InAppNotificationService
from datanika.ui.state.base_state import get_sync_session

logger = logging.getLogger(__name__)

_svc = InAppNotificationService()

# How far back to look when checking for duplicates on ``charge_incoming``.
# Longer than the longest plausible billing cycle (31d) so any prior
# notification within the current cycle is caught. ``charge_issued`` and
# ``charge_failed`` are deduped by the unique ``charge_id`` instead, no
# window needed — one notification per charge row, ever.
_INCOMING_DEDUP_WINDOW = timedelta(days=35)


def _format_money(amount_cents: int, currency: str = "USD") -> str:
    symbol = "$" if currency == "USD" else currency + " "
    return f"{symbol}{amount_cents / 100:.2f}"


def _format_cycle_end(period_end: datetime | str | None) -> str:
    """Render cycle end as ISO date (YYYY-MM-DD). Empty string on missing input."""
    if period_end is None:
        return ""
    if isinstance(period_end, datetime):
        return period_end.date().isoformat()
    return str(period_end).split("T")[0]


def _format_gb(byte_count: int | None) -> str:
    """Render a byte count as GB with one decimal (``"12.3"``)."""
    if not byte_count or byte_count < 0:
        return "0"
    return f"{byte_count / (1024**3):,.1f}"


def _dispatch(session: Session, org_id: int, event: str, payload: dict) -> None:
    """Send to configured Slack/Telegram/webhook/email channels."""
    from datanika.services.notification_service import NotificationService

    NotificationService().notify(session, org_id, event, payload)


def _existing_notification(
    session: Session,
    *,
    org_id: int,
    notif_type: NotificationType,
    resource_type: str,
    resource_id: int,
    since: datetime | None = None,
) -> Notification | None:
    """Return the most recent matching Notification, if any.

    ``since`` bounds the lookup window. For ``charge_issued`` / ``charge_failed``
    pass ``None`` — the resource_id (Paddle charge id) uniquely identifies
    the charge row, so a match at any time means we've already notified.

    For ``charge_incoming``, pass a recent cutoff so we catch retries within
    the same cycle without blocking next cycle's warning for the same
    subscription.
    """
    stmt = select(Notification).where(
        Notification.org_id == org_id,
        Notification.type == notif_type,
        Notification.resource_type == resource_type,
        Notification.resource_id == resource_id,
        Notification.deleted_at.is_(None),
    )
    if since is not None:
        stmt = stmt.where(Notification.created_at >= since)
    stmt = stmt.order_by(Notification.created_at.desc()).limit(1)
    return session.execute(stmt).scalar_one_or_none()


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
    overage_quantity: int | None = None,
    period_end: datetime | str | None = None,
    plan_name: str = "",
    **_kw,
) -> None:
    # Belt-and-suspenders dedup — the cloud's ``UsageLedger.charge_incoming_sent``
    # latch is the primary guard, but a Celery retry after a transient DB error
    # inside this hook (commit fails, row rolls back, latch stays False) would
    # re-fire emit on the next scheduler tick. A recent-window query catches it.
    # See ``datanika-cloud/docs/billing_contract.md § Notification dedup``.
    cutoff = datetime.now(UTC) - _INCOMING_DEDUP_WINDOW
    if _existing_notification(
        session,
        org_id=org_id,
        notif_type=NotificationType.CHARGE_INCOMING,
        resource_type="subscription",
        resource_id=subscription_id,
        since=cutoff,
    ):
        logger.info(
            "charge_incoming dedup hit for org=%d subscription_id=%d — skipping",
            org_id,
            subscription_id,
        )
        return

    amount_display = _format_money(amount_cents, currency)
    gb_display = _format_gb(overage_quantity)
    cycle_ends_at = _format_cycle_end(period_end)
    title = f"Upcoming overage charge: {amount_display}"
    message = (
        f"Your usage this cycle will add {amount_display} "
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
            # Display-formatted fields for notification templates
            # (email/Slack/Telegram). ``plan_name`` falls through to
            # the template's default of "your" when cloud doesn't
            # emit it.
            "amount_display": amount_display,
            "gb_display": gb_display,
            "cycle_ends_at": cycle_ends_at,
            "plan_name": plan_name,
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
    # ``charge_id`` uniquely identifies the charge row on the cloud side
    # (unique ``idempotency_key``), so any existing notification for this
    # charge_id is a duplicate — no time window needed.
    if _existing_notification(
        session,
        org_id=org_id,
        notif_type=NotificationType.CHARGE_ISSUED,
        resource_type="charge",
        resource_id=charge_id,
    ):
        logger.info(
            "charge_issued dedup hit for org=%d charge_id=%d — skipping",
            org_id,
            charge_id,
        )
        return

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
    # Dedup by charge_id — same as charge_issued. A given charge row can
    # only terminate in FAILED once, so any existing row with the same
    # charge_id is a replay.
    if _existing_notification(
        session,
        org_id=org_id,
        notif_type=NotificationType.CHARGE_FAILED,
        resource_type="charge",
        resource_id=charge_id,
    ):
        logger.info(
            "charge_failed dedup hit for org=%d charge_id=%d — skipping",
            org_id,
            charge_id,
        )
        return

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
