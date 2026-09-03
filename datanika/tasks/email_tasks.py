"""Celery tasks for async email dispatch."""

from datanika.tasks.celery_app import celery_app


@celery_app.task(
    name="datanika.send_email",
    autoretry_for=(OSError, ConnectionError, TimeoutError),
    retry_backoff=30,
    retry_backoff_max=300,
    max_retries=3,
)
def send_email_task(to: str, subject: str, html_body: str) -> bool:
    from datanika.config import settings
    from datanika.services.email_service import EmailService

    svc = EmailService(
        smtp_host=settings.smtp_host,
        smtp_port=settings.smtp_port,
        smtp_user=settings.smtp_user,
        smtp_password=settings.smtp_password,
        smtp_from_email=settings.smtp_from_email,
        smtp_from_name=settings.smtp_from_name,
        smtp_use_tls=settings.smtp_use_tls,
        frontend_url=settings.frontend_url,
        raise_on_error=True,
    )
    return svc.send(to, subject, html_body)


@celery_app.task(
    name="datanika.send_verification_email",
    autoretry_for=(OSError, ConnectionError, TimeoutError),
    retry_backoff=30,
    retry_backoff_max=300,
    max_retries=3,
)
def send_verification_email_task(to: str, token: str) -> bool:
    from datanika.config import settings
    from datanika.services.email_service import EmailService

    svc = EmailService(
        smtp_host=settings.smtp_host,
        smtp_port=settings.smtp_port,
        smtp_user=settings.smtp_user,
        smtp_password=settings.smtp_password,
        smtp_from_email=settings.smtp_from_email,
        smtp_from_name=settings.smtp_from_name,
        smtp_use_tls=settings.smtp_use_tls,
        frontend_url=settings.frontend_url,
        raise_on_error=True,
    )
    return svc.send_verification_email(to, token)


@celery_app.task(
    name="datanika.send_invitation_email",
    autoretry_for=(OSError, ConnectionError, TimeoutError),
    retry_backoff=30,
    retry_backoff_max=300,
    max_retries=3,
)
def send_invitation_email_task(to: str, org_name: str, inviter_name: str, token: str) -> bool:
    from datanika.config import settings
    from datanika.services.email_service import EmailService

    svc = EmailService(
        smtp_host=settings.smtp_host,
        smtp_port=settings.smtp_port,
        smtp_user=settings.smtp_user,
        smtp_password=settings.smtp_password,
        smtp_from_email=settings.smtp_from_email,
        smtp_from_name=settings.smtp_from_name,
        smtp_use_tls=settings.smtp_use_tls,
        frontend_url=settings.frontend_url,
        raise_on_error=True,
    )
    return svc.send_invitation_email(to, org_name, inviter_name, token)


@celery_app.task(
    name="datanika.send_password_reset_email",
    autoretry_for=(OSError, ConnectionError, TimeoutError),
    retry_backoff=30,
    retry_backoff_max=300,
    max_retries=3,
)
def send_password_reset_email_task(to: str, token: str) -> bool:
    # The raw token necessarily transits this argument list (Redis broker, JSON
    # serializer). That is acceptable — Redis is bound to 127.0.0.1 and is not
    # backed up off-box, while the database only ever holds the hash. It is also
    # exactly why nothing in this task may log its arguments.
    from datanika.config import settings
    from datanika.services.email_service import EmailService

    svc = EmailService(
        smtp_host=settings.smtp_host,
        smtp_port=settings.smtp_port,
        smtp_user=settings.smtp_user,
        smtp_password=settings.smtp_password,
        smtp_from_email=settings.smtp_from_email,
        smtp_from_name=settings.smtp_from_name,
        smtp_use_tls=settings.smtp_use_tls,
        frontend_url=settings.frontend_url,
        raise_on_error=True,
    )
    return svc.send_password_reset_email(to, token)


@celery_app.task(
    name="datanika.send_quota_warning_email",
    # 🚨 This task had `raise_on_error=True` and **no** `autoretry_for`, which is
    # the worst of both: it raises on a transient relay blip and then does not
    # retry, so the warning is simply lost. Its only caller is
    # `datanika_cloud/billing/meter.py`, which enqueues it inside its own
    # `except Exception` — so the loss was invisible from cloud as well (core#652).
    autoretry_for=(OSError, ConnectionError, TimeoutError),
    retry_backoff=30,
    retry_backoff_max=300,
    max_retries=3,
)
def send_quota_warning_email_task(
    to: str, plan_name: str, metric_label: str, used: int, limit: int
) -> bool:
    from datanika.config import settings
    from datanika.services.email_service import EmailService

    svc = EmailService(
        smtp_host=settings.smtp_host,
        smtp_port=settings.smtp_port,
        smtp_user=settings.smtp_user,
        smtp_password=settings.smtp_password,
        smtp_from_email=settings.smtp_from_email,
        smtp_from_name=settings.smtp_from_name,
        smtp_use_tls=settings.smtp_use_tls,
        frontend_url=settings.frontend_url,
        raise_on_error=True,
    )
    return svc.send_quota_warning_email(to, plan_name, metric_label, used, limit)


@celery_app.task(
    name="datanika.send_channel_notification_email",
    autoretry_for=(OSError, ConnectionError, TimeoutError),
    retry_backoff=30,
    retry_backoff_max=300,
    max_retries=3,
)
def send_channel_notification_email_task(
    channel_id: int, org_id: int, to: str, subject: str, html_body: str
) -> bool:
    """Deliver one alerting-channel email **and record what happened** (core#652).

    Separate from ``send_email_task`` because of the last argument-shaped word in
    that sentence: this task knows which ``notification_channels`` row it is
    acting for, and writes the terminal outcome back to it. Without that the
    channel's ``last_status`` could only ever say "enqueued", which is a claim
    about our broker rather than about the user's inbox — the same category of
    non-evidence as the green badge this issue is about.

    ⚠️ Every attempt is recorded, including the ones ``autoretry_for`` will
    retry. That is not a simplification — the columns are named ``last_attempt_at``
    and ``last_status``, and "the last attempt failed" is true at that moment
    whether or not a later one succeeds, which overwrites it. Deriving "is this
    the final attempt?" from ``self.request.retries`` would make the honesty of
    this row depend on retry arithmetic being right, and a row that is only
    correct when a calculation is correct is the kind of evidence this issue
    exists to stop producing.
    """
    from datanika.config import settings
    from datanika.services.email_service import EmailService

    svc = EmailService(
        smtp_host=settings.smtp_host,
        smtp_port=settings.smtp_port,
        smtp_user=settings.smtp_user,
        smtp_password=settings.smtp_password,
        smtp_from_email=settings.smtp_from_email,
        smtp_from_name=settings.smtp_from_name,
        smtp_use_tls=settings.smtp_use_tls,
        frontend_url=settings.frontend_url,
        raise_on_error=True,
    )
    try:
        sent = svc.send(to, subject, html_body)
    except Exception as exc:
        _record_channel_delivery(channel_id, org_id, "failed", f"{type(exc).__name__}: {exc}")
        raise
    _record_channel_delivery(
        channel_id,
        org_id,
        "success" if sent else "failed",
        None if sent else "the mail transport reported the message was not sent",
    )
    return sent


def _record_channel_delivery(channel_id: int, org_id: int, status: str, error: str | None) -> None:
    """Write the delivery outcome onto the channel row, in the task's own session.

    Routed through ``NotificationService._record_delivery`` rather than assigning
    the columns here, because that method is where redaction and truncation live
    — and ``last_error`` derived from an SMTP exception can carry the recipient
    address, which is personal data we do not put in a new place.

    Never raises. The mail has already been sent or has already failed by the
    time this runs; an observability write must not turn a delivered
    notification into a retried one.
    """
    import logging

    logger = logging.getLogger(__name__)
    try:
        from datanika.db import get_sync_session
        from datanika.services.notification_service import NotificationService

        svc = NotificationService()
        with get_sync_session() as session:
            channel = svc._get_channel(session, channel_id, org_id)
            if channel is None:
                return
            svc._record_delivery(session, channel, status, error)
            session.commit()
    except Exception:
        logger.exception("Could not record delivery state for channel %s", channel_id)
