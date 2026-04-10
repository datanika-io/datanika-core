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
    )
    return svc.send_invitation_email(to, org_name, inviter_name, token)


@celery_app.task(name="datanika.send_quota_warning_email")
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
    )
    return svc.send_quota_warning_email(to, plan_name, metric_label, used, limit)
