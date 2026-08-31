"""Request the "confirm your email address" mail.

Everything this needs already existed and none of it was connected: the column
(``users.email_verified``), the minter (``AuthService.create_email_verification_token``),
the template (``EmailService.send_verification_email``), the Celery task
(``tasks.email_tasks.send_verification_email_task``) and the route that spends the
token (``GET /api/verify-email``). This module is the missing caller.

It exists as a function rather than three lines inside the signup handler because
the decision it encodes — *what to do when there is no relay, and what to do when
the broker is down* — has to be the same everywhere it is made, and because a
Reflex event handler is close to untestable in isolation.

**Never let this raise into its caller.** By the time it runs the account is
already committed; turning a broker outage into "Signup failed. Please try again."
would tell a user their account does not exist while it does, and a second attempt
would then fail on the unique email constraint.
"""

import logging
from enum import StrEnum

from datanika.services.auth import AuthService
from datanika.tasks.email_tasks import send_verification_email_task

logger = logging.getLogger(__name__)


class VerificationMailResult(StrEnum):
    """Why the verification mail was or was not queued.

    Three outcomes, not two (core#700 AC2). The bool this replaced returned ``False``
    for both *a self-hoster has configured no relay* — an ordinary deployment where
    nothing is wrong — and *the broker was unreachable*, which is a real failure a user
    should be told about. Collapsing them meant the caller could not act on either, so
    it acted on neither and dropped the value.
    """

    QUEUED = "queued"
    NO_RELAY = "no_relay"
    FAILED = "failed"


def request_email_verification(
    user_id: int,
    email: str,
    auth: AuthService,
    *,
    smtp_host: str | None = None,
) -> VerificationMailResult:
    """Enqueue the verification mail for ``email``. Never raises.

    ``smtp_host`` is passed in rather than read here so the caller's settings
    snapshot is the one that decides, and so tests do not have to patch global
    config. Falling back to ``settings`` keeps the ordinary call site short.
    """
    if smtp_host is None:
        from datanika.config import settings

        smtp_host = settings.smtp_host

    if not smtp_host:
        # A self-hosted deployment with no relay is a normal deployment, not a
        # broken one. The account still works; the address simply stays
        # unconfirmed, which only ever costs the social-login auto-link.
        logger.info("Skipping verification email for user %s: no SMTP relay configured", user_id)
        return VerificationMailResult.NO_RELAY

    try:
        token = auth.create_email_verification_token(user_id, email)
        send_verification_email_task.delay(email, token)
    except Exception:
        logger.exception("Failed to enqueue verification email for user %s", user_id)
        return VerificationMailResult.FAILED
    return VerificationMailResult.QUEUED
