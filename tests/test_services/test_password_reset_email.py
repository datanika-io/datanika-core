"""The password-reset email (SPEC_PASSWORD_RESET §4).

Two properties are load-bearing and neither is obvious from reading the method:

* the link points at a **frontend** path (``/reset-password``), not an ``/api/``
  route like the two existing templates. A backend route outside ``/api/``
  silently serves the Reflex SPA in production — that exact failure hit ``/mcp``
  and every OAuth discovery document — and the page has to render a *form*
  anyway, which a redirecting route cannot;
* the message carries a **plaintext alternative**. This is the one email that
  must land; an HTML-only ``multipart/alternative`` is a spam-filter signal.
"""

from unittest.mock import MagicMock, patch

import pytest

from datanika.services.email_service import EmailService


@pytest.fixture
def svc():
    return EmailService(
        smtp_host="smtp.test.com",
        smtp_port=587,
        smtp_user="user",
        smtp_password="pass",
        smtp_from_email="noreply@datanika.io",
        smtp_from_name="Datanika",
        smtp_use_tls=True,
        frontend_url="https://app.datanika.io",
    )


def _sent_message(svc, call):
    with patch("datanika.services.email_service.smtplib.SMTP") as mock_smtp_class:
        mock_smtp = MagicMock()
        mock_smtp_class.return_value.__enter__ = MagicMock(return_value=mock_smtp)
        mock_smtp_class.return_value.__exit__ = MagicMock(return_value=False)
        assert call() is True
        return mock_smtp.sendmail.call_args[0][2]


class TestResetEmail:
    def test_link_is_a_frontend_path(self, svc):
        body = _sent_message(svc, lambda: svc.send_password_reset_email("a@b.com", "TOKEN123"))
        assert "https://app.datanika.io/reset-password?token=TOKEN123" in body
        assert "/api/reset-password" not in body

    def test_states_the_sixty_minute_expiry(self, svc):
        body = _sent_message(svc, lambda: svc.send_password_reset_email("a@b.com", "TOKEN123"))
        assert "60 minutes" in body

    def test_says_an_unrequested_email_can_be_ignored(self, svc):
        body = _sent_message(svc, lambda: svc.send_password_reset_email("a@b.com", "TOKEN123"))
        lowered = body.lower()
        assert "didn't request" in lowered or "did not request" in lowered

    def test_carries_a_plaintext_alternative(self, svc):
        body = _sent_message(svc, lambda: svc.send_password_reset_email("a@b.com", "TOKEN123"))
        assert "text/plain" in body
        assert "text/html" in body
        # least-preferred part first, per RFC 2046 §5.1.4
        assert body.index("text/plain") < body.index("text/html")

    def test_the_plaintext_part_carries_the_link_too(self, svc):
        body = _sent_message(svc, lambda: svc.send_password_reset_email("a@b.com", "TOKEN123"))
        plain_start = body.index("text/plain")
        html_start = body.index("text/html")
        assert "TOKEN123" in body[plain_start:html_start]

    def test_returns_false_on_an_instance_with_no_smtp(self):
        disabled = EmailService(
            smtp_host="",
            smtp_port=587,
            smtp_user="",
            smtp_password="",
            smtp_from_email="no@test.com",
            smtp_from_name="Test",
            smtp_use_tls=True,
            frontend_url="http://localhost:3000",
        )
        assert disabled.send_password_reset_email("a@b.com", "TOKEN123") is False

    def test_existing_templates_keep_working(self, svc):
        """The plaintext part is opt-in; the other two emails must be untouched."""
        body = _sent_message(svc, lambda: svc.send_verification_email("a@b.com", "VTOKEN"))
        assert "/api/verify-email?token=VTOKEN" in body


class TestCeleryTask:
    def test_task_is_registered_under_the_expected_name(self):
        from datanika.tasks.email_tasks import send_password_reset_email_task

        assert send_password_reset_email_task.name == "datanika.send_password_reset_email"

    def test_task_retries_like_the_other_email_tasks(self):
        from datanika.tasks.email_tasks import send_password_reset_email_task

        assert send_password_reset_email_task.max_retries == 3
        assert send_password_reset_email_task.retry_backoff == 30
        assert send_password_reset_email_task.retry_backoff_max == 300

    def test_task_arguments_are_not_logged(self):
        """The raw token transits the Celery argument list (Redis broker, JSON
        serializer). Redis is bound to 127.0.0.1 and is not backed up off-box,
        so that is acceptable — but the argument must not reach a log file."""
        from pathlib import Path

        from datanika.tasks import email_tasks

        source = Path(email_tasks.__file__).read_text(encoding="utf-8")
        reset_block = source[source.index("send_password_reset_email_task") :]
        assert "logger" not in reset_block
        assert "print(" not in reset_block
