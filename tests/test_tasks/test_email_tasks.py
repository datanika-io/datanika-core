"""TDD tests for email Celery tasks."""

from unittest.mock import patch

from datanika.tasks.email_tasks import (
    send_email_task,
    send_invitation_email_task,
    send_verification_email_task,
)


class TestSendEmailTask:
    def test_calls_email_service_send(self):
        with patch("datanika.services.email_service.EmailService") as mock_cls:
            mock_svc = mock_cls.return_value
            mock_svc.send.return_value = True

            result = send_email_task("to@test.com", "Subject", "<p>Hi</p>")

            assert result is True
            mock_svc.send.assert_called_once_with("to@test.com", "Subject", "<p>Hi</p>")


class TestSendVerificationEmailTask:
    def test_calls_send_verification_email(self):
        with patch("datanika.services.email_service.EmailService") as mock_cls:
            mock_svc = mock_cls.return_value
            mock_svc.send_verification_email.return_value = True

            result = send_verification_email_task("to@test.com", "token123")

            assert result is True
            mock_svc.send_verification_email.assert_called_once_with("to@test.com", "token123")


class TestSendInvitationEmailTask:
    def test_calls_send_invitation_email(self):
        with patch("datanika.services.email_service.EmailService") as mock_cls:
            mock_svc = mock_cls.return_value
            mock_svc.send_invitation_email.return_value = True

            result = send_invitation_email_task(
                "to@test.com", "Acme Corp", "John", "inv_token"
            )

            assert result is True
            mock_svc.send_invitation_email.assert_called_once_with(
                "to@test.com", "Acme Corp", "John", "inv_token"
            )
