"""TDD tests for EmailService — SMTP email sending."""

from unittest.mock import MagicMock, patch

import pytest

from datanika.services.email_service import EmailService


class TestEmailServiceDisabled:
    def test_disabled_when_no_host(self):
        svc = EmailService(
            smtp_host="",
            smtp_port=587,
            smtp_user="",
            smtp_password="",
            smtp_from_email="no@test.com",
            smtp_from_name="Test",
            smtp_use_tls=True,
            frontend_url="http://localhost:3000",
        )
        assert svc.is_enabled() is False

    def test_enabled_when_host_set(self):
        svc = EmailService(
            smtp_host="smtp.test.com",
            smtp_port=587,
            smtp_user="user",
            smtp_password="pass",
            smtp_from_email="no@test.com",
            smtp_from_name="Test",
            smtp_use_tls=True,
            frontend_url="http://localhost:3000",
        )
        assert svc.is_enabled() is True

    def test_send_returns_false_when_disabled(self):
        svc = EmailService(
            smtp_host="",
            smtp_port=587,
            smtp_user="",
            smtp_password="",
            smtp_from_email="no@test.com",
            smtp_from_name="Test",
            smtp_use_tls=True,
            frontend_url="http://localhost:3000",
        )
        assert svc.send("to@test.com", "Subject", "<p>Body</p>") is False


class TestEmailServiceSend:
    @pytest.fixture
    def svc(self):
        return EmailService(
            smtp_host="smtp.test.com",
            smtp_port=587,
            smtp_user="user",
            smtp_password="pass",
            smtp_from_email="noreply@datanika.io",
            smtp_from_name="Datanika",
            smtp_use_tls=True,
            frontend_url="http://localhost:3000",
        )

    def test_send_email_success(self, svc):
        with patch("datanika.services.email_service.smtplib.SMTP") as mock_smtp_class:
            mock_smtp = MagicMock()
            mock_smtp_class.return_value.__enter__ = MagicMock(return_value=mock_smtp)
            mock_smtp_class.return_value.__exit__ = MagicMock(return_value=False)

            result = svc.send("to@test.com", "Test Subject", "<p>Hello</p>")

            assert result is True
            mock_smtp.sendmail.assert_called_once()
            call_args = mock_smtp.sendmail.call_args
            assert call_args[0][0] == "noreply@datanika.io"
            assert call_args[0][1] == "to@test.com"

    def test_send_uses_starttls(self, svc):
        with patch("datanika.services.email_service.smtplib.SMTP") as mock_smtp_class:
            mock_smtp = MagicMock()
            mock_smtp_class.return_value.__enter__ = MagicMock(return_value=mock_smtp)
            mock_smtp_class.return_value.__exit__ = MagicMock(return_value=False)

            svc.send("to@test.com", "Subject", "<p>Body</p>")

            mock_smtp.starttls.assert_called_once()
            mock_smtp.login.assert_called_once_with("user", "pass")

    def test_send_returns_false_on_smtp_error(self, svc):
        with patch("datanika.services.email_service.smtplib.SMTP") as mock_smtp_class:
            mock_smtp_class.side_effect = Exception("Connection refused")

            result = svc.send("to@test.com", "Subject", "<p>Body</p>")

            assert result is False


class TestEmailTemplates:
    @pytest.fixture
    def svc(self):
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

    def test_verification_email_contains_token(self, svc):
        with patch.object(svc, "send", return_value=True) as mock_send:
            result = svc.send_verification_email("user@test.com", "abc123token")

            assert result is True
            mock_send.assert_called_once()
            html_body = mock_send.call_args[0][2]
            assert "abc123token" in html_body
            assert "https://app.datanika.io" in html_body

    def test_verification_email_url_has_api_prefix(self, svc):
        with patch.object(svc, "send", return_value=True) as mock_send:
            svc.send_verification_email("user@test.com", "tok123")
            html_body = mock_send.call_args[0][2]
            assert "/api/verify-email?token=tok123" in html_body

    def test_invitation_email_contains_org_name(self, svc):
        with patch.object(svc, "send", return_value=True) as mock_send:
            result = svc.send_invitation_email(
                "invitee@test.com", "Acme Corp", "John Doe", "invite_token_xyz"
            )

            assert result is True
            mock_send.assert_called_once()
            html_body = mock_send.call_args[0][2]
            assert "Acme Corp" in html_body
            assert "John Doe" in html_body
            assert "invite_token_xyz" in html_body

    def test_invitation_email_url_has_api_prefix(self, svc):
        with patch.object(svc, "send", return_value=True) as mock_send:
            svc.send_invitation_email("a@b.com", "Org", "User", "inv_tok")
            html_body = mock_send.call_args[0][2]
            assert "/api/accept-invite?token=inv_tok" in html_body
