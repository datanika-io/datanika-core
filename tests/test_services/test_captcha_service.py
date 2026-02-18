"""Tests for reCAPTCHA v3 verification service."""

from unittest.mock import MagicMock, patch

import httpx

from datanika.services.captcha_service import CaptchaService


class TestCaptchaDisabled:
    """When keys are empty, CAPTCHA is disabled (dev mode)."""

    def test_not_enabled(self):
        svc = CaptchaService(site_key="", secret_key="")
        assert svc.enabled is False

    def test_returns_true_when_disabled(self):
        svc = CaptchaService(site_key="", secret_key="")
        assert svc.verify("", "login") is True

    def test_partially_configured_not_enabled(self):
        svc = CaptchaService(site_key="key", secret_key="")
        assert svc.enabled is False
        assert svc.verify("", "login") is True


class TestCaptchaEnabled:
    """When both keys are set, CAPTCHA verification is active."""

    def _svc(self):
        return CaptchaService(site_key="site-key", secret_key="secret-key")

    def test_rejects_empty_token(self):
        svc = self._svc()
        assert svc.verify("", "login") is False

    @patch("datanika.services.captcha_service.httpx.Client")
    def test_verify_success(self, mock_client_cls):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "success": True,
            "action": "login",
            "score": 0.9,
        }
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        svc = self._svc()
        assert svc.verify("valid-token", "login") is True

    @patch("datanika.services.captcha_service.httpx.Client")
    def test_low_score(self, mock_client_cls):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "success": True,
            "action": "login",
            "score": 0.2,
        }
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        svc = self._svc()
        assert svc.verify("token", "login") is False

    @patch("datanika.services.captcha_service.httpx.Client")
    def test_action_mismatch(self, mock_client_cls):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "success": True,
            "action": "signup",
            "score": 0.9,
        }
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        svc = self._svc()
        assert svc.verify("token", "login") is False

    @patch("datanika.services.captcha_service.httpx.Client")
    def test_google_rejects(self, mock_client_cls):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "success": False,
            "error-codes": ["invalid-input-response"],
        }
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.return_value = mock_resp
        mock_client_cls.return_value = mock_client

        svc = self._svc()
        assert svc.verify("bad-token", "login") is False

    @patch("datanika.services.captcha_service.httpx.Client")
    def test_http_error(self, mock_client_cls):
        mock_client = MagicMock()
        mock_client.__enter__ = MagicMock(return_value=mock_client)
        mock_client.__exit__ = MagicMock(return_value=False)
        mock_client.post.side_effect = httpx.ConnectError("connection failed")
        mock_client_cls.return_value = mock_client

        svc = self._svc()
        assert svc.verify("token", "login") is False
