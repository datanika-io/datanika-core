"""reCAPTCHA v3 verification service."""

import logging

import httpx

from datanika.config import settings

_log = logging.getLogger(__name__)

VERIFY_URL = "https://www.google.com/recaptcha/api/siteverify"


class CaptchaService:
    def __init__(
        self,
        site_key: str = "",
        secret_key: str = "",
    ):
        self.site_key = site_key or settings.recaptcha_site_key
        self.secret_key = secret_key or settings.recaptcha_secret_key

    @property
    def enabled(self) -> bool:
        return bool(self.site_key and self.secret_key)

    def verify(self, token: str, action: str, min_score: float = 0.5) -> bool:
        if not self.enabled:
            return True

        if not token:
            return False

        try:
            with httpx.Client(timeout=5) as client:
                resp = client.post(
                    VERIFY_URL,
                    data={"secret": self.secret_key, "response": token},
                )
                resp.raise_for_status()
                data = resp.json()
        except Exception:
            _log.exception("reCAPTCHA verification request failed")
            return False

        if not data.get("success"):
            _log.warning("reCAPTCHA rejected: %s", data.get("error-codes"))
            return False

        if data.get("action") != action:
            _log.warning(
                "reCAPTCHA action mismatch: expected=%s got=%s",
                action,
                data.get("action"),
            )
            return False

        score = data.get("score", 0.0)
        if score < min_score:
            _log.warning("reCAPTCHA score too low: %.2f < %.2f", score, min_score)
            return False

        return True
