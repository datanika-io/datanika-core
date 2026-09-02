"""reCAPTCHA v3 verification service."""

import logging

import httpx

from datanika.config import settings

_log = logging.getLogger(__name__)

VERIFY_URL = "https://www.google.com/recaptcha/api/siteverify"


def log_captcha_status() -> None:
    """Say once, at startup, whether the CAPTCHA actually checks anything (core#639 D6).

    ``verify()`` returns ``True`` unconditionally when unconfigured, and that
    fallback is **correct**: forcing a self-hoster into a Google account to run
    a signup form would be worse than the protection is worth. What was wrong is
    that the state was invisible — the class exists, ``/login`` and ``/signup``
    both call it, and nothing anywhere said it was inert. Production has never
    had these keys, so every call has passed without checking anything, and
    reading the code gives no hint of it.

    Deliberately a WARNING and never a failure. A deployment with no reCAPTCHA
    keys is supported, not broken; failing startup here would turn an
    open-source default into a hard dependency on a third party.

    Neither key value is interpolated. The site key is public and the secret key
    is not, and a line that prints either is one ``grep`` away from being the
    leak it was written to prevent (WORKFLOW_RULES §7).
    """
    if CaptchaService().enabled:
        _log.info("reCAPTCHA enabled; /login and /signup submissions are verified")
        return
    # The two setting names are written inline rather than held in module
    # constants: ruff's S105 flags any string literal bound to a name matching
    # *SECRET*/*KEY*, and a `noqa` on a line that is genuinely about a
    # credential name is exactly the suppression nobody re-reads.
    _log.warning(
        "reCAPTCHA is NOT configured: %s and %s are unset, so CaptchaService.verify() "
        "accepts every submission on /login and /signup without checking. This is a "
        "supported configuration; the rate limit on /signup (core#639) is what bounds "
        "automated abuse in the meantime.",
        "RECAPTCHA_SITE_KEY",
        "RECAPTCHA_SECRET_KEY",
    )


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
