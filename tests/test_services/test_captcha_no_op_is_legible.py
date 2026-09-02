"""The production CAPTCHA is a no-op, and that must be asserted, not discovered.

Contract: ``docs/specs/SPEC_SIGNUP_ENUMERATION.md`` D6 / AC7.

``CaptchaService.verify()`` returns ``True`` unconditionally when ``enabled`` is
false, and ``enabled`` is ``bool(site_key and secret_key)``. Production's
``.env.docker`` carries no ``RECAPTCHA_*`` keys, so both are ``""`` — every
``verify()`` call on ``/login`` and ``/signup`` has returned ``True`` without
checking anything, for the life of the project.

**The fallback is correct and is not being changed.** Forcing a self-hoster into
a Google dependency to run a signup form would be worse. What is wrong is that
the state was *invisible*: the class exists, the call sites exist, both pages
call it, and nothing anywhere says it is inert. That is this project's signature
defect — a green that proves nothing — so the no-op becomes a documented,
asserted property with a startup line an operator can read.

⚠️ Deliberately **not** a deploy failure. That converts an open-source default
into a hard dependency on a Google account.
"""

import logging

import pytest

from datanika.services import captcha_service as cs
from datanika.services.captcha_service import CaptchaService, log_captcha_status


class TestTheNoOpIsAsserted:
    def test_captcha_is_disabled_when_no_keys_are_configured(self):
        """AC7. The shipped default, stated out loud."""
        assert CaptchaService(site_key="", secret_key="").enabled is False

    def test_a_disabled_captcha_admits_every_token(self):
        """Including no token at all. This is the property that makes the
        rate limit in core#639 the only bound on ``/signup``."""
        svc = CaptchaService(site_key="", secret_key="")
        assert svc.verify("", "signup") is True
        assert svc.verify("anything", "signup") is True

    def test_one_key_alone_is_not_enough_to_enable_it(self):
        """A half-configured instance is inert too, and silently so — worth an
        assertion because it is the state a partial rollout lands in."""
        assert CaptchaService(site_key="site", secret_key="").enabled is False
        assert CaptchaService(site_key="", secret_key="secret").enabled is False

    def test_both_keys_enable_it(self):
        """The negative control. Without this, the three assertions above are
        satisfied by a property that is *always* false — which would be a
        checker with one possible answer rather than a check."""
        assert CaptchaService(site_key="site", secret_key="secret").enabled is True


class TestTheStartupLineNamesWhatIsMissing:
    def test_an_unconfigured_instance_warns_at_startup(self, caplog, monkeypatch):
        monkeypatch.setattr(cs.settings, "recaptcha_site_key", "")
        monkeypatch.setattr(cs.settings, "recaptcha_secret_key", "")
        with caplog.at_level(logging.WARNING, logger="datanika.services.captcha_service"):
            log_captcha_status()
        records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(records) == 1, f"expected exactly one WARNING, got {records}"
        message = records[0].getMessage()
        assert "RECAPTCHA_SITE_KEY" in message, message
        assert "RECAPTCHA_SECRET_KEY" in message, message

    @pytest.mark.parametrize("level", [logging.CRITICAL, logging.ERROR])
    def test_it_is_a_warning_and_not_an_error(self, caplog, monkeypatch, level):
        """It must not read as a failure: an instance with no reCAPTCHA keys is
        a supported deployment, not a broken one."""
        monkeypatch.setattr(cs.settings, "recaptcha_site_key", "")
        monkeypatch.setattr(cs.settings, "recaptcha_secret_key", "")
        with caplog.at_level(level, logger="datanika.services.captcha_service"):
            log_captcha_status()
        assert caplog.records == []

    def test_a_configured_instance_says_so_without_warning(self, caplog, monkeypatch):
        monkeypatch.setattr(cs.settings, "recaptcha_site_key", "site")
        monkeypatch.setattr(cs.settings, "recaptcha_secret_key", "secret")
        with caplog.at_level(logging.INFO, logger="datanika.services.captcha_service"):
            log_captcha_status()
        assert [r for r in caplog.records if r.levelno >= logging.WARNING] == []
        assert caplog.records, "a configured instance should still say so at INFO"

    def test_the_startup_line_never_prints_a_key(self, caplog, monkeypatch):
        """§7 — never log a secret. The site key is public, the secret key is
        not, and a message that interpolates either is one grep away from being
        the leak it was written to prevent."""
        monkeypatch.setattr(cs.settings, "recaptcha_site_key", "SITEKEYVALUE")
        monkeypatch.setattr(cs.settings, "recaptcha_secret_key", "SECRETKEYVALUE")
        with caplog.at_level(logging.INFO, logger="datanika.services.captcha_service"):
            log_captcha_status()
        for record in caplog.records:
            assert "SECRETKEYVALUE" not in record.getMessage()
            assert "SITEKEYVALUE" not in record.getMessage()


class TestTheAppCallsIt:
    def test_startup_invokes_the_status_line(self):
        """A function nobody calls is the same defect as a counter nobody serves.

        Source-level, because importing ``datanika.datanika`` stands up the whole
        Reflex app.
        """
        import inspect
        import pathlib

        source = pathlib.Path(inspect.getfile(cs)).parent.parent / "datanika.py"
        text = source.read_text(encoding="utf-8")
        assert "log_captcha_status()" in text, (
            "datanika/datanika.py must call log_captcha_status() at startup"
        )
