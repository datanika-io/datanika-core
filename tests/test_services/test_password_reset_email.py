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


def _function_body_source(name: str) -> str:
    """Source of one top-level function in ``tasks/email_tasks.py``, by name.

    Reads the parse tree rather than slicing module text, so the region a guard
    inspects is the function it names — not "everything after the first mention
    of it", which changes meaning whenever a neighbour is added or moved.
    """
    import ast
    from pathlib import Path

    from datanika.tasks import email_tasks

    source = Path(email_tasks.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == name:
            return ast.get_source_segment(source, node) or ""
    raise AssertionError(f"{name} not found in email_tasks.py")


def _reset_task_body() -> str:
    return _function_body_source("send_password_reset_email_task")


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
        so that is acceptable — but the argument must not reach a log file.

        ⚠️ Scoped to the reset task's **own function body**, via the parse tree.
        This previously sliced the module text from the task's name to the end of
        the file, which had two defects pulling in opposite directions: it went
        red for any unrelated task appended after it (core#652 added two, neither
        touching this one), and it saw nothing at all in a task added *above* it.
        A guard whose reach depends on where someone types is not a guard.
        """
        assert "logger" not in _reset_task_body()
        assert "print(" not in _reset_task_body()

    def test_the_body_extractor_can_actually_see_a_logger(self):
        """Negative control for the guard above.

        `_reset_task_body()` returning a string with no "logger" in it because
        the extractor is broken is indistinguishable from it doing its job. The
        two guards above are the whole protection for a raw reset token, so the
        one thing they must not be is unfalsifiable.
        """
        source = _reset_task_body()
        assert "send_password_reset_email" in source, "extractor returned the wrong function"
        assert "raise_on_error=True" in source, "extractor returned a truncated body"
        assert "logger" in _function_body_source("_record_channel_delivery"), (
            "the extractor reports no logger even in a function that has one"
        )
