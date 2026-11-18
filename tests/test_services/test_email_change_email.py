"""The email-change message (SPEC_EMAIL_CHANGE).

The link is a frontend path. A backend route outside ``/api/`` serves the SPA,
and a GET that applied the change would let a mail scanner do it first.
"""

import ast
from pathlib import Path
from unittest.mock import MagicMock, patch

from datanika.services.email_service import EmailService
from datanika.tasks import email_tasks


def _function_body(name: str) -> str:
    source = Path(email_tasks.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == name:
            return ast.get_source_segment(source, node) or ""
    raise AssertionError(f"{name} not found")


def _service() -> EmailService:
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


def _sent(call) -> str:
    with patch("datanika.services.email_service.smtplib.SMTP") as smtp_cls:
        smtp = MagicMock()
        smtp_cls.return_value.__enter__ = MagicMock(return_value=smtp)
        smtp_cls.return_value.__exit__ = MagicMock(return_value=False)
        assert call() is True
        return smtp.sendmail.call_args[0][2]


def test_the_link_is_a_frontend_path():
    body = _sent(lambda: _service().send_email_change_email("a@b.com", "TOKEN123"))
    assert "https://app.datanika.io/confirm-email-change?token=TOKEN123" in body
    assert "/api/" not in body
    assert "text/plain" in body
    assert "24 hours" in body


def test_the_task_retries_and_does_not_log_the_token():
    from datanika.tasks.email_tasks import send_email_change_email_task

    assert send_email_change_email_task.name == "datanika.send_email_change_email"
    assert send_email_change_email_task.max_retries == 3
    assert send_email_change_email_task.retry_backoff == 30
    assert send_email_change_email_task.retry_backoff_max == 300
    body = _function_body("send_email_change_email_task")
    assert "logger" not in body
