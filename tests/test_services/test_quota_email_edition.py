"""The quota-warning email must not link a page the deployment does not serve.

`send_quota_warning_email_task` is a registered Celery task in **core**, so it is
invokable by name on any deployment. `EmailService.send_quota_warning_email` built
`{frontend_url}{BILLING_ROUTE}` unconditionally, and the billing page is registered
by the **cloud plugin** — so a self-hosted instance emails an "Upgrade Plan" button
pointing at a route nothing serves.

The other three upgrade CTAs are safe in core, but incidentally rather than
structurally: two gate on `is_quota_error`, which compares a class name that does
not exist in core, and the dashboard one is behind `has_usage_data`, which is only
ever written by the cloud usage hook. This one had neither a gate nor a render
condition.

Also closes #682 §3: the previous email test formatted the template with a URL
**it built itself** and never executed `email_service.py`'s own construction, so
the line the fix touches was never run.
"""

import pytest

from datanika.plugin_registry import BILLING_ROUTE
from datanika.services.email_service import EmailService


@pytest.fixture
def captured():
    """An EmailService whose `send` records instead of dialling SMTP.

    Deliberately captures the real arguments `send_quota_warning_email` produces
    rather than re-formatting the template: the construction under test is the
    thing that was wrong, so a test that rebuilds the URL tests nothing.
    """
    calls = []
    svc = EmailService(
        smtp_host="smtp.example.com",
        smtp_port=25,
        smtp_user="",
        smtp_password="",
        smtp_from_email="a@b.c",
        smtp_from_name="Datanika",
        smtp_use_tls=False,
        frontend_url="https://app.datanika.io/",
    )
    svc.send = lambda to, subject, html_body, text_body=None: (
        calls.append({"to": to, "subject": subject, "html": html_body}) or True
    )
    return svc, calls


class TestCoreEditionLinksNothing:
    def test_no_billing_url_when_billing_is_not_registered(self, captured):
        svc, calls = captured
        svc.send_quota_warning_email(
            "owner@example.com", "Pro", "runs", 9, 10, billing_enabled=False
        )
        assert len(calls) == 1, "the email must still be sent — only the CTA changes"
        html = calls[0]["html"]
        assert BILLING_ROUTE not in html, (
            "a core-edition deployment does not serve the billing page, so a link "
            "to it is a dead end sent from our own address"
        )

    def test_the_email_still_says_what_happened(self, captured):
        """Dropping the CTA must not drop the message. The recipient still needs
        to know they are near a limit, whatever they can do about it."""
        svc, calls = captured
        svc.send_quota_warning_email(
            "owner@example.com", "Pro", "runs", 9, 10, billing_enabled=False
        )
        html = calls[0]["html"]
        assert "90%" in html
        assert "runs" in html
        assert "Pro" in html

    def test_no_dangling_anchor_is_left_behind(self, captured):
        """Removing the href but leaving the button would be worse than the bug."""
        svc, calls = captured
        svc.send_quota_warning_email(
            "owner@example.com", "Pro", "runs", 9, 10, billing_enabled=False
        )
        html = calls[0]["html"]
        assert "Upgrade Plan" not in html
        assert 'href=""' not in html
        assert "{upgrade_url}" not in html, "an unsubstituted placeholder reached the recipient"


class TestCloudEditionStillLinksBilling:
    def test_the_service_builds_the_url_itself(self, captured):
        """The line under test is `email_service.py`'s own f-string. This test
        reads what the service produced; it does not rebuild it."""
        svc, calls = captured
        svc.send_quota_warning_email(
            "owner@example.com", "Pro", "runs", 9, 10, billing_enabled=True
        )
        html = calls[0]["html"]
        assert f"https://app.datanika.io{BILLING_ROUTE}" in html
        assert "Upgrade Plan" in html


class TestTheGateDefaultsToTheEdition:
    def test_core_edition_settings_produce_no_link(self, captured, monkeypatch):
        """`billing_enabled` is passed in for testability, but the caller that
        matters — the Celery task — passes nothing, so the default has to be
        right on its own."""
        import datanika.services.email_service as email_service_module

        svc, calls = captured
        monkeypatch.setattr(email_service_module.settings, "datanika_edition", "core")
        svc.send_quota_warning_email("owner@example.com", "Pro", "runs", 9, 10)
        assert BILLING_ROUTE not in calls[0]["html"]

    def test_cloud_edition_settings_produce_a_link(self, captured, monkeypatch):
        import datanika.services.email_service as email_service_module

        svc, calls = captured
        monkeypatch.setattr(email_service_module.settings, "datanika_edition", "cloud")
        svc.send_quota_warning_email("owner@example.com", "Pro", "runs", 9, 10)
        assert BILLING_ROUTE in calls[0]["html"]
