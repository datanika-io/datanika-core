"""Notification delivery must be observable (core#652, SPEC_NOTIFICATION_DELIVERY).

The issue is titled *"`notify()` is never passed an `email_service`"* and wiring that
argument closes **one of six** independent silences. Product measured the other five;
this module is the executable form of the spec's §3 acceptance criteria.

Each test here names the silence it holds down, because five of the six are invisible
in the success path and a reviewer reading the diff cannot tell which line matters:

* **Silence 1** — `_dispatch_email` returned at its first guard, always. Nothing was
  ever attempted, on any org, in any edition.
* **Silence 2** — the log line blamed "Email service disabled" for a condition that
  was a missing argument, sending every reader to check SMTP config.
* **Silence 3** — `notify()`'s bare `except Exception` swallowed every failure below
  it, for **all four** channel types.
* **Silence 4** — `email_service.send(...)`'s bool was discarded.
* **Silence 5** — zero `raise_for_status()` in the module, so a 401 from Slack, a 404
  webhook and a revoked Telegram token all read as success.
* **Silence 6** — no delivery record of any kind on `notification_channels`.

🚨 **The test that matters is the one that fakes the socket, not the mailer.** A test
asserting `notify()` was called already exists and already passes; it passed
throughout the entire period in which no email was ever sent.
"""

from unittest.mock import MagicMock, patch

import httpx
import pytest

from datanika.models.notification_channel import ChannelType, NotificationChannel
from datanika.models.user import Organization
from datanika.services.notification_service import NotificationService

WEBHOOK_SECRET = "s3cr3t-tok3n-do-not-leak"


def _code_only(func) -> str:
    """Source of ``func`` with comments and docstrings removed.

    🚨 A static guard over code that also *documents itself* must exclude the
    documentation, or the prose explaining a fix satisfies the test for the fix.
    That is not hypothetical: three guards shipped green this way in this repo,
    each because the comment describing the change quoted the string the
    assertion was grepping for. ``_delivery_badge``'s own docstring quotes the
    literal these tests reject.

    ``ast.unparse`` drops comments as a side effect of round-tripping; the
    docstring has to be popped explicitly.
    """
    import ast
    import inspect
    import textwrap

    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(body, list) or not body:
            continue
        first = body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            body.pop(0)
    return ast.unparse(tree)


def _badge_string_literals(func) -> list[str]:
    """Every ``rx.badge(...)`` label passed as a bare string literal."""
    import ast
    import inspect
    import textwrap

    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        target = node.func
        name = target.attr if isinstance(target, ast.Attribute) else getattr(target, "id", "")
        if name != "badge" or not node.args:
            continue
        first = node.args[0]
        if isinstance(first, ast.Constant) and isinstance(first.value, str):
            found.append(first.value)
    return found


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-notify-delivery")
    db_session.add(org)
    db_session.flush()
    return org


def _channel(db_session, org, channel_type, config, events=None):
    ch = NotificationChannel(
        org_id=org.id,
        name=f"{channel_type.value} channel",
        channel_type=channel_type,
        config=config,
        events=events or ["run_failure", "run_success", "quota_warning", "charge_incoming"],
        is_active=True,
    )
    db_session.add(ch)
    db_session.flush()
    return ch


@pytest.fixture
def email_channel(db_session, org):
    return _channel(db_session, org, ChannelType.EMAIL, {"email": "ops@example.com"})


@pytest.fixture
def webhook_channel(db_session, org):
    return _channel(
        db_session, org, ChannelType.WEBHOOK, {"url": f"https://hooks.example/{WEBHOOK_SECRET}"}
    )


@pytest.fixture
def smtp_configured(monkeypatch):
    """A relay that exists, so the *configuration* branch is not what we measure."""
    from datanika.config import settings

    monkeypatch.setattr(settings, "smtp_host", "smtp.example.com", raising=False)
    monkeypatch.setattr(settings, "smtp_port", 587, raising=False)
    monkeypatch.setattr(settings, "smtp_user", "", raising=False)
    monkeypatch.setattr(settings, "smtp_password", "", raising=False)
    monkeypatch.setattr(settings, "smtp_from_email", "noreply@datanika.io", raising=False)
    monkeypatch.setattr(settings, "smtp_from_name", "Datanika", raising=False)
    monkeypatch.setattr(settings, "smtp_use_tls", False, raising=False)
    return settings


@pytest.fixture
def eager_celery(monkeypatch, db_session):
    """Run enqueued tasks inline, and give them the test's own session.

    Without this the assertion degrades to "we called `.delay`", which is the
    class of test that let this ship: it proves an intent, not a delivery.
    """
    from datanika.tasks.celery_app import celery_app

    monkeypatch.setattr(celery_app.conf, "task_always_eager", True, raising=False)
    monkeypatch.setattr(celery_app.conf, "task_eager_propagates", False, raising=False)

    class _NonClosingSession:
        """`with get_sync_session() as s` must not close the test's session."""

        def __init__(self, session):
            self._session = session

        def __enter__(self):
            return self._session

        def __exit__(self, *_exc):
            return False

    # The task commits its own session, which is right in production and wrong
    # here: `db_session` joins an outer transaction the fixture rolls back, and a
    # real commit ends that transaction so the rollback silently does nothing.
    # Left alone this leaks rows *between tests* — it first showed up as a
    # UNIQUE violation on `organizations.slug` in an unrelated test's setup.
    monkeypatch.setattr(db_session, "commit", db_session.flush)
    monkeypatch.setattr(
        "datanika.db.get_sync_session", lambda: _NonClosingSession(db_session), raising=False
    )
    return celery_app


@pytest.fixture
def fake_socket():
    """The mail transport itself. `EmailService` does `with smtplib.SMTP(...)`."""
    with patch("smtplib.SMTP") as smtp:
        smtp.return_value.__enter__.return_value = MagicMock()
        yield smtp


class TestAC1TheMailTransportIsActuallyReached:
    """AC1 — a `run.failed` hook results in a mail transport call.

    Red against `dev`: `_dispatch_email` returns at `if email_service is None`
    before building anything, so `smtplib.SMTP` is never constructed.
    """

    def test_a_failed_run_dials_smtp(
        self, db_session, org, email_channel, smtp_configured, eager_celery, fake_socket
    ):
        NotificationService().notify(
            db_session,
            org.id,
            "run_failure",
            {"run_id": 42, "status": "failed", "error_message": "boom"},
        )

        assert fake_socket.called, (
            "no SMTP connection was opened — the email channel is configured, active "
            "and subscribed to run_failure, and nothing reached the transport"
        )

    def test_the_recipient_is_the_configured_address(
        self, db_session, org, email_channel, smtp_configured, eager_celery, fake_socket
    ):
        NotificationService().notify(
            db_session, org.id, "run_failure", {"run_id": 42, "status": "failed"}
        )

        server = fake_socket.return_value.__enter__.return_value
        assert server.sendmail.called, "connected but sent nothing"
        _from, to, body = server.sendmail.call_args.args
        assert to == "ops@example.com"
        assert "42" in body


class TestAC2UnconfiguredSMTPIsItsOwnSignal:
    """AC2 — records `skipped` naming SMTP configuration, distinctly.

    Silence 2: the old log line said "Email service disabled" for a condition
    that was a missing function argument. Configuration failure and transport
    failure are different facts and must stay different.
    """

    def test_no_relay_records_skipped(
        self, db_session, org, email_channel, eager_celery, monkeypatch
    ):
        from datanika.config import settings

        monkeypatch.setattr(settings, "smtp_host", "", raising=False)

        NotificationService().notify(
            db_session, org.id, "run_failure", {"run_id": 1, "status": "failed"}
        )

        db_session.refresh(email_channel)
        assert email_channel.last_status == "skipped"
        assert email_channel.last_attempt_at is not None
        assert "smtp" in (email_channel.last_error or "").lower(), (
            "the reason must name SMTP configuration — a reader who is told "
            "'disabled' goes and checks the relay, which is working"
        )

    def test_no_relay_does_not_read_as_a_transport_failure(
        self, db_session, org, email_channel, eager_celery, monkeypatch
    ):
        from datanika.config import settings

        monkeypatch.setattr(settings, "smtp_host", "", raising=False)

        NotificationService().notify(
            db_session, org.id, "run_failure", {"run_id": 1, "status": "failed"}
        )

        db_session.refresh(email_channel)
        assert email_channel.last_status != "failed", (
            "an unconfigured relay is a normal self-hosted deployment, not a fault"
        )


class TestAC3AllFourEventTypesDeliver:
    """AC3 — run_success, run_failure, quota_warning, charge_incoming."""

    @pytest.mark.parametrize(
        ("event", "payload"),
        [
            ("run_failure", {"run_id": 1, "status": "failed"}),
            ("run_success", {"run_id": 2, "status": "succeeded"}),
            (
                "quota_warning",
                {"metric_label": "runs", "used": 8, "limit": 10, "plan_name": "Pro", "pct": 80},
            ),
            (
                "charge_incoming",
                {"amount_display": "$4.20", "gb_display": "8.4", "cycle_ends_at": "2026-10-01"},
            ),
        ],
    )
    def test_event_delivers(
        self,
        db_session,
        org,
        email_channel,
        smtp_configured,
        eager_celery,
        fake_socket,
        event,
        payload,
    ):
        NotificationService().notify(db_session, org.id, event, payload)

        assert fake_socket.called, f"{event} reached no transport"
        db_session.refresh(email_channel)
        assert email_channel.last_status == "success"


class TestAC4NonSuccessResponsesAreRecorded:
    """AC4 — success path unchanged; a non-2xx now records `failed`.

    Silence 5: `httpx.post(...)` with no `raise_for_status()` anywhere in the
    module, so every one of these returned a response object and carried on.
    """

    def test_webhook_success_path_is_unchanged(self, db_session, org, webhook_channel):
        with patch("httpx.post") as post:
            post.return_value = httpx.Response(200, request=httpx.Request("POST", "https://h/"))
            NotificationService().notify(
                db_session, org.id, "run_failure", {"run_id": 5, "status": "failed"}
            )

        assert post.called
        url = post.call_args.args[0] if post.call_args.args else post.call_args.kwargs["url"]
        assert url == f"https://hooks.example/{WEBHOOK_SECRET}"
        body = post.call_args.kwargs["json"]
        assert body["event"] == "run_failure"
        assert body["run_id"] == 5
        db_session.refresh(webhook_channel)
        assert webhook_channel.last_status == "success"

    def test_a_500_records_failed(self, db_session, org, webhook_channel):
        with patch("httpx.post") as post:
            post.return_value = httpx.Response(
                500, request=httpx.Request("POST", f"https://hooks.example/{WEBHOOK_SECRET}")
            )
            NotificationService().notify(
                db_session, org.id, "run_failure", {"run_id": 5, "status": "failed"}
            )

        db_session.refresh(webhook_channel)
        assert webhook_channel.last_status == "failed", (
            "a 500 from the webhook endpoint was indistinguishable from delivery"
        )
        assert webhook_channel.last_error

    @pytest.mark.parametrize(
        ("channel_type", "config"),
        [
            (ChannelType.SLACK, {"webhook_url": "https://hooks.slack.com/services/AAA"}),
            (ChannelType.TELEGRAM, {"token": "bot-token-123", "chat_id": "42"}),
        ],
    )
    def test_slack_and_telegram_also_record_failure(self, db_session, org, channel_type, config):
        ch = _channel(db_session, org, channel_type, config)
        with patch("httpx.post") as post:
            post.return_value = httpx.Response(
                401, request=httpx.Request("POST", "https://example/")
            )
            NotificationService().notify(
                db_session, org.id, "run_failure", {"run_id": 5, "status": "failed"}
            )

        db_session.refresh(ch)
        assert ch.last_status == "failed"

    def test_a_transport_exception_records_failed(self, db_session, org, webhook_channel):
        """Silence 3 — `notify()`'s bare `except Exception` swallowed this."""
        with patch("httpx.post", side_effect=httpx.ConnectError("no route")):
            NotificationService().notify(
                db_session, org.id, "run_failure", {"run_id": 5, "status": "failed"}
            )

        db_session.refresh(webhook_channel)
        assert webhook_channel.last_status == "failed"

    def test_one_broken_channel_does_not_stop_the_others(self, db_session, org, webhook_channel):
        """The `except` may catch — D4 says it must record, not that it must stop."""
        slack = _channel(
            db_session, org, ChannelType.SLACK, {"webhook_url": "https://hooks.slack.com/x"}
        )

        def _responses(url, **_kw):
            if "hooks.example" in url:
                raise httpx.ConnectError("no route")
            return httpx.Response(200, request=httpx.Request("POST", url))

        with patch("httpx.post", side_effect=_responses):
            NotificationService().notify(
                db_session, org.id, "run_failure", {"run_id": 5, "status": "failed"}
            )

        db_session.refresh(webhook_channel)
        db_session.refresh(slack)
        assert webhook_channel.last_status == "failed"
        assert slack.last_status == "success", "a dead webhook silenced a healthy Slack channel"


class TestAC7TheErrorRecordLeaksNoCredential:
    """AC7 — `last_error` carries nothing from `channel.config`.

    A webhook URL and a Telegram bot token are credentials, and an HTTP error
    string routinely echoes the request URL — `httpx` puts it in the message of
    every `HTTPStatusError` it raises.
    """

    def test_the_webhook_url_is_absent_from_the_stored_error(
        self, db_session, org, webhook_channel
    ):
        with patch("httpx.post") as post:
            post.return_value = httpx.Response(
                500, request=httpx.Request("POST", f"https://hooks.example/{WEBHOOK_SECRET}")
            )
            NotificationService().notify(
                db_session, org.id, "run_failure", {"run_id": 5, "status": "failed"}
            )

        db_session.refresh(webhook_channel)
        assert webhook_channel.last_error
        assert WEBHOOK_SECRET not in webhook_channel.last_error, (
            "the stored error echoed the webhook URL, which is a credential"
        )

    def test_a_telegram_token_is_absent_from_the_stored_error(self, db_session, org):
        token = "1234567:AAH-very-secret-bot-token"
        ch = _channel(db_session, org, ChannelType.TELEGRAM, {"token": token, "chat_id": "42"})

        with patch("httpx.post") as post:
            post.return_value = httpx.Response(
                401,
                request=httpx.Request("POST", f"https://api.telegram.org/bot{token}/sendMessage"),
            )
            NotificationService().notify(
                db_session, org.id, "run_failure", {"run_id": 5, "status": "failed"}
            )

        db_session.refresh(ch)
        assert token not in (ch.last_error or "")

    def test_the_recipient_address_is_absent_from_the_stored_error(
        self, db_session, org, email_channel, smtp_configured, eager_celery
    ):
        # RuntimeError deliberately: it is **not** in the task's `autoretry_for`,
        # so this measures one attempt's record rather than the end of a retry
        # ladder. The retry behaviour is a separate assertion, below.
        with patch("smtplib.SMTP", side_effect=RuntimeError("relay refused ops@example.com")):
            NotificationService().notify(
                db_session, org.id, "run_failure", {"run_id": 5, "status": "failed"}
            )

        db_session.refresh(email_channel)
        assert email_channel.last_status == "failed"
        assert "ops@example.com" not in (email_channel.last_error or "")

    def test_the_error_is_bounded(self, db_session, org, webhook_channel):
        """An unbounded error string is a second way to store a payload."""
        with patch("httpx.post", side_effect=httpx.ConnectError("x" * 5000)):
            NotificationService().notify(
                db_session, org.id, "run_failure", {"run_id": 5, "status": "failed"}
            )

        db_session.refresh(webhook_channel)
        assert len(webhook_channel.last_error) <= 500


class TestAC6AChannelThatNeverDeliveredIsNotGreen:
    """AC6 — `is_active` answers "switched on?"; the user asks "working?"."""

    def test_a_fresh_channel_has_no_delivery_record(self, db_session, org, email_channel):
        assert email_channel.last_status is None
        assert email_channel.last_attempt_at is None

    def test_the_ui_row_carries_delivery_state(self):
        """The badge is the assertion the user reads. It must have the input."""
        from datanika.ui.state.notification_state import ChannelItem

        item = ChannelItem()
        assert hasattr(item, "last_status"), (
            "ChannelItem cannot render delivery state it does not carry"
        )

    def test_the_badge_reads_delivery_state(self):
        from datanika.ui.pages import settings as settings_page

        assert "last_status" in _code_only(settings_page._delivery_badge), (
            "the badge still renders is_active alone — a green affirmative beside "
            "a channel that has never delivered is the defect this issue is about"
        )

    def test_the_row_renders_the_delivery_badge(self):
        from datanika.ui.pages import settings as settings_page

        assert "_delivery_badge" in _code_only(settings_page.channel_row)

    def test_no_badge_label_is_a_bare_english_literal(self):
        """SPEC §4 — badge labels are on the translate list, and these are new.

        Structural rather than textual on purpose: this is the assertion that
        `rx.badge("On", color_scheme="green")` fails and `rx.badge(_t[...])`
        passes, and no amount of prose about the old literal can satisfy it.
        """
        from datanika.ui.pages import settings as settings_page

        literals = _badge_string_literals(settings_page._delivery_badge)
        literals += _badge_string_literals(settings_page.channel_row)
        assert literals == [], (
            f"untranslated badge label(s) {literals!r} — every user-visible badge "
            "label needs a key in all nine locale files"
        )

    def test_the_literal_extractor_can_actually_see_a_literal(self):
        """Negative control for the guard above.

        Without this, `_badge_string_literals` returning `[]` because it is
        broken is indistinguishable from it returning `[]` because the code is
        correct — which is the failure mode that made two of my own guards pass
        against unfixed code.
        """

        def _offender():
            import reflex as rx

            return rx.badge("On", color_scheme="green")

        assert _badge_string_literals(_offender) == ["On"]


class TestSilence1TheDefectiveSeamIsGone:
    """D1 — a parameter no caller passes is not a seam, it is a defect that compiles."""

    def test_notify_no_longer_takes_an_email_service(self):
        import inspect

        params = inspect.signature(NotificationService.notify).parameters
        assert "email_service" not in params, (
            "the argument nobody ever passed is still there; wiring or keeping it "
            "leaves the same defect one caller away"
        )


class TestTheQuotaWarningTaskKeepsItsCloudCaller:
    """🚨 The spec's D5 says this task has zero callers. It has one, in datanika-cloud.

    `datanika_cloud/billing/meter.py` imports it and calls `.delay(...)`. That grep
    was run over `datanika/` alone, and cloud is a separate repository, so a
    core-only search cannot see it. Deleting the task would raise `ImportError`
    inside meter's own `except Exception` — replacing a silent email failure with
    a differently-silent one, in the change whose whole purpose is to end them.
    """

    def test_the_task_still_exists(self):
        from datanika.tasks import email_tasks

        assert hasattr(email_tasks, "send_quota_warning_email_task")

    def test_it_retries_transient_relay_failures(self):
        """D5's surviving half: it carried `raise_on_error=True` and no
        `autoretry_for`, so it raised and did not retry — the worst pairing."""
        from datanika.tasks.email_tasks import send_quota_warning_email_task

        retry_for = send_quota_warning_email_task.autoretry_for
        assert retry_for, "raises on failure and never retries"
        assert OSError in retry_for


class TestAnUnreadableStatusIsNotAFailure:
    """A response we cannot judge must not be recorded as a failed delivery.

    Real `httpx` always carries an int `status_code`. Several **existing** tests
    in this repo patch `httpx.post` with a bare `MagicMock`, whose auto-created
    `.status_code` answers comparisons with `NotImplemented` — so a naive
    `status_code >= 400` raises `TypeError`, which `notify`'s `except` would then
    dutifully record as a *delivery failure*.

    That is the failure mode this whole issue is about, arriving from the other
    direction: the first version silently swallowed real failures, and the
    obvious fix invents fake ones. Unmeasurable is not the same as failed.
    """

    def test_a_mock_shaped_response_is_not_recorded_as_failed(
        self, db_session, org, webhook_channel
    ):
        with patch("httpx.post") as post:
            post.return_value = MagicMock()  # no status_code configured
            NotificationService().notify(
                db_session, org.id, "run_failure", {"run_id": 5, "status": "failed"}
            )

        db_session.refresh(webhook_channel)
        assert webhook_channel.last_status != "failed", (
            "an unreadable status was reported as a delivery failure — the user "
            "would see a red badge on a channel that is working"
        )

    def test_a_real_int_status_is_still_judged(self, db_session, org, webhook_channel):
        """Positive control: the tolerance above must not disarm the check.

        Without this, `isinstance(status_code, int)` returning False for
        *everything* would pass the test above and silently restore Silence 5.
        """
        with patch("httpx.post") as post:
            post.return_value = httpx.Response(
                503, request=httpx.Request("POST", "https://hooks.example/x")
            )
            NotificationService().notify(
                db_session, org.id, "run_failure", {"run_id": 5, "status": "failed"}
            )

        db_session.refresh(webhook_channel)
        assert webhook_channel.last_status == "failed"
