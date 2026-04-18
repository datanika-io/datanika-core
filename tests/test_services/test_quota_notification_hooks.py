"""Tests for quota_notification_hooks (closes core#240 + cloud#43).

The handler subscribes to ``quota.warning_threshold_reached``, creates
an in-app ``Notification(type=QUOTA_WARNING, ...)`` row, and dispatches
to the org's configured external channels via ``NotificationService``.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SASession
from sqlalchemy.pool import StaticPool

import datanika.models.invitation  # noqa: F401
import datanika.models.notification  # noqa: F401
import datanika.models.notification_channel  # noqa: F401
import datanika.models.sso_config  # noqa: F401
from datanika.models.base import Base
from datanika.models.notification import NotificationType
from datanika.models.notification_channel import ChannelType, NotificationChannel
from datanika.models.user import Organization
from datanika.services.in_app_notification_service import InAppNotificationService

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def engine():
    eng = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def db_session(engine):
    session = SASession(engine)
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def org(db_session):
    o = Organization(name="Acme", slug="acme-quota-notif")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def svc():
    return InAppNotificationService()


@pytest.fixture
def patched_session(db_session):
    """Patch get_sync_session so the handler picks up the test's session.

    The handler calls ``get_sync_session()`` to get a plain Session;
    tests substitute the in-memory session. ``close()`` is patched to
    no-op so subsequent assertions against ``db_session`` still see the
    written rows.
    """
    with (
        patch(
            "datanika.services.quota_notification_hooks.get_sync_session",
            return_value=db_session,
        ),
        patch.object(db_session, "close"),
        patch.object(db_session, "commit"),
    ):
        yield db_session


# ---------------------------------------------------------------------------
# Handler tests
# ---------------------------------------------------------------------------


class TestOnQuotaWarningHandler:
    def test_creates_in_app_notification(self, patched_session, org, svc):
        """Handler writes a Notification(type=QUOTA_WARNING) row."""
        from datanika.services.quota_notification_hooks import _on_quota_warning

        _on_quota_warning(
            org_id=org.id,
            metric="model_runs",
            used=400,
            limit=500,
            plan_name="Pro",
        )
        patched_session.flush()

        items = svc.list_for_user(patched_session, org.id, user_id=1)
        assert len(items) == 1
        assert items[0].type == NotificationType.QUOTA_WARNING
        assert items[0].title == "Quota warning: 80% of model runs used"
        assert "400" in items[0].message
        assert "500" in items[0].message
        assert "Pro" in items[0].message
        assert items[0].resource_type == "quota"

    @pytest.mark.parametrize(
        "metric,expected_label",
        [
            ("model_runs", "model runs"),
            ("bytes_processed", "GB processed"),
            ("connections", "connections"),
            ("schedules", "schedules"),
            ("seats", "seats"),
        ],
    )
    def test_metric_label_in_title_and_message(
        self, patched_session, org, svc, metric, expected_label
    ):
        """Each supported metric resolves to a human-readable label."""
        from datanika.services.quota_notification_hooks import _on_quota_warning

        _on_quota_warning(
            org_id=org.id,
            metric=metric,
            used=80,
            limit=100,
            plan_name="Pro",
        )
        patched_session.flush()
        item = svc.list_for_user(patched_session, org.id, user_id=1)[0]
        assert expected_label in item.title
        assert expected_label in item.message

    def test_unknown_metric_falls_back_to_raw(self, patched_session, org, svc):
        """Unknown metric passes through as-is instead of crashing."""
        from datanika.services.quota_notification_hooks import _on_quota_warning

        _on_quota_warning(
            org_id=org.id,
            metric="future_metric",
            used=80,
            limit=100,
            plan_name="Pro",
        )
        patched_session.flush()
        item = svc.list_for_user(patched_session, org.id, user_id=1)[0]
        assert "future_metric" in item.title

    def test_dispatches_to_subscribed_external_channels(self, patched_session, org, svc):
        """Active channels subscribed to 'quota_warning' receive dispatch."""
        from datanika.services.quota_notification_hooks import _on_quota_warning

        ch = NotificationChannel(
            org_id=org.id,
            name="ops-slack",
            channel_type=ChannelType.SLACK,
            config={"webhook_url": "https://hooks.slack.com/services/TEST"},
            events=["quota_warning"],
            is_active=True,
        )
        patched_session.add(ch)
        patched_session.flush()

        with patch("httpx.post") as mock_post:
            _on_quota_warning(
                org_id=org.id,
                metric="model_runs",
                used=400,
                limit=500,
                plan_name="Pro",
            )

        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "https://hooks.slack.com/services/TEST"
        slack_text = kwargs["json"]["text"]
        assert "quota warning" in slack_text.lower()
        assert "400" in slack_text
        assert "500" in slack_text

    def test_skips_channels_not_subscribed_to_quota_warning(self, patched_session, org, svc):
        """A Slack channel subscribed only to run_failure doesn't receive."""
        from datanika.services.quota_notification_hooks import _on_quota_warning

        ch = NotificationChannel(
            org_id=org.id,
            name="runs-only",
            channel_type=ChannelType.SLACK,
            config={"webhook_url": "https://hooks.slack.com/services/RUNS"},
            events=["run_failure"],  # NOT quota_warning
            is_active=True,
        )
        patched_session.add(ch)
        patched_session.flush()

        with patch("httpx.post") as mock_post:
            _on_quota_warning(
                org_id=org.id,
                metric="model_runs",
                used=400,
                limit=500,
                plan_name="Pro",
            )

        mock_post.assert_not_called()


class TestRegisterQuotaNotificationHooks:
    def test_subscribes_to_warning_event(self):
        """register_quota_notification_hooks binds _on_quota_warning."""
        import datanika.hooks as hooks_module
        from datanika.services.quota_notification_hooks import (
            _on_quota_warning,
            register_quota_notification_hooks,
        )

        # Save/restore state to keep the module-global hook registry clean
        saved = hooks_module._handlers.copy()
        hooks_module._handlers.clear()
        try:
            register_quota_notification_hooks()
            handlers = hooks_module._handlers.get("quota.warning_threshold_reached", [])
            assert _on_quota_warning in handlers
        finally:
            hooks_module._handlers = saved


class TestNotificationServiceQuotaEvent:
    """NotificationService now accepts 'quota_warning' as a valid event
    and renders appropriate dispatch bodies for each channel type."""

    def test_quota_warning_accepted_by_validator(self):
        from datanika.services.notification_service import VALID_EVENTS

        assert "quota_warning" in VALID_EVENTS

    def test_create_channel_accepts_quota_warning(self, db_session, org):
        from datanika.services.notification_service import NotificationService

        svc = NotificationService()
        ch = svc.create_channel(
            db_session,
            org_id=org.id,
            name="billing-slack",
            channel_type=ChannelType.SLACK,
            config={"webhook_url": "https://hooks.slack.com/services/BILL"},
            events=["quota_warning"],
        )
        assert ch.id is not None
        assert "quota_warning" in ch.events

    def test_slack_dispatch_formats_quota_payload(self):
        """Slack body uses the quota-specific template."""
        from datanika.services.notification_service import _build_quota_warning_slack_text

        text = _build_quota_warning_slack_text(
            {
                "metric_label": "GB processed",
                "used": 85,
                "limit": 100,
                "plan_name": "Pro",
                "pct": 85.0,
            }
        )
        assert "85" in text
        assert "100" in text
        assert "GB processed" in text
        assert "Pro" in text
        assert "quota warning" in text.lower()

    def test_telegram_dispatch_formats_quota_payload(self):
        from datanika.services.notification_service import (
            _build_quota_warning_telegram_text,
        )

        text = _build_quota_warning_telegram_text(
            {
                "metric_label": "connections",
                "used": 20,
                "limit": 25,
                "plan_name": "Pro",
                "pct": 80.0,
            }
        )
        assert "20" in text
        assert "25" in text
        assert "connections" in text

    def test_email_dispatch_formats_quota_payload(self):
        from datanika.services.notification_service import _build_quota_warning_email

        subject, body = _build_quota_warning_email(
            {
                "metric_label": "seats",
                "used": 4,
                "limit": 5,
                "plan_name": "Pro",
                "pct": 80.0,
            }
        )
        assert "seats" in subject
        assert "4" in body
        assert "5" in body
        assert "Pro" in body
