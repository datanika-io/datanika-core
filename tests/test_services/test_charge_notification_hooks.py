"""Tests for V2 P5 Option B charge-event subscribers (core#249).

The hooks create in-app ``Notification`` rows + dispatch to external
channels via ``NotificationService``. Subscriber wiring is pure — if
the cloud plugin never emits these events, the core tree is inert.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from datanika import hooks
from datanika.models.notification import NotificationType
from datanika.services.charge_notification_hooks import (
    _format_money,
    _on_charge_failed,
    _on_charge_incoming,
    _on_charge_issued,
    register_charge_notification_hooks,
)


class TestFormatMoney:
    def test_usd_default_symbol(self):
        assert _format_money(1299) == "$12.99"

    def test_exact_dollar(self):
        assert _format_money(5000) == "$50.00"

    def test_zero(self):
        assert _format_money(0) == "$0.00"

    def test_non_usd_falls_back_to_code(self):
        assert _format_money(1299, "EUR") == "EUR 12.99"


class _FakeNotificationSvc:
    """In-memory stand-in for ``NotificationService.notify`` and
    ``InAppNotificationService.create``. Each call appends to ``records``.
    """

    def __init__(self):
        self.in_app: list[dict] = []
        self.external: list[dict] = []

    def create(self, session, org_id, notif_type, *, title, resource_type, resource_id, message):
        self.in_app.append(
            {
                "org_id": org_id,
                "type": notif_type,
                "title": title,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "message": message,
            }
        )

    def notify(self, session, org_id, event, payload):
        self.external.append({"org_id": org_id, "event": event, "payload": payload})


@pytest.fixture
def fake_session():
    sess = MagicMock()
    sess.commit = MagicMock()
    sess.rollback = MagicMock()
    sess.close = MagicMock()
    return sess


@pytest.fixture
def fake_svc(monkeypatch):
    svc = _FakeNotificationSvc()
    # Replace both the module-level in-app service and the ad-hoc
    # NotificationService instantiation inside _dispatch.
    import datanika.services.charge_notification_hooks as m

    monkeypatch.setattr(m, "_svc", svc)
    monkeypatch.setattr(
        "datanika.services.notification_service.NotificationService",
        lambda: svc,
    )
    return svc


@pytest.fixture
def fake_session_context(fake_session, monkeypatch):
    """Patch ``get_sync_session`` in the hooks module so the decorator
    opens the mock session instead of a real DB.
    """
    monkeypatch.setattr(
        "datanika.services.charge_notification_hooks.get_sync_session",
        lambda: fake_session,
    )
    return fake_session


class TestChargeIncoming:
    def test_creates_in_app_notification(self, fake_session_context, fake_svc):
        _on_charge_incoming(
            org_id=42,
            subscription_id=7,
            amount_cents=1500,
            currency="USD",
            metric="bytes_processed",
        )
        assert len(fake_svc.in_app) == 1
        row = fake_svc.in_app[0]
        assert row["org_id"] == 42
        assert row["type"] == NotificationType.CHARGE_INCOMING
        assert "$15.00" in row["title"]
        assert row["resource_type"] == "subscription"
        assert row["resource_id"] == 7

    def test_dispatches_to_external_channels(self, fake_session_context, fake_svc):
        _on_charge_incoming(
            org_id=42,
            subscription_id=7,
            amount_cents=1500,
            currency="USD",
            metric="bytes_processed",
        )
        assert len(fake_svc.external) == 1
        call = fake_svc.external[0]
        assert call["event"] == "charge_incoming"
        assert call["payload"]["amount_cents"] == 1500
        assert call["payload"]["metric"] == "bytes_processed"

    def test_commit_called_on_success(self, fake_session_context, fake_svc, fake_session):
        _on_charge_incoming(org_id=42, subscription_id=7, amount_cents=1500)
        fake_session.commit.assert_called_once()
        fake_session.rollback.assert_not_called()


class TestChargeIssued:
    def test_creates_notification_with_charge_id(self, fake_session_context, fake_svc):
        _on_charge_issued(
            org_id=42,
            subscription_id=7,
            charge_id=99,
            amount_cents=2500,
            currency="USD",
            metric="bytes_processed",
        )
        assert fake_svc.in_app[0]["type"] == NotificationType.CHARGE_ISSUED
        assert fake_svc.in_app[0]["resource_type"] == "charge"
        assert fake_svc.in_app[0]["resource_id"] == 99

    def test_external_payload_includes_charge_id(self, fake_session_context, fake_svc):
        _on_charge_issued(org_id=42, subscription_id=7, charge_id=99, amount_cents=2500)
        assert fake_svc.external[0]["payload"]["charge_id"] == 99


class TestChargeFailed:
    def test_creates_failed_notification(self, fake_session_context, fake_svc):
        _on_charge_failed(
            org_id=42,
            subscription_id=7,
            charge_id=99,
            amount_cents=2500,
            attempts=6,
            last_error="Paddle API 500",
        )
        row = fake_svc.in_app[0]
        assert row["type"] == NotificationType.CHARGE_FAILED
        assert "action needed" in row["title"].lower()
        assert "6 attempts" in row["message"]

    def test_external_payload_includes_error_details(self, fake_session_context, fake_svc):
        _on_charge_failed(
            org_id=42,
            subscription_id=7,
            charge_id=99,
            amount_cents=2500,
            attempts=6,
            last_error="Paddle API 500",
        )
        payload = fake_svc.external[0]["payload"]
        assert payload["attempts"] == 6
        assert payload["last_error"] == "Paddle API 500"


class TestDecoratorErrorSafety:
    """The _with_session decorator must never let subscriber errors escape
    into emit() — the Celery task emitting the event should never fail
    because a notification couldn't be delivered.
    """

    def test_create_raises_rollback_not_commit(self, fake_session_context, fake_svc, fake_session):
        import datanika.services.charge_notification_hooks as m

        # Make the in-app service raise.
        bad = MagicMock()
        bad.create.side_effect = RuntimeError("DB down")

        with patch.object(m, "_svc", bad):
            _on_charge_incoming(org_id=42, subscription_id=7, amount_cents=1500)

        # rollback + close both called, commit NOT called.
        fake_session.rollback.assert_called_once()
        fake_session.close.assert_called_once()
        fake_session.commit.assert_not_called()


class TestRegistration:
    def test_register_subscribes_all_three_events(self, monkeypatch):
        # Fresh handler dict for the test so we don't pollute global state.
        fresh = {}
        monkeypatch.setattr(hooks, "_handlers", fresh)

        register_charge_notification_hooks()

        assert set(fresh.keys()) == {"charge_incoming", "charge_issued", "charge_failed"}
        for event in ("charge_incoming", "charge_issued", "charge_failed"):
            assert len(fresh[event]) == 1
