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


# ---------------------------------------------------------------------------
# SPEC_OVERAGE_BILLING_TESTS.md §3.3 coverage gaps — xfailed until core#265
# ---------------------------------------------------------------------------
#
# The existing tests above cover the happy path with mocked services.
# These supplementary tests cover three gaps surfaced while writing
# QA coverage for SPEC_OVERAGE_BILLING_TESTS.md §3.3:
#
#   1. `VALID_EVENTS` is missing 'charge_incoming' / 'charge_issued' /
#      'charge_failed'. `create_channel(events=['charge_incoming'])`
#      raises ValueError — users cannot opt-in to the new events.
#   2. No `notifications.charge_incoming.*` i18n keys in any of the
#      9 locale files. Hook strings are hard-coded English.
#   3. No idempotency dedup per (org, billing_period). A cloud-side
#      retry spawns duplicate in-app notifications.
#
# All three are tracked in core#265. When each ships, remove the
# corresponding xfail marker (or flip to strict=True).


class TestValidEventsIncludesChargeEvents:
    """core#265 gap 1: VALID_EVENTS rejects charge_* events."""

    @pytest.mark.parametrize(
        "event",
        ["charge_incoming", "charge_issued", "charge_failed"],
    )
    @pytest.mark.xfail(
        strict=False,
        reason="core#265 gap 1: VALID_EVENTS missing charge_* events",
    )
    def test_event_accepted_by_validator(self, event):
        from datanika.services.notification_service import VALID_EVENTS

        assert event in VALID_EVENTS, (
            f"{event!r} missing from VALID_EVENTS. Users cannot create a "
            "channel subscribed to this event. See core#265."
        )


class TestChargeIncomingI18n:
    """core#265 gap 2: charge_incoming notification has no i18n keys."""

    @pytest.mark.parametrize(
        "locale",
        ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"],
    )
    @pytest.mark.xfail(
        strict=False,
        reason="core#265 gap 2: notifications.charge_incoming.* i18n keys missing",
    )
    def test_charge_incoming_keys_present(self, locale):
        import json
        from pathlib import Path

        locale_file = Path(__file__).resolve().parents[2] / "datanika" / "i18n" / f"{locale}.json"
        assert locale_file.exists(), f"Missing locale file: {locale_file}"
        with locale_file.open(encoding="utf-8") as fp:
            strings = json.load(fp)
        required = (
            "notifications.charge_incoming.title",
            "notifications.charge_incoming.message",
        )
        missing = [k for k in required if k not in strings]
        assert not missing, (
            f"Locale {locale}.json missing i18n keys: {missing}. "
            "Hook strings are hard-coded — see core#265 for the refactor."
        )


class TestChargeIncomingIdempotency:
    """core#265 gap 3: two emissions for same (org, period) create two notifications."""

    @pytest.mark.xfail(
        strict=False,
        reason="core#265 gap 3: no idempotency dedup per (org, billing_period)",
    )
    def test_duplicate_emission_creates_single_notification(self, fake_session_context, fake_svc):
        common = dict(
            org_id=42,
            subscription_id=7,
            amount_cents=1500,
            currency="USD",
            metric="bytes_processed",
        )
        _on_charge_incoming(**common)
        _on_charge_incoming(**common)
        assert len(fake_svc.in_app) == 1, (
            f"Expected 1 notification for duplicate emission, got "
            f"{len(fake_svc.in_app)}. Cloud-side retry spawns duplicates. "
            "See core#265 gap 3 for the fix (dedupe key or cloud-side debounce guarantee)."
        )
