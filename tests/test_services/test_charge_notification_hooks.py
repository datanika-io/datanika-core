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
    # Default dedup check to "no existing row" so the baseline tests
    # exercise the create+dispatch path. Tests that want to exercise the
    # dedup-hit branch re-patch ``_existing_notification`` locally.
    monkeypatch.setattr(m, "_existing_notification", lambda *a, **kw: None)
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
# SPEC_OVERAGE_BILLING_TESTS.md §3.3 coverage — the three gaps tracked in
# core#265. QA shipped these as xfailed red-tests in core#264; core#265
# closes each gap, so the xfail markers are removed and the tests flip to
# asserting the fix. Layered on top are engineering-side unit tests that
# exercise the dedup helper in isolation.
# ---------------------------------------------------------------------------


class TestValidEventsIncludesChargeEvents:
    """core#265 gap 1: ``VALID_EVENTS`` must include all three charge events
    so users can opt Slack/Telegram/webhook/email channels into them.
    """

    @pytest.mark.parametrize(
        "event",
        ["charge_incoming", "charge_issued", "charge_failed"],
    )
    def test_event_accepted_by_validator(self, event):
        from datanika.services.notification_service import VALID_EVENTS

        assert event in VALID_EVENTS, (
            f"{event!r} missing from VALID_EVENTS. Users cannot create a "
            "channel subscribed to this event."
        )

    def test_create_channel_accepts_charge_events(self):
        """End-to-end: ``_validate_events`` no longer rejects charge_*."""
        from datanika.services.notification_service import _validate_events

        # Should not raise.
        _validate_events(["charge_incoming"])
        _validate_events(["charge_issued", "charge_failed"])
        _validate_events(["run_failure", "charge_incoming"])

    def test_create_channel_still_rejects_unknown_events(self):
        """Backstop: VALID_EVENTS is still an allow-list, not a free-for-all."""
        from datanika.services.notification_service import _validate_events

        with pytest.raises(ValueError, match="Invalid event"):
            _validate_events(["charge_nonexistent"])


class TestChargeIncomingI18n:
    """core#265 gap 2: charge notification i18n keys exist in all 9 locales.

    Key naming matches the ``notifications.quota_warning.title/body`` pattern
    already established by core#243. QA's ship-gate asserted ``.message`` —
    the name was aligned to ``.body`` in review to match the quota precedent.
    """

    _REQUIRED_KEYS = (
        "notifications.charge_incoming.title",
        "notifications.charge_incoming.body",
        "notifications.charge_issued.title",
        "notifications.charge_issued.body",
        "notifications.charge_failed.title",
        "notifications.charge_failed.body",
    )

    @pytest.mark.parametrize(
        "locale",
        ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"],
    )
    def test_charge_keys_present(self, locale):
        import json
        from pathlib import Path

        locale_file = Path(__file__).resolve().parents[2] / "datanika" / "i18n" / f"{locale}.json"
        assert locale_file.exists(), f"Missing locale file: {locale_file}"
        with locale_file.open(encoding="utf-8") as fp:
            strings = json.load(fp)
        missing = [k for k in self._REQUIRED_KEYS if k not in strings]
        assert not missing, f"Locale {locale}.json missing i18n keys: {missing}."
        # Non-empty translations.
        for key in self._REQUIRED_KEYS:
            assert strings[key].strip(), f"Empty value for {key!r} in {locale}.json"

    def test_charge_keys_referenced_in_code(self):
        """Orphan-keys guard (same policy as the i18n parity test): each
        key must be referenced in at least one Python file.
        """
        from pathlib import Path

        core_root = Path(__file__).resolve().parents[2] / "datanika"
        source = ""
        for py in core_root.rglob("*.py"):
            source += py.read_text(encoding="utf-8")
        for key in self._REQUIRED_KEYS:
            assert key in source, f"Key {key!r} is in JSON but not referenced anywhere in code"


class TestChargeIncomingIdempotency:
    """core#265 gap 3: duplicate emission for same (org, cycle) must not
    create duplicate in-app rows. Cloud's ``UsageLedger.charge_incoming_sent``
    latch is primary; this dedup is the belt-and-suspenders reception layer.
    """

    def test_duplicate_emission_creates_single_notification(self, fake_session_context, fake_svc):
        """High-level scenario — two back-to-back emissions for the same
        ``(org, subscription)`` result in one in-app row, not two.
        """
        common = dict(
            org_id=42,
            subscription_id=7,
            amount_cents=1500,
            currency="USD",
            metric="bytes_processed",
        )
        _on_charge_incoming(**common)
        # Second call: first row now exists; dedup must catch it. Re-patch
        # ``_existing_notification`` to return the first in-app row.
        import datanika.services.charge_notification_hooks as m

        first = fake_svc.in_app[0]  # baseline recording from fixture
        monkey_existing = MagicMock(**first)
        original = m._existing_notification

        def dedup_on_second(session, **kw):
            return (
                monkey_existing
                if kw.get("notif_type") == NotificationType.CHARGE_INCOMING
                else None
            )

        try:
            m._existing_notification = dedup_on_second
            _on_charge_incoming(**common)
        finally:
            m._existing_notification = original

        assert len(fake_svc.in_app) == 1, (
            f"Expected 1 notification for duplicate emission, got "
            f"{len(fake_svc.in_app)}. See core#265 gap 3."
        )


class TestIdempotencyDedupUnit:
    """Engineering-side unit tests for the ``_existing_notification``
    dedup helper — covers scope (resource_type / id) and time-window
    policy per event type.
    """

    def _fake_existing(self, monkeypatch, existing):
        """Pin ``_existing_notification`` to return a specific value."""
        import datanika.services.charge_notification_hooks as m

        monkeypatch.setattr(m, "_existing_notification", lambda *a, **kw: existing)

    def test_charge_incoming_skips_on_recent_duplicate(
        self, fake_session_context, fake_svc, monkeypatch
    ):
        sentinel = MagicMock()  # truthy existing row
        self._fake_existing(monkeypatch, sentinel)

        _on_charge_incoming(
            org_id=42,
            subscription_id=7,
            amount_cents=1500,
        )

        # Both in-app create and external dispatch skipped.
        assert fake_svc.in_app == []
        assert fake_svc.external == []

    def test_charge_incoming_fires_when_no_duplicate(
        self, fake_session_context, fake_svc, monkeypatch
    ):
        """Baseline — default fixture pins existing=None."""
        _on_charge_incoming(
            org_id=42,
            subscription_id=7,
            amount_cents=1500,
        )
        assert len(fake_svc.in_app) == 1
        assert len(fake_svc.external) == 1

    def test_charge_issued_skips_on_duplicate_charge_id(
        self, fake_session_context, fake_svc, monkeypatch
    ):
        sentinel = MagicMock()
        self._fake_existing(monkeypatch, sentinel)

        _on_charge_issued(
            org_id=42,
            subscription_id=7,
            charge_id=99,
            amount_cents=2500,
        )
        assert fake_svc.in_app == []
        assert fake_svc.external == []

    def test_charge_failed_skips_on_duplicate_charge_id(
        self, fake_session_context, fake_svc, monkeypatch
    ):
        sentinel = MagicMock()
        self._fake_existing(monkeypatch, sentinel)

        _on_charge_failed(
            org_id=42,
            subscription_id=7,
            charge_id=99,
            amount_cents=2500,
        )
        assert fake_svc.in_app == []
        assert fake_svc.external == []

    def test_charge_incoming_uses_bounded_time_window(
        self, fake_session_context, fake_svc, monkeypatch
    ):
        """``charge_incoming`` must pass a ``since`` cutoff so next cycle's
        warning for the same subscription isn't suppressed.
        """
        import datanika.services.charge_notification_hooks as m

        captured: dict = {}

        def spy(session, **kw):
            captured.update(kw)
            return None

        monkeypatch.setattr(m, "_existing_notification", spy)
        _on_charge_incoming(org_id=42, subscription_id=7, amount_cents=1500)

        assert "since" in captured
        assert captured["since"] is not None

    def test_charge_issued_uses_unbounded_dedup(self, fake_session_context, fake_svc, monkeypatch):
        """``charge_issued`` dedups by charge_id alone — no window. A charge
        row is globally unique; any prior notification is a replay.
        """
        import datanika.services.charge_notification_hooks as m

        captured: dict = {}

        def spy(session, **kw):
            captured.update(kw)
            return None

        monkeypatch.setattr(m, "_existing_notification", spy)
        _on_charge_issued(org_id=42, subscription_id=7, charge_id=99, amount_cents=2500)

        assert captured.get("since") is None
