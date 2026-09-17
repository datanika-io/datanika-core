"""Notification channel configuration VALUES follow the admin threshold on REST reads (core#681).

`SPEC_SERVICE_AUTHORIZATION` §2 puts notification channels at `admin` throughout because *"the row
holds the Slack webhook URL / Telegram bot token"*, and `NotificationService._redact` treats every
config value as a secret. The Reflex channel list renders no configuration for any role; only the
admin-gated edit form loads it. The REST reads follow the same line: an `admin` receives the
configuration, every other member receives its keys with each value replaced by ``[redacted]``.

Each redaction test sits beside the admin control that receives the real value, so the suite cannot
pass by redacting for everyone.
"""

from __future__ import annotations

import contextlib
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SASession
from sqlalchemy.pool import StaticPool
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.models.base import Base
from datanika.models.notification_channel import ChannelType
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.notification_service import NotificationService
from datanika.services.rate_limit_service import RateLimitResult
from tests.factories import make_user

ORG_ID = 10
SECRET = "https://hooks.slack.com/services/CHANNEL-SECRET-MARKER"
_RATE_OK = RateLimitResult(
    allowed=True, current_count=1, limit=60, remaining=59, retry_after=0, reset_at=9999999999
)


@contextlib.contextmanager
def _surface(role: MemberRole):
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    Base.metadata.create_all(engine)
    session = SASession(engine)
    session.add(Organization(id=ORG_ID, name="Acme", slug="acme-channel-config"))
    session.flush()
    member = make_user(session, email=f"channel-{role.value}@test.io", password_hash="x")
    session.add(Membership(user_id=member.id, org_id=ORG_ID, role=role))
    admin = make_user(session, email="channel-setup@test.io", password_hash="x")
    session.add(Membership(user_id=admin.id, org_id=ORG_ID, role=MemberRole.ADMIN))
    session.flush()
    channel = NotificationService().create_channel(
        session,
        ORG_ID,
        actor_user_id=admin.id,
        name="Alerts",
        channel_type=ChannelType.SLACK,
        config={"webhook_url": SECRET},
        events=["run_failure"],
    )
    session.commit()
    channel_id = channel.id

    key = MagicMock()
    key.id, key.org_id, key.user_id, key.scopes = 1, ORG_ID, member.id, None

    @contextlib.contextmanager
    def fake_session():
        yield session

    with (
        patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
        patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
        patch("datanika.services.api_middleware._get_session", fake_session),
    ):
        mock_svc.authenticate_api_key.return_value = key
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = _RATE_OK
        yield TestClient(Starlette(routes=api_v1_routes)), channel_id
    session.close()
    engine.dispose()


def _reads(client, channel_id):
    headers = {"Authorization": "Bearer etf_channel"}
    listed = client.get("/api/v1/notifications/channels", headers=headers)
    single = client.get(f"/api/v1/notifications/channels/{channel_id}", headers=headers)
    assert listed.status_code == 200, listed.text
    assert single.status_code == 200, single.text
    return listed.json()["items"][0]["config"], single.json()["config"], listed.text + single.text


@pytest.mark.parametrize("role", [MemberRole.VIEWER, MemberRole.EDITOR])
def test_a_member_below_admin_reads_the_keys_not_the_values(role):
    with _surface(role) as (client, channel_id):
        listed, single, raw = _reads(client, channel_id)

    assert listed == {"webhook_url": "[redacted]"}
    assert single == {"webhook_url": "[redacted]"}
    assert "CHANNEL-SECRET-MARKER" not in raw


def test_an_admin_reads_the_configuration():
    """The control: without it, redacting for everyone passes the test above."""
    with _surface(MemberRole.ADMIN) as (client, channel_id):
        listed, single, _raw = _reads(client, channel_id)

    assert listed == {"webhook_url": SECRET}
    assert single == {"webhook_url": SECRET}
