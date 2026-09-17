"""Tests for in-app notification center backend (#68)."""

from __future__ import annotations

import contextlib
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SASession
from sqlalchemy.pool import StaticPool
from starlette.applications import Starlette
from starlette.testclient import TestClient

import datanika.models.invitation  # noqa: F401
import datanika.models.notification  # noqa: F401
import datanika.models.notification_channel  # noqa: F401
import datanika.models.sso_config  # noqa: F401
from datanika.models.base import Base
from datanika.models.notification import NotificationType
from datanika.models.user import Organization
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.in_app_notification_service import InAppNotificationService
from datanika.services.rate_limit_service import RateLimitResult

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
    o = Organization(name="Test", slug="test")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def svc():
    return InAppNotificationService()


# ---------------------------------------------------------------------------
# Service layer tests
# ---------------------------------------------------------------------------


class TestInAppNotificationService:
    def test_create_notification(self, db_session, org, svc):
        n = svc.create(
            db_session,
            org.id,
            NotificationType.RUN_FAILED,
            title="Run #1 failed",
            resource_type="run",
            resource_id=1,
            message="Connection timeout",
        )
        assert n.id is not None
        assert n.type == NotificationType.RUN_FAILED
        assert n.read_at is None

    def test_list_for_user_returns_own_and_org_wide(self, db_session, org, svc):
        svc.create(
            db_session,
            org.id,
            NotificationType.RUN_SUCCEEDED,
            title="For user 1",
            resource_type="run",
            resource_id=1,
            user_id=1,
        )
        svc.create(
            db_session,
            org.id,
            NotificationType.RUN_FAILED,
            title="Org-wide",
            resource_type="run",
            resource_id=2,
        )
        svc.create(
            db_session,
            org.id,
            NotificationType.RUN_SUCCEEDED,
            title="For user 2",
            resource_type="run",
            resource_id=3,
            user_id=2,
        )
        db_session.flush()

        items = svc.list_for_user(db_session, org.id, user_id=1)
        titles = {n.title for n in items}
        assert "For user 1" in titles
        assert "Org-wide" in titles
        assert "For user 2" not in titles

    def test_list_unread_only(self, db_session, org, svc):
        svc.create(
            db_session,
            org.id,
            NotificationType.RUN_FAILED,
            title="Unread",
            resource_type="run",
            resource_id=1,
        )
        n2 = svc.create(
            db_session,
            org.id,
            NotificationType.RUN_SUCCEEDED,
            title="Read",
            resource_type="run",
            resource_id=2,
        )
        svc.mark_read(db_session, n2.id, org.id, user_id=1)
        db_session.flush()

        items = svc.list_for_user(db_session, org.id, user_id=1, unread_only=True)
        assert len(items) == 1
        assert items[0].title == "Unread"

    def test_unread_count(self, db_session, org, svc):
        svc.create(
            db_session,
            org.id,
            NotificationType.RUN_FAILED,
            title="A",
            resource_type="run",
            resource_id=1,
        )
        svc.create(
            db_session,
            org.id,
            NotificationType.RUN_FAILED,
            title="B",
            resource_type="run",
            resource_id=2,
        )
        n3 = svc.create(
            db_session,
            org.id,
            NotificationType.RUN_SUCCEEDED,
            title="C",
            resource_type="run",
            resource_id=3,
        )
        svc.mark_read(db_session, n3.id, org.id, user_id=1)
        db_session.flush()

        assert svc.unread_count(db_session, org.id, user_id=1) == 2

    def test_mark_read(self, db_session, org, svc):
        n = svc.create(
            db_session,
            org.id,
            NotificationType.RUN_FAILED,
            title="T",
            resource_type="run",
            resource_id=1,
        )
        db_session.flush()
        assert n.read_at is None
        result = svc.mark_read(db_session, n.id, org.id, user_id=1)
        assert result is not None
        assert result.read_at is not None

    def test_mark_read_wrong_org(self, db_session, org, svc):
        n = svc.create(
            db_session,
            org.id,
            NotificationType.RUN_FAILED,
            title="T",
            resource_type="run",
            resource_id=1,
        )
        db_session.flush()
        assert svc.mark_read(db_session, n.id, org_id=999, user_id=1) is None

    def test_mark_all_read(self, db_session, org, svc):
        svc.create(
            db_session,
            org.id,
            NotificationType.RUN_FAILED,
            title="A",
            resource_type="run",
            resource_id=1,
        )
        svc.create(
            db_session,
            org.id,
            NotificationType.RUN_FAILED,
            title="B",
            resource_type="run",
            resource_id=2,
        )
        db_session.flush()
        count = svc.mark_all_read(db_session, org.id, user_id=1)
        assert count == 2
        assert svc.unread_count(db_session, org.id, user_id=1) == 0

    def test_dismiss(self, db_session, org, svc):
        n = svc.create(
            db_session,
            org.id,
            NotificationType.RUN_FAILED,
            title="T",
            resource_type="run",
            resource_id=1,
        )
        db_session.flush()
        assert svc.dismiss(db_session, n.id, org.id, user_id=1) is True
        items = svc.list_for_user(db_session, org.id, user_id=1)
        assert len(items) == 0

    def test_dismiss_wrong_org(self, db_session, org, svc):
        n = svc.create(
            db_session,
            org.id,
            NotificationType.RUN_FAILED,
            title="T",
            resource_type="run",
            resource_id=1,
        )
        db_session.flush()
        assert svc.dismiss(db_session, n.id, org_id=999, user_id=1) is False

    def test_tenant_isolation(self, db_session, org, svc):
        svc.create(
            db_session,
            org.id,
            NotificationType.RUN_FAILED,
            title="Org A",
            resource_type="run",
            resource_id=1,
        )
        db_session.flush()
        items = svc.list_for_user(db_session, org_id=999, user_id=1)
        assert len(items) == 0


# ---------------------------------------------------------------------------
# Hook tests
# ---------------------------------------------------------------------------


class TestNotificationHooks:
    def test_hook_creates_run_failed_notification(self, db_session, org, svc):
        from datanika.services.in_app_notification_hooks import _on_run_completed

        _on_run_completed(
            session=db_session,
            org_id=org.id,
            run_id=42,
            status="failed",
            error_message="timeout",
            target_type="upload",
            target_id=1,
        )
        db_session.flush()
        items = svc.list_for_user(db_session, org.id, user_id=1)
        assert len(items) == 1
        assert items[0].type == NotificationType.RUN_FAILED
        assert "42" in items[0].title

    def test_hook_creates_run_succeeded_notification(self, db_session, org, svc):
        from datanika.services.in_app_notification_hooks import _on_run_completed

        _on_run_completed(
            session=db_session,
            org_id=org.id,
            run_id=7,
            status="success",
            target_type="pipeline",
            target_id=2,
        )
        db_session.flush()
        items = svc.list_for_user(db_session, org.id, user_id=1)
        assert len(items) == 1
        assert items[0].type == NotificationType.RUN_SUCCEEDED


# ---------------------------------------------------------------------------
# API endpoint tests
# ---------------------------------------------------------------------------


@pytest.fixture
def fake_api_key():
    key = MagicMock()
    key.id = 1
    key.org_id = 10
    key.user_id = 1
    key.scopes = None
    return key


@pytest.fixture
def rate_limit_ok():
    return RateLimitResult(
        allowed=True,
        current_count=1,
        limit=60,
        remaining=59,
        retry_after=0,
        reset_at=9999999999,
    )


@pytest.fixture
def client():
    return TestClient(Starlette(routes=api_v1_routes))


@contextlib.contextmanager
def _patch_auth(fake_api_key, rate_limit_ok, db_session):
    @contextlib.contextmanager
    def fake_session():
        yield db_session

    with (
        patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
        patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
        patch("datanika.services.api_middleware._get_session", fake_session),
    ):
        mock_svc.authenticate_api_key.return_value = fake_api_key
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = rate_limit_ok
        yield


def _seed(session, org_id, count=3):
    svc = InAppNotificationService()
    ids = []
    for i in range(count):
        n = svc.create(
            session,
            org_id,
            NotificationType.RUN_FAILED,
            title=f"Run #{i + 1} failed",
            resource_type="run",
            resource_id=i + 1,
        )
        ids.append(n.id)
    session.flush()
    return ids


class TestNotificationEndpoints:
    def test_list_empty(self, client, fake_api_key, rate_limit_ok, engine):
        session = SASession(engine)
        fake_api_key.org_id = 10
        with _patch_auth(fake_api_key, rate_limit_ok, session):
            resp = client.get(
                "/api/v1/notifications",
                headers={"Authorization": "Bearer etf_test"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []
        assert data["unread_count"] == 0
        session.close()

    def test_list_with_seeded(self, client, fake_api_key, rate_limit_ok, engine):
        session = SASession(engine)
        org = Organization(name="O", slug="o")
        session.add(org)
        session.flush()
        fake_api_key.org_id = org.id
        _seed(session, org.id, 3)
        session.commit()
        with _patch_auth(fake_api_key, rate_limit_ok, session):
            resp = client.get(
                "/api/v1/notifications",
                headers={"Authorization": "Bearer etf_test"},
            )
        assert resp.status_code == 200
        assert resp.json()["total"] == 3
        assert resp.json()["unread_count"] == 3
        session.close()

    def test_unread_count_endpoint(self, client, fake_api_key, rate_limit_ok, engine):
        session = SASession(engine)
        org = Organization(name="O2", slug="o2")
        session.add(org)
        session.flush()
        fake_api_key.org_id = org.id
        _seed(session, org.id, 2)
        session.commit()
        with _patch_auth(fake_api_key, rate_limit_ok, session):
            resp = client.get(
                "/api/v1/notifications/unread-count",
                headers={"Authorization": "Bearer etf_test"},
            )
        assert resp.status_code == 200
        assert resp.json()["count"] == 2
        session.close()

    def test_mark_read(self, client, fake_api_key, rate_limit_ok, engine):
        session = SASession(engine)
        org = Organization(name="O3", slug="o3")
        session.add(org)
        session.flush()
        fake_api_key.org_id = org.id
        ids = _seed(session, org.id, 1)
        session.commit()
        with _patch_auth(fake_api_key, rate_limit_ok, session):
            resp = client.patch(
                f"/api/v1/notifications/{ids[0]}/read",
                headers={"Authorization": "Bearer etf_test"},
            )
        assert resp.status_code == 200
        assert resp.json()["read_at"] is not None
        session.close()

    def test_mark_read_404(self, client, fake_api_key, rate_limit_ok, engine):
        session = SASession(engine)
        fake_api_key.org_id = 10
        with _patch_auth(fake_api_key, rate_limit_ok, session):
            resp = client.patch(
                "/api/v1/notifications/99999/read",
                headers={"Authorization": "Bearer etf_test"},
            )
        assert resp.status_code == 404
        session.close()

    def test_read_all(self, client, fake_api_key, rate_limit_ok, engine):
        session = SASession(engine)
        org = Organization(name="O4", slug="o4")
        session.add(org)
        session.flush()
        fake_api_key.org_id = org.id
        _seed(session, org.id, 3)
        session.commit()
        with _patch_auth(fake_api_key, rate_limit_ok, session):
            resp = client.post(
                "/api/v1/notifications/read-all",
                headers={"Authorization": "Bearer etf_test"},
            )
        assert resp.status_code == 200
        assert resp.json()["marked"] == 3
        session.close()

    def test_dismiss(self, client, fake_api_key, rate_limit_ok, engine):
        session = SASession(engine)
        org = Organization(name="O5", slug="o5")
        session.add(org)
        session.flush()
        fake_api_key.org_id = org.id
        ids = _seed(session, org.id, 1)
        session.commit()
        with _patch_auth(fake_api_key, rate_limit_ok, session):
            resp = client.delete(
                f"/api/v1/notifications/{ids[0]}",
                headers={"Authorization": "Bearer etf_test"},
            )
        assert resp.status_code == 200
        assert resp.json()["deleted"] is True
        session.close()

    def test_auth_required(self, client):
        resp = client.get("/api/v1/notifications")
        assert resp.status_code == 401


class TestAMembersInboxIsTheirOwn:
    """Marking read and dismissing act on the member's OWN notifications and on org-wide ones --
    the same rows ``list_for_user`` shows them -- and on nobody else's. Each refusal sits beside the
    member's own notification and an org-wide one, which do change."""

    @staticmethod
    def _notif(svc, db_session, org, user_id):
        n = svc.create(
            db_session,
            org.id,
            NotificationType.RUN_FAILED,
            title="T",
            resource_type="run",
            resource_id=1,
            user_id=user_id,
        )
        db_session.flush()
        return n

    def test_another_members_notification_is_not_marked_read(self, db_session, org, svc):
        theirs = self._notif(svc, db_session, org, user_id=2)

        assert svc.mark_read(db_session, theirs.id, org.id, user_id=1) is None
        db_session.refresh(theirs)
        assert theirs.read_at is None

    def test_another_members_notification_is_not_dismissed(self, db_session, org, svc):
        theirs = self._notif(svc, db_session, org, user_id=2)

        assert svc.dismiss(db_session, theirs.id, org.id, user_id=1) is False
        db_session.refresh(theirs)
        assert theirs.deleted_at is None

    def test_the_members_own_notification_is_marked_and_dismissed(self, db_session, org, svc):
        mine = self._notif(svc, db_session, org, user_id=1)

        assert svc.mark_read(db_session, mine.id, org.id, user_id=1) is not None
        assert svc.dismiss(db_session, mine.id, org.id, user_id=1) is True

    def test_an_org_wide_notification_is_marked_by_any_member(self, db_session, org, svc):
        everyone = self._notif(svc, db_session, org, user_id=None)

        assert svc.mark_read(db_session, everyone.id, org.id, user_id=2) is not None


class TestAnotherMembersNotificationOverRest:
    def test_mark_read_and_dismiss_answer_404(self, client, fake_api_key, rate_limit_ok, engine):
        """Not 403: the notification is not the caller's to know about, the same answer as an id
        that does not exist -- and the control, the caller's own, is served."""
        session = SASession(engine)
        org = Organization(name="Inbox", slug="inbox-own")
        session.add(org)
        session.flush()
        fake_api_key.org_id = org.id
        svc = InAppNotificationService()
        theirs = svc.create(
            session,
            org.id,
            NotificationType.RUN_FAILED,
            "T",
            "run",
            1,
            user_id=fake_api_key.user_id + 1,
        )
        mine = svc.create(
            session,
            org.id,
            NotificationType.RUN_FAILED,
            "T",
            "run",
            2,
            user_id=fake_api_key.user_id,
        )
        session.commit()
        headers = {"Authorization": "Bearer etf_test"}
        with _patch_auth(fake_api_key, rate_limit_ok, session):
            read_theirs = client.patch(f"/api/v1/notifications/{theirs.id}/read", headers=headers)
            dismiss_theirs = client.delete(f"/api/v1/notifications/{theirs.id}", headers=headers)
            read_mine = client.patch(f"/api/v1/notifications/{mine.id}/read", headers=headers)
        session.close()

        assert read_theirs.status_code == 404
        assert dismiss_theirs.status_code == 404
        assert read_mine.status_code == 200
