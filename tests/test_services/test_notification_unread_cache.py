"""Tests for the Redis-backed unread-count cache (PLAN_ENGINEERING §Deferred).

Target: notifications_unread p95 265ms → <200ms. The cache collapses
the read path to a single Redis GET on warm hits; writes invalidate
to prevent staleness.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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
from datanika.models.user import Organization
from datanika.services import notification_unread_cache
from datanika.services.in_app_notification_service import InAppNotificationService


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
    o = Organization(name="CacheOrg", slug="cache-org")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def mock_redis():
    """Replace the `redis.from_url` factory inside the cache module with an
    in-memory fake. All .get/.set/.delete/.scan_iter calls hit it instead.
    """
    store: dict[str, str] = {}
    ttls: dict[str, int] = {}

    fake = MagicMock()

    def fake_get(key):
        return store.get(key)

    def fake_set(key, value, ex=None):
        store[key] = str(value)
        if ex:
            ttls[key] = ex
        return True

    def fake_delete(*keys):
        n = 0
        for k in keys:
            if k in store:
                del store[k]
                n += 1
        return n

    def fake_scan_iter(match=None, count=None):
        if match is None:
            return iter(list(store.keys()))
        # Convert Redis-style glob to simple prefix check.
        prefix = match.rstrip("*")
        return iter([k for k in list(store.keys()) if k.startswith(prefix)])

    fake.get.side_effect = fake_get
    fake.set.side_effect = fake_set
    fake.delete.side_effect = fake_delete
    fake.scan_iter.side_effect = fake_scan_iter

    with patch.object(notification_unread_cache, "_redis", return_value=fake):
        yield {"store": store, "ttls": ttls, "client": fake}


# ---------------------------------------------------------------------------
# Cache module primitives
# ---------------------------------------------------------------------------


class TestCachePrimitives:
    def test_get_miss_returns_none(self, mock_redis):
        assert notification_unread_cache.get(1, 5) is None

    def test_set_then_get(self, mock_redis):
        notification_unread_cache.set(1, 5, 7)
        assert notification_unread_cache.get(1, 5) == 7

    def test_set_writes_ttl(self, mock_redis):
        notification_unread_cache.set(1, 5, 7)
        assert mock_redis["ttls"]["notif_unread:1:5"] == 60

    def test_invalidate_user_drops_just_that_key(self, mock_redis):
        notification_unread_cache.set(1, 5, 7)
        notification_unread_cache.set(1, 6, 3)
        notification_unread_cache.invalidate_user(1, 5)
        assert notification_unread_cache.get(1, 5) is None
        assert notification_unread_cache.get(1, 6) == 3

    def test_invalidate_org_drops_all_users_in_org(self, mock_redis):
        notification_unread_cache.set(1, 5, 7)
        notification_unread_cache.set(1, 6, 3)
        notification_unread_cache.set(2, 5, 9)  # different org
        notification_unread_cache.invalidate_org(1)
        assert notification_unread_cache.get(1, 5) is None
        assert notification_unread_cache.get(1, 6) is None
        assert notification_unread_cache.get(2, 5) == 9  # untouched

    def test_redis_failure_is_swallowed_on_read(self):
        with patch.object(
            notification_unread_cache,
            "_redis",
            side_effect=RuntimeError("connection refused"),
        ):
            assert notification_unread_cache.get(1, 5) is None

    def test_redis_failure_is_swallowed_on_write(self):
        with patch.object(
            notification_unread_cache,
            "_redis",
            side_effect=RuntimeError("connection refused"),
        ):
            # Must not raise.
            notification_unread_cache.set(1, 5, 7)
            notification_unread_cache.invalidate_user(1, 5)
            notification_unread_cache.invalidate_org(1)

    def test_garbage_cached_value_returns_none(self, mock_redis):
        # Simulate a stale/corrupted value from a prior incompatible version.
        mock_redis["store"]["notif_unread:1:5"] = "not_a_number"
        assert notification_unread_cache.get(1, 5) is None


# ---------------------------------------------------------------------------
# Service integration — read path + invalidation on writes
# ---------------------------------------------------------------------------


def _make_notif(svc, session, org, **kw):
    defaults = dict(
        notification_type=NotificationType.RUN_FAILED,
        title="X",
        resource_type="run",
        resource_id=1,
    )
    defaults.update(kw)
    return svc.create(session, org.id, **defaults)


class TestServiceReadPath:
    def test_first_call_queries_db_and_populates_cache(self, db_session, org, mock_redis):
        svc = InAppNotificationService()
        _make_notif(svc, db_session, org, user_id=42)
        # The create itself invalidated the org — cache is empty now.
        assert notification_unread_cache.get(org.id, 42) is None

        count = svc.unread_count(db_session, org.id, user_id=42)
        assert count == 1
        # Cache populated after DB read.
        assert notification_unread_cache.get(org.id, 42) == 1

    def test_second_call_served_from_cache(self, db_session, org, mock_redis):
        svc = InAppNotificationService()
        _make_notif(svc, db_session, org, user_id=42)
        svc.unread_count(db_session, org.id, user_id=42)  # warm

        # Blow away the DB row — cache should still serve the cached value.
        from datanika.models.notification import Notification

        db_session.query(Notification).delete()
        db_session.commit()

        # Still cached — no DB query consulted.
        assert svc.unread_count(db_session, org.id, user_id=42) == 1

    def test_different_users_cached_separately(self, db_session, org, mock_redis):
        svc = InAppNotificationService()
        # User 1 has one user-specific row; user 2 has none. Both share an
        # org-wide row → both see count=2 and count=1 respectively.
        _make_notif(svc, db_session, org, user_id=1)
        _make_notif(svc, db_session, org, user_id=None)  # org-wide

        assert svc.unread_count(db_session, org.id, user_id=1) == 2
        assert svc.unread_count(db_session, org.id, user_id=2) == 1


class TestServiceInvalidation:
    def test_create_invalidates_org(self, db_session, org, mock_redis):
        svc = InAppNotificationService()
        _make_notif(svc, db_session, org, user_id=1)
        svc.unread_count(db_session, org.id, user_id=1)  # warm
        svc.unread_count(db_session, org.id, user_id=2)  # warm
        assert notification_unread_cache.get(org.id, 1) == 1
        assert notification_unread_cache.get(org.id, 2) == 0

        _make_notif(svc, db_session, org, user_id=None)  # org-wide
        # Both users' cached counts dropped.
        assert notification_unread_cache.get(org.id, 1) is None
        assert notification_unread_cache.get(org.id, 2) is None

    def test_mark_read_invalidates_when_row_was_unread(self, db_session, org, mock_redis):
        svc = InAppNotificationService()
        n = _make_notif(svc, db_session, org, user_id=1)
        svc.unread_count(db_session, org.id, user_id=1)
        assert notification_unread_cache.get(org.id, 1) == 1

        svc.mark_read(db_session, n.id, org.id)
        assert notification_unread_cache.get(org.id, 1) is None

    def test_mark_read_skips_invalidation_when_already_read(self, db_session, org, mock_redis):
        svc = InAppNotificationService()
        n = _make_notif(svc, db_session, org, user_id=1)
        svc.mark_read(db_session, n.id, org.id)  # first mark: invalidates
        svc.unread_count(db_session, org.id, user_id=1)  # warm cache with 0
        assert notification_unread_cache.get(org.id, 1) == 0

        # Second mark_read on an already-read row should not invalidate.
        svc.mark_read(db_session, n.id, org.id)
        assert notification_unread_cache.get(org.id, 1) == 0

    def test_mark_all_read_invalidates_user_when_no_org_wide(self, db_session, org, mock_redis):
        svc = InAppNotificationService()
        _make_notif(svc, db_session, org, user_id=1)
        _make_notif(svc, db_session, org, user_id=1)
        _make_notif(svc, db_session, org, user_id=2)  # other user
        svc.unread_count(db_session, org.id, user_id=1)
        svc.unread_count(db_session, org.id, user_id=2)

        svc.mark_all_read(db_session, org.id, user_id=1)
        # User 1's key dropped, user 2's survives.
        assert notification_unread_cache.get(org.id, 1) is None
        assert notification_unread_cache.get(org.id, 2) == 1

    def test_mark_all_read_invalidates_org_when_org_wide_rows_touched(
        self, db_session, org, mock_redis
    ):
        svc = InAppNotificationService()
        _make_notif(svc, db_session, org, user_id=1)
        _make_notif(svc, db_session, org, user_id=None)  # org-wide
        svc.unread_count(db_session, org.id, user_id=1)
        svc.unread_count(db_session, org.id, user_id=2)

        svc.mark_all_read(db_session, org.id, user_id=1)
        # Org-wide transition → every user's count affected.
        assert notification_unread_cache.get(org.id, 1) is None
        assert notification_unread_cache.get(org.id, 2) is None

    def test_dismiss_invalidates_when_unread(self, db_session, org, mock_redis):
        svc = InAppNotificationService()
        n = _make_notif(svc, db_session, org, user_id=1)
        svc.unread_count(db_session, org.id, user_id=1)
        assert notification_unread_cache.get(org.id, 1) == 1

        svc.dismiss(db_session, n.id, org.id)
        assert notification_unread_cache.get(org.id, 1) is None

    def test_dismiss_skips_invalidation_when_already_read(self, db_session, org, mock_redis):
        svc = InAppNotificationService()
        n = _make_notif(svc, db_session, org, user_id=1)
        svc.mark_read(db_session, n.id, org.id)
        svc.unread_count(db_session, org.id, user_id=1)
        assert notification_unread_cache.get(org.id, 1) == 0

        # Dismissing an already-read row doesn't change anyone's unread count.
        svc.dismiss(db_session, n.id, org.id)
        assert notification_unread_cache.get(org.id, 1) == 0
