from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from datanika.models.base import Base


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def _disable_notif_unread_cache(request):
    """Autouse fallback: make the unread-count Redis cache a no-op for every
    test that hasn't explicitly wired its own `mock_redis` fixture.

    Without this, every service call to ``InAppNotificationService`` tries
    to reach a real Redis on ``localhost:6379``; in local/CI runs with no
    container up, that's ~1s × 50+ calls = minutes of dead wait. Tests
    that care about the cache path declare ``mock_redis`` explicitly and
    this fixture sees it and steps aside.
    """
    if "mock_redis" in request.fixturenames:
        yield
        return
    with patch(
        "datanika.services.notification_unread_cache._redis",
        side_effect=RuntimeError("redis disabled in tests"),
    ):
        yield


@pytest.fixture(scope="session")
def engine():
    """SQLite in-memory engine for fast model tests."""
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def db_session(engine):
    """Provide a transactional session that rolls back after each test."""
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection, join_transaction_mode="create_savepoint")
    yield session
    session.close()
    if transaction.is_active:
        transaction.rollback()
    connection.close()


@pytest.fixture
def sqlite_aware_invitation_expiry():
    """Make the SQLite harness return ``Invitation.expires_at`` timezone-aware, as PostgreSQL does.

    🚨 **Without this an invitation test fails for a reason production does not have, and the
    failure looks like the feature being broken.** ``Invitation.expires_at`` is
    ``DateTime(timezone=True)``; PostgreSQL returns it tz-aware and SQLite — which has no
    ``timestamptz`` — silently returns it naive. ``accept_invitation`` compares it to
    ``datetime.now(UTC)``, so once the row is re-loaded from the database rather than served from
    the identity map, the comparison raises
    ``TypeError: can't compare offset-naive and offset-aware datetimes``.

    **Not autouse, deliberately.** It changes what a test runs against, so a module opts in by
    requesting it — ``test_invited_signup_lands_in_one_org.py`` (core#981) and
    ``test_oauth_signup_invitation.py`` (core#624) do. It lives here rather than in either of them
    because an imported fixture is registered as a second FixtureDef in the importing module
    (``tests/test_fixture_sharing.py``).
    """
    from datetime import UTC

    from sqlalchemy import event

    from datanika.models.invitation import Invitation

    def _make_aware(target, _context):
        if target.expires_at is not None and target.expires_at.tzinfo is None:
            target.expires_at = target.expires_at.replace(tzinfo=UTC)

    event.listen(Invitation, "load", _make_aware, propagate=True)
    try:
        yield
    finally:
        event.remove(Invitation, "load", _make_aware)
