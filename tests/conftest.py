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
