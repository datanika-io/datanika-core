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


class _NoSignupMarkers:
    """A reachable store that holds no signup markers: writes succeed, nothing is ever claimed."""

    def setex(self, *_args, **_kwargs):
        return True

    def delete(self, *_keys):
        return 0


@pytest.fixture(autouse=True)
def _no_signup_marker_store():
    """Autouse: keep ``signup_conversion`` off a real Redis in every test (core#1369).

    Both sign-in callbacks and ``/auth/complete`` now touch the marker store, so without this every
    test that drives one would wait on ``localhost:6379`` — the same dead wait
    ``_disable_notif_unread_cache`` exists for. A null store rather than a raising one, so the
    helpers' failure logging is not emitted by tests that are about something else. Tests of the
    marker itself patch ``signup_conversion._redis`` again, which takes precedence inside them.
    """
    with patch("datanika.services.signup_conversion._redis", return_value=_NoSignupMarkers()):
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


@pytest.fixture
def production_session_factory(tmp_path):
    """Sessions built the way production builds them, on ONE database they can all see (core#1412).

    ``db_session`` above is a single session, so its identity map is never stale relative to another
    writer. In this product the other writer is routine: the API request and the Celery worker never
    share a session. core#657's guards passed every single-session test and failed in production for
    exactly that reason.

    Call the factory once per actor — ``worker = factory()``, ``api = factory()`` — and commit from
    each as the real code would.

    * **The options are read off ``datanika.db.sync_session_factory``, not restated**, so a change
      there (``expire_on_commit``, ``autoflush``) reaches every test using this fixture the day it
      is made. Only the bind is replaced.
    * **The database is a file.** SQLite ``:memory:`` gives each connection its own database, so a
      second session would see nothing the first committed.
    * ⚠️ **Code under test that opens its own session** (``get_sync_session()``) binds production's
      engine, not this one. For a service call, hand it a session from this factory, as
      ``test_cancel_guards_read_the_database.py`` does.
    * 🚨 **For a TASK, patch ``datanika.db.get_sync_session`` to this factory; do not pass
      ``session=``.** A task given a session skips its own commits (``run_upload``'s
      ``own_session`` branches), so its first write stays uncommitted across the engine call. SQLite
      allows ONE writer, so another session's commit then fails ``database is locked`` — which
      Postgres, locking rows, would not do — and the harness manufactures a failed run and loses the
      other session's write.
      Measured on ``run_upload`` (core#1412): passed session, an API soft-delete mid-run → run
      ``failed``, delete lost; production's shape → run ``success``, delete kept.

    The fixture's own properties are pinned in ``tests/test_production_session_factory.py``.
    """
    from sqlalchemy.orm import sessionmaker

    from datanika.db import sync_session_factory

    engine = create_engine(f"sqlite:///{tmp_path / 'sessions.db'}")
    Base.metadata.create_all(engine)
    options = {key: value for key, value in sync_session_factory.kw.items() if key != "bind"}
    # `sessionmaker` stores a generated SUBCLASS of the class it was given; build from that base.
    session_class = sync_session_factory.class_.__mro__[1]
    try:
        yield sessionmaker(bind=engine, class_=session_class, **options)
    finally:
        engine.dispose()
