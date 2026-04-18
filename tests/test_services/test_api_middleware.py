"""Tests for API middleware — auth + rate limiting integration."""

from unittest.mock import MagicMock, patch

import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from datanika.services.api_middleware import api_endpoint
from datanika.services.rate_limit_service import RateLimitResult


# Sample endpoints decorated with api_endpoint
@api_endpoint(required_scope="pipeline:read")
async def sample_handler(request, api_key, session):
    return JSONResponse({"ok": True, "org_id": api_key.org_id})


@api_endpoint()
async def sample_handler_no_scope(request, api_key, session):
    return JSONResponse({"ok": True})


@pytest.fixture
def fake_api_key():
    key = MagicMock()
    key.id = 1
    key.org_id = 10
    key.name = "Test Key"
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
def rate_limit_exceeded():
    return RateLimitResult(
        allowed=False,
        current_count=61,
        limit=60,
        remaining=0,
        retry_after=45,
        reset_at=9999999999,
    )


class TestApiEndpointAuth:
    def test_missing_auth_header(self):
        app = Starlette(routes=[Route("/test", sample_handler)])
        client = TestClient(app)
        resp = client.get("/test")
        assert resp.status_code == 401
        assert "Missing" in resp.json()["error"]

    def test_invalid_auth_scheme(self):
        app = Starlette(routes=[Route("/test", sample_handler)])
        client = TestClient(app)
        resp = client.get("/test", headers={"Authorization": "Basic abc"})
        assert resp.status_code == 401

    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_invalid_key(self, mock_session, mock_svc):
        mock_svc.authenticate_api_key.return_value = None
        mock_session.return_value.__enter__ = lambda s: MagicMock()
        mock_session.return_value.__exit__ = lambda s, *a: None

        app = Starlette(routes=[Route("/test", sample_handler)])
        client = TestClient(app)
        resp = client.get("/test", headers={"Authorization": "Bearer etf_badkey"})
        assert resp.status_code == 401
        assert "Invalid" in resp.json()["error"]


class TestApiEndpointRateLimit:
    @patch("datanika.services.api_middleware._rate_limit_svc")
    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_rate_limited(self, mock_session, mock_svc, mock_rl, fake_api_key, rate_limit_exceeded):
        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = lambda s: session_ctx
        mock_session.return_value.__exit__ = lambda s, *a: None
        mock_svc.authenticate_api_key.return_value = fake_api_key
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = rate_limit_exceeded

        app = Starlette(routes=[Route("/test", sample_handler)])
        client = TestClient(app)
        resp = client.get("/test", headers={"Authorization": "Bearer etf_validkey"})
        assert resp.status_code == 429
        assert "Retry-After" in resp.headers
        assert resp.headers["Retry-After"] == "45"

    @patch("datanika.services.api_middleware._rate_limit_svc")
    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_success_with_rate_headers(
        self, mock_session, mock_svc, mock_rl, fake_api_key, rate_limit_ok
    ):
        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = lambda s: session_ctx
        mock_session.return_value.__exit__ = lambda s, *a: None
        mock_svc.authenticate_api_key.return_value = fake_api_key
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = rate_limit_ok

        app = Starlette(routes=[Route("/test", sample_handler)])
        client = TestClient(app)
        resp = client.get("/test", headers={"Authorization": "Bearer etf_validkey"})
        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        assert resp.headers["X-RateLimit-Limit"] == "60"
        assert resp.headers["X-RateLimit-Remaining"] == "59"


# ---------------------------------------------------------------------------
# E12 — sync handler path (asyncio.to_thread)
# ---------------------------------------------------------------------------


@api_endpoint(required_scope="pipeline:read")
def sync_sample_handler(request, api_key, session):
    """A handler declared as ``def`` instead of ``async def`` — should be
    routed through the E12 to_thread path."""
    return JSONResponse({"ok": True, "org_id": api_key.org_id, "sync": True})


class TestApiEndpointSyncHandler:
    """E12 — sync (def) handlers run in asyncio.to_thread to unblock the
    event loop for concurrent DB work.
    """

    def test_sync_handler_still_reaches_handler(self, fake_api_key, rate_limit_ok):
        """A sync handler returns 200 with the expected body."""
        with (
            patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
            patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
            patch("datanika.services.api_middleware._get_session") as mock_session,
        ):
            mock_svc.authenticate_api_key.return_value = fake_api_key
            mock_rl.get_limit_for_org.return_value = 60
            mock_rl.check_rate_limit.return_value = rate_limit_ok
            mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_session.return_value.__exit__ = MagicMock(return_value=False)

            app = Starlette(routes=[Route("/sync", sync_sample_handler)])
            client = TestClient(app)
            resp = client.get("/sync", headers={"Authorization": "Bearer etf_validkey"})
            assert resp.status_code == 200
            body = resp.json()
            assert body["ok"] is True
            assert body["sync"] is True
            assert body["org_id"] == 10

    def test_sync_handler_rejects_missing_auth(self):
        """Sync handler path still enforces auth — no handler-level check
        needed. If auth fails, handler never runs."""
        app = Starlette(routes=[Route("/sync", sync_sample_handler)])
        client = TestClient(app)
        resp = client.get("/sync")
        assert resp.status_code == 401

    def test_sync_handler_rate_limit(self, fake_api_key, rate_limit_exceeded):
        """Rate limit still applies in the sync path."""
        with (
            patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
            patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
            patch("datanika.services.api_middleware._get_session") as mock_session,
        ):
            mock_svc.authenticate_api_key.return_value = fake_api_key
            mock_rl.get_limit_for_org.return_value = 60
            mock_rl.check_rate_limit.return_value = rate_limit_exceeded
            mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_session.return_value.__exit__ = MagicMock(return_value=False)

            app = Starlette(routes=[Route("/sync", sync_sample_handler)])
            client = TestClient(app)
            resp = client.get("/sync", headers={"Authorization": "Bearer etf_validkey"})
            assert resp.status_code == 429

    def test_sync_handler_commits_session(self, fake_api_key, rate_limit_ok):
        """Successful sync handler triggers session.commit() (write-path
        correctness guarantee unchanged from the async path)."""
        mock_sess = MagicMock()
        with (
            patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
            patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
            patch("datanika.services.api_middleware._get_session") as mock_sess_ctor,
        ):
            mock_svc.authenticate_api_key.return_value = fake_api_key
            mock_rl.get_limit_for_org.return_value = 60
            mock_rl.check_rate_limit.return_value = rate_limit_ok
            mock_sess_ctor.return_value.__enter__ = MagicMock(return_value=mock_sess)
            mock_sess_ctor.return_value.__exit__ = MagicMock(return_value=False)

            app = Starlette(routes=[Route("/sync", sync_sample_handler)])
            client = TestClient(app)
            resp = client.get("/sync", headers={"Authorization": "Bearer etf_validkey"})
            assert resp.status_code == 200
            mock_sess.commit.assert_called_once()

    def test_sync_handler_rolls_back_on_exception(self, fake_api_key, rate_limit_ok):
        """A raised exception rolls back and returns 500."""
        mock_sess = MagicMock()

        @api_endpoint()
        def raising_handler(request, api_key, session):
            raise RuntimeError("boom")

        with (
            patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
            patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
            patch("datanika.services.api_middleware._get_session") as mock_sess_ctor,
        ):
            mock_svc.authenticate_api_key.return_value = fake_api_key
            mock_rl.get_limit_for_org.return_value = 60
            mock_rl.check_rate_limit.return_value = rate_limit_ok
            mock_sess_ctor.return_value.__enter__ = MagicMock(return_value=mock_sess)
            mock_sess_ctor.return_value.__exit__ = MagicMock(return_value=False)

            app = Starlette(routes=[Route("/boom", raising_handler)])
            client = TestClient(app)
            resp = client.get("/boom", headers={"Authorization": "Bearer etf_validkey"})
            assert resp.status_code == 500
            mock_sess.rollback.assert_called_once()
            mock_sess.commit.assert_not_called()

    def test_async_handler_path_unchanged(self, fake_api_key, rate_limit_ok):
        """Regression: async-def handlers still take the original path."""
        with (
            patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
            patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
            patch("datanika.services.api_middleware._get_session") as mock_session,
        ):
            mock_svc.authenticate_api_key.return_value = fake_api_key
            mock_rl.get_limit_for_org.return_value = 60
            mock_rl.check_rate_limit.return_value = rate_limit_ok
            mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_session.return_value.__exit__ = MagicMock(return_value=False)

            app = Starlette(routes=[Route("/async", sample_handler)])
            client = TestClient(app)
            resp = client.get("/async", headers={"Authorization": "Bearer etf_validkey"})
            assert resp.status_code == 200

    def test_sync_handler_concurrent_requests_not_serialized(self, fake_api_key, rate_limit_ok):
        """Core E12 guarantee: sync handlers run in a thread pool, so a slow
        handler does NOT block a concurrent request on the same worker.

        Simulation: 2 requests where each handler sleeps 0.2s. If they were
        serialized (pre-E12 path), total time >= 0.4s. In the threadpool,
        they overlap and total time stays close to 0.2s.
        """
        import threading
        import time

        call_enter = threading.Event()
        handler_calls = []

        @api_endpoint()
        def slow_handler(request, api_key, session):
            handler_calls.append(threading.get_ident())
            call_enter.set()
            time.sleep(0.2)
            return JSONResponse({"thread": threading.get_ident()})

        with (
            patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
            patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
            patch("datanika.services.api_middleware._get_session") as mock_session,
        ):
            mock_svc.authenticate_api_key.return_value = fake_api_key
            mock_rl.get_limit_for_org.return_value = 60
            mock_rl.check_rate_limit.return_value = rate_limit_ok
            mock_session.return_value.__enter__ = MagicMock(return_value=MagicMock())
            mock_session.return_value.__exit__ = MagicMock(return_value=False)

            app = Starlette(routes=[Route("/slow", slow_handler)])
            client = TestClient(app)

            # Fire two concurrent requests from separate client threads.
            results = []

            def go():
                results.append(
                    client.get("/slow", headers={"Authorization": "Bearer etf_validkey"})
                )

            t1 = threading.Thread(target=go)
            t2 = threading.Thread(target=go)
            start = time.monotonic()
            t1.start()
            t2.start()
            t1.join(timeout=2.0)
            t2.join(timeout=2.0)
            elapsed = time.monotonic() - start

            assert len(results) == 2
            assert all(r.status_code == 200 for r in results)
            # Two distinct threads executed the handlers.
            assert len(set(handler_calls)) == 2
            # Overlap: total < 2 × 0.2 + margin. Serialized would be ~0.4s.
            assert elapsed < 0.35, f"handlers serialized (elapsed={elapsed:.2f}s)"
