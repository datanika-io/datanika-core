"""Security tests — rate limiting bypass attempts, header injection, Redis failure."""

from unittest.mock import MagicMock, patch

import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from datanika.services.api_middleware import api_endpoint
from datanika.services.rate_limit_service import RateLimitResult, RateLimitService

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_redis():
    """Fresh per-test fake Redis with sliding-window semantics."""
    store: dict[str, int] = {}
    ttls: dict[str, float] = {}

    class FakePipeline:
        def __init__(self):
            self._ops = []

        def incr(self, key):
            self._ops.append(("incr", key))
            return self

        def expire(self, key, seconds):
            self._ops.append(("expire", key, seconds))
            return self

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self):
            results = []
            for op in self._ops:
                if op[0] == "incr":
                    store[op[1]] = store.get(op[1], 0) + 1
                    results.append(store[op[1]])
                elif op[0] == "expire":
                    ttls[op[1]] = op[2]
                    results.append(True)
            self._ops.clear()
            return results

    redis_mock = MagicMock()
    redis_mock.pipeline.side_effect = lambda: FakePipeline()
    redis_mock.ttl.side_effect = lambda key: int(ttls.get(key, 60))
    redis_mock._store = store
    return redis_mock


@pytest.fixture
def svc(mock_redis):
    return RateLimitService(redis_client=mock_redis)


# Sample endpoint for middleware tests
@api_endpoint(required_scope="pipeline:read")
async def scoped_handler(request, api_key, session):
    return JSONResponse({"ok": True})


@api_endpoint()
async def unscoped_handler(request, api_key, session):
    return JSONResponse({"ok": True})


def _fake_api_key(*, key_id=1, org_id=10, scopes=None):
    key = MagicMock()
    key.id = key_id
    key.org_id = org_id
    key.scopes = scopes
    key.name = "Test Key"
    return key


def _rate_ok():
    return RateLimitResult(
        allowed=True,
        current_count=1,
        limit=60,
        remaining=59,
        retry_after=0,
        reset_at=9999999999,
    )


# ---------------------------------------------------------------------------
# Header injection attacks
# ---------------------------------------------------------------------------


class TestHeaderInjection:
    """Authorization header injection — newlines, nulls, oversized values."""

    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_newline_in_bearer_token(self, mock_session, mock_svc):
        """CRLF injection in Authorization header must not crash or leak."""
        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = lambda s: session_ctx
        mock_session.return_value.__exit__ = lambda s, *a: None
        mock_svc.authenticate_api_key.return_value = None

        app = Starlette(routes=[Route("/api/test", unscoped_handler)])
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get(
            "/api/test",
            headers={"Authorization": "Bearer etf_key\r\nX-Injected: true"},
        )
        assert resp.status_code == 401
        assert "X-Injected" not in resp.headers

    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_null_byte_in_bearer_token(self, mock_session, mock_svc):
        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = lambda s: session_ctx
        mock_session.return_value.__exit__ = lambda s, *a: None
        mock_svc.authenticate_api_key.return_value = None

        app = Starlette(routes=[Route("/api/test", unscoped_handler)])
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get(
            "/api/test",
            headers={"Authorization": "Bearer etf_key\x00malicious"},
        )
        assert resp.status_code == 401

    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_oversized_bearer_token(self, mock_session, mock_svc):
        """Extremely long token should be rejected, not OOM."""
        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = lambda s: session_ctx
        mock_session.return_value.__exit__ = lambda s, *a: None
        mock_svc.authenticate_api_key.return_value = None

        app = Starlette(routes=[Route("/api/test", unscoped_handler)])
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get(
            "/api/test",
            headers={"Authorization": f"Bearer {'A' * 100000}"},
        )
        assert resp.status_code == 401

    def test_bearer_case_sensitivity(self):
        """Only 'Bearer ' (capital B) should be accepted, not 'bearer '."""
        app = Starlette(routes=[Route("/api/test", unscoped_handler)])
        client = TestClient(app)
        resp = client.get(
            "/api/test",
            headers={"Authorization": "bearer etf_something"},
        )
        assert resp.status_code == 401

    def test_empty_bearer_value(self):
        """'Bearer ' with no key after it."""
        app = Starlette(routes=[Route("/api/test", unscoped_handler)])
        client = TestClient(app)
        resp = client.get(
            "/api/test",
            headers={"Authorization": "Bearer "},
        )
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Rate limit bypass attempts
# ---------------------------------------------------------------------------


class TestRateLimitBypass:
    def test_keys_from_same_org_have_independent_limits(self, svc):
        """Different API keys in the same org get independent counters (by design).
        This is not a bypass — it's expected behavior. Verify it works correctly."""
        for _ in range(10):
            svc.check_rate_limit(api_key_id=1, org_id=10, limit_rpm=10)
        # Key 1 is at limit
        r1 = svc.check_rate_limit(api_key_id=1, org_id=10, limit_rpm=10)
        assert r1.allowed is False
        # Key 2 in the same org still has full quota
        r2 = svc.check_rate_limit(api_key_id=2, org_id=10, limit_rpm=10)
        assert r2.allowed is True

    def test_counter_increments_even_when_blocked(self, svc):
        """Blocked requests must still increment the counter to prevent
        attackers from timing retries to reset their count."""
        for _ in range(10):
            svc.check_rate_limit(api_key_id=1, org_id=10, limit_rpm=10)
        # Now over limit — send 5 more
        for _ in range(5):
            result = svc.check_rate_limit(api_key_id=1, org_id=10, limit_rpm=10)
        assert result.current_count == 15
        assert result.allowed is False

    def test_burst_and_minute_limits_work_together(self, svc):
        """Burst limit should block even when minute limit isn't reached."""
        # Burst of 3/sec, but minute allows 100
        for _ in range(3):
            svc.check_rate_limit(api_key_id=1, org_id=10, limit_rpm=100, burst_per_sec=3)
        result = svc.check_rate_limit(
            api_key_id=1,
            org_id=10,
            limit_rpm=100,
            burst_per_sec=3,
        )
        assert result.allowed is False
        assert result.current_count <= 100  # minute limit not reached


# ---------------------------------------------------------------------------
# Redis failure scenarios
# ---------------------------------------------------------------------------


class TestRedisFailure:
    def test_redis_connection_error_propagates(self):
        """When Redis is down, rate limiting should raise, not silently allow."""
        broken_redis = MagicMock()
        broken_redis.pipeline.side_effect = ConnectionError("Redis is down")
        svc = RateLimitService(redis_client=broken_redis)
        with pytest.raises(ConnectionError):
            svc.check_rate_limit(api_key_id=1, org_id=10, limit_rpm=60)

    def test_redis_timeout_propagates(self):
        """Redis timeout should not silently pass requests through."""
        broken_redis = MagicMock()
        broken_redis.pipeline.side_effect = TimeoutError("Redis timeout")
        svc = RateLimitService(redis_client=broken_redis)
        with pytest.raises(TimeoutError):
            svc.check_rate_limit(api_key_id=1, org_id=10, limit_rpm=60)


# ---------------------------------------------------------------------------
# Scope enforcement at middleware level
# ---------------------------------------------------------------------------


class TestScopeEnforcement:
    @patch("datanika.services.api_middleware._rate_limit_svc")
    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_scoped_endpoint_rejects_wrong_scope(self, mock_session, mock_svc, mock_rl):
        """Middleware passes required_scope to authenticate — key with wrong scope is rejected."""
        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = lambda s: session_ctx
        mock_session.return_value.__exit__ = lambda s, *a: None
        # authenticate_api_key returns None when scope doesn't match
        mock_svc.authenticate_api_key.return_value = None

        app = Starlette(routes=[Route("/api/test", scoped_handler)])
        client = TestClient(app)
        resp = client.get("/api/test", headers={"Authorization": "Bearer etf_valid"})
        assert resp.status_code == 401
        mock_svc.authenticate_api_key.assert_called_once_with(
            session_ctx,
            "etf_valid",
            required_scope="pipeline:read",
        )

    @patch("datanika.services.api_middleware._rate_limit_svc")
    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_unscoped_endpoint_passes_none_scope(self, mock_session, mock_svc, mock_rl):
        """Endpoints without required_scope pass None, allowing any key."""
        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = lambda s: session_ctx
        mock_session.return_value.__exit__ = lambda s, *a: None
        mock_svc.authenticate_api_key.return_value = _fake_api_key()
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = _rate_ok()

        app = Starlette(routes=[Route("/api/test", unscoped_handler)])
        client = TestClient(app)
        resp = client.get("/api/test", headers={"Authorization": "Bearer etf_valid"})
        assert resp.status_code == 200
        mock_svc.authenticate_api_key.assert_called_once_with(
            session_ctx,
            "etf_valid",
            required_scope=None,
        )


# ---------------------------------------------------------------------------
# Error response info leakage
# ---------------------------------------------------------------------------


class TestInfoLeakage:
    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_401_does_not_reveal_key_existence(self, mock_session, mock_svc):
        """Both invalid and expired keys should return the same generic error."""
        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = lambda s: session_ctx
        mock_session.return_value.__exit__ = lambda s, *a: None
        mock_svc.authenticate_api_key.return_value = None

        app = Starlette(routes=[Route("/api/test", unscoped_handler)])
        client = TestClient(app)
        r1 = client.get("/api/test", headers={"Authorization": "Bearer etf_nonexistent"})
        r2 = client.get("/api/test", headers={"Authorization": "Bearer etf_different"})
        # Same status and same error structure
        assert r1.status_code == r2.status_code == 401
        assert r1.json().keys() == r2.json().keys()
        assert r1.json()["error"] == r2.json()["error"]

    @patch("datanika.services.api_middleware._rate_limit_svc")
    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_429_does_not_reveal_org_or_key_id(self, mock_session, mock_svc, mock_rl):
        """Rate limit error should not include org_id or api_key_id."""
        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = lambda s: session_ctx
        mock_session.return_value.__exit__ = lambda s, *a: None
        mock_svc.authenticate_api_key.return_value = _fake_api_key(key_id=42, org_id=99)
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = RateLimitResult(
            allowed=False,
            current_count=61,
            limit=60,
            remaining=0,
            retry_after=30,
            reset_at=9999999999,
        )

        app = Starlette(routes=[Route("/api/test", unscoped_handler)])
        client = TestClient(app)
        resp = client.get("/api/test", headers={"Authorization": "Bearer etf_valid"})
        body = resp.text
        assert "42" not in body or "key_id" not in body  # key_id not in error
        assert "99" not in body or "org_id" not in body  # org_id not in error

    @patch("datanika.services.api_middleware._rate_limit_svc")
    @patch("datanika.services.api_middleware._api_key_svc")
    @patch("datanika.services.api_middleware._get_session")
    def test_500_does_not_reveal_stack_trace(self, mock_session, mock_svc, mock_rl):
        """Handler errors should return generic 500, not stack traces."""
        session_ctx = MagicMock()
        mock_session.return_value.__enter__ = lambda s: session_ctx
        mock_session.return_value.__exit__ = lambda s, *a: None
        mock_svc.authenticate_api_key.return_value = _fake_api_key()
        mock_rl.get_limit_for_org.return_value = 60
        mock_rl.check_rate_limit.return_value = _rate_ok()

        @api_endpoint()
        async def crashing_handler(request, api_key, session):
            raise RuntimeError("database credential leak here")

        app = Starlette(routes=[Route("/api/test", crashing_handler)])
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.get("/api/test", headers={"Authorization": "Bearer etf_valid"})
        assert resp.status_code == 500
        assert "credential" not in resp.text
        assert "Traceback" not in resp.text
        assert resp.json()["error"] == "Internal server error"
