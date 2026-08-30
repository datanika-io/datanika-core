"""TDD tests for RateLimitService — Redis-based API rate limiting."""

from unittest.mock import MagicMock

import pytest

from datanika.services.rate_limit_service import (
    RateLimitError,
    RateLimitResult,
    RateLimitService,
)


@pytest.fixture
def mock_redis():
    """Fake Redis that implements the subset we use (pipeline with incr/expire/execute)."""
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
    redis_mock._store = store
    redis_mock._ttls = ttls

    def ttl_side_effect(key):
        return int(ttls.get(key, 60))

    redis_mock.ttl.side_effect = ttl_side_effect
    return redis_mock


@pytest.fixture
def svc(mock_redis):
    return RateLimitService(redis_client=mock_redis)


class TestCheckRateLimit:
    def test_first_request_allowed(self, svc):
        result = svc.check_rate_limit(bucket="1", org_id=10, limit_rpm=60)
        assert isinstance(result, RateLimitResult)
        assert result.allowed is True
        assert result.current_count == 1
        assert result.limit == 60
        assert result.remaining == 59

    def test_under_limit_allowed(self, svc):
        for _ in range(29):
            svc.check_rate_limit(bucket="1", org_id=10, limit_rpm=30)
        result = svc.check_rate_limit(bucket="1", org_id=10, limit_rpm=30)
        assert result.allowed is True
        assert result.current_count == 30
        assert result.remaining == 0

    def test_over_limit_blocked(self, svc):
        for _ in range(30):
            svc.check_rate_limit(bucket="1", org_id=10, limit_rpm=30)
        result = svc.check_rate_limit(bucket="1", org_id=10, limit_rpm=30)
        assert result.allowed is False
        assert result.current_count == 31
        assert result.remaining == 0
        assert result.retry_after > 0

    def test_different_keys_independent(self, svc):
        for _ in range(30):
            svc.check_rate_limit(bucket="1", org_id=10, limit_rpm=30)
        result = svc.check_rate_limit(bucket="2", org_id=10, limit_rpm=30)
        assert result.allowed is True
        assert result.current_count == 1

    def test_burst_limit(self, svc):
        """Burst limit (per-second) blocks before minute limit if exceeded."""
        for _ in range(5):
            svc.check_rate_limit(bucket="1", org_id=10, limit_rpm=300, burst_per_sec=5)
        result = svc.check_rate_limit(bucket="1", org_id=10, limit_rpm=300, burst_per_sec=5)
        assert result.allowed is False


class TestRateLimitResult:
    def test_headers(self, svc):
        result = svc.check_rate_limit(bucket="1", org_id=10, limit_rpm=60)
        headers = result.headers()
        assert headers["X-RateLimit-Limit"] == "60"
        assert headers["X-RateLimit-Remaining"] == "59"
        assert "X-RateLimit-Reset" in headers

    def test_headers_when_blocked(self, svc):
        for _ in range(10):
            svc.check_rate_limit(bucket="1", org_id=10, limit_rpm=10)
        result = svc.check_rate_limit(bucket="1", org_id=10, limit_rpm=10)
        headers = result.headers()
        assert "Retry-After" in headers


class TestGetDefaultLimit:
    def test_returns_config_default(self, svc):
        """Core edition returns the configured default RPM."""
        limit = svc.get_limit_for_org(org_id=10)
        assert isinstance(limit, int)
        assert limit > 0


class TestRateLimitError:
    def test_is_value_error(self):
        err = RateLimitError("too fast", retry_after=30)
        assert isinstance(err, ValueError)
        assert err.retry_after == 30
        assert str(err) == "too fast"
