"""Tests for per-org task concurrency limiter."""

from unittest.mock import MagicMock, patch

import pytest

from datanika.services import concurrency_service


@pytest.fixture
def mock_redis():
    """Patch redis client with a fake in-memory store."""
    store = {}

    class FakeRedis:
        def get(self, key):
            return store.get(key)

        def set(self, key, value, ex=None):
            store[key] = str(value)

        def incr(self, key):
            val = int(store.get(key, 0)) + 1
            store[key] = str(val)
            return val

        def decr(self, key):
            val = int(store.get(key, 0)) - 1
            store[key] = str(val)
            return val

        def expire(self, key, seconds):
            pass

        def pipeline(self, transaction=False):
            return FakePipeline(self)

    class FakePipeline:
        def __init__(self, redis_instance):
            self._redis = redis_instance
            self._ops = []

        def incr(self, key):
            self._ops.append(("incr", key))

        def expire(self, key, seconds):
            self._ops.append(("expire", key, seconds))

        def execute(self):
            results = []
            for op in self._ops:
                if op[0] == "incr":
                    results.append(self._redis.incr(op[1]))
                elif op[0] == "expire":
                    self._redis.expire(op[1], op[2])
                    results.append(True)
            return results

    fake = FakeRedis()
    with patch.object(concurrency_service, "_redis_client", return_value=fake):
        yield fake, store


class TestConcurrencyService:
    def test_acquire_under_limit(self, mock_redis):
        """Should grant slot when under the limit."""
        with patch.object(concurrency_service, "get_max_parallel", return_value=5):
            assert concurrency_service.acquire(org_id=1) is True
            assert concurrency_service.running_count(1) == 1

    def test_acquire_at_limit(self, mock_redis):
        """Should deny slot when at the limit."""
        with patch.object(concurrency_service, "get_max_parallel", return_value=2):
            assert concurrency_service.acquire(org_id=1) is True
            assert concurrency_service.acquire(org_id=1) is True
            assert concurrency_service.acquire(org_id=1) is False
            assert concurrency_service.running_count(1) == 2

    def test_release_frees_slot(self, mock_redis):
        """After release, a new acquire should succeed."""
        with patch.object(concurrency_service, "get_max_parallel", return_value=1):
            assert concurrency_service.acquire(org_id=1) is True
            assert concurrency_service.acquire(org_id=1) is False
            concurrency_service.release(org_id=1)
            assert concurrency_service.acquire(org_id=1) is True

    def test_orgs_are_isolated(self, mock_redis):
        """Different orgs have independent counters."""
        with patch.object(concurrency_service, "get_max_parallel", return_value=1):
            assert concurrency_service.acquire(org_id=1) is True
            assert concurrency_service.acquire(org_id=2) is True
            assert concurrency_service.acquire(org_id=1) is False
            assert concurrency_service.acquire(org_id=2) is False

    def test_release_below_zero(self, mock_redis):
        """Release without prior acquire should not go below 0."""
        concurrency_service.release(org_id=1)
        assert concurrency_service.running_count(1) == 0

    def test_get_max_parallel_default(self):
        """Without hooks, returns DEFAULT_MAX_PARALLEL."""
        with patch("datanika.hooks.emit"):
            result = concurrency_service.get_max_parallel(org_id=1)
            assert result == concurrency_service.DEFAULT_MAX_PARALLEL
