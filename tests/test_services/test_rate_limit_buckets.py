"""RateLimitService generalised from API keys to named buckets (D5).

``check_rate_limit`` keyed its Redis entry on an ``api_key_id: int``, which is
the only thing it ever did with that parameter. Password reset needs the same
sliding window on a different subject and a different period, so the parameter
becomes a ``bucket: str`` and the window becomes explicit — rather than a second
limiter growing up beside the first.

The API-key behaviour must be **byte-identical** afterwards, key format
included; those assertions are the point of this file as much as the new ones.
"""

import time
from unittest.mock import MagicMock

import pytest

from datanika.services.rate_limit_service import RateLimitService


class FakeRedis:
    """Minimal INCR/EXPIRE/TTL store with a real pipeline."""

    def __init__(self):
        self.store: dict[str, int] = {}
        self.ttls: dict[str, int] = {}

    def pipeline(self):
        return FakePipeline(self)

    def ttl(self, key):
        return self.ttls.get(key, -1)


class FakePipeline:
    def __init__(self, redis):
        self._redis = redis
        self._ops = []

    def incr(self, key):
        self._ops.append(("incr", key))
        return self

    def expire(self, key, seconds):
        self._ops.append(("expire", key, seconds))
        return self

    def execute(self):
        results = []
        for op in self._ops:
            if op[0] == "incr":
                self._redis.store[op[1]] = self._redis.store.get(op[1], 0) + 1
                results.append(self._redis.store[op[1]])
            else:
                self._redis.ttls[op[1]] = op[2]
                results.append(True)
        self._ops = []
        return results


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.fixture
def svc(fake_redis):
    return RateLimitService(redis_client=fake_redis)


class TestApiKeyBehaviourIsUnchanged:
    def test_the_redis_key_format_is_preserved(self, svc, fake_redis):
        """A changed key format silently resets everyone's window on deploy."""
        svc.check_rate_limit(bucket="42", org_id=1, limit_rpm=60)
        expected = f"rl:42:{int(time.time()) // 60}"
        assert expected in fake_redis.store

    def test_allows_under_the_limit(self, svc):
        for _ in range(9):
            svc.check_rate_limit(bucket="1", org_id=1, limit_rpm=10)
        assert svc.check_rate_limit(bucket="1", org_id=1, limit_rpm=10).allowed is True

    def test_blocks_over_the_limit(self, svc):
        for _ in range(10):
            svc.check_rate_limit(bucket="1", org_id=1, limit_rpm=10)
        assert svc.check_rate_limit(bucket="1", org_id=1, limit_rpm=10).allowed is False

    def test_buckets_are_independent(self, svc):
        for _ in range(10):
            svc.check_rate_limit(bucket="1", org_id=1, limit_rpm=10)
        assert svc.check_rate_limit(bucket="2", org_id=1, limit_rpm=10).allowed is True

    def test_burst_key_format_is_preserved(self, svc, fake_redis):
        svc.check_rate_limit(bucket="42", org_id=1, limit_rpm=60, burst_per_sec=5)
        assert any(k.startswith("rl:42:s:") for k in fake_redis.store)

    def test_redis_failure_still_propagates(self):
        """Fail closed: a Redis outage must not silently pass requests through."""
        broken = MagicMock()
        broken.pipeline.side_effect = ConnectionError("Redis is down")
        with pytest.raises(ConnectionError):
            RateLimitService(redis_client=broken).check_rate_limit(
                bucket="1", org_id=1, limit_rpm=60
            )


class TestNamedBucketsAndWindows:
    def test_a_string_bucket_is_used_verbatim(self, svc, fake_redis):
        svc.check_window("pwreset:email:abc123", limit=3, window_seconds=3600)
        assert any(k.startswith("rl:pwreset:email:abc123:") for k in fake_redis.store)

    def test_an_hour_window_partitions_by_the_hour(self, svc, fake_redis):
        svc.check_window("pwreset:email:abc123", limit=3, window_seconds=3600)
        expected = f"rl:pwreset:email:abc123:{int(time.time()) // 3600}"
        assert expected in fake_redis.store

    def test_three_per_hour_allows_three_and_blocks_the_fourth(self, svc):
        results = [
            svc.check_window("pwreset:email:abc", limit=3, window_seconds=3600) for _ in range(4)
        ]
        assert [r.allowed for r in results] == [True, True, True, False]

    def test_over_limit_requests_are_still_counted(self, svc, fake_redis):
        """Counting the blocked ones is what stops a caller manipulating the window."""
        for _ in range(6):
            svc.check_window("pwreset:ip:1.2.3.4", limit=3, window_seconds=3600)
        key = f"rl:pwreset:ip:1.2.3.4:{int(time.time()) // 3600}"
        assert fake_redis.store[key] == 6

    def test_ttl_covers_the_window_with_overlap(self, svc, fake_redis):
        svc.check_window("pwreset:email:abc", limit=3, window_seconds=3600)
        key = f"rl:pwreset:email:abc:{int(time.time()) // 3600}"
        assert fake_redis.ttls[key] >= 3600

    def test_different_buckets_do_not_share_a_window(self, svc):
        for _ in range(4):
            svc.check_window("pwreset:email:aaa", limit=3, window_seconds=3600)
        assert svc.check_window("pwreset:email:bbb", limit=3, window_seconds=3600).allowed

    def test_the_result_reports_the_limit_it_enforced(self, svc):
        r = svc.check_window("pwreset:email:abc", limit=3, window_seconds=3600)
        assert r.limit == 3
        assert r.remaining == 2


class TestEmailBucketKeyIsHashed:
    """D5: the Redis keyspace must not become a readable list of accounts."""

    def test_the_helper_hashes_the_address(self):
        from datanika.services.password_reset_service import PasswordResetService

        bucket = PasswordResetService.email_bucket("Alice@Example.com")
        assert "alice@example.com" not in bucket
        assert "Alice" not in bucket
        assert bucket.startswith("pwreset:email:")

    def test_normalisation_means_one_bucket_per_account(self):
        from datanika.services.password_reset_service import PasswordResetService

        assert PasswordResetService.email_bucket("Alice@Example.com  ") == (
            PasswordResetService.email_bucket("alice@example.com")
        )

    def test_different_addresses_get_different_buckets(self):
        from datanika.services.password_reset_service import PasswordResetService

        assert PasswordResetService.email_bucket("a@example.com") != (
            PasswordResetService.email_bucket("b@example.com")
        )
