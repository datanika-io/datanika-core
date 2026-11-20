"""The API allowance is one budget per account (core#706).

Two keys of one org share the minute window and the per-second ceiling.
Two orgs do not. Auth-failure shedding stays on the credential bucket.
"""

from unittest.mock import MagicMock, patch

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from datanika.config import settings
from datanika.services.api_middleware import api_endpoint
from datanika.services.rate_limit_service import RateLimitService


@api_endpoint(required_scope="pipeline:read")
async def _async_handler(request, api_key, session):
    return JSONResponse({"ok": True, "org_id": api_key.org_id})


@api_endpoint(required_scope="pipeline:read")
def _sync_handler(request, api_key, session):
    return JSONResponse({"ok": True, "org_id": api_key.org_id, "sync": True})


def _key(key_id: int, org_id: int) -> MagicMock:
    key = MagicMock()
    key.id = key_id
    key.org_id = org_id
    key.name = f"key-{key_id}"
    return key


def _fake_redis():
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
            return False

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

    redis = MagicMock()
    redis.pipeline.side_effect = lambda: FakePipeline()
    redis.mget.side_effect = lambda keys: [None] * len(keys)
    redis.set.return_value = True
    redis._store = store
    return redis


class _RecordingLimiter(RateLimitService):
    def __init__(self, redis_client, limit_rpm: int):
        super().__init__(redis_client=redis_client)
        self.limit_rpm = limit_rpm
        self.buckets: list[str] = []

    def get_limit_for_org(self, org_id: int) -> int:
        return self.limit_rpm

    def check_rate_limit(self, bucket, org_id, limit_rpm, burst_per_sec=None):
        self.buckets.append(bucket)
        return super().check_rate_limit(bucket, org_id, limit_rpm, burst_per_sec=burst_per_sec)


def _statuses(handler, path: str, limiter: _RecordingLimiter, tokens: list[str]) -> list[int]:
    keys = {
        "etf_org1_a": _key(11, 1),
        "etf_org1_b": _key(22, 1),
        "etf_org2": _key(33, 2),
    }

    def authenticate(session, raw_key, required_scope=None):
        return keys[raw_key]

    session = MagicMock()
    with (
        patch("datanika.services.api_middleware._rate_limit_svc", limiter),
        patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
        patch("datanika.services.api_middleware._get_session") as mock_session,
    ):
        mock_svc.authenticate_api_key.side_effect = authenticate
        mock_session.return_value.__enter__ = MagicMock(return_value=session)
        mock_session.return_value.__exit__ = MagicMock(return_value=False)
        app = Starlette(routes=[Route(path, handler)])
        client = TestClient(app)
        return [
            client.get(path, headers={"Authorization": f"Bearer {token}"}).status_code
            for token in tokens
        ]


class TestAccountAllowance:
    def test_two_keys_of_one_org_share_one_minute_and_two_orgs_do_not(self, monkeypatch):
        monkeypatch.setattr(settings, "api_rate_limit_burst", 10)
        limiter = _RecordingLimiter(_fake_redis(), limit_rpm=1)
        tokens = ["etf_org1_a", "etf_org1_b", "etf_org2"]

        assert _statuses(_async_handler, "/async", limiter, tokens) == [200, 429, 200]
        assert limiter.buckets == ["org:1", "org:1", "org:2"]
        assert "11" not in limiter.buckets
        assert "22" not in limiter.buckets

        limiter = _RecordingLimiter(_fake_redis(), limit_rpm=1)
        assert _statuses(_sync_handler, "/sync", limiter, tokens) == [200, 429, 200]
        assert limiter.buckets == ["org:1", "org:1", "org:2"]

    def test_the_per_second_ceiling_uses_the_same_account_bucket(self, monkeypatch):
        monkeypatch.setattr(settings, "api_rate_limit_burst", 1)
        limiter = _RecordingLimiter(_fake_redis(), limit_rpm=30)
        tokens = ["etf_org1_a", "etf_org1_b", "etf_org2"]

        assert _statuses(_async_handler, "/async", limiter, tokens) == [200, 429, 200]
        assert limiter.buckets == ["org:1", "org:1", "org:2"]

        limiter = _RecordingLimiter(_fake_redis(), limit_rpm=30)
        assert _statuses(_sync_handler, "/sync", limiter, tokens) == [200, 429, 200]
        assert limiter.buckets == ["org:1", "org:1", "org:2"]
