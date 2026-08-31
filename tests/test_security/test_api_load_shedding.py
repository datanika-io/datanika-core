"""The API limiter must shed load *before* it costs a database session (#774).

QA measured two holes on production, 2026-08-31 (`plans/qa/notes/probe-705/`):

* **an invalid API key was never rate limited at all** — 40 of 40 requests
  answered 401, every one of them after a session checkout, a sha256 and an
  indexed ``SELECT``. Nothing counted them, so there was nothing to exceed;
* **a 429 cost exactly what a 200 cost** — 9 Redis commands and 2 DB sessions
  either way, because the limiter ran *after* authentication and after the
  cloud hook's own session. A client hammering at 10x its limit generated 10x
  the load and discarded all of it.

That second shape is the documented mechanism of Runs 7/8 in
``plans/infra/LOAD_TEST_BASELINE_2026-04-21.md``, where a 429 storm pegged
``max_connections`` with 99 connections idle in ``ClientRead`` and blocked even
superuser ``psql`` for 50+ minutes after the run ended.

Every test here asserts on **what the request cost**, not only on its status
code. A limiter that answers 429 while still opening a session has not shed
anything, and a status-only assertion cannot tell the two apart — which is
precisely how this survived: the 429s were correct, and expensive.
"""

import hashlib
import time as real_time
from unittest.mock import MagicMock, patch

import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from datanika.config import settings
from datanika.services.api_middleware import api_endpoint
from datanika.services.rate_limit_service import RateLimitService

# ---------------------------------------------------------------------------
# A Redis good enough to be wrong in the same ways
# ---------------------------------------------------------------------------


class FakeClock:
    """Wall clock the limiter reads, under the test's control.

    The window key is ``floor(now / window)``, so a test that cannot move the
    clock cannot distinguish "the refusal expired" from "the refusal was never
    recorded". Both look like a served request.
    """

    def __init__(self, start: float | None = None):
        self.now = float(start if start is not None else 1_800_000_000)

    def time(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeRedis:
    """INCR/EXPIRE/TTL/GET/MGET/SET with real TTL semantics and a call log.

    ``decode_responses=True`` in production, so reads return ``str``.
    """

    def __init__(self, clock: FakeClock):
        self._clock = clock
        self.store: dict[str, str] = {}
        self.expires: dict[str, float] = {}
        self.calls: list[tuple] = []

    # -- internals ---------------------------------------------------
    def _live(self, key: str) -> str | None:
        if key not in self.store:
            return None
        deadline = self.expires.get(key)
        if deadline is not None and deadline <= self._clock.time():
            self.store.pop(key, None)
            self.expires.pop(key, None)
            return None
        return self.store[key]

    def keys_alive(self) -> list[str]:
        return [k for k in list(self.store) if self._live(k) is not None]

    def count(self, verb: str) -> int:
        return sum(1 for c in self.calls if c[0] == verb)

    def round_trips(self) -> int:
        """Calls that cross the wire. A pipeline is one, whatever it holds."""
        return len(self.calls)

    # -- commands ----------------------------------------------------
    def get(self, key):
        self.calls.append(("get", key))
        return self._live(key)

    def mget(self, keys):
        keys = list(keys)
        self.calls.append(("mget", tuple(keys)))
        return [self._live(k) for k in keys]

    def set(self, key, value, ex=None):
        self.calls.append(("set", key, ex))
        self.store[key] = str(value)
        if ex is not None:
            self.expires[key] = self._clock.time() + ex
        return True

    def ttl(self, key):
        self.calls.append(("ttl", key))
        if self._live(key) is None:
            return -2
        deadline = self.expires.get(key)
        if deadline is None:
            return -1
        return int(deadline - self._clock.time())

    def pipeline(self):
        return FakePipeline(self)


class FakePipeline:
    def __init__(self, redis: FakeRedis):
        self._redis = redis
        self._ops: list[tuple] = []

    def incr(self, key):
        self._ops.append(("incr", key))
        return self

    def expire(self, key, seconds):
        self._ops.append(("expire", key, seconds))
        return self

    def get(self, key):
        self._ops.append(("get", key))
        return self

    def execute(self):
        self._redis.calls.append(("pipeline", tuple(op[:2] for op in self._ops)))
        results = []
        for op in self._ops:
            if op[0] == "incr":
                current = self._redis._live(op[1])
                value = (int(current) if current is not None else 0) + 1
                self._redis.store[op[1]] = str(value)
                results.append(value)
            elif op[0] == "expire":
                self._redis.expires[op[1]] = self._redis._clock.time() + op[2]
                results.append(True)
            else:
                results.append(self._redis._live(op[1]))
        self._ops = []
        return results


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def fake_redis(clock):
    return FakeRedis(clock)


@pytest.fixture
def limiter(fake_redis, clock):
    """The real service, on a fake Redis and a controllable clock."""
    with patch("datanika.services.rate_limit_service.time", clock):
        yield RateLimitService(redis_client=fake_redis)


# ---------------------------------------------------------------------------
# Middleware harness
# ---------------------------------------------------------------------------


@api_endpoint()
def sync_probe_handler(request, api_key, session):
    return JSONResponse({"ok": True, "path": "sync"})


@api_endpoint()
async def async_probe_handler(request, api_key, session):
    return JSONResponse({"ok": True, "path": "async"})


# ``api_endpoint`` has two duplicated bodies — ``_run_sync_handler`` for ``def``
# handlers (E12's threadpool path) and ``_run_async_handler`` for ``async def``.
# Every behavioural test below runs against **both**, because a shedding rule
# present in one and absent from the other is not a rule, and a suite that
# exercises one path cannot tell the difference. This is not hypothetical: an
# early revision of this file drove only the sync route, and deleting the shed
# from the async path left it entirely green.
HANDLER_PATHS = [("/api/sync", sync_probe_handler), ("/api/async", async_probe_handler)]


class SessionCounter:
    """Stands in for ``_get_session`` and counts checkouts.

    This is the measurement the issue is about. A request that answers 429 and
    still increments this has cost the pool exactly what a served request costs.
    """

    def __init__(self):
        self.opened = 0

    def __call__(self):
        self.opened += 1
        ctx = MagicMock()
        ctx.__enter__ = lambda _s: MagicMock()
        ctx.__exit__ = lambda _s, *_a: None
        return ctx


class Harness:
    def __init__(self, client, path, sessions, redis, clock, api_key_svc):
        self.client = client
        self.path = path
        self.sessions = sessions
        self.redis = redis
        self.clock = clock
        self.api_key_svc = api_key_svc

    def get(self, key: str, **headers):
        return self.client.get(
            self.path,
            headers={"Authorization": f"Bearer {key}", **headers},
        )


@pytest.fixture(params=[p for p, _ in HANDLER_PATHS], ids=["sync-handler", "async-handler"])
def harness(request, fake_redis, clock):
    """The real middleware and the real limiter; only the DB is faked.

    Parametrised over both handler paths — see ``HANDLER_PATHS``.
    """
    sessions = SessionCounter()
    api_key_svc = MagicMock()
    api_key_svc.authenticate_api_key.return_value = None  # every key invalid by default

    real_limiter = RateLimitService(redis_client=fake_redis)
    app = Starlette(routes=[Route(path, handler) for path, handler in HANDLER_PATHS])

    with (
        patch("datanika.services.rate_limit_service.time", clock),
        patch("datanika.services.api_middleware._get_session", sessions),
        patch("datanika.services.api_middleware._api_key_svc", api_key_svc),
        patch("datanika.services.api_middleware._rate_limit_svc", real_limiter),
    ):
        yield Harness(TestClient(app), request.param, sessions, fake_redis, clock, api_key_svc)


def _valid_key(key_id: int = 1, org_id: int = 10):
    key = MagicMock()
    key.id = key_id
    key.org_id = org_id
    return key


# ---------------------------------------------------------------------------
# 1. The auth-failure path is rate limited
# ---------------------------------------------------------------------------


class TestInvalidKeysAreShed:
    def test_repeated_invalid_keys_are_eventually_refused(self, harness):
        """QA's measurement, inverted: 40 of 40 unthrottled must become bounded."""
        limit = settings.api_auth_failure_limit
        assert limit > 0, "the default must enforce something, or this test proves nothing"

        statuses = [harness.get("etf_nonexistent").status_code for _ in range(limit + 5)]

        assert statuses[:limit] == [401] * limit, "the budget must be spendable before it bites"
        assert statuses[limit] == 429, f"request {limit + 1} should be refused, got {statuses}"
        assert set(statuses[limit:]) == {429}

    def test_a_refused_request_opens_no_database_session(self, harness):
        """The whole point. A 429 that still checks out a session sheds nothing."""
        limit = settings.api_auth_failure_limit
        for _ in range(limit):
            harness.get("etf_nonexistent")

        before = harness.sessions.opened
        for _ in range(20):
            assert harness.get("etf_nonexistent").status_code == 429
        assert harness.sessions.opened == before, (
            "refused requests still opened database sessions — "
            f"{harness.sessions.opened - before} of them"
        )

    def test_a_refused_request_never_reaches_the_key_lookup(self, harness):
        """Independent channel: the session count and the SELECT can drift apart."""
        limit = settings.api_auth_failure_limit
        for _ in range(limit):
            harness.get("etf_nonexistent")

        harness.api_key_svc.authenticate_api_key.reset_mock()
        for _ in range(10):
            harness.get("etf_nonexistent")
        assert harness.api_key_svc.authenticate_api_key.call_count == 0

    def test_a_valid_key_is_still_served_while_another_credential_is_refused(self, harness):
        """Negative control. A deny-everything limiter passes the test above."""
        limit = settings.api_auth_failure_limit
        for _ in range(limit + 5):
            harness.get("etf_nonexistent")

        harness.api_key_svc.authenticate_api_key.return_value = _valid_key()
        resp = harness.get("etf_good")
        assert resp.status_code == 200, "a valid credential must not inherit another's refusal"

    def test_the_budget_is_per_credential(self, harness):
        """One bad key must not spend a different bad key's budget."""
        limit = settings.api_auth_failure_limit
        for _ in range(limit + 5):
            harness.get("etf_first_bad_key")

        assert harness.get("etf_second_bad_key").status_code == 401

    def test_the_budget_refills_with_the_window(self, harness):
        limit = settings.api_auth_failure_limit
        for _ in range(limit + 1):
            harness.get("etf_nonexistent")
        assert harness.get("etf_nonexistent").status_code == 429

        harness.clock.advance(settings.api_auth_failure_window_seconds + 1)
        assert harness.get("etf_nonexistent").status_code == 401

    def test_the_refusal_states_a_retry_after(self, harness):
        limit = settings.api_auth_failure_limit
        for _ in range(limit):
            harness.get("etf_nonexistent")
        resp = harness.get("etf_nonexistent")
        assert resp.status_code == 429
        assert "Retry-After" in resp.headers
        assert 1 <= int(resp.headers["Retry-After"]) <= settings.api_auth_failure_window_seconds

    def test_the_refusal_describes_no_entitlement(self, harness):
        """Pre-auth we do not know the caller, so we must not state their plan."""
        limit = settings.api_auth_failure_limit
        for _ in range(limit):
            harness.get("etf_nonexistent")
        resp = harness.get("etf_nonexistent")
        assert resp.status_code == 429
        assert not [h for h in resp.headers if h.lower().startswith("x-ratelimit")]

    def test_the_keyspace_never_contains_the_raw_credential(self, harness):
        """Same reasoning as ``PasswordResetService.email_bucket`` (core#623 D5).

        Keying on the presented secret turns the Redis keyspace into a list of
        live credentials, readable by anyone who can read Redis — a much lower
        bar than reading the database.
        """
        harness.get("etf_super_secret_value")
        joined = "\n".join(harness.redis.keys_alive())
        assert "etf_super_secret_value" not in joined
        digest = hashlib.sha256(b"etf_super_secret_value").hexdigest()
        assert digest in joined, "the credential bucket must still be derived from the key"


# ---------------------------------------------------------------------------
# 2. Rotating credentials from one client
# ---------------------------------------------------------------------------


class TestClientAddressBudget:
    def test_rotating_credentials_from_one_address_are_bounded(self, harness):
        """A per-credential budget alone is defeated by a fresh key each time."""
        limit = settings.api_auth_failure_ip_limit
        assert limit > 0

        statuses = [
            harness.get(f"etf_rotating_{i}", **{"CF-Connecting-IP": "203.0.113.7"}).status_code
            for i in range(limit + 3)
        ]
        assert statuses[:limit] == [401] * limit
        assert statuses[limit] == 429, statuses

    def test_a_different_address_keeps_its_own_budget(self, harness):
        limit = settings.api_auth_failure_ip_limit
        for i in range(limit + 3):
            harness.get(f"etf_rotating_{i}", **{"CF-Connecting-IP": "203.0.113.7"})

        resp = harness.get("etf_someone_else", **{"CF-Connecting-IP": "198.51.100.9"})
        assert resp.status_code == 401, "one address must not lock out another"

    def test_an_unnameable_client_is_not_bucketed_at_all(self, harness):
        """``services/client_ip.py``'s contract, and the reason it exists.

        Production is Cloudflare → Apache → 127.0.0.1, so a limiter that trusts
        the socket peer collapses the entire internet into one bucket. When the
        client cannot be named the address bucket must be *skipped*, not filled
        with a placeholder.
        """
        limit = settings.api_auth_failure_ip_limit
        ambiguous = {"X-Forwarded-For": "10.0.0.1, 10.0.0.2, 10.0.0.3"}
        for i in range(limit + 3):
            harness.get(f"etf_rotating_{i}", **ambiguous)

        assert harness.get("etf_yet_another", **ambiguous).status_code == 401
        assert not [k for k in harness.redis.keys_alive() if ":ip:" in k]

    def test_a_valid_key_from_a_spent_address_is_still_served(self, harness):
        """Negative control for the address bucket.

        The address budget counts *failures* only, so a legitimate caller
        sharing a NAT with a broken one keeps working right up until the
        address itself is over budget — and this asserts the common case.
        """
        for i in range(settings.api_auth_failure_ip_limit - 1):
            harness.get(f"etf_rotating_{i}", **{"CF-Connecting-IP": "203.0.113.7"})

        harness.api_key_svc.authenticate_api_key.return_value = _valid_key()
        resp = harness.get("etf_good", **{"CF-Connecting-IP": "203.0.113.7"})
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# 3. A valid key over its plan limit
# ---------------------------------------------------------------------------


class TestOverLimitValidKeyIsShed:
    def test_the_second_over_limit_request_costs_no_database_session(self, harness):
        """Run 7/8's mechanism. That storm needed a *valid* key.

        The first rejection has to pay for itself — we only learn the limit by
        authenticating. Every rejection after it must not.
        """
        harness.api_key_svc.authenticate_api_key.return_value = _valid_key()

        # settings.api_rate_limit_burst in one second → the next is refused.
        for _ in range(settings.api_rate_limit_burst):
            assert harness.get("etf_good").status_code == 200
        assert harness.get("etf_good").status_code == 429

        before = harness.sessions.opened
        for _ in range(10):
            assert harness.get("etf_good").status_code == 429
        assert harness.sessions.opened == before, (
            "an already-refused key still opened database sessions — "
            f"{harness.sessions.opened - before} of them"
        )

    def test_the_refusal_lifts_when_the_window_moves(self, harness):
        """A penalty box that outlives its window is an outage, not a limiter."""
        harness.api_key_svc.authenticate_api_key.return_value = _valid_key()
        for _ in range(settings.api_rate_limit_burst):
            harness.get("etf_good")
        assert harness.get("etf_good").status_code == 429

        harness.clock.advance(2)
        assert harness.get("etf_good").status_code == 200

    def test_one_keys_refusal_does_not_refuse_another(self, harness):
        harness.api_key_svc.authenticate_api_key.return_value = _valid_key(key_id=1)
        for _ in range(settings.api_rate_limit_burst + 1):
            harness.get("etf_key_one")

        harness.api_key_svc.authenticate_api_key.return_value = _valid_key(key_id=2)
        assert harness.get("etf_key_two").status_code == 200


# ---------------------------------------------------------------------------
# 4. The limiter's own cost
# ---------------------------------------------------------------------------


class TestEnforcementCost:
    def test_an_allowed_check_issues_no_discarded_ttl_round_trip(self, limiter, fake_redis):
        """QA counted 200 TTL calls for 200 admitted requests, all discarded.

        ``retry_after`` is ``reset_at - now``; the round trip bought nothing.
        """
        for _ in range(5):
            limiter.check_window("some-bucket", limit=100, window_seconds=60)
        assert fake_redis.count("ttl") == 0

    def test_retry_after_never_exceeds_the_window(self, limiter, clock):
        """The old value came from a TTL set to ``window * 2``.

        So a 60-second window told clients to wait up to 120 seconds.
        """
        for _ in range(4):
            result = limiter.check_window("b", limit=3, window_seconds=60)
        assert result.allowed is False
        assert 1 <= result.retry_after <= 60
        assert result.retry_after == result.reset_at - int(clock.time())

    def test_the_plan_limit_is_not_re_read_from_the_hook_every_request(self, limiter, fake_redis):
        """89% of enforcement cost was a DB session re-reading a monthly value.

        Measured on the production box: 4.20 ms of a 4.74 ms total
        (`plans/qa/notes/probe-705/` finding 5).
        """
        assert settings.api_plan_limit_cache_seconds > 0
        calls: list[int] = []

        def handler(*, context, **_kw):
            calls.append(context["org_id"])
            context["limit_rpm"] = 123

        from datanika.hooks import off, on

        on("api.get_rate_limit", handler)
        try:
            limits = [limiter.get_limit_for_org(77) for _ in range(10)]
        finally:
            off("api.get_rate_limit", handler)

        assert limits == [123] * 10, "the cache must return the hook's answer, not the default"
        assert len(calls) == 1, f"the hook ran {len(calls)} times for one org"

    def test_the_cached_plan_limit_expires(self, limiter, clock):
        from datanika.hooks import off, on

        answers = iter([30, 300])

        def handler(*, context, **_kw):
            context["limit_rpm"] = next(answers)

        on("api.get_rate_limit", handler)
        try:
            assert limiter.get_limit_for_org(88) == 30
            clock.advance(settings.api_plan_limit_cache_seconds + 1)
            assert limiter.get_limit_for_org(88) == 300
        finally:
            off("api.get_rate_limit", handler)

    def test_orgs_do_not_share_a_cached_limit(self, limiter):
        from datanika.hooks import off, on

        def handler(*, context, **_kw):
            context["limit_rpm"] = 10 * context["org_id"]

        on("api.get_rate_limit", handler)
        try:
            assert limiter.get_limit_for_org(1) == 10
            assert limiter.get_limit_for_org(2) == 20
        finally:
            off("api.get_rate_limit", handler)

    def test_the_preauth_check_is_one_round_trip(self, limiter, fake_redis):
        """It runs on every request including served ones; it must stay cheap."""
        fake_redis.calls.clear()
        limiter.preauth_check(
            credential=RateLimitService.credential_bucket("etf_x"),
            client=RateLimitService.client_bucket("203.0.113.1"),
            window_seconds=60,
            credential_failure_limit=10,
            client_failure_limit=60,
        )
        assert fake_redis.round_trips() == 1, fake_redis.calls


# ---------------------------------------------------------------------------
# 5. Failure modes
# ---------------------------------------------------------------------------


class TestShedLayerIsAdvisory:
    """A Redis outage must not make the shed layer *stricter* than the limiter.

    The authoritative window (``check_window``) fails **closed** and stays that
    way — ``test_rate_limit_buckets.py::test_redis_failure_still_propagates``
    pins it. The pre-auth layer is an optimisation on top: when it cannot read
    Redis it must fall through to the behaviour we had before this change,
    rather than invent a refusal or a 500.
    """

    def test_a_broken_redis_does_not_turn_a_401_into_a_500(self, clock):
        broken = MagicMock()
        broken.mget.side_effect = ConnectionError("Redis is down")
        broken.pipeline.side_effect = ConnectionError("Redis is down")

        sessions = SessionCounter()
        api_key_svc = MagicMock()
        api_key_svc.authenticate_api_key.return_value = None
        app = Starlette(routes=[Route(path, handler) for path, handler in HANDLER_PATHS])

        with (
            patch("datanika.services.rate_limit_service.time", clock),
            patch("datanika.services.api_middleware._get_session", sessions),
            patch("datanika.services.api_middleware._api_key_svc", api_key_svc),
            patch(
                "datanika.services.api_middleware._rate_limit_svc",
                RateLimitService(redis_client=broken),
            ),
        ):
            client = TestClient(app)
            statuses = [
                client.get(path, headers={"Authorization": "Bearer etf_bad"}).status_code
                for path, _ in HANDLER_PATHS
            ]
        assert statuses == [401, 401]

    def test_the_authoritative_window_still_fails_closed(self, clock):
        broken = MagicMock()
        broken.pipeline.side_effect = ConnectionError("Redis is down")
        with (
            patch("datanika.services.rate_limit_service.time", clock),
            pytest.raises(ConnectionError),
        ):
            RateLimitService(redis_client=broken).check_window("b", limit=1)


def test_the_module_clock_patch_is_load_bearing(clock):
    """Guard the harness itself: if the patch stops taking, every window test
    silently starts measuring the real clock and the expiry assertions become
    unfalsifiable."""
    with patch("datanika.services.rate_limit_service.time", clock):
        from datanika.services import rate_limit_service

        assert rate_limit_service.time.time() == clock.time()
        assert clock.time() != pytest.approx(real_time.time(), abs=1)
