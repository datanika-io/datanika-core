"""Bounding the ``/signup`` account-existence oracle (core#639).

Contract: ``docs/specs/SPEC_SIGNUP_ENUMERATION.md``. Numbered comments below
cite its acceptance criteria.

🚨 **A bound is not opacity, and these tests must not be read as saying it is.**
``/signup`` still answers *"does this address have an account?"* — deliberately,
per #128, because a signup form that refuses without saying why is worse for the
user. What changes is that the answer now costs a rate-limit budget instead of
being free and unlimited. A **targeted** single-address query is one request and
one request is under any limit worth having; only option 2 in the spec (accept
the submission either way and mail the address) closes that, and it stays
deferred. Do not close #639 on the strength of this file.

The limiter here is the **real** ``RateLimitService`` over a fake Redis store,
not a patched ``_allow``. That matters for two of the criteria: AC1 asserts the
boundary in both directions, which a patched predicate cannot show, and AC4
reads the key back out of the store, which requires a store to read.
"""

import re
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

import datanika.ui.state.auth_state as auth_state
from datanika.services.rate_limit_service import RateLimitService
from datanika.services.user_service import UserService, UserServiceError
from datanika.ui.state.auth_state import AuthState

REGISTERED = "alice@example.com"
UNREGISTERED = "nobody@example.com"


class FakeRedis:
    """Minimal INCR/EXPIRE store with a real pipeline.

    Copied from ``tests/test_services/test_rate_limit_buckets.py`` rather than
    imported, so a change made there for an API-key reason cannot silently
    weaken the signup assertions.
    """

    def __init__(self):
        self.store: dict[str, int] = {}
        self.ttls: dict[str, int] = {}

    def pipeline(self):
        return _FakePipeline(self)

    def ttl(self, key):
        return self.ttls.get(key, -1)


class _FakePipeline:
    def __init__(self, redis):
        self._redis = redis
        self._ops: list[tuple] = []

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


def _service(*, duplicate: bool = False, email: str = REGISTERED) -> MagicMock:
    """A ``UserService`` stand-in that records whether it was consulted at all.

    ``method_calls`` is the AC3 instrument: a refused request must not reach
    *any* method on this object, which is a stronger and more durable statement
    than "``get_user_by_email`` was not called" — ``register_user`` performs the
    lookup internally, so naming one method leaves the other as a way to lose
    the property silently.

    The return values are real objects rather than bare mocks so the **happy
    path actually completes**. With mocks, ``UserInfo(...)`` fails pydantic
    validation, the handler lands in its catch-all, and every assertion about
    an *allowed* signup is then satisfied by a signup that errored — which is
    the shape AC6 exists to rule out.
    """
    svc = MagicMock()
    user = SimpleNamespace(id=7, email=email, full_name="Alice")
    if duplicate:
        svc.register_user.side_effect = UserServiceError("Email already exists")
    else:
        svc.register_user.return_value = user
    svc.create_org.return_value = SimpleNamespace(id=3, name="Alice's Org", slug="org-7")
    svc.authenticate.return_value = {
        "access_token": "at",
        "refresh_token": "rt",
        "user": user,
    }
    return svc


def _state(svc: MagicMock, *, client_ip: str = "203.0.113.9") -> MagicMock:
    st = MagicMock()
    for name, field in AuthState.__fields__.items():
        default = field.default_factory() if field.default_factory else field.default
        setattr(st, name, default)
    st._get_user_service = lambda: svc
    # core#1081 — signup refuses a live session first. Bound explicitly because a
    # bare MagicMock would return a truthy mock and refuse every signup here.
    st._revalidate_session = lambda: False
    st._client_ip = lambda: client_ip
    st._post_auth_redirect_target = lambda: "/"
    # A bare MagicMock returns a truthy mock for any query parameter, which
    # sends the handler down the invite-acceptance branch on every call.
    st.router = MagicMock(page=MagicMock(params={}))
    return st


def _signup(st, email: str = REGISTERED):
    """Drive the real handler far enough to reach (or be refused before) the lookup."""
    with (
        patch.object(auth_state, "CaptchaService") as captcha,
        patch.object(auth_state, "get_sync_session") as session,
        patch.object(auth_state, "request_email_verification"),
    ):
        captcha.return_value.verify.return_value = True
        session.return_value.__enter__.return_value = MagicMock()
        session.return_value.__exit__.return_value = False
        return AuthState.signup.fn(
            st,
            {
                "email": email,
                "password": "correct horse battery",
                "full_name": "Alice",
                "captcha_token": "tok",
            },
        )


@pytest.fixture
def redis_store():
    """Point the module's limiter at a fake store for the duration of a test."""
    store = FakeRedis()
    with patch.object(auth_state, "_limiter", RateLimitService(redis_client=store)):
        yield store


def _probe(address: str) -> str:
    """A fresh address per attempt, so only the per-IP bucket can refuse.

    ⚠️ Reusing one address across ten attempts does **not** exercise the IP
    dimension: the per-address bucket is 3, so it refuses on the fourth and the
    test then passes for the wrong reason. That is exactly what the enumeration
    attack looks like anyway — one network, many addresses.
    """
    return address


class TestTheBoundExists:
    def test_the_tenth_attempt_from_one_ip_is_allowed(self, redis_store):
        """AC1, the permitting half.

        A limiter tested only at its refusal has not been shown to permit — and
        a limiter that refuses everything passes every test that only looks for
        a refusal.
        """
        for i in range(auth_state._SIGNUP_IP_LIMIT - 1):
            _signup(_state(_service()), _probe(f"probe{i}@example.com"))
        st = _state(_service())
        _signup(st, _probe("probe9@example.com"))
        assert st.signup_blocked == "", "the tenth attempt from one IP must be allowed"

    def test_the_eleventh_attempt_from_one_ip_is_refused(self, redis_store):
        """AC1, the refusing half."""
        for i in range(auth_state._SIGNUP_IP_LIMIT):
            _signup(_state(_service()), _probe(f"probe{i}@example.com"))
        st = _state(_service())
        _signup(st, _probe("probe10@example.com"))
        assert st.signup_blocked == "rate_limited"

    def test_one_address_is_bounded_more_tightly_than_one_network(self, redis_store):
        """AC1 / D3 — the per-address bucket is 3, well inside the per-IP 10.

        Each attempt uses a *different* client IP so the IP bucket cannot be
        what refuses; only the address bucket can.
        """
        for i in range(auth_state._SIGNUP_EMAIL_LIMIT):
            _signup(_state(_service(), client_ip=f"198.51.100.{i}"), REGISTERED)
        st = _state(_service(), client_ip="198.51.100.200")
        _signup(st, REGISTERED)
        assert st.signup_blocked == "rate_limited"

    def test_an_unnameable_client_skips_the_ip_bucket_and_still_gets_the_address_bucket(
        self, redis_store
    ):
        """``resolve_client_ip`` returns "" in production behind an ambiguous
        proxy chain. Bucketing on a placeholder would collapse the internet into
        one bucket and lock everyone out on the eleventh signup — so the IP
        bucket is skipped, and the address bucket is all that remains.
        """
        for i in range(auth_state._SIGNUP_IP_LIMIT + 2):
            _signup(_state(_service(), client_ip=""), _probe(f"probe{i}@example.com"))
        assert not any(k.startswith("rl:signup:ip:") for k in redis_store.store), (
            "an empty client IP must not create an IP bucket"
        )
        assert any(":signup:email:" in k for k in redis_store.store), (
            "the address bucket must still apply when the client cannot be named"
        )


class TestTheRefusalIsNotASecondOracle:
    def test_the_refusal_is_identical_for_a_registered_and_an_unregistered_address(
        self, redis_store
    ):
        """AC2. The criterion that stops the fix becoming a new oracle (D5).

        Both addresses are driven to refusal on their own address bucket, and
        the *entire* user-visible outcome is compared — not just a flag.
        """
        outcomes = {}
        for address in (REGISTERED, UNREGISTERED):
            for i in range(auth_state._SIGNUP_EMAIL_LIMIT):
                _signup(_state(_service(duplicate=True), client_ip=f"192.0.2.{i}"), address)
            st = _state(_service(duplicate=True), client_ip="192.0.2.200")
            _signup(st, address)
            outcomes[address] = (st.signup_blocked, st.auth_error)

        assert outcomes[REGISTERED] == outcomes[UNREGISTERED], outcomes
        assert outcomes[REGISTERED] == ("rate_limited", "")

    def test_a_refused_request_never_consults_the_user_service(self, redis_store):
        """AC3 / D5 — the limit is checked *before* the existence lookup.

        A test that only reads the response passes on the wrong order, because
        both orders produce the same message. This one reads the service.
        """
        for i in range(auth_state._SIGNUP_IP_LIMIT):
            _signup(_state(_service()), _probe(f"probe{i}@example.com"))
        svc = _service()
        st = _state(svc)
        # A never-seen address, so its own bucket is empty and the IP bucket is
        # the only thing that can refuse — which is the ordering under test.
        _signup(st, REGISTERED)
        assert st.signup_blocked == "rate_limited"
        assert svc.method_calls == [], (
            f"a refused signup reached the user service: {svc.method_calls}"
        )

    def test_the_redis_key_carries_no_plaintext_address(self, redis_store):
        """AC4 / D3.

        A plaintext key turns the Redis keyspace into a readable list of the
        addresses people have tried — a *lower* bar than reading the database,
        which is the whole reason ``PasswordResetService.email_bucket`` hashes.
        """
        _signup(_state(_service()), REGISTERED)
        keys = list(redis_store.store)
        assert keys, "the limiter recorded nothing"
        joined = " ".join(keys)
        assert REGISTERED not in joined, joined
        assert "alice" not in joined and "example.com" not in joined, joined
        email_keys = [k for k in keys if ":signup:email:" in k]
        assert email_keys, keys
        assert re.search(r":signup:email:[0-9a-f]{64}:", email_keys[0]), email_keys[0]

    def test_the_address_bucket_is_case_and_whitespace_insensitive(self, redis_store):
        """Otherwise ``  Alice@Example.com `` is a fresh budget for the same account."""
        _signup(_state(_service()), "  Alice@Example.com  ")
        _signup(_state(_service()), REGISTERED)
        email_keys = {k for k in redis_store.store if ":signup:email:" in k}
        assert len(email_keys) == 1, email_keys


class TestFailClosed:
    def test_redis_unreachable_refuses_the_signup(self, redis_store):
        """AC5 / D4.

        Fail closed, and the reason is specific to this deployment rather than a
        principle: since core#646 Redis holds Reflex *session* state, so with
        Redis down a user who registered could not stay signed in anyway. The
        limiter silently disappearing during exactly the incident an attacker
        would pick is the worse outcome.
        """
        svc = _service()
        st = _state(svc)
        with patch.object(
            auth_state._limiter, "check_window", side_effect=RedisConnectionError("down")
        ):
            _signup(st, REGISTERED)
        assert st.signup_blocked == "unavailable"
        assert svc.method_calls == []

    def test_the_redis_failure_does_not_surface_a_limiter_error(self, redis_store):
        """D4 — the user sees the generic unavailable state, never the exception."""
        st = _state(_service())
        with patch.object(
            auth_state._limiter, "check_window", side_effect=RedisConnectionError("nodename")
        ):
            _signup(st, REGISTERED)
        assert st.auth_error == "", st.auth_error
        assert "nodename" not in st.signup_blocked


class TestTheFunnelIsUnchanged:
    def test_a_first_signup_is_not_blocked(self, redis_store):
        """AC6. The whole point of option 1 over option 2 is that the funnel does
        not change; a criterion that does not assert this cannot tell the two
        options apart."""
        svc = _service()
        st = _state(svc)
        _signup(st, UNREGISTERED)
        assert st.signup_blocked == ""
        assert svc.register_user.called

    def test_a_duplicate_under_the_limit_still_says_email_already_exists(self, redis_store):
        """AC6 / #128 — the verbatim message survives, which is the disclosure
        this change deliberately keeps."""
        st = _state(_service(duplicate=True))
        _signup(st, REGISTERED)
        assert st.signup_blocked == ""
        assert st.auth_error == "Email already exists"


class TestBucketNaming:
    def test_the_signup_buckets_are_distinct_from_the_password_reset_ones(self):
        """Sharing a namespace would let three password-reset requests spend a
        signup budget, and the two have deliberately different limits."""
        from datanika.services.password_reset_service import PasswordResetService

        assert UserService.signup_ip_bucket("1.2.3.4") != PasswordResetService.ip_bucket("1.2.3.4")
        assert UserService.signup_email_bucket(REGISTERED) != PasswordResetService.email_bucket(
            REGISTERED
        )
