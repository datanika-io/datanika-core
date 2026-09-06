"""Tests for per-org task concurrency limiter."""

from unittest.mock import patch

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


class TestTheHookContractCoreOwns:
    """Core defines this contract and cloud reads it; core asserted none of it.

    ``datanika-cloud`` has ``test_the_key_core_reads_is_the_key_we_write`` pointed at
    core's source. Core had nothing pointed at its own, so a rename here breaks the
    limiter **silently in one direction**: every org falls back to
    ``DEFAULT_MAX_PARALLEL`` and no signal distinguishes that from working.

    That is not hypothetical — core#780 lived in exactly that gap for five months.
    """

    _EVENT = "concurrency.get_limit"

    @staticmethod
    def _with_handler(handler):
        from datanika import hooks

        hooks.on(TestTheHookContractCoreOwns._EVENT, handler)
        try:
            return concurrency_service.get_max_parallel(org_id=77)
        finally:
            hooks.off(TestTheHookContractCoreOwns._EVENT, handler)

    def test_the_event_name_and_the_seeded_keys_are_what_cloud_expects(self):
        seen = {}

        def handler(context):
            seen.update(context)

        self._with_handler(handler)

        assert seen, (
            f"no handler was called for {self._EVENT!r} — core renamed the event and "
            "cloud's limiter is now inert for every org"
        )
        assert seen["org_id"] == 77, "cloud reads context['org_id'] to resolve the plan"
        assert seen["max_parallel"] == concurrency_service.DEFAULT_MAX_PARALLEL

    def test_a_handlers_mutation_is_what_gets_returned(self):
        """The whole mechanism. Cloud does not return a value; it writes one in."""

        def handler(context):
            context["max_parallel"] = 20

        assert self._with_handler(handler) == 20

    def test_a_raising_handler_degrades_to_the_default_and_says_so(self, caplog):
        """Degrading is right for a limiter. Degrading silently is core#780.

        ``contextlib.suppress(Exception)`` made "every org resolved to the default"
        indistinguishable from "the plan tiers are being honoured" — which is the same
        blindness ``PLAN_DEFAULTS`` produced for a *reader*, from the other side.
        """
        import logging

        def handler(context):
            raise RuntimeError("plan lookup exploded")

        with caplog.at_level(logging.WARNING):
            result = self._with_handler(handler)

        assert result == concurrency_service.DEFAULT_MAX_PARALLEL, (
            "a failing plan lookup must not block runs"
        )
        assert "77" in caplog.text, (
            "the warning does not name the org, so an operator cannot tell whether one "
            f"org degraded or all of them did. Logged: {caplog.text!r}"
        )
        assert "RuntimeError" in caplog.text or "exploded" in caplog.text, (
            f"the warning does not carry the cause. Logged: {caplog.text!r}"
        )


class TestTheDecoyIsGone:
    """core#915. ``PLAN_DEFAULTS`` was read by nothing and looked authoritative.

    core#780's own issue body was written from it and got the scope wrong as a result:
    it concluded the defect was confined to slugs missing from the dict, when in fact
    *every* tier resolved to 5 because nobody read it.
    """

    def test_plan_defaults_no_longer_exists(self):
        assert not hasattr(concurrency_service, "PLAN_DEFAULTS"), (
            "PLAN_DEFAULTS is back. The authoritative per-slug values are "
            "PUBLISHED_MAX_PARALLEL_RUNS in migration f6a7b8c9d0e1, and the plans rows "
            "it writes — a second copy here is read by nobody and audited by everybody"
        )

    def test_the_default_that_is_real_is_still_there(self):
        """Negative control, and it is not decoration.

        A bare absence assertion is equally satisfied by deleting
        ``DEFAULT_MAX_PARALLEL`` alongside the decoy — which would break every
        self-hosted install, since core edition registers no handler and this value is
        the whole answer there.
        """
        assert concurrency_service.DEFAULT_MAX_PARALLEL == 5
