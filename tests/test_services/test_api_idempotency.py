"""core#660 — the v1 API's idempotency gate on the **async** handler path.

⚠️ Read the issue's TITLE, not its body's first table
------------------------------------------------------
#660's body says the gate is untested on *both* handler paths and quotes **6 of 23 (26%)**.
Both numbers are pre-correction. ``plans/engineering/AUDIT_TESTS_THAT_CANNOT_FAIL.md`` records
the correction and says the omission is worth more than the score:
``tests/test_services/test_tier4_agent.py::TestIdempotencyKey`` **does** cover the gate —
through the real ``api_endpoint`` decorator via a Starlette ``TestClient`` — and was simply
left out of that run's test selection. Including it moves the module to **11/23 (48%)** and
kills exactly the **sync**-path mutants.

So the defect that stands is the narrow one in the issue's title. Measured on today's tree,
with ``test_tier4_agent.py`` in the selection, over ``_run_async_handler``'s block (L302-310):

===================================================  ==============
mutant                                               baseline
===================================================  ==============
``L303 cmp '==' -> '!='``                            SURVIVED
``L303 str 'POST' -> 'MUTANT_POST'``                 SURVIVED
``L307 cmp ' is not ' -> ' is '``                    SURVIVED
===================================================  ==============

**0 of 3.** With this file: **3 of 3.**

That path is not a backwater. ``asyncio.iscoroutinefunction(handler)`` routes to it, and the
three ``async def`` handlers are ``POST /uploads/{id}/run``, ``/pipelines/{id}/run`` and
``/transformations/{id}/run`` — **every pipeline-trigger endpoint**, the ones a CI/CD caller
retries, and the ones where a duplicated ``POST`` is a duplicated pipeline run. Idempotency is
also one of the three named ship gates for V2 overage billing, where it is a duplicate charge.

Why these tests are shaped the way they are
-------------------------------------------
**The cache is real.** Only ``redis`` itself is replaced, by a dict-backed double;
``get_idempotency_key``, ``get_cached_response`` and ``cache_response`` all run their own
logic, including the JSON round-trip through ``response.body`` and the TTL argument.

⚠️ This is the substantive difference from ``TestIdempotencyKey``, which hand-feeds the second
read (``mock_r.get.return_value = json.dumps({...})``) rather than letting the first call's
write be read back. That exercises the branch but cannot show that the write and the read
agree — a `cache_response` that stored the wrong shape would pass it.

**The handler counts its own invocations.** *"Returns the cached response"* and *"does not
re-run the handler"* are different claims, and only the second matters for a duplicate charge:
a handler that runs again and has its response discarded has already metered, already
dispatched the Celery task, already written. The handler also returns a value that **changes
every call**, so a replay is provable rather than merely consistent with a deterministic
handler.

**Both paths, every time.** ``_run_async_handler`` and ``_run_sync_handler`` are
near-duplicates, and #660's other finding is that coverage between them is asymmetric in *both*
directions on different lines — the byte-identical ``"Internal server error"`` literal was
killed on the async path and survived on the sync one, while the idempotency block was the
reverse. Parametrising over both is the only way not to have to know which way round it is
this time.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from datanika.services.api_middleware import api_endpoint
from datanika.services.rate_limit_service import RateLimitResult

# --------------------------------------------------------------------------
# A real-enough Redis. `get`/`set` over a dict, so idempotency.py's own
# serialisation, key construction and TTL argument all execute.
# --------------------------------------------------------------------------


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.set_calls: list[tuple[str, int | None]] = []

    def get(self, key: str) -> str | None:
        return self.store.get(key)

    def set(self, key: str, value: str, ex: int | None = None) -> None:
        self.store[key] = value
        self.set_calls.append((key, ex))


# --------------------------------------------------------------------------
# Handlers. Each counts its invocations and returns a value that CHANGES every
# call, so "the second response came from the cache" is provable rather than
# merely consistent with a handler that happens to be deterministic.
# --------------------------------------------------------------------------

CALLS: dict[str, int] = {}
SEEN_BODY_ATTR: dict[str, bool] = {}


def _payload(name: str) -> JSONResponse:
    CALLS[name] = CALLS.get(name, 0) + 1
    return JSONResponse({"handler": name, "invocation": CALLS[name]}, status_code=201)


@api_endpoint()
async def async_handler(request, api_key, session):
    SEEN_BODY_ATTR["async"] = hasattr(request, "_body")
    return _payload("async")


@api_endpoint()
def sync_handler(request, api_key, session):
    SEEN_BODY_ATTR["sync"] = hasattr(request, "_body")
    return _payload("sync")


@api_endpoint()
async def async_rejecting_handler(request, api_key, session):
    CALLS["reject"] = CALLS.get("reject", 0) + 1
    return JSONResponse({"error": "nope"}, status_code=400)


@api_endpoint()
async def async_raising_handler(request, api_key, session):
    CALLS["raise"] = CALLS.get("raise", 0) + 1
    raise RuntimeError("boom")


@api_endpoint()
def sync_raising_handler(request, api_key, session):
    CALLS["raise"] = CALLS.get("raise", 0) + 1
    raise RuntimeError("boom")


HANDLERS = {"async": async_handler, "sync": sync_handler}
RAISING = {"async": async_raising_handler, "sync": sync_raising_handler}


@pytest.fixture(autouse=True)
def _reset():
    CALLS.clear()
    SEEN_BODY_ATTR.clear()
    yield
    CALLS.clear()
    SEEN_BODY_ATTR.clear()


@pytest.fixture
def fake_redis():
    redis = FakeRedis()
    with patch("datanika.services.idempotency._redis", return_value=redis):
        yield redis


@pytest.fixture
def client_for():
    """Build a TestClient for one handler with auth + rate limiting satisfied."""
    from contextlib import contextmanager

    @contextmanager
    def _build(handler, org_id: int = 10):
        api_key = MagicMock()
        api_key.id = 1
        api_key.org_id = org_id
        api_key.name = "Test Key"
        ok = RateLimitResult(
            allowed=True,
            current_count=1,
            limit=60,
            remaining=59,
            retry_after=0,
            reset_at=9999999999,
        )
        with (
            patch("datanika.services.api_middleware._get_session") as mock_session,
            patch("datanika.services.api_middleware._api_key_svc") as mock_svc,
            patch("datanika.services.api_middleware._rate_limit_svc") as mock_rl,
        ):
            session_ctx = MagicMock()
            mock_session.return_value.__enter__ = lambda s: session_ctx
            mock_session.return_value.__exit__ = lambda s, *a: None
            mock_svc.authenticate_api_key.return_value = api_key
            mock_rl.get_limit_for_org.return_value = 60
            mock_rl.check_rate_limit.return_value = ok

            app = Starlette(
                routes=[Route("/t", handler, methods=["GET", "POST", "PUT", "PATCH", "DELETE"])]
            )
            yield TestClient(app)

    return _build


AUTH = {"Authorization": "Bearer etf_validkey"}
BOTH = pytest.mark.parametrize("path", ["async", "sync"])


class TestTheGateIsArmed:
    """Arming checks. Every assertion below is about a *second* request being handled
    differently from the first; if the first never reaches the handler, or the cache is
    never written, the interesting assertions are satisfied by a broken pipeline."""

    @BOTH
    def test_a_single_post_reaches_the_handler_and_is_cached(self, path, fake_redis, client_for):
        with client_for(HANDLERS[path]) as client:
            resp = client.post("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})
        assert resp.status_code == 201
        assert CALLS[path] == 1
        assert len(fake_redis.store) == 1, (
            "nothing was written to the idempotency cache, so every replay test below "
            "would pass against a gate that does nothing"
        )

    def test_the_cached_entry_carries_a_ttl(self, fake_redis, client_for):
        with client_for(async_handler) as client:
            client.post("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})
        ((_, ttl),) = fake_redis.set_calls
        assert ttl and ttl > 0, f"idempotency entries must expire; ex={ttl!r}"


class TestARepeatIsServedFromTheCache:
    @BOTH
    def test_the_second_post_returns_the_first_response(self, path, fake_redis, client_for):
        with client_for(HANDLERS[path]) as client:
            first = client.post("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})
            second = client.post("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})

        assert first.json() == second.json(), (
            "the replay returned a different body, so it was recomputed rather than replayed"
        )
        assert second.json()["invocation"] == 1
        assert second.status_code == first.status_code == 201, (
            "the cached response must keep the original status, not collapse to 200"
        )

    @BOTH
    def test_the_second_post_does_not_re_run_the_handler(self, path, fake_redis, client_for):
        """The claim that matters for a duplicate charge.

        A handler that runs again and has its response discarded has already metered,
        already dispatched the Celery task and already written. Identical bodies would
        not reveal that.
        """
        with client_for(HANDLERS[path]) as client:
            client.post("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})
            client.post("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})

        assert CALLS[path] == 1, (
            f"handler ran {CALLS[path]} times for one idempotency key; the gate opened but "
            "did not stop the second execution (core#660)"
        )


class TestTheGateAppliesOnlyWhereItShould:
    @BOTH
    def test_a_post_without_the_header_is_never_cached(self, path, fake_redis, client_for):
        with client_for(HANDLERS[path]) as client:
            client.post("/t", headers=AUTH, json={})
            client.post("/t", headers=AUTH, json={})

        assert CALLS[path] == 2, "idempotency is opt-in; an unkeyed POST must run every time"
        assert fake_redis.store == {}, "an unkeyed POST wrote to the idempotency cache"

    @BOTH
    def test_a_blank_header_is_treated_as_absent(self, path, fake_redis, client_for):
        with client_for(HANDLERS[path]) as client:
            client.post("/t", headers={**AUTH, "Idempotency-Key": "   "}, json={})
            client.post("/t", headers={**AUTH, "Idempotency-Key": "   "}, json={})
        assert CALLS[path] == 2
        assert fake_redis.store == {}

    @BOTH
    def test_a_get_never_takes_the_idempotency_branch(self, path, fake_redis, client_for):
        """`== "POST"` → `!= "POST"` survived mutation on both paths.

        Under that mutant a GET is cached and a POST is not — which is the gate pointed at
        exactly the methods it exists to exclude.
        """
        with client_for(HANDLERS[path]) as client:
            client.get("/t", headers={**AUTH, "Idempotency-Key": "k1"})
            client.get("/t", headers={**AUTH, "Idempotency-Key": "k1"})

        assert CALLS[path] == 2, "a GET was served from the idempotency cache"
        assert fake_redis.store == {}, "a GET wrote to the idempotency cache"

    @BOTH
    def test_a_put_is_not_cached(self, path, fake_redis, client_for):
        """PUT pre-consumes its body but is deliberately outside the idempotency gate."""
        with client_for(HANDLERS[path]) as client:
            client.put("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})
            client.put("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})
        assert CALLS[path] == 2
        assert fake_redis.store == {}

    @BOTH
    def test_a_different_key_is_a_different_request(self, path, fake_redis, client_for):
        with client_for(HANDLERS[path]) as client:
            client.post("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})
            client.post("/t", headers={**AUTH, "Idempotency-Key": "k2"}, json={})
        assert CALLS[path] == 2, "two distinct keys collapsed into one cached response"


class TestTheCacheIsScopedToTheOrg:
    def test_two_orgs_using_the_same_key_do_not_collide(self, fake_redis, client_for):
        """`get_idempotency_key` is passed `api_key.org_id` and the key is built from it.

        Without that, one tenant's `Idempotency-Key: 1` would replay another tenant's
        response — a cross-tenant read, not merely a wrong answer. Nothing else in the
        suite covers it.
        """
        with client_for(async_handler, org_id=10) as client:
            first = client.post("/t", headers={**AUTH, "Idempotency-Key": "same"}, json={})
        with client_for(async_handler, org_id=99) as client:
            second = client.post("/t", headers={**AUTH, "Idempotency-Key": "same"}, json={})

        assert CALLS["async"] == 2, (
            "org 99 was served org 10's cached response — the idempotency key is not "
            "org-scoped, which is a cross-tenant cache read"
        )
        assert first.json() != second.json()
        assert len(fake_redis.store) == 2
        assert len({k for k in fake_redis.store}) == 2, f"keys collided: {list(fake_redis.store)}"


class TestARejectedRequestIsNotReplayed:
    def test_a_400_with_no_commit_is_not_cached(self, fake_redis, client_for):
        """`if idem_key and (not _is_rejection(response) or watch.committed)`.

        Caching a rejection would make a client's retry-after-fixing-the-request replay
        the original refusal forever, for the TTL.
        """
        with client_for(async_rejecting_handler) as client:
            first = client.post("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})
            second = client.post("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})

        assert first.status_code == second.status_code == 400
        assert CALLS["reject"] == 2, "a rejected request was replayed from the cache"
        assert fake_redis.store == {}


class TestTheBodyIsPreConsumed:
    """`if request.method in ("POST", "PUT", "PATCH"): await request.body()`

    `in` → `not in` survived, and so did each of the three method literals. The line exists
    so a SYNC handler — running in `asyncio.to_thread` — can read the body without touching
    the outer event loop's `receive`. Inverted, every write to the v1 API loses its body
    while GET gains one, and nothing notices.

    Asserted via `hasattr(request, "_body")`, which is Starlette's own cache of a consumed
    body and is precisely the mechanism this line exists to populate.
    """

    @BOTH
    def test_the_body_is_already_consumed_for_post(self, path, fake_redis, client_for):
        with client_for(HANDLERS[path]) as client:
            client.post("/t", headers=AUTH, json={"a": 1})
        assert SEEN_BODY_ATTR[path] is True, (
            "the body was not pre-consumed before the handler ran; a sync handler would "
            "have to await `receive` from the threadpool"
        )

    @BOTH
    def test_the_body_is_already_consumed_for_put(self, path, fake_redis, client_for):
        with client_for(HANDLERS[path]) as client:
            client.put("/t", headers=AUTH, json={"a": 1})
        assert SEEN_BODY_ATTR[path] is True

    @BOTH
    def test_a_get_body_is_not_pre_consumed(self, path, fake_redis, client_for):
        """The discriminating half. Without it, `not in` passes both assertions above."""
        with client_for(HANDLERS[path]) as client:
            client.get("/t", headers=AUTH)
        assert SEEN_BODY_ATTR[path] is False, (
            "a GET had its body pre-consumed, so the method check is not discriminating — "
            "which is what `in` → `not in` looks like from the POST side alone"
        )


class TestAPatchIsPreConsumedToo:
    """The third literal in `("POST", "PUT", "PATCH")`.

    #660 records that each of the three survived individually. Covering two of them and
    reporting an improved score would leave the same class of mutant alive while looking
    like progress.
    """

    @BOTH
    def test_the_body_is_already_consumed_for_patch(self, path, fake_redis, client_for):
        with client_for(HANDLERS[path]) as client:
            client.patch("/t", headers=AUTH, json={"a": 1})
        assert SEEN_BODY_ATTR[path] is True, (
            "PATCH did not have its body pre-consumed; a sync handler would have to await "
            "`receive` from the threadpool"
        )

    @BOTH
    def test_a_patch_is_not_idempotency_cached(self, path, fake_redis, client_for):
        with client_for(HANDLERS[path]) as client:
            client.patch("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})
            client.patch("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})
        assert CALLS[path] == 2
        assert fake_redis.store == {}

    @BOTH
    def test_a_delete_body_is_not_pre_consumed(self, path, fake_redis, client_for):
        """DELETE is outside the tuple, and is the control that keeps the three assertions
        above from being satisfied by `await request.body()` running unconditionally."""
        with client_for(HANDLERS[path]) as client:
            client.delete("/t", headers=AUTH)
        assert SEEN_BODY_ATTR[path] is False


class TestAHandlerThatRaisesIsHandledTheSameOnBothPaths:
    """#660's clearest single piece of evidence.

    `"Internal server error"` was KILLED on the async path and SURVIVED on the byte-identical
    sync one — `_run_sync_handler` is what every non-coroutine handler takes, i.e. most of
    them. Asserting the status alone does not kill a message-literal mutant, so the message is
    asserted, on both paths.
    """

    @BOTH
    def test_it_answers_500_with_the_same_message(self, path, fake_redis, client_for):
        with client_for(RAISING[path]) as client:
            resp = client.post("/t", headers=AUTH, json={})
        assert resp.status_code == 500
        assert resp.json()["error"] == "Internal server error", (
            "the two handler paths must answer an exception identically; they are "
            "near-duplicates and the sync one is far less exercised (core#660)"
        )
        assert CALLS["raise"] == 1

    @BOTH
    def test_a_raised_handler_is_never_idempotency_cached(self, path, fake_redis, client_for):
        """A 500 is a rejection with no commit, so a retry must reach the handler again.

        Caching it would pin a transient failure for the whole TTL — the client retries
        correctly and is served the same 500 from Redis.
        """
        with client_for(RAISING[path]) as client:
            client.post("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})
            client.post("/t", headers={**AUTH, "Idempotency-Key": "k1"}, json={})
        assert CALLS["raise"] == 2, "a 500 was replayed from the idempotency cache"
        assert fake_redis.store == {}


class TestTheFakeCacheBehavesLikeTheRealOne:
    """Guard the double. If FakeRedis diverged from what idempotency.py expects, every
    test above could pass over a cache that is never really read or written."""

    def test_a_stored_entry_round_trips_through_the_real_helpers(self, fake_redis):
        from datanika.services.idempotency import cache_response, get_cached_response

        cache_response("k", JSONResponse({"v": 7}, status_code=202))
        got = get_cached_response("k")
        assert got is not None
        assert got.status_code == 202
        assert json.loads(got.body.decode()) == {"v": 7}

    def test_a_missing_entry_reads_as_none(self, fake_redis):
        from datanika.services.idempotency import get_cached_response

        assert get_cached_response("absent") is None
