"""RateLimitService — Redis fixed-window rate limiting for API keys.

Two layers, and the distinction is load-bearing (#774):

* :meth:`check_window` is the **authoritative** limiter. It always counts, and
  a Redis outage **propagates** — a limiter that fails open is not a limiter.
* :meth:`preauth_check`, :meth:`record_auth_failure` and :meth:`mark_refused`
  are the **advisory** layer in front of it. They exist so a request we already
  know we will refuse never reaches the database, and their failure mode is to
  fall through to the layer above rather than to invent a refusal. Redis errors
  are therefore suppressed *by the caller* on that path only.

Before the advisory layer existed, a request bearing an invalid key was never
counted at all: QA measured 40 of 40 answered 401, each after a session
checkout, a sha256 and an indexed SELECT (`plans/qa/notes/probe-705/`). And a
429 cost exactly what a 200 cost, because the limiter ran after both database
sessions — the documented mechanism of Runs 7/8, which pegged
``max_connections`` for 50+ minutes.
"""

import contextlib
import hashlib
import time
from dataclasses import dataclass

from redis import Redis

from datanika.config import settings
from datanika.errors import UserFacingError


class RateLimitError(UserFacingError):
    """Raised when an API key exceeds its rate limit."""

    def __init__(self, message: str, retry_after: int = 60):
        super().__init__(message)
        self.retry_after = retry_after


@dataclass
class RateLimitResult:
    allowed: bool
    current_count: int
    limit: int
    remaining: int
    retry_after: int
    reset_at: int  # Unix timestamp when the window resets

    def headers(self) -> dict[str, str]:
        """Return standard rate-limit HTTP headers."""
        h = {
            "X-RateLimit-Limit": str(self.limit),
            "X-RateLimit-Remaining": str(self.remaining),
            "X-RateLimit-Reset": str(self.reset_at),
        }
        if not self.allowed:
            h["Retry-After"] = str(self.retry_after)
        return h


class RateLimitService:
    """Fixed-window rate limiter backed by Redis.

    Uses per-key counters with TTL for minute-level and second-level windows.

    ⚠️ **Fixed, not sliding** — the window key is ``floor(now / window)``, a
    wall-clock partition, so identical traffic flips verdict depending only on
    where it lands relative to a boundary (probe-699 measured ~8% of runs).
    This docstring said "sliding" until #774; the published API page was
    corrected the same day, and the two now agree with the code.
    """

    KEY_PREFIX = "rl:"
    SHED_PREFIX = "rl:shed:"
    PLAN_LIMIT_PREFIX = "rl:planlimit:"

    def __init__(self, redis_client: Redis | None = None):
        self._redis = redis_client

    def _get_redis(self) -> Redis:
        if self._redis is None:
            self._redis = Redis.from_url(settings.redis_url, decode_responses=True)
        return self._redis

    # -- Bucket names --------------------------------------------------

    @staticmethod
    def credential_bucket(raw_key: str) -> str:
        """Failure bucket for one presented credential, with the key hashed.

        Keying on the plaintext would turn the Redis keyspace into a list of
        live API keys, readable by anyone who can read Redis — a much lower bar
        than reading the database. Same reasoning as
        ``PasswordResetService.email_bucket`` (core#623 D5). The digest is the
        same one ``ApiKeyService`` stores, so a bucket is derivable from a key
        but not the other way round.
        """
        return f"apiauth:key:{hashlib.sha256((raw_key or '').encode()).hexdigest()}"

    @staticmethod
    def client_bucket(client_ip: str) -> str:
        """Failure bucket for one client address.

        Callers must pass an address they can stand behind — see
        ``services/client_ip.py``. An empty address means *skip this bucket*,
        never *use a placeholder*: in production every socket peer is
        127.0.0.1, so a placeholder collapses the internet into one bucket and
        the tenth failure from anyone locks out everyone.
        """
        return f"apiauth:ip:{client_ip}"

    def _window_key(self, bucket: str, window_seconds: int, now: int) -> str:
        return f"{self.KEY_PREFIX}{bucket}:{now // window_seconds}"

    @staticmethod
    def _window_reset(window_seconds: int, now: int) -> int:
        return (now // window_seconds + 1) * window_seconds

    def check_rate_limit(
        self,
        bucket: str,
        org_id: int,
        limit_rpm: int,
        burst_per_sec: int | None = None,
    ) -> RateLimitResult:
        """Check and record a request against a per-minute rate limit.

        ``bucket`` names the subject being limited. It used to be an
        ``api_key_id: int``, which was the only thing this method ever did with
        it — password reset (core#623) needs the same sliding window keyed on an
        email digest or a client IP, so the parameter became a string rather
        than a second limiter growing up beside this one. API-key callers pass
        ``f"{api_key.id}"``, which produces byte-identical Redis keys.
        """
        return self.check_window(bucket, limit_rpm, window_seconds=60, burst_per_sec=burst_per_sec)

    def check_window(
        self,
        bucket: str,
        limit: int,
        window_seconds: int = 60,
        burst_per_sec: int | None = None,
    ) -> RateLimitResult:
        """Check and record a request against an arbitrary fixed window.

        Returns a RateLimitResult indicating whether the request is allowed.
        The request is always counted (even if over limit) to prevent
        counter manipulation.
        """
        r = self._get_redis()
        now = int(time.time())
        window_key = self._window_key(bucket, window_seconds, now)
        limit_rpm = limit

        # Atomic increment + set TTL
        pipe = r.pipeline()
        pipe.incr(window_key)
        # Double the window to cover overlap, as the per-minute case always did.
        pipe.expire(window_key, window_seconds * 2)
        results = pipe.execute()
        count = results[0]

        reset_at = self._window_reset(window_seconds, now)
        # #774: this used to be a `r.ttl(window_key)` round trip whose result was
        # then discarded on every admitted request — 200 TTL calls for 200
        # admitted requests, one of the three round trips. It was also wrong in
        # the one case it was used: the key's TTL is `window_seconds * 2`, so a
        # 60-second window told a rejected client to come back in up to 120
        # seconds. The window's own reset is both free and correct.
        retry_after = max(reset_at - now, 1)

        # Check burst limit (per-second window)
        if burst_per_sec is not None:
            burst_key = f"{self.KEY_PREFIX}{bucket}:s:{now}"
            pipe = r.pipeline()
            pipe.incr(burst_key)
            pipe.expire(burst_key, 2)
            burst_results = pipe.execute()
            burst_count = burst_results[0]
            if burst_count > burst_per_sec:
                return RateLimitResult(
                    allowed=False,
                    current_count=count,
                    limit=limit_rpm,
                    remaining=0,
                    retry_after=1,
                    reset_at=now + 1,
                )

        remaining = max(0, limit_rpm - count)
        allowed = count <= limit_rpm

        return RateLimitResult(
            allowed=allowed,
            current_count=count,
            limit=limit_rpm,
            remaining=remaining,
            retry_after=retry_after if not allowed else 0,
            reset_at=reset_at,
        )

    # -- Advisory pre-auth layer (#774) --------------------------------

    def preauth_check(
        self,
        *,
        credential: str,
        client: str,
        window_seconds: int,
        credential_failure_limit: int,
        client_failure_limit: int,
    ) -> RateLimitResult:
        """Decide, from Redis alone, whether this request may reach the database.

        One round trip (a single ``MGET``), no writes. Three reasons to refuse,
        all of them things we already learned from an earlier request:

        * the credential is in the refusal window armed by :meth:`mark_refused`
          — it was authenticated once, found to be over its own plan limit, and
          the window it broke has not reset yet;
        * the credential has failed authentication ``credential_failure_limit``
          times in this window;
        * the client address has, across however many credentials it rotated.

        ``allowed=True`` means *proceed to authentication*; it is not a
        statement about the caller's entitlement, which is unknown here. The
        result deliberately carries ``limit=0``: pre-auth we must not describe
        an allowance, and the caller must not emit ``X-RateLimit-*`` headers
        from it.

        A caller that skips a bucket passes an empty ``client`` or a limit of
        ``0``. Skipping is the correct handling of an unnameable client — see
        :meth:`client_bucket`.
        """
        now = int(time.time())
        reset_at = self._window_reset(window_seconds, now)
        window_retry = max(reset_at - now, 1)

        keys: list[str] = [f"{self.SHED_PREFIX}{credential}"]
        checks: list[tuple[str, int]] = []  # (redis key, limit)
        if credential_failure_limit > 0:
            checks.append(
                (self._window_key(credential, window_seconds, now), credential_failure_limit)
            )
        if client and client_failure_limit > 0:
            checks.append((self._window_key(client, window_seconds, now), client_failure_limit))
        keys.extend(key for key, _ in checks)

        values = self._get_redis().mget(keys)

        refused_until = values[0]
        if refused_until is not None:
            return self._refusal(
                retry_after=max(int(refused_until) - now, 1),
                count=0,
                reset_at=max(int(refused_until), now + 1),
            )

        for (_, limit), value in zip(checks, values[1:], strict=True):
            count = int(value) if value is not None else 0
            if count >= limit:
                return self._refusal(retry_after=window_retry, count=count, reset_at=reset_at)

        return RateLimitResult(
            allowed=True,
            current_count=0,
            limit=0,
            remaining=0,
            retry_after=0,
            reset_at=reset_at,
        )

    @staticmethod
    def _refusal(*, retry_after: int, count: int, reset_at: int) -> RateLimitResult:
        return RateLimitResult(
            allowed=False,
            current_count=count,
            limit=0,
            remaining=0,
            retry_after=retry_after,
            reset_at=reset_at,
        )

    def record_auth_failure(self, *, credential: str, client: str, window_seconds: int) -> None:
        """Count one failed authentication against the credential and the client.

        **Failures only.** A request that authenticates never touches these
        counters, which is what keeps a legitimate caller sharing an address
        with a broken one working: the address bucket fills only when that
        address is itself failing.

        One round trip. Counted after the fact rather than before, so the
        budget is spent by requests that actually cost us a lookup.
        """
        buckets = [credential] + ([client] if client else [])
        now = int(time.time())
        pipe = self._get_redis().pipeline()
        for bucket in buckets:
            key = self._window_key(bucket, window_seconds, now)
            pipe.incr(key)
            pipe.expire(key, window_seconds * 2)
        pipe.execute()

    def mark_refused(self, *, credential: str, retry_after: int) -> None:
        """Remember that ``credential`` is over its limit, until its window resets.

        This is what makes the *second* rejection free. The first one cannot be
        — we only learn a caller's limit by authenticating them — but every one
        after it is answered from :meth:`preauth_check` with no session, no
        ``SELECT`` and no plan lookup.

        The TTL is the caller's own ``retry_after``, so the marker expires
        exactly when the window it refers to resets. Nothing is refused for
        longer than the authoritative limiter would have refused it, and no
        state outlives the window.
        """
        seconds = max(int(retry_after), 1)
        now = int(time.time())
        self._get_redis().set(f"{self.SHED_PREFIX}{credential}", now + seconds, ex=seconds)

    # -- Plan resolution -----------------------------------------------

    def get_limit_for_org(self, org_id: int) -> int:
        """Get the rate limit RPM for an org.

        In core edition, returns the configured default.
        In cloud edition, hooks override this to return plan-specific limits.

        Cached in Redis for ``api_plan_limit_cache_seconds`` (#774). The cloud
        handler opens its **own** database session, measured at **4.20 ms of
        the 4.74 ms** it costs to enforce a rate limit on the production box —
        89% of the total, spent re-reading a value that changes about monthly,
        on every request including every request then rejected. A plan change
        therefore takes up to one cache window to take effect, which is the
        deliberate trade.

        Cache failures fall through to the hook: a broken cache must not break
        rate limiting, and the authoritative window still fails closed.
        """
        cache_seconds = settings.api_plan_limit_cache_seconds
        cache_key = f"{self.PLAN_LIMIT_PREFIX}{org_id}"
        if cache_seconds > 0:
            with contextlib.suppress(Exception):
                cached = self._get_redis().get(cache_key)
                if cached is not None:
                    return int(cached)

        from datanika.hooks import emit

        context = {"org_id": org_id, "limit_rpm": settings.api_rate_limit_rpm}

        # Cloud plugin can override limit_rpm via hook
        with contextlib.suppress(Exception):
            emit("api.get_rate_limit", context=context)

        limit = context["limit_rpm"]
        if cache_seconds > 0:
            with contextlib.suppress(Exception):
                self._get_redis().set(cache_key, int(limit), ex=cache_seconds)
        return limit
