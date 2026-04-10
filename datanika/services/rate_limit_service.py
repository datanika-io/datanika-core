"""RateLimitService — Redis sliding-window rate limiting for API keys."""

import contextlib
import time
from dataclasses import dataclass

from redis import Redis

from datanika.config import settings


class RateLimitError(ValueError):
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
    """Sliding-window rate limiter backed by Redis.

    Uses per-key counters with TTL for minute-level and second-level windows.
    """

    KEY_PREFIX = "rl:"

    def __init__(self, redis_client: Redis | None = None):
        self._redis = redis_client

    def _get_redis(self) -> Redis:
        if self._redis is None:
            self._redis = Redis.from_url(settings.redis_url, decode_responses=True)
        return self._redis

    def check_rate_limit(
        self,
        api_key_id: int,
        org_id: int,
        limit_rpm: int,
        burst_per_sec: int | None = None,
    ) -> RateLimitResult:
        """Check and record a request against the rate limit.

        Returns a RateLimitResult indicating whether the request is allowed.
        The request is always counted (even if over limit) to prevent
        counter manipulation.
        """
        r = self._get_redis()
        now = int(time.time())
        window_key = f"{self.KEY_PREFIX}{api_key_id}:{now // 60}"

        # Atomic increment + set TTL
        pipe = r.pipeline()
        pipe.incr(window_key)
        pipe.expire(window_key, 120)  # 2 minutes to cover window overlap
        results = pipe.execute()
        count = results[0]

        reset_at = (now // 60 + 1) * 60
        ttl = r.ttl(window_key)
        retry_after = max(ttl, 1) if ttl > 0 else 60

        # Check burst limit (per-second window)
        if burst_per_sec is not None:
            burst_key = f"{self.KEY_PREFIX}{api_key_id}:s:{now}"
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

    def get_limit_for_org(self, org_id: int) -> int:
        """Get the rate limit RPM for an org.

        In core edition, returns the configured default.
        In cloud edition, hooks override this to return plan-specific limits.
        """
        from datanika.hooks import emit

        context = {"org_id": org_id, "limit_rpm": settings.api_rate_limit_rpm}

        # Cloud plugin can override limit_rpm via hook
        with contextlib.suppress(Exception):
            emit("api.get_rate_limit", context=context)

        return context["limit_rpm"]
