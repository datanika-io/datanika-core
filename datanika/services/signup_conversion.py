"""Hand the "an account was just created" fact from a sign-in callback to ``/auth/complete``.

core#1369, ``SPEC_SIGNUP_SOCIAL_AUTH`` §8h. ``user.signup_completed`` lets a plugin contribute
Reflex events after a signup — cloud's ``handle_signup_conversion`` returns the conversion script.
A Reflex event reaches the browser only when a Reflex **handler** returns it, so on the Google,
GitHub and SSO paths the emit has to happen in ``AuthState.handle_oauth_complete``: the ``on_load``
handler of the frontend page each callback redirects to.

The fact that decides it lives in the **backend** callback, though, and two ways of carrying it
across are wrong:

* **the URL.** The callback already writes ``is_new=1`` into the completion redirect, and the query
  string is the user's to edit — a forged ``is_new=1`` on an existing account would fire a
  conversion (§8h contract 3);
* **the page load.** ``on_load`` re-runs on a reload, so anything the page reads from its own URL
  fires once per load, not once per account (§8h contract 4).

So the callback records a **one-shot marker, server-side**, and the completion page **claims** it.
The claim is a single Redis ``DEL``, which is atomic and reports how many keys it removed, and the
page emits only when that is 1:

* a returning user has no marker, and neither does a forged ``is_new=1`` — nothing fires;
* a reload, or a second tab racing the first, finds the marker claimed — nothing fires again.

🚨 **Both halves fail closed AND quiet toward the user.** A marker that cannot be written, or cannot
be claimed, costs one conversion and one log line. It must never cost a sign-in: analytics may not
be able to break authentication.
"""

import logging

from datanika.config import settings

logger = logging.getLogger(__name__)

#: How long a marker waits to be claimed, in seconds. The completion page loads on the callback's
#: own redirect, so a real claim arrives within seconds; the slack is for a slow connection. Bounded
#: so that a flow which never reached the page cannot fire on some later, unrelated sign-in. The
#: same lifetime as the OAuth state cookie (``oauth_routes``), which is the flow's own clock.
PENDING_TTL_SECONDS = 600


def _redis():
    import redis

    return redis.from_url(settings.redis_url, decode_responses=True)


def _key(user_id: int) -> str:
    return f"auth:signup_completed:{user_id}"


def mark_signup_completed(user_id: int) -> None:
    """Record that ``user_id`` was just created. Called by a callback, after its commit."""
    try:
        _redis().setex(_key(user_id), PENDING_TTL_SECONDS, "1")
    except Exception:
        logger.exception(
            "Could not record a completed signup; its signup_completed event will not fire: "
            "user_id=%s",
            user_id,
        )


def claim_signup_completed(user_id: int) -> bool:
    """True exactly once per marked account; False for everyone else, and on any failure."""
    try:
        return _redis().delete(_key(user_id)) == 1
    except Exception:
        logger.exception(
            "Could not claim a completed signup; its signup_completed event will not fire: "
            "user_id=%s",
            user_id,
        )
        return False
