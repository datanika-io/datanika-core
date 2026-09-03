"""Test object factories that maintain the invariants production maintains.

``SPEC_PII_SEPARATION.md`` §8a.2 / §8a.6 Step A, core#1009.

**Why this module exists.** Release N dual-writes personal data: `UserService.register_user`
writes the legacy `users` columns *and* the `user_pii` sidecar, `find_or_create_oauth_user`
mirrors, and the expand migration backfilled every pre-existing row. So in production
**every user has a sidecar**, and `get_user_by_email`'s legacy `or_` half is a blue/green
window rather than a fallback for missing sidecars.

A test that constructs `User(email=...)` by hand produces a row **the dual-write invariant
says cannot exist**. That is survivable under N — which is exactly why 46 such sites
accumulated unnoticed — and it stops being survivable at N+1, where the legacy half is
deleted and a sidecar-less user becomes invisible to every lookup by address while their
`Membership` row is untouched.

**Why a factory rather than a sidecar line beside each construction.** §8a.7: N+2 drops
`users.email`, `users.full_name` and `users.oauth_provider_id`, turning every hand-written
`User(email=...)` into a `TypeError`. Routed through here, that release is **one** edit.
Written inline, it is one per site.

🚨 **Deliberately NOT implemented by calling `UserService._sync_user_pii`** — core#939 item 3
deletes that method in N+1, so a fixture built on it would break at exactly the release this
module exists to protect.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from datanika.models.pii import UserPII
from datanika.models.user import User

__all__ = ["make_user"]


def make_user(
    session: Session,
    *,
    email: str,
    full_name: str = "Test User",
    **kwargs,
) -> User:
    """Create a ``User`` and its ``user_pii`` sidecar, flushed, the way production does.

    ``**kwargs`` passes through to the ``User`` constructor (``password_hash``,
    ``is_active``, ``email_verified``, ``oauth_provider_id``, …). ``oauth_provider_id``
    is mirrored onto the sidecar as well, because it lives on both — for SSO it is the
    **email address verbatim**, which is why `models/pii.py` carries it at all.

    ``full_name`` defaults rather than being required: ``user_pii.full_name`` is
    ``nullable=False``, so a caller who does not care still has to supply something, and
    making every call site invent a name would be noise.
    """
    user = User(email=email, full_name=full_name, **kwargs)
    session.add(user)
    session.flush()
    session.add(
        UserPII(
            user_id=user.id,
            email=email,
            full_name=full_name,
            oauth_provider_id=kwargs.get("oauth_provider_id"),
        )
    )
    session.flush()
    return user
