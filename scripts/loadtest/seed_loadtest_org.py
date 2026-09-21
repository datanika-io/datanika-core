"""Seed (and revoke) the load-test org's API keys on STAGING (core#778).

Run INSIDE the staging app container, which is where the installed package and the database
credentials are::

    docker exec -i datanika-staging-app /app/.venv/bin/python - 161 < seed_loadtest_org.py
    docker exec -i datanika-staging-app /app/.venv/bin/python - revoke < seed_loadtest_org.py

``mint`` prints one raw key per line on **stdout and nothing else** — ``run.sh`` redirects
stdout straight into the keys file, so every diagnostic here must go to stderr.

── Why many keys, and why this is a seeder rather than a constant ────────────────────────
``rate_limit_rpm`` resolves per **org** (from the plan row) and buckets per **key**
(``datanika/services/api_key_service.py``; migration ``e5f6a7b8c9d0`` says the same). Free is
30 rpm. So an org's achievable authed arrival rate is::

    req/s_ceiling = keys * rate_limit_rpm / 60

161 keys on a free-plan org is 80.5 req/s, which is what Run 9 used and why its 60 req/s top
stage was not limiter-bound. Raising the plan's rpm and using one key is **not** equivalent —
161 keys is 161 Redis buckets — and would make the result non-comparable to Run 9.

⚠️ ``create_api_key`` emits ``api_key.before_create``, which cloud's ``check_api_key_quota``
subscribes to. ``plans.max_api_keys`` is NULL on every row, which cloud reads as *uncapped*
(core#706), so minting 161 is expected to pass today. **If that column is ever given a value,
this seeder is the thing that starts failing**, and the refusal will be correct rather than a
bug in this file.

⚠️ Both ``create_api_key`` and ``revoke_api_key`` require the actor to be an org **admin**
(core#681). The actor here is the org's own seeded admin, never a borrowed identity.
"""

from __future__ import annotations

import sys

ORG_NAME = "loadtest-core778"
USER_EMAIL = "loadtest-core778@invalid.local"
KEY_PREFIX = "core778-loadtest-"


def _die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def _guard_not_production(session) -> None:
    """Refuse to run anywhere that is not POSITIVELY staging.

    🚨 The most important function in this file. April's load runs went at production and
    left its database unusable for the better part of an hour. The check is **positive** — it
    requires evidence that this IS staging — because "no evidence it is production" is
    satisfied by a failed lookup.

    🔴 **CORRECTED 2026-09-21, after the first real use. The original version could not tell
    staging from production at all**, and that is worth more than the fix.

    It tested for ``"staging"`` in ``settings.database_url`` or ``app_env``. Measured on the
    box: **both** stacks report ``postgres:5432/datanika`` and an empty ``app_env``, because
    each Compose project has its own network in which the hostname ``postgres`` resolves to
    *that* project's database. The isolation is at the network layer and is invisible in the
    URL. So the guard refused **both** — which is the safe direction, and is why nothing was
    damaged — but it was not discriminating; it was failing closed on everything, and the
    obvious "fix" for a guard that always refuses is to loosen it, which would have permitted
    production.

    Two independent signals are now required, both measured as actually differing:

    ========================  ==============================  ==========================
    signal                    staging                         production
    ========================  ==============================  ==========================
    ``settings.frontend_url`` ``staging-app.datanika.io``     ``app.datanika.io``
    ``PADDLE_ENVIRONMENT``    ``sandbox``                     ``production``
    ========================  ==============================  ==========================

    ⚠️ **Both, not either.** One misconfigured value must not be able to unlock production, and
    ``app.datanika.io`` does not contain ``staging`` so the first test is genuinely positive.

    📎 The durable form of this is an explicit opt-in that only the staging manifest sets —
    a value production has no reason to carry at all. That needs a deploy; these two are
    properties the two environments already have, and they discriminate today.
    """
    import os

    from datanika.config import settings

    frontend = str(getattr(settings, "frontend_url", "") or "")
    paddle = os.environ.get("PADDLE_ENVIRONMENT", "")

    if not is_staging(frontend, paddle):
        _die(
            "REFUSING: this is not positively staging. Seeding load-test keys anywhere else "
            f"is forbidden. frontend_url={frontend!r} PADDLE_ENVIRONMENT={paddle!r} "
            "(staging must be a staging frontend_url AND paddle sandbox — both).",
            20,
        )


def is_staging(frontend_url: str, paddle_environment: str) -> bool:
    """The whole decision, as a pure function of two measured signals.

    🔑 Pure and exported **so a test can drive it with the real values of BOTH environments**
    rather than merely asserting that a guard exists somewhere. The predecessor of this guard
    was "tested" by a check that it was present and called on both code paths — which it was,
    and it still could not tell staging from production. **A guard never seen refusing the
    thing it exists to refuse is not evidence.**
    """
    return "staging" in (frontend_url or "") and (paddle_environment or "") == "sandbox"


def _org_and_admin(session):
    """The dedicated load-test org and its admin, created once and reused."""
    # Defect 5 (core#778), found by RUNNING this: the import was
    # `datanika.models.organization`, a module that does not exist. Organization is
    # declared in `datanika/models/user.py`. Importing from the package root instead
    # of either file means a future move does not break this again.
    from datanika.models import Organization, User

    org = session.query(Organization).filter(Organization.name == ORG_NAME).one_or_none()
    user = session.query(User).filter(User.email == USER_EMAIL).one_or_none()
    if org is None or user is None:
        _die(
            "REFUSING: the load-test org/user does not exist. Create it deliberately once "
            f"(an org named {ORG_NAME!r} with an ADMIN member {USER_EMAIL!r}) rather than "
            "having a load-test script mint organisations as a side effect.",
            21,
        )
    return org, user


def mint(count: int) -> None:
    from datanika.db import get_sync_session
    from datanika.services.api_key_service import ApiKeyService

    session = get_sync_session()
    try:
        _guard_not_production(session)
        org, user = _org_and_admin(session)
        svc = ApiKeyService()
        made = 0
        for i in range(count):
            _key, raw = svc.create_api_key(
                session,
                org_id=org.id,
                user_id=user.id,
                actor_user_id=user.id,
                name=f"{KEY_PREFIX}{i:04d}",
            )
            print(raw)  # stdout is the keys file — nothing else may go here
            made += 1
        session.commit()
        print(f"minted {made} key(s) for org_id={org.id}", file=sys.stderr)
    finally:
        session.close()


def revoke() -> None:
    """Revoke every key this seeder minted, and ASSERT the remaining active count is 0.

    Returns the evidence on stdout in one line, because `run.sh` logs it: a cleanup that was
    merely invoked is the same class of evidence as a green that could never fail.
    """
    from datanika.db import get_sync_session
    from datanika.services.api_key_service import ApiKeyService

    session = get_sync_session()
    try:
        _guard_not_production(session)
        org, user = _org_and_admin(session)
        svc = ApiKeyService()
        keys = [k for k in svc.list_api_keys(session, org.id) if k.name.startswith(KEY_PREFIX)]
        total = len(keys)
        revoked = 0
        for k in keys:
            if svc.revoke_api_key(session, org.id, k.id, actor_user_id=user.id):
                revoked += 1
        session.commit()
        # Re-read rather than trusting the loop's own count.
        left = [k for k in svc.list_api_keys(session, org.id) if k.name.startswith(KEY_PREFIX)]
        print(f"revoked={revoked} active_remaining={len(left)} (control: total key rows={total})")
        if left:
            raise SystemExit(22)
    finally:
        session.close()


if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else ""
    if arg == "revoke":
        revoke()
    elif arg.isdigit() and int(arg) > 0:
        mint(int(arg))
    else:
        _die("usage: seed_loadtest_org.py <count> | revoke", 2)
