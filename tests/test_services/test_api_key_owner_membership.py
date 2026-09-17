"""An API key acts only while its owner is a member of the key's organization (core#681).

`SPEC_SERVICE_AUTHORIZATION` §8, founder decision: *a key's authority is intersected with its
owner's **current** org role at authentication time.* The role thresholds for individual operations
live in
the services (§4, §8a); what belongs to AUTHENTICATION is the floor under all of them — an owner who
holds no membership has no role to intersect with, so the key authorizes nothing, on any route.

Two conditions from §8 shape the refusal, and both are asserted:

* it names the cause — `403` with `code == "insufficient_role"` and a `required_role` field — and
  is **not** a `401`, because a caller told "invalid or expired" re-mints a key that was never the
  problem;
* the membership is read **now**, from the database, on every request — so a removal takes effect on
  the key's next use with no deploy and nothing about the key changing.

🔑 Each refusal has its control beside it: the same key, with the membership present, is served.
Without that, every assertion here is satisfied by a middleware that refuses everyone.
"""

from __future__ import annotations

import contextlib
import re
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session as SASession
from sqlalchemy.pool import StaticPool
from starlette.applications import Starlette
from starlette.testclient import TestClient

from datanika.models.base import Base
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.api_key_service import ApiKeyService
from datanika.services.api_v1_routes import api_v1_routes
from datanika.services.authorization import InsufficientRoleError
from datanika.services.meta_routes import meta_routes
from datanika.services.rate_limit_service import RateLimitResult
from tests.factories import make_org_admin, make_user

_RATE_OK = RateLimitResult(
    allowed=True, current_count=1, limit=60, remaining=59, retry_after=0, reset_at=9999999999
)


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-key-owner-membership")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def owner(db_session, org):
    user = make_user(db_session, email="key-owner@example.com", password_hash="x")
    membership = Membership(user_id=user.id, org_id=org.id, role=MemberRole.VIEWER)
    db_session.add(membership)
    db_session.flush()
    user.membership = membership
    return user


@pytest.fixture
def raw_key(db_session, org, owner):
    _key, raw = ApiKeyService().create_api_key(
        db_session, org.id, owner.id, "K", actor_user_id=make_org_admin(db_session, org.id)
    )
    return raw


def _remove(db_session, membership):
    membership.deleted_at = datetime.now(UTC)
    db_session.flush()


class TestAuthentication:
    def test_a_member_owned_key_authenticates(self, db_session, raw_key, owner):
        """The control for every refusal below."""
        key = ApiKeyService().authenticate_api_key(db_session, raw_key)

        assert key is not None
        assert key.user_id == owner.id

    def test_a_removed_owners_key_is_refused_naming_the_role(self, db_session, raw_key, owner):
        _remove(db_session, owner.membership)

        with pytest.raises(InsufficientRoleError) as exc:
            ApiKeyService().authenticate_api_key(db_session, raw_key)

        assert exc.value.required_role == "viewer"

    def test_the_removal_applies_on_the_next_use(self, db_session, raw_key, owner):
        """Read now, not cached from minting: the same key, used once before and once after."""
        svc = ApiKeyService()
        assert svc.authenticate_api_key(db_session, raw_key) is not None

        _remove(db_session, owner.membership)

        with pytest.raises(InsufficientRoleError):
            svc.authenticate_api_key(db_session, raw_key)

    def test_membership_in_another_org_does_not_count(self, db_session, org, raw_key, owner):
        other = Organization(name="Other", slug="other-key-owner-membership")
        db_session.add(other)
        db_session.flush()
        db_session.add(Membership(user_id=owner.id, org_id=other.id, role=MemberRole.ADMIN))
        _remove(db_session, owner.membership)

        with pytest.raises(InsufficientRoleError):
            ApiKeyService().authenticate_api_key(db_session, raw_key)

    def test_an_unknown_key_is_still_none_not_a_role_refusal(self, db_session):
        """The 401 path is unchanged: no key, no owner, nothing to name a role about."""
        assert ApiKeyService().authenticate_api_key(db_session, "etf_nope") is None

    def test_a_refused_key_does_not_record_a_use(self, db_session, raw_key, owner):
        from datanika.models.api_key import ApiKey

        _remove(db_session, owner.membership)
        with pytest.raises(InsufficientRoleError):
            ApiKeyService().authenticate_api_key(db_session, raw_key)

        key = db_session.query(ApiKey).one()
        assert key.last_used_at is None


# ------------------------------------------------------------------------------------------------
# Over HTTP, with REAL authentication, on every route the API serves
# ------------------------------------------------------------------------------------------------
#
# ⚠️ Its own engine, not the shared `db_session`: `api_middleware` runs synchronous handlers in a
# worker thread, and the shared SQLite engine refuses a connection from another thread. The AC6
# harness (`test_ac6_endpoint_refusals.py`) builds its surface the same way.


@pytest.fixture
def http_surface():
    engine = create_engine(
        "sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    Base.metadata.create_all(engine)
    session = SASession(engine)
    org = Organization(name="Acme", slug="acme-key-owner-http")
    session.add(org)
    session.flush()
    owner = make_user(session, email="http-owner@example.com", password_hash="x")
    membership = Membership(user_id=owner.id, org_id=org.id, role=MemberRole.VIEWER)
    session.add(membership)
    session.flush()
    _key, raw = ApiKeyService().create_api_key(
        session, org.id, owner.id, "K", actor_user_id=make_org_admin(session, org.id)
    )
    session.commit()

    @contextlib.contextmanager
    def fake_session():
        yield session

    rl = MagicMock()
    rl.get_limit_for_org.return_value = 60
    rl.check_rate_limit.return_value = _RATE_OK
    rl.preauth_check.return_value = MagicMock(allowed=True, retry_after=0)
    with (
        patch("datanika.services.api_middleware._rate_limit_svc", rl),
        patch("datanika.services.api_middleware._get_session", fake_session),
    ):
        client = TestClient(Starlette(routes=list(api_v1_routes) + list(meta_routes)))
        yield client, session, membership, raw, rl
    session.close()
    engine.dispose()


def _every_route() -> list[tuple[str, str]]:
    rows = []
    for route in list(api_v1_routes) + list(meta_routes):
        path = re.sub(r"\{id:int\}", "1", route.path).replace("{type:str}", "postgres")
        for method in sorted(set(route.methods) - {"HEAD"}):
            rows.append((method, path))
    return rows


def test_the_route_census_is_not_empty():
    """Anti-vacuity: the parametrisation below reads the real route tables."""
    assert len(_every_route()) >= 50


@pytest.mark.parametrize(("method", "path"), _every_route())
def test_every_route_refuses_a_removed_owners_key(http_surface, method, path):
    client, session, membership, raw_key, _rl = http_surface
    _remove(session, membership)
    session.commit()

    resp = client.request(method, path, headers={"Authorization": f"Bearer {raw_key}"})

    assert resp.status_code == 403, (method, path, resp.status_code, resp.text[:200])
    err = resp.json()["error"]
    assert err["code"] == "insufficient_role"
    assert err["required_role"] == "viewer"
    for word in ("expired", "revoked", "invalid"):
        assert word not in resp.text.lower(), resp.text


@pytest.mark.parametrize(
    "path", ["/api/v1/transformations", "/api/v1/runs", "/api/v1/meta/materializations"]
)
def test_the_same_key_is_served_while_the_owner_is_a_member(http_surface, path):
    """The control: reads a viewer may make, answered 200 for the identical request."""
    client, _session, _membership, raw_key, _rl = http_surface

    resp = client.get(path, headers={"Authorization": f"Bearer {raw_key}"})

    assert resp.status_code == 200, resp.text[:200]


def test_a_refused_key_counts_toward_its_own_credential_bucket_only(http_surface):
    """#774: a request refused after a database lookup must still be counted, or a former
    member's key costs a session per request with nothing to exceed. It is counted against
    the CREDENTIAL only -- the key is real, and charging the caller's address would let one
    removed key lock out everyone behind the same address."""
    client, session, membership, raw_key, rl = http_surface
    _remove(session, membership)
    session.commit()

    client.get("/api/v1/transformations", headers={"Authorization": f"Bearer {raw_key}"})

    assert rl.record_auth_failure.call_count == 1
    assert rl.record_auth_failure.call_args.kwargs["client"] == ""
