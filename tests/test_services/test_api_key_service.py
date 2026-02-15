"""TDD tests for ApiKeyService — API key CRUD and authentication."""

from datetime import UTC, datetime, timedelta

import pytest

from etlfabric.models.api_key import ApiKey
from etlfabric.models.user import Organization, User
from etlfabric.services.api_key_service import ApiKeyService


@pytest.fixture
def svc():
    return ApiKeyService()


@pytest.fixture
def org(db_session):
    org = Organization(name="Acme", slug="acme-apikey-svc")
    db_session.add(org)
    db_session.flush()
    return org


@pytest.fixture
def user(db_session):
    user = User(
        email="user@example.com",
        password_hash="hashed",
        full_name="Test User",
    )
    db_session.add(user)
    db_session.flush()
    return user


class TestCreateApiKey:
    def test_basic(self, svc, db_session, org, user):
        key, raw_key = svc.create_api_key(
            db_session, org.id, user.id, "My Key", scopes=["pipeline:read"]
        )
        assert isinstance(key, ApiKey)
        assert isinstance(key.id, int)
        assert key.name == "My Key"
        assert key.org_id == org.id
        assert key.user_id == user.id
        assert key.key_hash != raw_key  # hash, not plain
        assert raw_key.startswith("etf_")
        assert key.scopes == ["pipeline:read"]

    def test_no_scopes(self, svc, db_session, org, user):
        key, _ = svc.create_api_key(db_session, org.id, user.id, "Full Access")
        assert key.scopes is None

    def test_with_expiry(self, svc, db_session, org, user):
        expires = datetime.now(UTC) + timedelta(days=30)
        key, _ = svc.create_api_key(db_session, org.id, user.id, "Temp", expires_at=expires)
        assert key.expires_at is not None

    def test_key_hash_is_stored(self, svc, db_session, org, user):
        key, raw_key = svc.create_api_key(db_session, org.id, user.id, "K")
        assert key.key_hash
        assert len(key.key_hash) > 0
        assert key.key_hash != raw_key


class TestAuthenticateApiKey:
    def test_valid_key(self, svc, db_session, org, user):
        _, raw_key = svc.create_api_key(db_session, org.id, user.id, "K")
        result = svc.authenticate_api_key(db_session, raw_key)
        assert result is not None
        assert result.name == "K"

    def test_invalid_key(self, svc, db_session):
        result = svc.authenticate_api_key(db_session, "etf_bad_key")
        assert result is None

    def test_expired_key(self, svc, db_session, org, user):
        past = datetime.now(UTC) - timedelta(days=1)
        _, raw_key = svc.create_api_key(db_session, org.id, user.id, "Expired", expires_at=past)
        result = svc.authenticate_api_key(db_session, raw_key)
        assert result is None

    def test_revoked_key(self, svc, db_session, org, user):
        key, raw_key = svc.create_api_key(db_session, org.id, user.id, "K")
        svc.revoke_api_key(db_session, org.id, key.id)
        result = svc.authenticate_api_key(db_session, raw_key)
        assert result is None

    def test_updates_last_used_at(self, svc, db_session, org, user):
        key, raw_key = svc.create_api_key(db_session, org.id, user.id, "K")
        assert key.last_used_at is None
        svc.authenticate_api_key(db_session, raw_key)
        db_session.refresh(key)
        assert key.last_used_at is not None

    def test_scope_check(self, svc, db_session, org, user):
        _, raw_key = svc.create_api_key(db_session, org.id, user.id, "K", scopes=["pipeline:read"])
        result = svc.authenticate_api_key(db_session, raw_key, required_scope="pipeline:read")
        assert result is not None

    def test_scope_mismatch(self, svc, db_session, org, user):
        _, raw_key = svc.create_api_key(db_session, org.id, user.id, "K", scopes=["pipeline:read"])
        result = svc.authenticate_api_key(db_session, raw_key, required_scope="pipeline:run")
        assert result is None

    def test_no_scopes_allows_all(self, svc, db_session, org, user):
        """A key with no scopes (None) allows any scope."""
        _, raw_key = svc.create_api_key(db_session, org.id, user.id, "Full")
        result = svc.authenticate_api_key(db_session, raw_key, required_scope="pipeline:run")
        assert result is not None


class TestListApiKeys:
    def test_empty(self, svc, db_session, org):
        assert svc.list_api_keys(db_session, org.id) == []

    def test_multiple(self, svc, db_session, org, user):
        svc.create_api_key(db_session, org.id, user.id, "A")
        svc.create_api_key(db_session, org.id, user.id, "B")
        result = svc.list_api_keys(db_session, org.id)
        assert len(result) == 2

    def test_excludes_revoked(self, svc, db_session, org, user):
        key, _ = svc.create_api_key(db_session, org.id, user.id, "A")
        svc.create_api_key(db_session, org.id, user.id, "B")
        svc.revoke_api_key(db_session, org.id, key.id)
        result = svc.list_api_keys(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "B"

    def test_filters_by_org(self, svc, db_session, org, user):
        other_org = Organization(name="Other", slug="other-apikey")
        db_session.add(other_org)
        db_session.flush()
        svc.create_api_key(db_session, org.id, user.id, "A")
        svc.create_api_key(db_session, other_org.id, user.id, "B")
        result = svc.list_api_keys(db_session, org.id)
        assert len(result) == 1
        assert result[0].name == "A"


class TestRevokeApiKey:
    def test_sets_deleted_at(self, svc, db_session, org, user):
        key, _ = svc.create_api_key(db_session, org.id, user.id, "K")
        result = svc.revoke_api_key(db_session, org.id, key.id)
        assert result is True
        db_session.refresh(key)
        assert key.deleted_at is not None

    def test_nonexistent(self, svc, db_session, org):
        assert svc.revoke_api_key(db_session, org.id, 99999) is False

    def test_wrong_org(self, svc, db_session, org, user):
        other_org = Organization(name="Other", slug="other-revoke")
        db_session.add(other_org)
        db_session.flush()
        key, _ = svc.create_api_key(db_session, org.id, user.id, "K")
        assert svc.revoke_api_key(db_session, other_org.id, key.id) is False
