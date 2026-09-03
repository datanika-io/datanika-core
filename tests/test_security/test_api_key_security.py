"""Security tests — API key leakage, brute force resistance, cross-org isolation."""

import uuid

import pytest

from datanika.models.user import Organization
from datanika.services.api_key_service import ApiKeyService
from tests.factories import make_user


@pytest.fixture
def svc():
    return ApiKeyService()


@pytest.fixture
def org(db_session):
    o = Organization(name="KeySecOrg", slug=f"keysec-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def user(db_session):
    u = make_user(
        db_session,
        email=f"keysec-{uuid.uuid4().hex[:6]}@test.com",
        full_name="K",
        password_hash="h",
    )
    return u


class TestKeyLeakagePrevention:
    """Raw API key values must never appear in stored data or list responses."""

    def test_raw_key_not_stored_in_db(self, svc, db_session, org, user):
        """The raw key must not be stored — only the SHA-256 hash."""
        key, raw_key = svc.create_api_key(db_session, org.id, user.id, "K")
        # The hash should NOT be the raw key
        assert key.key_hash != raw_key
        # The raw key should not appear anywhere in the key object's string fields
        assert raw_key not in str(key.key_hash)
        assert raw_key not in str(key.name)

    def test_list_keys_does_not_contain_raw_key(self, svc, db_session, org, user):
        """list_api_keys should never return raw key values."""
        _, raw_key = svc.create_api_key(db_session, org.id, user.id, "K")
        keys = svc.list_api_keys(db_session, org.id)
        for key in keys:
            assert raw_key not in (key.key_hash or "")
            assert raw_key not in (key.name or "")
            # ApiKey model has no raw_key field
            assert not hasattr(key, "raw_key")

    def test_key_hash_is_sha256_hex(self, svc, db_session, org, user):
        """Key hash should be exactly 64 hex chars (SHA-256)."""
        key, _ = svc.create_api_key(db_session, org.id, user.id, "K")
        assert len(key.key_hash) == 64
        assert all(c in "0123456789abcdef" for c in key.key_hash)


class TestBruteForceResistance:
    """API key format should make brute force impractical."""

    def test_key_prefix_present(self, svc, db_session, org, user):
        """Keys start with 'etf_' prefix for easy identification in logs."""
        _, raw_key = svc.create_api_key(db_session, org.id, user.id, "K")
        assert raw_key.startswith("etf_")

    def test_key_entropy_sufficient(self, svc, db_session, org, user):
        """Raw key must be long enough to resist brute force (32+ bytes of randomness)."""
        _, raw_key = svc.create_api_key(db_session, org.id, user.id, "K")
        # etf_ prefix + 32 bytes URL-safe base64 = etf_ + ~43 chars
        assert len(raw_key) >= 40

    def test_keys_are_unique(self, svc, db_session, org, user):
        """Two keys created in sequence must be different."""
        _, raw1 = svc.create_api_key(db_session, org.id, user.id, "K1")
        _, raw2 = svc.create_api_key(db_session, org.id, user.id, "K2")
        assert raw1 != raw2

    def test_invalid_key_format_rejected(self, svc, db_session, org, user):
        """Completely random strings that don't match any hash should return None."""
        # Short key
        assert svc.authenticate_api_key(db_session, "etf_short") is None
        # No prefix
        assert svc.authenticate_api_key(db_session, "no_prefix_key") is None
        # Empty
        assert svc.authenticate_api_key(db_session, "") is None

    def test_partial_key_rejected(self, svc, db_session, org, user):
        """A prefix of a valid key should not authenticate."""
        _, raw_key = svc.create_api_key(db_session, org.id, user.id, "K")
        partial = raw_key[:20]
        assert svc.authenticate_api_key(db_session, partial) is None


class TestCrossOrgKeyIsolation:
    """API keys must be scoped to their org and not accessible cross-org."""

    def test_revoke_key_from_wrong_org_fails(self, svc, db_session, org, user):
        """Cannot revoke another org's key."""
        other_org = Organization(name="Other", slug=f"other-{uuid.uuid4().hex[:8]}")
        db_session.add(other_org)
        db_session.flush()

        key, _ = svc.create_api_key(db_session, org.id, user.id, "K")
        result = svc.revoke_api_key(db_session, other_org.id, key.id)
        assert result is False

    def test_list_keys_does_not_leak_other_orgs(self, svc, db_session, org, user):
        """list_api_keys for org A must not return org B's keys."""
        other_org = Organization(name="Other", slug=f"other2-{uuid.uuid4().hex[:8]}")
        db_session.add(other_org)
        db_session.flush()

        svc.create_api_key(db_session, org.id, user.id, "OrgA Key")
        svc.create_api_key(db_session, other_org.id, user.id, "OrgB Key")

        a_keys = svc.list_api_keys(db_session, org.id)
        b_keys = svc.list_api_keys(db_session, other_org.id)

        a_names = {k.name for k in a_keys}
        b_names = {k.name for k in b_keys}

        assert "OrgA Key" in a_names
        assert "OrgB Key" not in a_names
        assert "OrgB Key" in b_names
        assert "OrgA Key" not in b_names

    def test_authenticated_key_returns_correct_org_id(self, svc, db_session, org, user):
        """After authentication, the key's org_id must match the owning org."""
        key, raw_key = svc.create_api_key(db_session, org.id, user.id, "K")
        authenticated = svc.authenticate_api_key(db_session, raw_key)
        assert authenticated is not None
        assert authenticated.org_id == org.id
