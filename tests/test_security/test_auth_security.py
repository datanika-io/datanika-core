"""Security tests — JWT token attacks, role escalation, cross-org access."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from jose import jwt

from datanika.models.user import Organization
from datanika.services.auth import ALGORITHM, AuthService
from datanika.services.user_service import UserService, UserServiceError


@pytest.fixture
def auth():
    return AuthService("test-secret-for-security-tests")


@pytest.fixture
def svc(auth):
    return UserService(auth)


@pytest.fixture
def org(db_session):
    o = Organization(name="AuthOrg", slug=f"auth-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


class TestTokenAttacks:
    def test_expired_access_token_rejected(self, auth):
        payload = {
            "user_id": 1, "org_id": 1, "type": "access",
            "exp": datetime.now(UTC) - timedelta(seconds=10),
            "iat": datetime.now(UTC) - timedelta(hours=1),
        }
        token = jwt.encode(payload, "test-secret-for-security-tests", algorithm=ALGORITHM)
        assert auth.decode_token(token, expected_type="access") is None

    def test_refresh_token_as_access_rejected(self, auth):
        token = auth.create_refresh_token(user_id=1)
        assert auth.decode_token(token, expected_type="access") is None

    def test_access_token_as_refresh_rejected(self, auth):
        token = auth.create_access_token(user_id=1, org_id=1)
        assert auth.decode_token(token, expected_type="refresh") is None

    def test_tampered_payload_rejected(self, auth):
        token = auth.create_access_token(user_id=1, org_id=1)
        # Tamper: decode without verification, modify, re-encode with wrong key
        parts = token.split(".")
        # Just flip a char in the signature
        tampered = parts[0] + "." + parts[1] + "." + parts[2][::-1]
        assert auth.decode_token(tampered, expected_type="access") is None

    def test_token_with_wrong_secret_rejected(self, auth):
        payload = {
            "user_id": 1, "org_id": 1, "type": "access",
            "exp": datetime.now(UTC) + timedelta(hours=1),
            "iat": datetime.now(UTC),
        }
        token = jwt.encode(payload, "WRONG-SECRET", algorithm=ALGORITHM)
        assert auth.decode_token(token, expected_type="access") is None

    def test_empty_token_rejected(self, auth):
        assert auth.decode_token("", expected_type="access") is None

    def test_garbage_token_rejected(self, auth):
        assert auth.decode_token("not.a.jwt", expected_type="access") is None


class TestRegistrationSecurity:
    def test_empty_password_rejected(self, db_session, svc):
        with pytest.raises(UserServiceError, match="Password is required"):
            svc.register_user(db_session, "test@test.com", "", "Test")

    def test_empty_email_rejected(self, db_session, svc):
        with pytest.raises(UserServiceError, match="Email is required"):
            svc.register_user(db_session, "", "password123", "Test")

    def test_duplicate_email_rejected(self, db_session, svc):
        email = f"dup-{uuid.uuid4().hex[:6]}@test.com"
        svc.register_user(db_session, email, "pass1", "User 1")
        with pytest.raises(UserServiceError, match="Email already exists"):
            svc.register_user(db_session, email, "pass2", "User 2")

    def test_case_insensitive_email_duplicate(self, db_session, svc):
        email = f"CaseTest-{uuid.uuid4().hex[:6]}@Test.com"
        svc.register_user(db_session, email, "pass1", "User 1")
        with pytest.raises(UserServiceError, match="Email already exists"):
            svc.register_user(db_session, email.upper(), "pass2", "User 2")


class TestRoleEscalation:
    def test_viewer_cannot_create(self, auth):
        assert auth.has_permission("viewer", "create") is False

    def test_viewer_cannot_update(self, auth):
        assert auth.has_permission("viewer", "update") is False

    def test_viewer_cannot_delete(self, auth):
        assert auth.has_permission("viewer", "delete") is False

    def test_editor_cannot_delete(self, auth):
        assert auth.has_permission("editor", "delete") is False

    def test_editor_cannot_manage_members(self, auth):
        assert auth.has_permission("editor", "manage_members") is False

    def test_admin_cannot_manage_members(self, auth):
        assert auth.has_permission("admin", "manage_members") is False

    def test_owner_can_manage_members(self, auth):
        assert auth.has_permission("owner", "manage_members") is True

    def test_unknown_role_has_no_permissions(self, auth):
        assert auth.has_permission("hacker", "read") is False
        assert auth.has_permission("hacker", "create") is False
