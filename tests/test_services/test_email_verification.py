"""TDD tests for email verification flow."""

import uuid

import pytest

from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService


@pytest.fixture
def auth():
    return AuthService("test-secret-key-for-verification")


@pytest.fixture
def svc(auth):
    return UserService(auth)


@pytest.fixture
def org(db_session):
    o = Organization(name="VerifyOrg", slug=f"verify-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


class TestEmailVerificationToken:
    def test_create_and_decode_verification_token(self, auth):
        token = auth.create_email_verification_token(user_id=42, email="user@test.com")
        payload = auth.decode_token(token, expected_type="email_verify")

        assert payload is not None
        assert payload["user_id"] == 42
        assert payload["email"] == "user@test.com"
        assert payload["type"] == "email_verify"

    def test_wrong_type_rejected(self, auth):
        token = auth.create_email_verification_token(user_id=42, email="user@test.com")
        payload = auth.decode_token(token, expected_type="access")
        assert payload is None

    def test_expired_token_rejected(self, auth):
        from datetime import UTC, datetime, timedelta

        from jose import jwt

        # Manually create a token that's already expired
        payload = {
            "user_id": 42,
            "email": "user@test.com",
            "type": "email_verify",
            "exp": datetime.now(UTC) - timedelta(seconds=10),
            "iat": datetime.now(UTC) - timedelta(hours=1),
        }
        token = jwt.encode(payload, "test-secret-key-for-verification", algorithm="HS256")
        result = auth.decode_token(token, expected_type="email_verify")
        assert result is None


class TestRegisterUserEmailVerified:
    def test_new_user_email_not_verified(self, db_session, svc):
        user = svc.register_user(db_session, "newuser@test.com", "password123", "New User")
        assert user.email_verified is False

    def test_oauth_user_email_verified(self, db_session, svc):
        user, is_new = svc.find_or_create_oauth_user(
            db_session,
            email="oauth@test.com",
            full_name="OAuth User",
            oauth_provider="google",
            oauth_provider_id="123",
        )
        assert is_new is True
        assert user.email_verified is True


class TestAuthenticateWithVerification:
    def test_login_fails_when_email_not_verified(self, db_session, svc, org):
        user = svc.register_user(db_session, "unverified@test.com", "password123", "Unverified")
        # user.email_verified is False by default
        m = Membership(user_id=user.id, org_id=org.id, role=MemberRole.OWNER)
        db_session.add(m)
        db_session.flush()

        result = svc.authenticate(
            db_session,
            "unverified@test.com",
            "password123",
            require_email_verified=True,
        )
        assert result is None

    def test_login_succeeds_after_verification(self, db_session, svc, org):
        user = svc.register_user(db_session, "verified@test.com", "password123", "Verified")
        user.email_verified = True
        db_session.flush()
        m = Membership(user_id=user.id, org_id=org.id, role=MemberRole.OWNER)
        db_session.add(m)
        db_session.flush()

        result = svc.authenticate(
            db_session,
            "verified@test.com",
            "password123",
            require_email_verified=True,
        )
        assert result is not None
        assert result["user"].id == user.id

    def test_login_works_without_verification_requirement(self, db_session, svc, org):
        user = svc.register_user(db_session, "norequire@test.com", "password123", "NoRequire")
        # email_verified=False but require_email_verified=False (default)
        m = Membership(user_id=user.id, org_id=org.id, role=MemberRole.OWNER)
        db_session.add(m)
        db_session.flush()

        result = svc.authenticate(db_session, "norequire@test.com", "password123")
        assert result is not None
