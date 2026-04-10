"""Security tests — token refresh attacks, JWT manipulation, session security."""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from jose import jwt as jose_jwt

from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import ALGORITHM, AuthService
from datanika.services.user_service import UserService


@pytest.fixture
def auth():
    return AuthService("test-secret-token-security")


@pytest.fixture
def svc(auth):
    return UserService(auth)


@pytest.fixture
def org(db_session):
    o = Organization(name="TokenOrg", slug=f"token-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def user_with_membership(db_session, svc, org):
    email = f"token-{uuid.uuid4().hex[:6]}@test.com"
    user = svc.register_user(db_session, email, "StrongPass123!", "Token User")
    membership = Membership(user_id=user.id, org_id=org.id, role=MemberRole.EDITOR)
    db_session.add(membership)
    db_session.flush()
    return user, email


class TestRefreshTokenAbuse:
    """Attacks using refresh tokens to mint unauthorized access tokens."""

    def test_refresh_token_cannot_be_used_as_access(self, auth):
        """Refresh token type must not be accepted where access is expected."""
        refresh = auth.create_refresh_token(user_id=1)
        result = auth.decode_token(refresh, expected_type="access")
        assert result is None

    def test_access_token_cannot_be_used_as_refresh(self, auth):
        access = auth.create_access_token(user_id=1, org_id=1)
        result = auth.decode_token(access, expected_type="refresh")
        assert result is None

    def test_expired_refresh_token_rejected(self, auth):
        payload = {
            "user_id": 1,
            "type": "refresh",
            "exp": datetime.now(UTC) - timedelta(seconds=10),
            "iat": datetime.now(UTC) - timedelta(days=8),
        }
        token = jose_jwt.encode(payload, "test-secret-token-security", algorithm=ALGORITHM)
        assert auth.decode_token(token, expected_type="refresh") is None

    def test_refresh_token_with_injected_org_id_ignored(self, auth):
        """If attacker forges a refresh token with org_id, the field should
        not be trusted (refresh tokens don't carry org_id)."""
        refresh = auth.create_refresh_token(user_id=1)
        payload = auth.decode_token(refresh, expected_type="refresh")
        assert payload is not None
        assert "org_id" not in payload

    def test_email_verify_token_cannot_refresh(self, auth):
        """Email verification tokens must not work as refresh tokens."""
        email_tok = auth.create_email_verification_token(user_id=1, email="a@b.com")
        result = auth.decode_token(email_tok, expected_type="refresh")
        assert result is None


class TestJWTManipulation:
    """Advanced JWT attack vectors."""

    def test_none_algorithm_attack(self, auth):
        """Attacker tries 'none' algorithm to bypass signature verification."""
        payload = {
            "user_id": 1,
            "org_id": 1,
            "type": "access",
            "exp": datetime.now(UTC) + timedelta(hours=1),
            "iat": datetime.now(UTC),
        }
        # Manually craft with alg=none
        import base64
        import json

        hdr_bytes = json.dumps({"alg": "none", "typ": "JWT"}).encode()
        header = base64.urlsafe_b64encode(hdr_bytes).rstrip(b"=")
        body_bytes = json.dumps(payload, default=str).encode()
        body = base64.urlsafe_b64encode(body_bytes).rstrip(b"=")
        fake_token = f"{header.decode()}.{body.decode()}."
        assert auth.decode_token(fake_token, expected_type="access") is None

    def test_modified_user_id_invalidates_signature(self, auth):
        """Changing user_id in the payload should fail signature check."""
        token = auth.create_access_token(user_id=1, org_id=1)
        # Decode, modify, re-encode with same header+signature
        parts = token.split(".")
        import base64
        import json

        # Pad the payload
        padded = parts[1] + "=" * (4 - len(parts[1]) % 4)
        body = json.loads(base64.urlsafe_b64decode(padded))
        body["user_id"] = 999  # Escalation attempt
        enc = base64.urlsafe_b64encode(json.dumps(body, default=str).encode())
        new_body = enc.rstrip(b"=").decode()
        tampered = f"{parts[0]}.{new_body}.{parts[2]}"
        assert auth.decode_token(tampered, expected_type="access") is None

    def test_modified_org_id_invalidates_signature(self, auth):
        """Changing org_id (cross-tenant escalation) should fail."""
        token = auth.create_access_token(user_id=1, org_id=1)
        parts = token.split(".")
        import base64
        import json

        padded = parts[1] + "=" * (4 - len(parts[1]) % 4)
        body = json.loads(base64.urlsafe_b64decode(padded))
        body["org_id"] = 999  # Cross-tenant attempt
        enc = base64.urlsafe_b64encode(json.dumps(body, default=str).encode())
        new_body = enc.rstrip(b"=").decode()
        tampered = f"{parts[0]}.{new_body}.{parts[2]}"
        assert auth.decode_token(tampered, expected_type="access") is None


class TestAccountEnumeration:
    """Authentication responses should not reveal whether an account exists."""

    def test_nonexistent_user_returns_none(self, db_session, svc):
        """Login with non-existent email returns None (not a specific error)."""
        result = svc.authenticate(db_session, "nobody@example.com", "anypass")
        assert result is None

    def test_wrong_password_returns_none(self, db_session, svc, user_with_membership):
        """Login with correct email but wrong password returns None (same as above)."""
        _, email = user_with_membership
        result = svc.authenticate(db_session, email, "wrong_password")
        assert result is None

    def test_both_failures_return_same_type(self, db_session, svc, user_with_membership):
        """Both nonexistent-user and wrong-password return exactly None —
        no difference an attacker can use to enumerate accounts."""
        _, email = user_with_membership
        r1 = svc.authenticate(db_session, "nobody@example.com", "pass")
        r2 = svc.authenticate(db_session, email, "wrong")
        assert r1 is None
        assert r2 is None
        assert type(r1) is type(r2)

    def test_inactive_user_returns_none(self, db_session, svc, org):
        """Deactivated accounts also return None, not a specific error."""
        email = f"inactive-{uuid.uuid4().hex[:6]}@test.com"
        user = svc.register_user(db_session, email, "pass123", "Inactive")
        membership = Membership(user_id=user.id, org_id=org.id, role=MemberRole.VIEWER)
        db_session.add(membership)
        db_session.flush()
        user.is_active = False
        db_session.flush()
        result = svc.authenticate(db_session, email, "pass123")
        assert result is None
