"""TDD tests for authentication service."""

from datetime import UTC, datetime

import pytest

from datanika.services.auth import AuthService


@pytest.fixture
def auth():
    return AuthService(secret_key="test-secret-key-for-jwt-signing-only")


class TestPasswordHashing:
    def test_hash_password_returns_string(self, auth):
        hashed = auth.hash_password("my_password")
        assert isinstance(hashed, str)
        assert hashed != "my_password"

    def test_hash_password_different_each_time(self, auth):
        """bcrypt salts should produce different hashes."""
        h1 = auth.hash_password("same_password")
        h2 = auth.hash_password("same_password")
        assert h1 != h2

    def test_verify_password_correct(self, auth):
        hashed = auth.hash_password("correct_password")
        assert auth.verify_password("correct_password", hashed) is True

    def test_verify_password_wrong(self, auth):
        hashed = auth.hash_password("correct_password")
        assert auth.verify_password("wrong_password", hashed) is False

    def test_verify_password_empty(self, auth):
        hashed = auth.hash_password("something")
        assert auth.verify_password("", hashed) is False


class TestAccessToken:
    def test_create_access_token(self, auth):
        token = auth.create_access_token(user_id=1, org_id=10)
        assert isinstance(token, str)
        assert len(token) > 0

    def test_decode_access_token(self, auth):
        token = auth.create_access_token(user_id=42, org_id=7)
        payload = auth.decode_token(token)
        assert payload["user_id"] == 42
        assert payload["org_id"] == 7
        assert payload["type"] == "access"

    def test_access_token_expires(self, auth):
        # Create a token that's already expired by encoding with a past exp
        from jose import jwt as _jwt

        payload = {
            "user_id": 1,
            "org_id": 1,
            "type": "access",
            "exp": datetime(2020, 1, 1, tzinfo=UTC),
            "iat": datetime(2020, 1, 1, tzinfo=UTC),
        }
        token = _jwt.encode(payload, "test-secret-key-for-jwt-signing-only", algorithm="HS256")
        assert auth.decode_token(token) is None

    def test_decode_invalid_token(self, auth):
        payload = auth.decode_token("not.a.valid.token")
        assert payload is None

    def test_decode_token_wrong_secret(self, auth):
        token = auth.create_access_token(user_id=1, org_id=1)
        other = AuthService(secret_key="different-secret")
        payload = other.decode_token(token)
        assert payload is None


class TestRefreshToken:
    def test_create_refresh_token(self, auth):
        token = auth.create_refresh_token(user_id=1)
        assert isinstance(token, str)

    def test_decode_refresh_token(self, auth):
        token = auth.create_refresh_token(user_id=5)
        payload = auth.decode_token(token)
        assert payload["user_id"] == 5
        assert payload["type"] == "refresh"
        assert "org_id" not in payload

    def test_refresh_token_expires(self, auth):
        from jose import jwt as _jwt

        payload = {
            "user_id": 1,
            "type": "refresh",
            "exp": datetime(2020, 1, 1, tzinfo=UTC),
            "iat": datetime(2020, 1, 1, tzinfo=UTC),
        }
        token = _jwt.encode(payload, "test-secret-key-for-jwt-signing-only", algorithm="HS256")
        assert auth.decode_token(token) is None


class TestRolePermissions:
    def test_owner_has_all_permissions(self, auth):
        assert auth.has_permission("owner", "create") is True
        assert auth.has_permission("owner", "read") is True
        assert auth.has_permission("owner", "update") is True
        assert auth.has_permission("owner", "delete") is True
        assert auth.has_permission("owner", "manage_members") is True

    def test_admin_permissions(self, auth):
        assert auth.has_permission("admin", "create") is True
        assert auth.has_permission("admin", "read") is True
        assert auth.has_permission("admin", "update") is True
        assert auth.has_permission("admin", "delete") is True
        assert auth.has_permission("admin", "manage_members") is False

    def test_editor_permissions(self, auth):
        assert auth.has_permission("editor", "create") is True
        assert auth.has_permission("editor", "read") is True
        assert auth.has_permission("editor", "update") is True
        assert auth.has_permission("editor", "delete") is False
        assert auth.has_permission("editor", "manage_members") is False

    def test_viewer_permissions(self, auth):
        assert auth.has_permission("viewer", "read") is True
        assert auth.has_permission("viewer", "create") is False
        assert auth.has_permission("viewer", "update") is False
        assert auth.has_permission("viewer", "delete") is False
        assert auth.has_permission("viewer", "manage_members") is False

    def test_unknown_role_has_no_permissions(self, auth):
        assert auth.has_permission("unknown", "read") is False
